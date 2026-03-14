import asyncio
import logging

from paravon.core.cluster.probe import ProbeManager
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.request import (
    RequestContext,
    GetRequest,
    DeleteRequest,
    PutRequest,
    Request,
    ReplicaSet
)
from paravon.core.models.version import ValueVersion
from paravon.core.ports.storage import Storage
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.topology import TopologyManager
from paravon.core.space.partition import PartitionPlacement
from paravon.core.space.vnode import VNode


class Coordinator:
    def __init__(
        self,
        meta_manager: NodeMetaManager,
        probe_manager: ProbeManager,
        topology_manager: TopologyManager,
        peer_clients: ClientConnectionPool,
        peer_config: PeerConfig,
        spawner: TaskSpawner,
        storage: Storage,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._meta_manager = meta_manager
        self._probe_manager = probe_manager
        self._peer_clients = peer_clients
        self._peer_config = peer_config
        self._spawner = spawner
        self._storage = storage
        self._topology = topology_manager

        if loop is None:
            loop = asyncio.get_running_loop()

        self._loop = loop

        self._subscribe_messages()

        self._ctx: dict[str, RequestContext] = {}

        self._logger = logging.getLogger("core.service.coordinator")

    async def handle(self, message: Message) -> None:
        self._logger.debug(
            f"[{message.type}] received from replica"
        )
        match message.type:
            case "ko":
                self._handle_ko(message.data)
            case _:
                self._handle_ok(message.data)

    async def get(
        self,
        request: GetRequest,
        placement: PartitionPlacement
    ) -> Message:
        ctx = await self._build_req_ctx(request, placement)
        keyspace = placement.keyspace
        key = request.key

        if ctx.replicas.has_local():
            self._logger.info(
                f"[{ctx.request_id}] Local node is replica "
                f"-> coordinating GET request"
            )
            return await self.coordinate_get(keyspace, key, ctx)
        else:
            self._logger.info(
                f"[{ctx.request_id}] Local node is NOT replica "
                f"-> forwarding to primary {ctx.replicas.primary}"
            )
            return await self.forward_get(keyspace, key, ctx)

    async def put(
        self,
        request: PutRequest,
        placement: PartitionPlacement
    ) -> Message:
        ctx = await self._build_req_ctx(request, placement)
        keyspace = placement.keyspace
        key = request.key
        value = request.value

        if ctx.replicas.has_local():
            return await self.coordinate_put(keyspace, key, value, ctx)
        return await self.forward_put(keyspace, key, value, ctx)

    async def delete(
        self,
        request: DeleteRequest,
        placement: PartitionPlacement
    ) -> Message:
        ctx = await self._build_req_ctx(request, placement)
        keyspace = placement.keyspace
        key = request.key

        if ctx.replicas.has_local():
            return await self.coordinate_delete(keyspace, key, ctx)
        return await self.forward_delete(keyspace, key, ctx)

    async def coordinate_get(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
    ) -> Message:
        self._logger.debug(
            f"[{ctx.request_id}] Coordinating GET on "
            f"{key.hex()} with replicas={ctx.replicas.candidates}"
        )
        tasks = [self._spawner.spawn(self._local_read(keyspace, key, ctx))]
        message = Message(
            type="read",
            data={
                "key": key,
                "keyspace": keyspace,
                "request_id": ctx.request_id,
                "quorum": ctx.quorum,
            },
        )
        for replica in ctx.replicas.remotes:
            task = self._spawner.spawn(self._send_message(replica, message, ctx))
            tasks.append(task)

        return await self._wait_remote(ctx, tasks)

    async def coordinate_put(
        self,
        keyspace: bytes,
        key: bytes,
        value: bytes,
        ctx: RequestContext
    ) -> Message:
        return await self._coordinate_write(keyspace, key, ctx, value)

    async def coordinate_delete(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext
    ) -> Message:
        return await self._coordinate_write(keyspace, key, ctx)

    async def forward_get(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext
    ) -> Message:
        self._logger.info(
            f"[{ctx.request_id}] Forwarding request "
            f"to primary {ctx.replicas.primary}"
        )
        message = Message(
            type="forward/fetch",
            data={
                "key": key,
                "keyspace": keyspace,
                "request_id": ctx.request_id,
                "quorum": ctx.quorum,
                "timeout": ctx.timeout
            },
        )
        task = self._spawner.spawn(
            self._send_message(ctx.replicas.primary, message, ctx)
        )
        return await self._wait_remote(ctx, [task])

    async def forward_put(
        self,
        keyspace: bytes,
        key: bytes,
        value: bytes,
        ctx: RequestContext
    ) -> Message:
        return await self._forward_write(keyspace, key, ctx, value)

    async def forward_delete(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
    ) -> Message:
        return await self._forward_write(keyspace, key, ctx)

    async def select_replicas(self, primary: VNode, count: int) -> ReplicaSet:
        remotes: list[str] = []
        ring = await self._topology.get_ring()
        membership = await self._meta_manager.get_membership()
        local = None
        self._logger.debug(
            f"Selecting {count} replicas for "
            f"vnode={primary.node_id}, local={membership.node_id}"
        )

        for replica in ring.iter_from(primary):
            if replica.node_id == membership.node_id:
                local = replica.node_id
            elif (
                replica.node_id not in remotes and
                self._probe_manager.is_alive(replica.node_id)
            ):
                remotes.append(replica.node_id)

            total = len(remotes) if local is None else len(remotes) + 1
            if total ==  count:
                break

        replicas = ReplicaSet(local=local, remotes=tuple(remotes))
        self._logger.debug(
            f"Replica selection result: local={local}, remotes={remotes}"
        )
        return replicas

    async def _build_req_ctx(
        self,
        request: Request,
        placement: PartitionPlacement
    ) -> RequestContext:
        rf = self._peer_config.replication_factor
        quorum = request.quorum
        if quorum > rf:
            raise ValueError(
                "Quorum read cannot reach because replication factor "
                f"is {rf} while quorum is {quorum}"
            )

        replicas = await self.select_replicas(placement.vnode, rf)
        if quorum > len(replicas.candidates):
            raise ValueError("Internal error: not enough replicas to reach quorum")

        request_ctx = RequestContext(
            request_id=request.request_id,
            replicas=replicas,
            quorum=request.quorum,
            future=self._loop.create_future(),
            timeout=request.timeout
        )
        self._ctx[request.request_id] = request_ctx
        return request_ctx

    async def _coordinate_write(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
        value: bytes | None = None,
    ) -> Message:
        self._logger.debug(
            f"[{ctx.request_id}] Coordinating WRITE on {key.hex()} "
            f"with replicas={ctx.replicas.candidates}"
        )
        tasks = []
        version = await self._try_primary_write(keyspace, key, ctx, value)
        if version is not None:
            for replica in ctx.replicas.remotes:
                task = self._spawner.spawn(
                    self._remote_apply(replica, keyspace, key, version, ctx)
                )
                tasks.append(task)

        return await self._wait_remote(ctx, tasks)

    async def _forward_write(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
        value: bytes | None = None,
    ) -> Message:
        data = {
            "keyspace": keyspace,
            "key": key,
            "value": value,
            "request_id": ctx.request_id,
            "quorum": ctx.quorum,
            "timeout": ctx.timeout,
        }
        message = Message(type="forward/write", data=data)
        replica = ctx.replicas.primary
        await self._send_message(replica, message, ctx)
        return await self._wait_remote(ctx, [])

    async def _remote_apply(
        self,
        replica: str,
        keyspace: bytes,
        key: bytes,
        version: ValueVersion,
        ctx: RequestContext,
    ) -> None:
        data = {
            "keyspace": keyspace,
            "key": key,
            "version": version.to_dict(),
            "request_id": ctx.request_id
        }
        message = Message(type="apply", data=data)
        self._logger.debug(
            f"[{ctx.request_id}] Sending {message.type} to {replica}"
        )
        await self._send_message(replica, message, ctx)

    def _handle_ko(self, data: dict) -> None:
        request_id = data["request_id"]
        ctx = self._ctx.get(request_id)
        if not ctx:
            return

        sender = data["source"]
        ctx.failures += 1

        if sender != ctx.replicas.local:
            self._probe_manager.mark_suspect(sender)

        if not ctx.replicas.has_local():
            message = Message(
                type="ko",
                data={"message": "Primary failed to process forwarded request"}
            )
            ctx.future.set_result(message)
            self._ctx.pop(request_id, None)
            return

        self._logger.warning(
            f"[{ctx.request_id}] Failure received from {sender}, "
            f"failures={ctx.failures}"
        )

        max_failures = len(ctx.replicas.candidates) - ctx.quorum
        if ctx.failures > max_failures:
            self._logger.critical(
                f"[{ctx.request_id}] Quorum impossible: failures={ctx.failures}, "
                f"required={ctx.quorum}, total={len(ctx.replicas.candidates)}"
            )
            message = Message(
                type="ko",
                data={"message": "Quorum not reach due to replicas failures"}
            )
            ctx.future.set_result(message)
            self._ctx.pop(request_id, None)

    def _handle_ok(self, data: dict) -> None:
        request_id = data["request_id"]
        ctx = self._ctx.get(request_id)
        if not ctx:
            return

        if not ctx.replicas.has_local():
            value = Message(type="ok", data=data)
            ctx.future.set_result(value)
            self._ctx.pop(request_id, None)
            return

        ctx.responses.append(data)
        source = data["source"]
        self._logger.debug(
            f"[{ctx.request_id}] Success received from replica {source}, "
            f"total_ok={len(ctx.responses)}"
        )

        if len(ctx.responses) >= ctx.quorum:
            self._logger.info(
                f"[{ctx.request_id}] Quorum reached "
                f"({ctx.quorum}/{len(ctx.replicas.candidates)})"
            )
            message = self._resolve_message(request_id, ctx.responses)
            ctx.future.set_result(message)
            self._ctx.pop(request_id, None)
            return

        max_failures = len(ctx.replicas.candidates) - ctx.quorum
        if ctx.failures > max_failures:
            message = Message(
                type="ko",
                data={"message": "Quorum not reach due to replicas failures"}
            )
            ctx.future.set_result(message)
            self._ctx.pop(request_id, None)

    async def _local_read(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
    ) -> None:
        self._logger.debug(
            f"[{ctx.request_id}] Local read key={key.hex()}"
        )

        try:
            version = await self._storage.get(keyspace, key)
            self._handle_ok({
                "version": version.to_dict() if version else None,
                "request_id": ctx.request_id,
                "source": ctx.replicas.local
            })
        except Exception as ex:
            self._logger.error(
                f"[{ctx.request_id}] Local storage error: {ex}"
            )
            self._handle_ko({
                "request_id": ctx.request_id,
                "source": ctx.replicas.local
            })

    @staticmethod
    def _resolve_message(
        request_id: str,
        responses: list[dict],
    ) -> Message:
        instances = [
            ValueVersion.from_dict(r["version"])
            for r in responses
            if r["version"] is not None
        ]
        versions = [v.to_dict() for v in set(instances)]
        data = {"versions": versions, "request_id": request_id}
        return Message(type="ok", data=data)

    async def _send_message(
        self,
        peer: str,
        message: Message,
        ctx: RequestContext
    ) -> None:
        self._logger.debug(f"[{ctx.request_id}] Sending {message.type} to {peer}")

        try:
            await self._peer_clients.send(peer, message)
        except Exception as ex:
            await self._probe_manager.mark_suspect(peer)
            self._logger.error(f"[{ctx.request_id}] Remote failed on {peer}: {ex}")
            self._handle_ko({
                "request_id": ctx.request_id,
                "source": peer,
            })

    def _subscribe_messages(self) -> None:
        self._peer_clients.subscribe("read", self)
        self._peer_clients.subscribe("apply", self)
        self._peer_clients.subscribe("forward/fetch", self)
        self._peer_clients.subscribe("forward/write", self)

    async def _try_primary_write(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
        value: bytes | None = None,
    ) -> ValueVersion | None:
        """
        Attempt to write on the primary replica to generate
        the canonical ValueVersion.

        Current behavior (no hinted handoff yet):
          - The primary MUST generate the canonical version for this request.
          - If the primary write succeeds, this version is used for replication.
          - If the primary write fails, the entire request fails immediately.
            Remote replicas are NOT allowed to generate their own versions.
            This guarantees that a single request never produces multiple versions.

        Future behavior (once hinted handoff is implemented):
          - If the primary is temporarily unavailable, another node will store
            the canonical version on its behalf (hinted handoff).
          - The request will still fail from the client's perspective, but
            replication will remain consistent and no divergent versions will appear.
        """

        self._logger.debug(f"[{ctx.request_id}] Primary write key={key.hex()}")

        try:
            if value is not None:
                version = await self._storage.put(keyspace, key, value)
            else:
                version = await self._storage.delete(keyspace, key)

            self._handle_ok({
                "request_id": ctx.request_id,
                "source": ctx.replicas.local,
                "version": version.to_dict(),
            })

            return version

        except Exception as ex:
            ctx.failures += 1
            self._logger.error(
                f"[{ctx.request_id}] Primary storage error: {ex}"
            )
            message = Message(
                type="ko",
                data={"message": "Can't obtain value version from primary replica"}
            )
            ctx.future.set_result(message)
            return None

    async def _wait_remote(
        self,
        ctx: RequestContext,
        tasks: list[asyncio.Task],
    ) -> Message:
        timeout = ctx.timeout
        try:
            if timeout:
                message = await asyncio.wait_for(ctx.future, timeout=timeout)
            else:
                message = await ctx.future
        except asyncio.TimeoutError:
            self._logger.error(
                f"[{ctx.request_id}] Timeout after "
                f"{timeout}s waiting for quorum"
            )
            message = Message(
                type="ko",
                data={"message": f"Quorum timeout exceeded: {timeout}"}
            )
        finally:
            self._ctx.pop(ctx.request_id, None)
            pending_tasks = [task for task in tasks if not task.done()]
            if pending_tasks:
                self._logger.debug(
                    f"[{ctx.request_id}] Cancelling {len(pending_tasks)} "
                    f"pending tasks"
                )
                for task in pending_tasks:
                    task.cancel()

        return message
