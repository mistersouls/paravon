import asyncio
import logging


from paravon.core.cluster.probe import ProbeManager
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.request import (
    GetRequest,
    RequestContext,
    PutRequest,
    Request,
    DeleteRequest,
    ReplicaSet
)
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
        for replica in ctx.replicas.remotes:
            task = self._spawner.spawn(
                self._remote_get(replica, keyspace, key, ctx)
            )
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
        """
        Forward the GET to the primary coordinator and wait for its result.

        From this node's perspective, quorum is 1: the primary's response,
        which itself is based on its own read quorum.
        """
        self._logger.info(
            f"[{ctx.request_id}] Forwarding request "
            f"to primary {ctx.replicas.primary}"
        )
        task = self._spawner.spawn(
            self._remote_get(ctx.replicas.primary, keyspace, key, ctx)
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
        tasks = [self._spawner.spawn(self._local_write(keyspace, key, ctx, value))]
        for replica in ctx.replicas.remotes:
            task = self._spawner.spawn(
                self._remote_write(replica, keyspace, key, ctx, value)
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
        self._logger.info(
            f"[{ctx.request_id}] Forwarding request "
            f"to primary {ctx.replicas.primary}"
        )
        task = self._spawner.spawn(
            self._remote_write(ctx.replicas.primary, keyspace, key, ctx, value)
        )
        return await self._wait_remote(ctx, [task])

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
            value = self._resolve_read(ctx.responses)
            ctx.future.set_result(value)
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
            value = await self._storage.get(keyspace, key)
            self._handle_ok({"value": value, "request_id": ctx.request_id})
        except Exception as ex:
            ctx.failures += 1
            self._logger.error(
                f"[{ctx.request_id}] Local storage error: {ex}"
            )

    async def _local_write(
        self,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
        value: bytes | None = None,
    ) -> None:
        self._logger.debug(
            f"[{ctx.request_id}] Local write key={key.hex()}"
        )

        try:
            if value is not None:
                await self._storage.put(keyspace, key, value)
            else:
                await self._storage.delete(keyspace, key)
            self._handle_ok({
                "request_id": ctx.request_id,
                "source": ctx.replicas.local
            })
        except Exception as ex:
            ctx.failures += 1
            self._logger.error(
                f"[{ctx.request_id}] Local storage error: {ex}"
            )

    def _subscribe_messages(self) -> None:
        self._peer_clients.subscribe("replica/get", self)
        self._peer_clients.subscribe("replica/put", self)
        self._peer_clients.subscribe("replica/delete", self)
        self._peer_clients.subscribe("forward/get", self)
        self._peer_clients.subscribe("forward/put", self)
        self._peer_clients.subscribe("forward/delete", self)

    async def _remote_get(
        self,
        replica: str,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
    ) -> None:
        kind = "replica" if ctx.replicas.has_local() else "forward"

        self._logger.debug(
            f"[{ctx.request_id}] Sending {kind}/get to {replica}"
        )

        try:
            message = Message(
                type=f"{kind}/get",
                data={
                    "key": key,
                    "keyspace": keyspace,
                    "request_id": ctx.request_id,
                    "quorum": ctx.quorum,
                },
            )
            await self._peer_clients.send(replica, message)
        except Exception as ex:
            ctx.failures += 1
            await self._probe_manager.mark_suspect(replica)
            self._logger.error(
                f"[{ctx.request_id}] Remote failed on {replica}: {ex}"
            )

    @staticmethod
    def _resolve_read(responses: list[dict]) -> Message:
        # For now: a naive approach, we just take the first response.
        # Later: compare versions, vector clocks, and so on.
        return Message(type="ok", data=responses[0])

    async def _remote_write(
        self,
        replica: str,
        keyspace: bytes,
        key: bytes,
        ctx: RequestContext,
        value: bytes | None = None,
    ) -> None:
        data = {
            "keyspace": keyspace,
            "key": key,
            "request_id": ctx.request_id,
        }
        kind = "replica" if ctx.replicas.has_local() else "forward"
        if value is not None:
            data["value"] = value
            action = "put"
        else:
            action = "delete"

        self._logger.debug(
            f"[{ctx.request_id}] Sending {kind}/{action} to {replica}"
        )

        try:
            message = Message(type=f"{kind}/{action}", data=data)
            await self._peer_clients.send(replica, message)
        except Exception as ex:
            ctx.failures += 1
            await self._probe_manager.mark_suspect(replica)
            self._logger.error(
                f"[{ctx.request_id}] Remote {action} failed on {replica}: {ex}"
            )

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
