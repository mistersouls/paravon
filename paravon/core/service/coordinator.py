import asyncio
import logging
from typing import Literal

from paravon.core.cluster.probe import ProbeManager
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.request import (
    GetRequest,
    RequestContext,
    PutRequest,
    WriteRequest,
    DeleteRequest
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
        membership = await self._meta_manager.get_membership()
        if membership.node_id != placement.vnode.node_id:
            return await self.forward_get(request, placement)
        return await self.coordinate_get(request, placement)

    async def put(
        self,
        request: PutRequest,
        placement: PartitionPlacement
    ) -> Message:
        membership = await self._meta_manager.get_membership()
        if membership.node_id != placement.vnode.node_id:
            return await self.forward_put(request, placement)
        return await self.coordinate_put(request, placement)

    async def delete(
        self,
        request: DeleteRequest,
        placement: PartitionPlacement
    ) -> Message:
        membership = await self._meta_manager.get_membership()
        if membership.node_id != placement.vnode.node_id:
            return await self.forward_delete(request, placement)
        return await self.coordinate_delete(request, placement)

    async def coordinate_get(
        self,
        request: GetRequest,
        placement: PartitionPlacement
    ) -> Message:
        rf = self._peer_config.replication_factor
        quorum_read = request.quorum_read

        if quorum_read > rf:
            return Message(
                type="ko",
                data={
                    "message": (
                        "Quorum read cannot reach because replication factor "
                        f"is {rf} while quorum is {quorum_read}"
                    )
                }
            )

        primary = placement.vnode
        replicas = await self.select_replicas(primary, rf - 1)
        if quorum_read - 1 > len(replicas):
            return Message(
                type="ko",
                data={
                    "message": "Internal error: not enough replicas to reach quorum"
                }
            )

        ctx = RequestContext(
            request_id=request.request_id,
            replicas=replicas + [primary.node_id],
            quorum=quorum_read,
            future=self._loop.create_future(),
        )
        self._ctx[request.request_id] = ctx

        tasks = [self._spawner.spawn(self._local_read(placement, request, ctx))]
        for replica in replicas:
            task = self._spawner.spawn(
                self._remote_get(replica, request, ctx, "replica")
            )
            tasks.append(task)

        return await self._wait_remote(ctx, tasks, request.timeout)

    async def coordinate_put(
        self,
        request: PutRequest,
        placement: PartitionPlacement
    ) -> Message:
        return await self._coordinate_write(request, placement)

    async def coordinate_delete(
        self,
        request: DeleteRequest,
        placement: PartitionPlacement
    ) -> Message:
        return await self._coordinate_write(request, placement)

    async def forward_get(
        self,
        request: GetRequest,
        placement: PartitionPlacement
    ) -> Message:
        """
        Forward the GET to the primary coordinator and wait for its result.

        From this node's perspective, quorum is 1: the primary's response,
        which itself is based on its own read quorum.
        """
        remote = placement.vnode.node_id
        ctx = RequestContext(
            request_id=request.request_id,
            replicas=[remote],
            quorum=1,
            future=self._loop.create_future(),
        )
        self._ctx[request.request_id] = ctx
        task = self._spawner.spawn(
            self._remote_get(remote, request, ctx, "forward")
        )
        return await self._wait_remote(ctx, [task], request.timeout)

    async def forward_put(
        self,
        request: PutRequest,
        placement: PartitionPlacement
    ) -> Message:
        return await self._forward_write(request, placement)

    async def forward_delete(
        self,
        request: DeleteRequest,
        placement: PartitionPlacement
    ) -> Message:
        return await self._forward_write(request, placement)

    async def select_replicas(self, primary: VNode, count: int) -> list[str]:
        candidates: list[str] = []
        ring = await self._topology.get_ring()

        for replica in ring.iter_from(primary):
            if (
                primary.node_id != replica.node_id and
                replica.node_id not in candidates
                # self._probe_manager.is_alive(replica.node_id)
            ):
                candidates.append(replica.node_id)
            if len(candidates) ==  count:
                break

        self._logger.debug(f"Replicas from {primary}: {candidates}")
        return candidates

    async def _coordinate_write(
        self,
        request: PutRequest | DeleteRequest,
        placement: PartitionPlacement,
    ) -> Message:
        rf = self._peer_config.replication_factor
        quorum_write = request.quorum_write

        if quorum_write > rf:
            return Message(
                type="ko",
                data={
                    "message": (
                        "Quorum read cannot reach because replication factor "
                        f"is {rf} while quorum is {quorum_write}"
                    )
                }
            )

        primary = placement.vnode
        replicas = await self.select_replicas(primary, rf - 1)
        if quorum_write - 1 > len(replicas):
            return Message(
                type="ko",
                data={
                    "message": "Internal error: not enough replicas to reach quorum"
                }
            )

        ctx = RequestContext(
            request_id=request.request_id,
            replicas=replicas + [primary.node_id],
            quorum=quorum_write,
            future=self._loop.create_future(),
        )
        self._ctx[request.request_id] = ctx

        tasks = [self._spawner.spawn(self._local_write(placement, request, ctx))]
        for replica in replicas:
            task = self._spawner.spawn(
                self._remote_write(replica, request, ctx, "replica")
            )
            tasks.append(task)

        return await self._wait_remote(ctx, tasks, request.timeout)

    async def _forward_write(
        self,
        request: PutRequest | DeleteRequest,
        placement: PartitionPlacement,
    ) -> Message:
        remote = placement.vnode.node_id
        ctx = RequestContext(
            request_id=request.request_id,
            replicas=[remote],
            quorum=1,
            future=self._loop.create_future(),
        )
        self._ctx[request.request_id] = ctx
        task = self._spawner.spawn(
            self._remote_write(remote, request, ctx, "forward")
        )
        return await self._wait_remote(ctx, [task], request.timeout)

    def _handle_ko(self, data: dict) -> None:
        request_id = data["request_id"]
        ctx = self._ctx.get(request_id)
        if not ctx:
            return

        sender = data["source"]
        ctx.failures += 1
        self._probe_manager.mark_suspect(sender)

    def _handle_ok(self, data: dict) -> None:
        request_id = data["request_id"]
        ctx = self._ctx.get(request_id)
        if not ctx:
            return

        ctx.responses.append(data)

        if len(ctx.responses) >= ctx.quorum:
            value = self._resolve_read(ctx.responses)
            ctx.future.set_result(value)
            del self._ctx[request_id]
        elif ctx.failures > len(ctx.replicas) - ctx.quorum:
            message = Message(
                type="ko",
                data={"message": "Quorum not reach due to replicas failures"}
            )
            ctx.future.set_result(message)
            del self._ctx[request_id]

    async def _local_read(
        self,
        placement: PartitionPlacement,
        request: GetRequest,
        ctx: RequestContext
    ) -> None:
        try:
            value = await self._storage.get(
                placement.keyspace, request.key
            )
            self._handle_ok({"value": value, "request_id": ctx.request_id})
        except Exception as ex:
            ctx.failures += 1
            self._logger.error(f"Error in local: {str(ex)}")

    async def _local_write(
        self,
        placement: PartitionPlacement,
        request: PutRequest | WriteRequest,
        ctx: RequestContext
    ) -> None:
        try:
            if isinstance(request, PutRequest):
                await self._storage.put(
                    placement.keyspace, request.key, request.value
                )
            else:
                await self._storage.delete(
                    placement.keyspace, request.key
                )
            self._handle_ok({"request_id": ctx.request_id})
        except Exception as ex:
            ctx.failures += 1
            self._logger.error(f"Error in local: {str(ex)}")

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
        request: GetRequest,
        ctx: RequestContext,
        kind: Literal["replica", "forward"]
    ) -> None:
        try:
            message = Message(
                type=f"{kind}/get",
                data={
                    "key": request.key,
                    "request_id": request.request_id,
                    "quorum_read": request.quorum_read,
                },
            )
            await self._peer_clients.send(replica, message)
        except Exception as ex:
            ctx.failures += 1
            await self._probe_manager.mark_suspect(replica)
            self._logger.error(f"Error get in remote {replica}: {str(ex)}")

    @staticmethod
    def _resolve_read(responses: list[dict]) -> Message:
        # For now: a naive approach, we just take the first response.
        # Later: compare versions, vector clocks, and so on.
        return Message(type="ok", data=responses[0])

    async def _remote_write(
        self,
        replica: str,
        request: PutRequest | DeleteRequest,
        ctx: RequestContext,
        kind: Literal["replica", "forward"]
    ) -> None:
        data = {
            "key": request.key,
            "request_id": request.request_id,
        }
        if isinstance(request, PutRequest):
            data["value"] = request.value
            action = "put"
        else:
            action = "delete"

        try:
            message = Message(type=f"{kind}/{action}", data=data)
            await self._peer_clients.send(replica, message)
        except Exception as ex:
            ctx.failures += 1
            await self._probe_manager.mark_suspect(replica)
            self._logger.error(f"Error get in remote {replica}: {str(ex)}")

    async def _wait_remote(
        self,
        ctx: RequestContext,
        tasks: list[asyncio.Task],
        timeout: float | None = None
    ) -> Message:
        try:
            message = await asyncio.wait_for(ctx.future, timeout=timeout)
        except asyncio.TimeoutError:
            self._logger.error("Timeout waiting get forward")
            message = Message(
                type="ko",
                data={"message": f"Quorum timeout exceeded: {timeout}"}
            )

        for task in tasks:
            if not task.done():
                task.cancel()

        return message
