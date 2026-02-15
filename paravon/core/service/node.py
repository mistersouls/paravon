import asyncio
import logging

from paravon.core.gossip.gossiper import Gossiper
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.service.bootstrapper import SeedBootstrapper
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.membership import Membership, NodePhase
from paravon.core.ports.serializer import Serializer
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.topology import TopologyManager
from paravon.core.transport.server import MessageServer


class NodeService:
    def __init__(
        self,
        api_server: MessageServer,
        meta_manager: NodeMetaManager,
        spawner: TaskSpawner,
        peer_config: PeerConfig,
        gossiper: Gossiper,
        serializer: Serializer,
        topology_manager: TopologyManager,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._api_server = api_server
        self._meta_manager = meta_manager
        self._spawner = spawner
        self._topology = topology_manager
        self._peer_config = peer_config
        self._gossiper = gossiper
        self._serializer = serializer
        self._loop = loop
        self._ready_event = asyncio.Event()
        self._idle_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("core.service.node")

    async def join(self) -> Message:
        self._logger.info("Trying to join...")
        async with self._lock:
            membership = await self._meta_manager.get_membership()
            match membership.phase:
                case NodePhase.idle:
                    await self._meta_manager.set_phase(NodePhase.joining)
                    self._idle_event.clear()
                    self._spawner.spawn(self._complete_join(membership))
                    message = "Received JOIN command."
                    self._logger.info(message)
                    return Message(type="ok", data={"message": message})
                case NodePhase.joining | NodePhase.ready:
                    message = "Already joining/ready, ignored."
                    self._logger.info(message)
                    return Message(
                        type="ok",
                        data={"message": message}
                    )
                case _:
                    message = f"Cannot join from {membership.phase}"
                    return Message(
                        type="ko",
                        data={"message": message}
                    )

    async def drain(self) -> Message:
        self._logger.info("Trying to drain...")
        async with self._lock:
            membership = await self._meta_manager.get_membership()
            match membership.phase:
                case NodePhase.ready:
                    await self._meta_manager.set_phase(NodePhase.draining)
                    self._ready_event.clear()
                    self._spawner.spawn(self._complete_drain(membership))
                    message = "Drain scheduled."
                    self._logger.info(message)
                    return Message(type="ok", data={"message": message})
                case NodePhase.draining:
                    message = "Already draining, ignored."
                    self._logger.info(message)
                    return Message(
                        type="ko",
                        data={"message": message}
                    )
                case _:
                    message = f"Cannot drain from {membership.phase}"
                    self._logger.info(message)
                    return Message(
                        type="ko",
                        data={"message": message}
                    )

    @staticmethod
    async def remove() -> Message:
        return Message(type="ko", data={"message": "Not implemented yet"})

    async def apply_checksums(self, data: dict) -> Message:
        local_checksums = await self._gossiper.apply_checksums(data)
        source = await self._meta_manager.get_membership()
        return Message(
            type="gossip/checksums",
            data={
                "source": source.to_dict(),
                "checksums": local_checksums
            }
        )

    async def wait_for_idle(self) -> None:
        await self._idle_event.wait()

    async def wait_for_ready(self) -> None:
        await self._ready_event.wait()

    async def recover_ring(self, membership: Membership) -> None:
        ...

    # async def _bootstrap_seeds(self) -> None:
    #     tasks = []
    #     await self._gossiper.send_checksums()

    async def _complete_drain(self, membership: Membership) -> None:
        await asyncio.sleep(0.1)
        async with self._lock:
            if membership.phase == NodePhase.draining:
                await self._meta_manager.set_phase(NodePhase.idle)
                self._idle_event.set()

    async def _complete_join(self, membership: Membership) -> None:
        try:
            bootstrapper = SeedBootstrapper(
                membership=membership,
                peer_config=self._peer_config,
                serializer=self._serializer,
                spawner=self._spawner,
                gossiper=self._gossiper,
                loop=self._loop
            )
            async with bootstrapper as memberships:
                await self._topology.restore(memberships, excludes=[membership.node_id])
                # need may be to add local member later; waiting fetch partitions
                await self._topology.add_membership(membership)
        except Exception as ex:
            # should set_phase ? with which value ?
            self._logger.error(f"Error during joining: {ex}")

        async with self._lock:
            if membership.phase == NodePhase.joining:
                await self._meta_manager.set_phase(NodePhase.ready)
                self._ready_event.set()
