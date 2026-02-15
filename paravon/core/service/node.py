import asyncio
import logging

from paravon.core.gossip.gossiper import Gossiper
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.membership import Membership, NodePhase
from paravon.core.ports.serializer import Serializer
from paravon.core.service.bootstrapper import SeedBootstrapper
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.topology import TopologyManager
from paravon.core.space.hashspace import HashSpace
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

    async def bootstrap_node(self, membership: Membership) -> None:
        node_id = membership.node_id
        size = membership.size.value
        phase = membership.phase
        tokens = membership.tokens

        if not tokens:
            tokens = HashSpace.generate_tokens(node_id, size)
            if phase not in (NodePhase.idle, NodePhase.joining):
                self._logger.warning(
                    f"Expected local membership to have tokens "
                    f"when phase={phase}, but empty."
                )
            await self._meta_manager.bump_epoch()
            await self._meta_manager.set_tokens(list(tokens))
            self._logger.info(
                f"Created local Vnodes for node with {membership.size}"
            )

        if phase != NodePhase.ready:
            await self._meta_manager.bump_epoch()
            await self._meta_manager.set_phase(NodePhase.ready)
            self._logger.info(f"Set local membership phase to ready from {phase}.")
        else:
            self._logger.debug("Local membership is ready, skip persisting phase.")

        await self._topology.add_membership(membership)
        self._logger.info(f"Added Local membership {node_id} to the ring.")

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

    async def apply_bucket(self, data: dict) -> Message:
        source = await self._meta_manager.get_membership()
        local_memberships = await self._gossiper.apply_bucket(data)
        raw_memberships = {
            m.node_id: m.to_dict()
            for m in local_memberships.values()
        }
        return Message(
            type="gossip/bucket",
            data={
                "bucket_id": data["bucket_id"],
                "memberships": raw_memberships,
                "source": source.to_dict(),
            }
        )

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

    async def _complete_drain(self, membership: Membership) -> None:
        await asyncio.sleep(0.1)
        async with self._lock:
            if membership.phase == NodePhase.draining:
                await self._meta_manager.set_phase(NodePhase.idle)
                self._idle_event.set()

    async def _complete_join(self, membership: Membership) -> None:
        try:
            async with SeedBootstrapper(
                membership=membership,
                peer_config=self._peer_config,
                serializer=self._serializer,
                spawner=self._spawner,
                gossiper=self._gossiper,
                loop=self._loop
            ) as memberships:
                await self._topology.restore(
                    memberships,
                    excludes=[membership.node_id]
                )
                # to review: fetch partitions or missing keys

        except Exception as ex:
            await self._meta_manager.set_phase(NodePhase.failed)
            self._logger.error(f"Error during joining: {ex}")

        async with self._lock:
            if membership.phase == NodePhase.joining:
                await self.bootstrap_node(membership)
                self._ready_event.set()
