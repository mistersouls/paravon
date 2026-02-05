import asyncio
import logging

from paravon.core.models.config import PeerConfig
from paravon.core.models.meta import Membership, NodePhase
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.node import NodeService
from paravon.core.transport.server import MessageServer


class LifecycleService:
    def __init__(
        self,
        node_service: NodeService,
        api_server: MessageServer,
        peer_server: MessageServer,
        peer_config: PeerConfig,
        meta_manager: NodeMetaManager,
    ) -> None:
        self._node_service = node_service
        self._api_server = api_server
        self._peer_server = peer_server
        self._peer_config = peer_config
        self._meta_manager = meta_manager
        self._logger = logging.getLogger("core.service.lifecycle")

    async def start(self, stop_event) -> None:
        membership = await self._meta_manager.get_membership()
        seeds = self._peer_config.seeds
        bootstrapped = membership.node_id in seeds or seeds == set()

        if bootstrapped:
            await self.bootstrap(membership)
        else:
            await self.start_normal(stop_event, membership)

    async def stop(self) -> None:
        if self._api_server.running:
            self._logger.info("Shutting down API server.")
            await self._api_server.shutdown()
        else:
            self._logger.info("API server is not running, skip shutting down.")

        if self._peer_server.running:
            self._logger.info("Shutting down Peer server.")
            await self._peer_server.shutdown()
        else:
            self._logger.info("Peer server is not running, skip shutting down.")

    async def bootstrap(self, membership: Membership) -> None:
        self._logger.info(f"Starting node {membership.node_id} in bootstrap mode")
        await self._peer_server.start()
        self._logger.info("Peer server started at %s:%d", *self._peer_server.listen)
        await self._api_server.start()
        self._logger.info("API server started at %s:%d", *self._api_server.listen)

        phase = membership.phase
        if phase != NodePhase.ready:
            await self._meta_manager.set_phase(NodePhase.ready)
            self._logger.debug(
                f"Persist membership phase as ready, previous: {phase}"
            )
        else:
            self._logger.debug(
                "Membership phase is already ready, skip persisting membership phase"
            )

        self._logger.info(
            "Node in standalone mode is now fully operational (PEER + API)."
        )

    async def start_normal(self, stop_event: asyncio.Event, membership: Membership) -> None:
        self._logger.info(f"Starting node {membership.node_id} in normal mode")
        self._logger.debug("Starting Peer server.")
        await self._peer_server.start()
        self._logger.info("Peer server started at %s:%d", *self._peer_server.listen)

        if membership.phase == NodePhase.idle:
            self._logger.info("Waiting for receiving JOIN command.")
            stop_task = asyncio.create_task(stop_event.wait())
            join_task = asyncio.create_task(self._node_service.wait_join())
            done, pending = await asyncio.wait(
                [join_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()

            if stop_event.is_set():
                self._logger.info("Stop signal received, cancel starting.")
                return
        else:
            self._logger.debug(f"Recovering Ring since phase={membership.phase}")
            await self._node_service.recover_ring(membership)
            self._logger.info("Ring recovered")

        await self._api_server.start()
        self._logger.info("API server started at %s:%d", *self._api_server.listen)
        self._logger.info(
            "Node in normal mode is now fully operational (PEER + API)."
        )
