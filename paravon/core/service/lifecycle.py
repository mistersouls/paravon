import asyncio
import logging

from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.gossip.gossiper import Gossiper
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.membership import Membership, NodePhase
from paravon.core.models.state import PeerState
from paravon.core.ports.serializer import Serializer
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.node import NodeService
from paravon.core.throttling.cubic import CubicRateController, CubicRateLimiter
from paravon.core.transport.server import MessageServer


class LifecycleService:
    def __init__(
        self,
        node_service: NodeService,
        api_server: MessageServer,
        peer_server: MessageServer,
        peer_config: PeerConfig,
        meta_manager: NodeMetaManager,
        serializer: Serializer,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()

        self._node_service = node_service
        self._api_server = api_server
        self._peer_server = peer_server
        self._peer_config = peer_config
        self._meta_manager = meta_manager
        self._serializer = serializer
        self._loop = loop

        self._spawner = TaskSpawner(loop=self._loop)
        self._peer_state = PeerState(spawner=self._spawner)
        self._peer_clients = ClientConnectionPool(
            serializer=self._serializer,
            spawner=self._spawner,
            ssl_context=self._peer_config.client_ssl_ctx
        )
        self._logger = logging.getLogger("core.service.lifecycle")

    async def start(self, stop_event) -> None:
        membership = await self._meta_manager.get_membership()
        seeds = self._peer_config.seeds
        bootstrapped = membership.node_id in seeds or seeds == set()

        if bootstrapped:
            self._logger.info(f"Starting node {membership.node_id} in bootstrap mode")
            await self.bootstrap(membership)
        else:
            self._logger.info(f"Starting node {membership.node_id} in normal mode")
            await self.start_normal(stop_event, membership)

        await self.start_gossip(stop_event)
        self._logger.info("Started Gossip service")

        self._spawner.spawn(
            self._peer_clients.dispatch_forever(stop_event)
        )
        self._logger.info("Started dispatch loop for incoming peer messages")

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

        self._logger.info("Closing Peer clients")
        await self._peer_clients.close()

        while remaining := self._spawner.remaining_tasks:
            self._logger.info(f"Waiting for {remaining} background tasks to complete.")
            await asyncio.sleep(0.1)

    async def bootstrap(self, membership: Membership) -> None:
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

    async def start_gossip(self, stop_event: asyncio.Event) -> None:
        gossiper = Gossiper(
            peer_state=self._peer_state,
            serializer=self._serializer,
            meta_manager=self._meta_manager,
            peer_clients=self._peer_clients,
        )
        cubic_controller = CubicRateController()
        rate_limiter = CubicRateLimiter(controller=cubic_controller)
        self._spawner.spawn(gossiper.run(stop_event, rate_limiter))
