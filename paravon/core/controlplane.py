import asyncio
import logging
import ssl

from paravon.bootstrap.config.settings import ParavonConfig
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.facade import ParaCore
from paravon.core.gossip.gossiper import Gossiper
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import ServerConfig, PeerConfig
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import StorageFactory
from paravon.core.service.lifecycle import LifecycleService
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.node import NodeService
from paravon.core.service.storage import StorageService
from paravon.core.service.topology import TopologyManager
from paravon.core.transport.application import Application
from paravon.core.transport.server import MessageServer


class ControlPlane:
    def __init__(
        self,
        config: ParavonConfig, # todo(souls): break hexagonal design
        api_app: Application,
        peer_app: Application,
        serializer: Serializer,
        storage_factory: StorageFactory,
    ) -> None:
        self._config = config
        self._api_app = api_app
        self._peer_app = peer_app
        self._loop = self._create_event_loop()
        self._server_ssl_ctx = self._config.get_server_ssl_ctx()
        self._client_ssl_ctx = self._config.get_client_ssl_ctx()
        self._serializer = serializer
        self._storage_factory = storage_factory
        self._api_config = self._build_api_config(self._server_ssl_ctx)
        self._peer_config = self._build_peer_config(
            self._server_ssl_ctx, self._client_ssl_ctx
        )
        self._background_tasks: set[asyncio.Task] = set()

        self._logger = logging.getLogger("paravon.controlplane")

        self._meta_manager = NodeMetaManager(
            peer_config=self._peer_config,
            system_storage=self._storage_factory.create("system"),
            serializer=self._serializer
        )
        self._api_server = MessageServer(
            config=self._api_config,
            serializer=self._serializer,
            loop=self._loop,
        )
        self._peer_server = MessageServer(
            config=self._peer_config,
            serializer=self._serializer,
            loop=self._loop,
        )
        self._spawner = TaskSpawner(loop=self._loop)
        self._topology_manager = TopologyManager(
            meta_manager=self._meta_manager,
            serializer=self._serializer,
        )
        self._peer_clients = ClientConnectionPool(
            serializer=self._serializer,
            spawner=self._spawner,
            ssl_context=self._peer_config.client_ssl_ctx
        )
        self._gossiper = Gossiper(
            spawner=self._spawner,
            serializer=self._serializer,
            meta_manager=self._meta_manager,
            peer_clients=self._peer_clients,
            topology_manager=self._topology_manager
        )
        self._node_service = NodeService(
            api_server=self._api_server,
            meta_manager=self._meta_manager,
            peer_config=self._peer_config,
            serializer=self._serializer,
            topology_manager=self._topology_manager,
            gossiper=self._gossiper,
            spawner=self._spawner,
            loop=self._loop,
        )
        self._storage_service = StorageService()
        self._lifecycle_service = LifecycleService(
            node_service=self._node_service,
            api_server=self._api_server,
            peer_server=self._peer_server,
            peer_config=self._peer_config,
            meta_manager=self._meta_manager,
            spawner=self._spawner,
            gossiper=self._gossiper,
            peer_clients=self._peer_clients,
            serializer=self._serializer,
            topology_manager=self._topology_manager
        )

    def build_core(self) -> ParaCore:
        return ParaCore(
            node_service=self._node_service,
            storage_service=self._storage_service
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    async def start(self, stop_event: asyncio.Event) -> None:
        await self._lifecycle_service.start(stop_event)
        await stop_event.wait()
        await self._lifecycle_service.stop()

    def _build_api_config(self, server_ctx: ssl.SSLContext) -> ServerConfig:
        server_config = self._config.server
        api_config = server_config.api

        config = ServerConfig(
            app=self._api_app,
            host=api_config.host,
            port=api_config.port,
            backlog=server_config.backlog,
            ssl_ctx=server_ctx,
            limit_concurrency=server_config.limit_concurrency,
            max_buffer_size=server_config.max_buffer_size,
            max_message_size=server_config.max_message_size,
            timeout_graceful_shutdown=server_config.timeout_graceful_shutdown,
        )

        return config

    def _build_peer_config(
        self,
        server_ctx: ssl.SSLContext,
        client_ctx: ssl.SSLContext,
    ) -> PeerConfig:
        server_config = self._config.server
        peer_config = server_config.peer
        node_config = self._config.node

        config = PeerConfig(
            node_id=node_config.id,
            node_size=node_config.size,
            app=self._peer_app,
            host=peer_config.host,
            port=peer_config.port,
            backlog=server_config.backlog,
            ssl_ctx=server_ctx,
            limit_concurrency=server_config.limit_concurrency,
            max_buffer_size=server_config.max_buffer_size,
            max_message_size=server_config.max_message_size,
            timeout_graceful_shutdown=server_config.timeout_graceful_shutdown,
            seeds=set(peer_config.seeds),
            peer_listener=peer_config.listener,
            client_ssl_ctx=client_ctx
        )

        return config

    @staticmethod
    def _create_event_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
