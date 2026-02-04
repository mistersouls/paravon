import asyncio
import logging
import ssl

from paravon.bootstrap.config.settings import ParavonConfig
from paravon.core.facade import ParaCore
from paravon.core.models.config import ServerConfig
from paravon.core.ports.serializer import Serializer
from paravon.core.service.lifecycle import LifecycleService
from paravon.core.service.node import NodeService
from paravon.core.service.storage import StorageService
from paravon.core.transport.application import Application
from paravon.core.transport.server import MessageServer


class ControlPlane:
    def __init__(
        self,
        config: ParavonConfig,
        api_app: Application,
        peer_app: Application,
        serializer: Serializer,
    ) -> None:
        self._config = config
        self._api_app = api_app
        self._peer_app = peer_app
        self._loop = self._create_event_loop()
        self._server_ssl_ctx = self._config.get_server_ssl_ctx()
        self._serializer = serializer
        self._api_config = self._build_api_config(self._server_ssl_ctx)
        self._peer_config = self._build_peer_config(self._server_ssl_ctx)
        self._background_tasks: set[asyncio.Task] = set()

        self._logger = logging.getLogger("paravon.controlplane")

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
        self._node_service = NodeService(
            api_server=self._api_server,
        )
        self._storage_service = StorageService()
        self._lifecycle_service = LifecycleService(
            node_service=self._node_service,
            api_server=self._api_server,
            peer_server=self._peer_server,
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

    def _build_peer_config(self, server_ctx: ssl.SSLContext) -> ServerConfig:
        server_config = self._config.server
        peer_config = server_config.peer

        config = ServerConfig(
            app=self._api_app,
            host=peer_config.host,
            port=peer_config.port,
            backlog=server_config.backlog,
            ssl_ctx=server_ctx,
            limit_concurrency=server_config.limit_concurrency,
            max_buffer_size=server_config.max_buffer_size,
            max_message_size=server_config.max_message_size,
            timeout_graceful_shutdown=server_config.timeout_graceful_shutdown,
        )

        return config

    @staticmethod
    def _create_event_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
