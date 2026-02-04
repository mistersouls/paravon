import asyncio
import logging

from paravon.core.models.config import ServerConfig
from paravon.core.models.state import ServerState
from paravon.core.ports.serializer import Serializer
from paravon.core.transport.protocol import Protocol


class MessageServer:
    """
    Owns the lifecycle of a TCP server that accepts client
    connections, instantiates Protocol instances for each connection, and
    coordinates graceful shutdown.

    It binds to the configured host and port, using asyncio's create_server
    to create an asyncio.Server that dispatches new connections to Protocol
    instances. Each Protocol is constructed with a shared ServerState, which
    tracks active connections and background tasks spawned by the per‑connection
    Streamer.

    The server does not implement any application logic itself. Instead, it
    wires together the configured application callable, the Serializer, and
    the transport Protocol so that incoming frames are decoded into Message
    objects and passed to the application.

    On shutdown, MessageServer closes the listening socket, asks all active
    connections to shut down, and waits for both client connections and
    background tasks to complete. If the graceful shutdown timeout is exceeded,
    any remaining tasks are cancelled and an error is logged.
    """
    def __init__(
        self,
        config: ServerConfig,
        serializer: Serializer,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._config = config
        self._serializer = serializer
        self._loop = loop or asyncio.get_event_loop()
        self.state = ServerState()
        self._logger = logging.getLogger("core.transport.server")

        self._server: asyncio.AbstractServer | None = None

    def create_protocol(self) -> asyncio.Protocol:
        loop = self._loop
        return Protocol(
            config=self._config,
            server_state=self.state,
            serializer=self._serializer,
            loop=loop
        )

    async def start(self) -> None:
        config = self._config
        host = config.host
        port = config.port

        self._server = await self._loop.create_server(
            self.create_protocol,
            host=host,
            port=port,
            backlog=config.backlog,
            ssl=config.ssl_ctx
        )

    async def shutdown(self) -> None:
        if self._server:
            self._server.close()

        for connection in self.state.connections.copy():
            connection.shutdown()

        try:
            await asyncio.wait_for(
                self._wait_task_complete(),
                timeout=self._config.timeout_graceful_shutdown
            )
        except asyncio.TimeoutError:
            self._logger.error(
                f"Cancel {len(self.state.tasks)} running task(s), "
                f"timeout graceful shutdown: {self.state.tasks}"
            )
            for task in self.state.tasks:
                task.cancel("Task cancelled, timeout graceful shutdown exceeded")

    async def _wait_task_complete(self) -> None:
        if self.state.connections:
            self._logger.info("Waiting for client connections to close.")

        while self.state.connections:
            await asyncio.sleep(0.1)

        if self.state.tasks:
            self._logger.info("Waiting for background tasks to complete.")

        while self.state.tasks:
            await asyncio.sleep(0.1)

        if self._server:
            await self._server.wait_closed()
