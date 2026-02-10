import asyncio
import logging
import struct

from paravon.core.models.config import ServerConfig
from paravon.core.models.message import Message
from paravon.core.models.state import ServerState
from paravon.core.ports.serializer import Serializer
from paravon.core.transport.addr import get_remote_addr
from paravon.core.transport.flow import FlowControl
from paravon.core.transport.stream import Streamer


class Protocol(asyncio.Protocol):
    """
    Implements the low‑level framing and connection lifecycle for a
    single TCP client. It receives raw bytes from the transport, reconstructs
    framed messages, deserializes them, and forwards decoded Message objects to
    the Streamer instance associated with the connection.

    When a connection is established, Protocol creates a FlowControl instance,
    registers itself in the server's connection set, and starts the Streamer
    task responsible for running the application logic. Incoming frames are
    accumulated in an internal buffer until a complete message is available.
    Each frame begins with a 4‑byte big‑endian length prefix followed by the
    serialized payload.

    If the buffer grows beyond the configured maximum size, or if a frame
    declares an invalid length, the connection is closed immediately. Valid
    frames are deserialized into Message objects and pushed into the Streamer’s
    queue for processing by the application.

    When the connection is lost, Protocol removes itself from the server state,
    resumes writing if flow control was active, closes the transport if the
    disconnection was clean, and signals termination to the Streamer by pushing
    a sentinel value into its queue.

    Protocol does not interpret message contents, run application logic, or
    perform serialization. These responsibilities belong to the Streamer and
    the configured Serializer.
    """
    def __init__(
        self,
        config: ServerConfig,
        server_state: ServerState,
        serializer: Serializer,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._transport: asyncio.Transport = None   # type: ignore[assignment]
        self._flow: FlowControl = None  # type: ignore[assignment]
        self._streamer: Streamer = None   # type: ignore[assignment]

        self._config = config
        self._app = config.app
        self._loop = loop or asyncio.get_event_loop()
        self._connections = server_state.connections
        self._tasks = server_state.tasks
        self._serializer = serializer
        self._buffer = bytearray()
        self._expected_length: int | None = None
        self._client: tuple[str, int] | None = None
        self._logger = logging.getLogger("core.transport.protocol")

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        self._flow = FlowControl()
        self._connections.add(self)
        self._streamer = Streamer(
            transport=self._transport,
            flow=self._flow,
            queue=asyncio.Queue(),
            serializer=self._serializer
        )
        task = self._loop.create_task(self._streamer.run_app(self._app))
        task.add_done_callback(self._tasks.discard)
        self._tasks.add(task)

        self._client = get_remote_addr(transport)
        who = f"%s:%d" % self._client if self._client else ""
        self._logger.debug(f"{who} - Connection made")

    def connection_lost(self, exc: Exception | None) -> None:
        self._connections.discard(self)

        who = f"%s:%d" % self._client if self._client else "" # noqa
        self._logger.debug(f"{who} - Connection lost.")

        if self._flow is not None:
            self._flow.resume_writing()
        if exc is None:
            self._transport.close()

        self._streamer.queue.put_nowait(None)

    def eof_received(self) -> None:
        pass

    def data_received(self, data: bytes) -> None:
        self._buffer.extend(data)

        if len(self._buffer) > self._config.max_buffer_size:
            self._logger.warning("Buffer overflow, closing connection")
            self._transport.close()
            return

        while True:
            if self._expected_length is None:
                if len(self._buffer) < 4:
                    return

                # "!I" = uint32 big-endian (network order)
                self._expected_length = struct.unpack("!I", self._buffer[:4])[0]
                del self._buffer[:4]

                if self._expected_length > self._config.max_buffer_size:
                    self._logger.warning("Message too large, closing connection")
                    self._transport.close()
                    return

            if len(self._buffer) < self._expected_length:
                return

            payload = self._buffer[:self._expected_length]
            del self._buffer[:self._expected_length]
            self._expected_length = None

            msg = self._decode_message(payload)
            try:
                self._streamer.queue.put_nowait(msg)
            except Exception as exc:
                self._logger.error(f"Queue error: {exc}")
                continue

    def pause_writing(self) -> None:
        self._flow.pause_writing()

    def resume_writing(self) -> None:
        self._flow.resume_writing()

    def shutdown(self) -> None:
        self._transport.close()

    def _decode_message(self, frame: bytes) -> Message:
        try:
            payload = self._serializer.deserialize(frame)
            return Message(**payload)
        except Exception as exc:
            self._logger.warning(f"Invalid frame format: {exc}")
            return Message(type="ko", data={"message": "Invalid frame format"})
