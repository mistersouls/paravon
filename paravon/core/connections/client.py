import asyncio
import logging
import ssl
import struct

from paravon.core.helpers.sub import Subscription
from paravon.core.throttling.backoff import ExponentialBackoff
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer


class ClientConnection:
    """
    Maintains a persistent TCP connection to a remote peer and provides
    asynchronous send/receive capabilities. The connection is long‑lived
    and is reused for multiple messages, which avoids repeated TLS
    handshakes and reduces connection churn in large clusters.

    The connection attempts to reconnect automatically when needed,
    unless it has been explicitly shut down. A bounded exponential
    backoff controls retry frequency to prevent reconnection storms
    when a peer is unavailable.

    Incoming messages are processed by a background receive loop.
    The loop stops when the connection is closed or when the client
    is permanently shut down. Outgoing messages are framed using a
    4‑byte big‑endian length prefix followed by the serialized payload.

    Shutdown is terminal: once the client is closed, it will not
    reconnect or accept new send requests. This ensures predictable
    teardown behavior and prevents tasks from being restarted after
    system shutdown.
    """
    def __init__(
        self,
        address: str,
        ssl_context: ssl.SSLContext,
        serializer: Serializer,
        spawner: TaskSpawner,
        subscription: Subscription[Message | None],
        backoff: ExponentialBackoff | None = None,
        max_retries: int = 10,
    ) -> None:
        self._address = address
        self._ssl_context = ssl_context
        self._serializer = serializer
        self._spawner = spawner
        self._subscription = subscription
        self._backoff = backoff or ExponentialBackoff()
        self._max_retries = max_retries

        self._reader: asyncio.StreamReader = None  # type: ignore[assignment]
        self._writer: asyncio.StreamWriter = None  # type: ignore[assignment]
        self._receive_task: asyncio.Task | None = None

        self.connected = False
        self._stopped = False

        self._logger = logging.getLogger("core.connections.client")

    async def close(self) -> None:
        """
        Permanently shut down the connection.

        This method stops the receive loop, closes the underlying
        TCP stream, and prevents any future reconnection attempts.
        It is safe to call multiple times.
        """
        self._stopped = True

        if self._receive_task:
            self._receive_task.cancel()

        await self._disconnect()
        self._receive_task = None

    async def connect(self) -> None:
        """
        Establish a TCP connection to the peer.

        The method retries with exponential backoff until either the
        connection succeeds, the retry limit is reached, or the client
        has been shut down. On success, the background receive loop is
        started automatically.
        """
        if self._stopped:
            return

        retries = 0
        while not self.connected and retries < self._max_retries and not self._stopped:
            try:
                host, port = self._address.split(":")
                self._reader, self._writer = await asyncio.open_connection(
                    host=host,
                    port=int(port),
                    ssl=self._ssl_context,
                    server_hostname=None,
                )
                self.connected = True

                if self._receive_task:
                    self._receive_task.cancel()
                self._receive_task = self._spawner.spawn(self.recv())

                self._backoff.reset()
                break
            except Exception as ex:
                delay = self._backoff.next_delay()
                retries += 1
                self._logger.warning(
                    f"Connect failed to {self._address}: {ex}. "
                    f"Retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)

    async def recv(self) -> None:
        """
        Continuously read framed messages from the peer.

        The loop terminates when the connection is closed, when the
        peer disconnects, or when the client is shut down. Each frame
        consists of a 4‑byte length prefix followed by the serialized
        message payload. Deserialized messages are dispatched to the
        internal handler.
        """
        try:
            while self.connected and not self._stopped:
                header = await self._reader.readexactly(4)
                length = struct.unpack("!I", header)[0]
                payload = await self._reader.readexactly(length)
                data = self._serializer.deserialize(payload)
                if not data:
                    break

                self._handle_message(Message(**data))
        except asyncio.IncompleteReadError:
            self._logger.info(f"Peer {self._address} disconnected")
        except Exception as ex:
            self._logger.error(
                f"Error received for {self._address}: {ex}", exc_info=ex
            )
        finally:
            await self._disconnect()

    async def send(self, message: Message) -> None:
        """
        Send a message to the peer.

        If the connection is not currently established, the method
        attempts to reconnect unless the client has been shut down.
        The message is serialized and sent as a length‑prefixed frame.
        """
        if self._stopped:
            self._logger.warning("Connection already closed")
            return

        if not self.connected:
            await self.connect()
            if not self.connected:
                raise RuntimeError(f"Unable to connect to {self._address}")

        payload = self._serializer.serialize(message.to_dict())
        frame = struct.pack("!I", len(payload)) + payload

        try:
            self._writer.write(frame)
            await self._writer.drain()
        except ConnectionResetError as ex:
            self.connected = False
            self._logger.error(f"Connection reset by {self._address}: {ex}")
            raise ex

    async def _disconnect(self) -> None:
        self.connected = False
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:  # noqa
                pass
            self._writer = None
            self._reader = None

    def _handle_message(self, message: Message) -> None:
        self._subscription.publish(message)
