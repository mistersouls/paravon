import asyncio
import logging

from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer
from paravon.core.transport.application import Application
from paravon.core.transport.flow import FlowControl


class Streamer:
    """
    Manages the bidirectional flow of messages for a single TCP connection.

    It receives decoded Message objects from the Protocol through an internal queue
    and exposes them to the Application via the asynchronous `receive()` method.
    When the Application sends a response, the Streamer serializes the Message and
    writes the resulting frame to the transport.

    Streamer enforces backpressure using FlowControl. If the transport signals that
    writing is paused, `send()` will wait until writing becomes possible again
    before transmitting data. This prevents the Application from overwhelming the
    network buffer.

    The `run_app()` method executes the Application for the lifetime of the
    connection. When the Application returns or raises an exception, the Streamer
    closes the transport and terminates the connection cleanly.

    Streamer does not parse frames, deserialize incoming data, or interpret message
    contents. These tasks are handled by the Protocol and the Application.
    """
    def __init__(
        self,
        transport: asyncio.Transport,
        flow: FlowControl,
        serializer: Serializer,
        queue: asyncio.Queue[Message | None]
    ) -> None:
        self.queue = queue
        self._transport = transport
        self._flow = flow
        self._serializer = serializer
        self._logger = logging.getLogger("core.transport.stream")

    async def send(self, message: Message) -> None:
        if self._flow.write_paused:
            await self._flow.drain()

        try:
            frame = self._serializer.serialize(message.to_dict())
            self._transport.write(frame)
        except Exception as exc:
            self._logger.error(f"Failed to send message: {exc}")
            self._transport.close()

    async def receive(self) -> Message:
        return await self.queue.get()

    async def run_app(self, app: Application) -> None:
        try:
            await app(self.receive, self.send)
        except BaseException as exc:
            self._logger.error("Exception in Application", exc_info=exc)
        finally:
            self._transport.close()
