import logging
import uuid
from typing import Callable

from paravon.core.models.message import Message, ReceiveMessage, SendMessage
from paravon.core.routing.router import Router, RouteHandler


class RoutedApplication:
    """
    Application implementation that dispatches incoming messages to handlers
    registered in a `Router`.

    - For each incoming event, the application resolves the handler associated
      with `event.type` using the Router.
    - If no handler exists, a `ko` Message is returned to the client.
    - If a handler exists, it is awaited with a mutable copy of the event data.
      A `request_id` is injected if missing.
    - If the handler returns a Message, it is sent back to the peer.
    - Any exception raised by a handler is logged and results in a `ko` Message.

    The application terminates when `receive()` returns None, at which point the
    Streamer closes the underlying connection.

    This component is intentionally minimal: it does not perform framing,
    serialization, or transport management. These responsibilities belong to
    the Protocol and the Streamer.
    """

    def __init__(self) -> None:
        self.router = Router()
        self._logger = logging.getLogger("core.routing.app")

    async def __call__(self, receive: ReceiveMessage, send: SendMessage) -> None:
        while True:
            msg = await receive()
            if msg is None:
                break

            handler = self.router.resolve(msg.type)

            if handler is None:
                msg = f"Unknown message type '{msg.type}'"
                await send(Message(type="ko", data={"message": msg}))
                continue

            try:
                data = dict(msg.data)
                data.setdefault("request_id", str(uuid.uuid4()))
                result = await handler(data)
                if result is None:
                    result = Message(type="ko", data={"message": "Empty response"})
                await send(result)
                self._logger.debug(f"Sent message: {result}")
            except Exception as exc:
                self._logger.error(f"Error in handler '{msg.type}': {exc}", exc_info=exc)
                await send(Message(type="ko", data={"message": str(exc)}))

    def request(self, event_type: str) -> Callable[[RouteHandler], RouteHandler]:
        return self.router.request(event_type)
