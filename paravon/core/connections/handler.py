from typing import Protocol

from paravon.core.models.message import Message


class MessageHandler(Protocol):
    """
    Functional interface for components that process incoming messages.

    A MessageHandler is registered for one or more message types and is invoked
    asynchronously by the ClientConnectionPool whenever a matching message is
    received. Implementations should be lightweight, non-blocking, and resilient,
    as each handler runs in its own task within the dispatch pipeline.
    """

    async def handle(self, message: Message) -> None:
        """
        Process a single incoming message.

        This method is called for every message of the type the handler is
        subscribed to. Implementations should perform the necessary logic
        (state updates, propagation, business rules, etc.) without blocking
        the event loop. Exceptions may be raised intentionally or handled
        internally; each handler runs in isolation from others.
        """
