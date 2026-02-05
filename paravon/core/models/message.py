from dataclasses import dataclass, asdict
from typing import Any, Callable, Awaitable


@dataclass
class Message:
    """
    Internal representation of an application-level message.
    The transport layer encodes/decodes messages via the Serializer,
    while the application manipulates them in this native Python form.
    """
    type: str
    """
    type of message, e.g. "ping", "write", "error"
    """

    data: dict[Any, Any]
    """
    A dictionary of serializable data
    """

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the message."""
        return asdict(self)


ReceiveMessage = Callable[[], Awaitable[Message]]
"""
Coroutine provided to the application for receiving a message.
It suspends until a message is available.
"""


SendMessage = Callable[[Message], Awaitable[None]]
"""
Coroutine provided to the application for sending a message to the client.
"""
