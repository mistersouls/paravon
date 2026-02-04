from paravon.core.models.message import ReceiveMessage, SendMessage
from typing import Protocol


class Application(Protocol):
    """
    This interface defines the per‑connection handler executed by the Streamer.

    An Application is an asynchronous callable that receives two functions:
    `receive`, which waits for and returns the next incoming Message, and `send`,
    which transmits a Message to the remote peer. The Application implements the
    business logic for a single TCP connection by repeatedly calling `receive()`
    to consume messages and `send(message)` to produce responses.

    The Application runs until it returns or raises an exception. When it exits,
    the underlying connection is closed by the Streamer.

    The Application does not handle framing, serialization, or transport-level
    concerns. These responsibilities belong to the Protocol and the Streamer.
    """
    async def __call__(self, receive: ReceiveMessage, send: SendMessage) -> None:
        ...
