import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from paravon.core.transport.protocol import Protocol


@dataclass
class ServerState:
    """
    Shared runtime state for a MessageServer.

    This object is mutated by:
    - Protocol: adds/removes active connections
    - Streamer: registers background tasks created by the application
    - MessageServer.shutdown(): waits for connections and tasks to complete
    """
    connections: set[Protocol] = field(default_factory=set)
    """
    Set of active Protocol instances. Each TCP connection corresponds
    to one Protocol.
    """

    tasks: set[asyncio.Task[None]] = field(default_factory=set)
    """
    Set of background tasks spawned by the application.
    Each task must be registered and later removed via a
    task.add_done_callback(tasks.discard) to enable clean shutdown.
    """
