import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.space.ring import Ring

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


@dataclass
class PeerState:
    """
    Shared state used by the Gossiper to coordinate background tasks and
    maintain the local view of the cluster ring.

    This object acts as a lightweight container for:
    - a TaskSpawner used to schedule asynchronous gossip operations
    - a Ring structure representing the local consistent-hash ring or
      peer topology (depending on your implementation)

    PeerState is intentionally minimal: it does not implement any logic
    itself. It simply groups together the mutable state that multiple
    components (gossip loop, membership updates, RPC handlers) need to
    access concurrently.
    """

    spawner: TaskSpawner
    """Spawner responsible for launching and supervising background tasks."""

    ring: Ring = field(default_factory=Ring)
    """Local consistent-hash ring or peer topology structure."""
