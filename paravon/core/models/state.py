import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from paravon.core.gossip.table import BucketTable
from paravon.core.models.membership import Membership
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
    membership: Membership

    table: BucketTable

    ring: Ring
    """Local consistent-hash ring or peer topology structure."""

    def to_dict(self) -> dict:
        local = self.membership
        data = {
            "node_id": local.node_id,
            "incarnation": local.incarnation,
            "epoch": local.epoch,
            "phase": local.phase,
            "peer_listener": local.peer_address,
            "dirty_global": self.table.dirty_global,
            "buckets": self._buckets_dict(),
            "ring": [
                str(vnode)
                for vnode in self.ring
            ]
        }
        return data

    def _buckets_dict(self) -> dict:
        buckets = {}
        for bucket_id, bucket in self.table.buckets.items():
            if checksum := bucket.get_checksum():
                memberships = {
                    node_id: self._membership_dict(m)
                    for node_id, m in bucket.memberships.items()
                }
                buckets[bucket_id] = {
                    "dirty": bucket.dirty,
                    "checksum": checksum,
                    "memberships": memberships,
                }

        return buckets

    @staticmethod
    def _membership_dict(membership: Membership) -> dict:
        return {
            "incarnation": membership.incarnation,
            "epoch": membership.epoch,
            "phase": membership.phase,
            "size": membership.size,
            "tokens": [hex(t) for t in membership.tokens],
            "peer_listener": membership.peer_address
        }
