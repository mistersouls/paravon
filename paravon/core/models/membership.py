from dataclasses import dataclass
from enum import StrEnum, IntEnum
from typing import Any


class NodeSize(IntEnum):
    """
    Represents the logical capacity class of a node.
    Each size corresponds to a power‑of‑two number of virtual nodes,
    allowing the ring to scale smoothly as nodes join or leave.
    """
    XS      = 1 << 0    # 1 vnode
    S       = 1 << 1
    M       = 1 << 2
    L       = 1 << 3
    XL      = 1 << 4
    XXL     = 1 << 5


class NodePhase(StrEnum):
    """
    Describes the lifecycle phase of a node within the cluster.
    Phases are used by the membership subsystem to coordinate safe
    transitions during scaling operations.
    """
    idle = "idle"
    joining = "joining"
    draining = "draining"
    ready = "ready"
    failed = "failed"


@dataclass
class Membership:
    """
    Represents the membership state of a single node.
    This structure is exchanged during gossip and used
    to maintain a consistent view of the ring.
    """
    epoch: int
    """
    A monotonically increasing version counter for the membership record.

    Each time a node updates its membership phase, it increments the epoch.
    Gossip uses this value to determine which version of a node’s state is
    newer and to resolve conflicts deterministically.

    Higher epochs always supersede lower ones, ensuring convergence of
    membership information across the cluster.
    """

    incarnation: int
    """
    Ring‑wide logical generation counter maintained locally by each peer.

    Unlike `epoch`, which versions an individual node’s membership record,
    `incarnation` represents the global “generation” of the ring’s state.
    Each peer maintains its own local incarnation as:

        incarnation = max(membership.epoch for all known nodes )

    This value increases whenever any node in the cluster publishes a newer
    membership epoch. It provides a monotonic, ring‑level notion of time
    that allows peers to:

    - detect when the overall ring has advanced to a newer generation,
    - invalidate stale views of the ring,
    - coordinate transitions that depend on global ordering rather than
      per‑node updates.

    Because it is derived from the maximum observed epoch, `incarnation`
    never decreases and converges naturally across all peers through gossip.
    """

    node_id: str
    """
    A stable identifier for the node within the cluster.
    It does not encode topology or location;
    it simply distinguishes one node from another.
    """

    size: NodeSize
    """
    The logical capacity class assigned to the node.
    This value determines how many virtual nodes (vnodes)
    the node contributes to the ring and therefore influences
    its share of the token space. Larger sizes correspond
    to proportionally higher load and ownership expectations.
    """

    phase: NodePhase
    """
    The current lifecycle phase of the node.
    This field indicates whether the node is idle, joining the ring,
    draining its responsibilities, or fully ready. Membership transitions
    rely on this phase to coordinate safe rebalancing and
    avoid inconsistent token ownership.
    """

    tokens: list[int]
    """
    The set of 128‑bit tokens owned by the node.
    These tokens define the node’s position(s) on
    the consistent hashing ring. They are stored as integers
    internally but serialized as fixed‑width 16‑byte big‑endian
    values to ensure compact and deterministic gossip propagation.
    """

    peer_address: str

    def is_remove_phase(self) -> bool:
        return self.phase in (NodePhase.idle, NodePhase.draining)

    def to_dict(self) -> dict[str, Any]:
        """
        Converts the membership record into a compact, gossip‑friendly dictionary.
        Tokens are stored as fixed‑width 16‑byte values to minimize overhead and
        ensure deterministic encoding across nodes.
        """
        return {
            "incarnation": self.incarnation,
            "epoch": self.epoch,
            "node_id": self.node_id,
            "size": self.size.name,
            "phase": self.phase.name,
            "tokens": self.tokens_bytes(self.tokens),
            "peer_address": self.peer_address
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Membership:
        """
        Reconstructs a Membership instance from its serialized form.
        Token values are decoded from their 16‑byte big‑endian
        representation back into 128‑bit integers.
        """
        size = data["size"]
        phase = data["phase"]

        if isinstance(size, str):
            size = NodeSize[size]
        if isinstance(phase, str):
            phase = NodePhase(phase)

        return cls(
            incarnation=data["incarnation"],
            epoch=data["epoch"],
            node_id=data["node_id"],
            size=size,
            phase=phase,
            tokens=cls.tokens_from(data["tokens"]),
            peer_address=data["peer_address"]
        )

    @staticmethod
    def tokens_bytes(tokens: list[int]) -> list[bytes]:
        return [token.to_bytes(16, "big") for token in tokens]

    @staticmethod
    def tokens_from(tokens: list[bytes]) -> list[int]:
        return [int.from_bytes(token, "big") for token in tokens]


@dataclass
class View:
    incarnation: int
    checksums: dict[str, int]
    address: str
    peer: str


@dataclass
class MembershipChange:
    before: Membership
    after: Membership


@dataclass(frozen=True)
class MembershipDiff:
    added: list[Membership]
    removed: list[Membership]
    updated: list[MembershipChange]
    bucket_id: str

    @property
    def changed(self) -> bool:
        return bool(self.added or self.removed or self.updated)

    @classmethod
    def from_empty(cls, bucket_id: str) -> MembershipDiff:
        return MembershipDiff(added=[], removed= [], updated=[], bucket_id=bucket_id)
