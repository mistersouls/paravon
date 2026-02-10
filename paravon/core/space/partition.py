from dataclasses import dataclass

from paravon.core.space.hashspace import HashSpace
from paravon.core.space.ring import Ring
from paravon.core.space.vnode import VNode


class Partitioner:
    """
    Maps 128‑bit hash values to logical partitions and ring placements.

    The partitioner divides the 128‑bit hash space into a fixed number of
    equal‑sized logical partitions, identified by a partition id (pid).
    These partitions are purely logical and independent of the physical
    ring layout. Higher‑level components use the partitioner to:

        - compute the partition id for a given hash or key,
        - derive the hash‑space segment covered by a partition,
        - locate the vnode responsible for a partition on the ring.

    This abstraction decouples the logical partitioning of the hash space
    from the physical distribution of data across nodes and vnodes.
    """
    def __init__(self, partition_shift: int) -> None:
        self._partition_shift = partition_shift

    @property
    def total_partitions(self) -> int:
        """
        Return the total number of logical partitions in the hash space.

        The partitioner divides the 128‑bit hash space into 2^partition_shift
        equally sized segments. This value defines the global, static number
        of logical partitions used by the system.
        """
        return 1 << self._partition_shift

    @property
    def step(self) -> int:
        """
        Return the size of a single logical partition in the 128‑bit hash space.

        The hash space is treated as a continuous range [0, 2^128). Dividing
        this range into total_partitions segments yields a fixed step size,
        which is used to compute partition boundaries.
        """
        return (1 << 128) >> self._partition_shift

    def pid_for_hash(self, h: int) -> int:
        """
        Compute the partition id for a 128-bit hash.
        Equivalent to: pid = h // step
        Implemented as: pid = h >> (128 - partition_shift)
        """
        return h >> (128 - self._partition_shift)

    def start_for_pid(self, pid: int) -> int:
        """
        Return the inclusive start boundary of the logical partition identified by pid.
        The start boundary is computed as: start = pid * step.
        """
        return pid * self.step

    def end_for_pid(self, pid: int) -> int:
        """
        Return the exclusive end boundary of the logical partition identified by pid.
        The end boundary is computed as: end = (pid + 1) * step.
        """
        return (pid + 1) * self.step

    def segment_for_pid(self, pid: int) -> tuple[int, int]:
        """
        Return the (start, end) boundaries for the logical partition pid.

        The returned interval is half‑open: (start, end], matching the
        semantics used by LogicalPartition.contains().
        """
        return self.start_for_pid(pid), self.end_for_pid(pid)

    def partition_for_hash(self, h: int) -> LogicalPartition:
        """
        Construct and return the LogicalPartition that contains the hash h.
        """
        pid = self.pid_for_hash(h)
        start, end = self.segment_for_pid(pid)
        return LogicalPartition(pid, start, end)

    def find_partition_by_key(self, key: bytes) -> LogicalPartition:
        """
        Hash the provided key and return the LogicalPartition that contains it.

        This method applies the system's hash function to the key, maps the
        resulting 128‑bit token to a partition id, and constructs the
        corresponding LogicalPartition object.
        """
        token = HashSpace.hash(key)
        partition = self.partition_for_hash(token)
        return partition

    def find_placement_by_key(self, key: bytes, ring: Ring) -> PartitionPlacement:
        """
        Compute the placement of the given key on the ring.

        The method determines the logical partition for the key, then locates
        the vnode responsible for the end boundary of that partition. The
        resulting PartitionPlacement ties together the logical partition and
        the physical vnode that owns it.
        """
        partition = self.find_partition_by_key(key)
        vnode = ring.find_successor(partition.end)
        return PartitionPlacement(partition, vnode)


@dataclass(frozen=True, slots=True)
class LogicalPartition:
    """
    LogicalPartition represents a fixed segment of the 128-bit hash space.

    The hash space is divided into Q equal-sized logical partitions. These
    partitions are static, globally defined, and independent of cluster topology.
    Each partition covers a half-open interval (start, end] on the ring.

    Logical partitions do not store data and do not know anything about nodes,
    vnodes, or replication. They are purely a mathematical subdivision of the
    hash space. Higher-level components map partitions to VNodes and physical
    nodes.
    """
    pid: int
    start: int
    end: int

    @property
    def pid_bytes(self) -> bytes:
        """
        Return the partition id encoded as lowercase hexadecimal bytes.

        This representation is suitable for use as a stable keyspace prefix
        in storage engines or replication metadata.
        """
        return format(self.pid, "x").encode("ascii")

    def contains(self, h: int) -> bool:
        """Reports whether the hash h belongs to this partition."""
        return self.start < h <= self.end


@dataclass(frozen=True, slots=True)
class PartitionPlacement:
    """
    Represents the placement of a logical partition on the ring.

    A PartitionPlacement ties together a LogicalPartition (a fixed segment
    of the 128‑bit hash space) and the VNode that currently owns that
    segment on the consistent‑hashing ring. This structure is used by
    higher‑level components to reason about data ownership, routing, and
    replication decisions.
    """
    partition: LogicalPartition
    vnode: VNode

    @property
    def keyspace(self) -> bytes:
        """
        Return the keyspace prefix associated with this placement.

        The keyspace is derived from the logical partition id and can be used
        as a stable namespace for storing or indexing data belonging to this
        partition.
        """
        return self.partition.pid_bytes
