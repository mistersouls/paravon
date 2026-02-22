import time
from dataclasses import dataclass
from typing import Any, Protocol, Iterable


@dataclass(frozen=True, order=True, slots=True)
class HLC:
    """
    Hybrid Logical Clock (HLC).

    A timestamp composed of:
        - physical: physical time in milliseconds
        - logical: logical counter for concurrency
        - node_id: node_id identifier to break ties

    The natural ordering (order=True) provides a total order:
        (physical, logical, node_id)

    This makes HLC suitable for deterministic conflict resolution
    in leaderless distributed systems.
    """
    physical: int
    logical: int
    node_id: str

    def encode(self) -> bytes:
        return (
            self.physical.to_bytes(8, "big") +
            self.logical.to_bytes(4, "big") +
            self.node_id.encode("utf-8")
        )

    @classmethod
    def decode(cls, data: bytes) -> HLC:
        if len(data) < 12:
            raise ValueError("Invalid HLC encoding: too short")

        physical = int.from_bytes(data[0:8], "big")
        logical = int.from_bytes(data[8:12], "big")
        node_id = data[12:].decode("utf-8")

        return cls(
            physical=physical,
            logical=logical,
            node_id=node_id
        )

    @staticmethod
    def now_millis() -> int:
        """Return current system time in milliseconds."""
        return int(time.time() * 1000)

    @classmethod
    def initial(cls, node_id: str) -> HLC:
        """
        Create an initial HLC for a node_id.

        The physical time is set to the current system time,
        and the logical counter starts at zero.
        """
        return cls(physical=cls.now_millis(), logical=0, node_id=node_id)

    def tick_local(self, now_ms: int | None = None) -> HLC:
        """
        Advance the clock for a local event (e.g., a write).

        Rules:
        - physical_local = max(now, self.physical)
        - if now > self.physical: logical = 0
        - else: logical = self.logical + 1

        This ensures monotonicity even if the system clock moves backward.
        """
        if now_ms is None:
            now_ms = self.now_millis()

        if now_ms > self.physical:
            return HLC(physical=now_ms, logical=0, node_id=self.node_id)
        else:
            return HLC(
                physical=self.physical,
                logical=self.logical + 1,
                node_id=self.node_id,
            )

    def tick_on_receive(self, remote: HLC, now_ms: int | None = None) -> HLC:
        """
        Advance the clock when receiving a remote HLC.

        Standard HLC merge rules:
        - pt = max(self.physical, remote.physical, now)
        - lt depends on which physical time dominates:
            * if all equal: max(logicals) + 1
            * if local dominates: local.logical + 1
            * if remote dominates: remote.logical + 1
            * if now dominates: logical = 0

        This ensures a total order across node_ids.
        """
        if now_ms is None:
            now_ms = self.now_millis()

        pt = max(self.physical, remote.physical, now_ms)

        if pt == self.physical and pt == remote.physical:
            lt = max(self.logical, remote.logical) + 1
        elif pt == self.physical and pt > remote.physical:
            lt = self.logical + 1
        elif pt == remote.physical and pt > self.physical:
            lt = remote.logical + 1
        else:
            lt = 0

        return HLC(physical=pt, logical=lt, node_id=self.node_id)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON‑serializable representation of the HLC."""
        return {
            "physical": self.physical,
            "logical": self.logical,
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HLC:
        """Reconstruct an HLC from a serialized dictionary."""
        return cls(
            physical=int(data["physical"]),
            logical=int(data["logical"]),
            node_id=str(data["node_id"]),
        )


class ConflictResolver(Protocol):
    def resolve(self, versions: Iterable[HLC]) -> HLC | None:
        ...


class LWWConflictResolver:
    """
    Last‑Writer‑Wins (LWW) conflict resolver based on HLC.

    Invariant:
        Given a set of concurrent versions, all nodes must deterministically
        choose the same "winning" version.

    Rule:
        - The version with the largest HLC wins.
        - HLC provides a total order, so ties are impossible unless the
          timestamps are identical down to node_id (extremely rare).
    """

    @staticmethod
    def resolve(versions: Iterable[HLC]) -> HLC | None:
        """
        Resolve a set of concurrent versions into a single version.

        Parameters:
            versions: iterable of ValueVersion objects (from quorum reads,
                      anti‑entropy sync, or gossip repair)

        Returns:
            The LWW winner, or None if the set is empty.
        """
        iterator = iter(versions)
        try:
            best = next(iterator)
        except StopIteration:
            return None

        for candidate in iterator:
            if candidate > best:
                best = candidate

        return best
