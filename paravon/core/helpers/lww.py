from typing import Iterable

from paravon.core.models.version import ValueVersion


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
    def resolve(versions: Iterable[ValueVersion]) -> ValueVersion | None:
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
            if candidate.hlc > best.hlc:
                best = candidate

        return best
