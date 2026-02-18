import bisect
from typing import Generator, Iterator

from paravon.core.space.vnode import VNode


class Ring:
    """
    Represents a consistent‑hashing ring composed of virtual nodes (vnodes).

    The ring maintains an ordered list of VNode instances, each defined by a
    (node_id, token) pair. The list is sorted in ascending order of vnode.token,
    forming a circular token space. This ordering enables efficient successor
    lookups and deterministic ownership assignment.

    Key properties:
        - The ring is immutable: operations such as adding or removing vnodes
          return a new Ring instance rather than mutating the existing one.
        - VNode ordering is based solely on the token value. The node_id is
          carried as metadata but does not influence ordering or placement.
        - Successor lookups use `bisect_right` with a key function that extracts
          vnode.token, providing O(log n) lookup performance.
        - The ring wraps around at the end of the token space, preserving the
          circular structure required by consistent hashing.

    This ring abstraction is used by the partitioner and replication subsystem
    to determine data placement, ownership transitions, and replica selection.
    """
    def __init__(self, vnodes: list[VNode] = None, *, _sorted: bool = False) -> None:
        vnodes = vnodes or []
        self._vnodes = vnodes if _sorted else sorted(vnodes, key=lambda v: v.token)

    def find_successor(self, token: int) -> VNode:
        """
        Return the vnode responsible for the given token.

        The lookup uses `bisect_right` with a key function that extracts the
        vnode's token. This allows us to search directly on the integer token
        space, without constructing a synthetic VNode or relying on tuple
        lexicographical ordering.

        Because the vnode list is sorted by vnode.token, the bisect operation
        returns the index of the first vnode whose token is strictly greater
        than the search token. If the token is greater than or equal to the
        last vnode's token, the search wraps around to index 0, preserving the
        circular nature of the ring.

        Complexity: O(log n)
        """
        idx = bisect.bisect_right(self._vnodes, token, key=lambda v: v.token)
        if idx == len(self._vnodes):
            idx = 0  # wrap-around
        return self._vnodes[idx]

    def add_vnodes(self, vnodes: list[VNode]) -> Ring:
        """
        Return a new Ring containing the existing vnodes plus the provided ones.

        The method sorts the new vnodes locally and merges them with the
        existing sorted list using a linear-time merge step. This is more
        efficient than inserting vnodes one by one.

        Complexity:
            - local sort: O(k log k)
            - merge: O(n + k)
        """
        if not vnodes:
            return self

        new_vnodes = sorted(vnodes, key=lambda v: v.token)
        sorted_vnodes = self._merge_sorted(self._vnodes, new_vnodes)
        return Ring(sorted_vnodes, _sorted=True)

    def drop_nodes(self, node_ids: set[str]) -> Ring:
        """
        Return a new Ring with all vnodes belonging to the given node_ids removed.

        This is typically used during node decommissioning or failure handling.
        The resulting ring preserves sorted order.
        """
        remaining = [v for v in self._vnodes if v.node_id not in node_ids]
        return Ring(remaining, _sorted=True)

    def iter_from(self, vnode: VNode) -> Generator[VNode, None, None]:
        """
        Yield vnodes in ring order starting from the given vnode.

        The iteration wraps around at the end of the vnode list, ensuring
        full traversal of the ring. This is used for successor walks,
        replication placement, and range scans.
        """
        start = self._vnodes.index(vnode)
        for i in range(len(self._vnodes)):
            yield self._vnodes[(start + i) % len(self._vnodes)]

    def preference_list(self, vnode: VNode, replication_factor: int) -> list[VNode]:
        """
        Return a list of distinct nodes for replication, starting from the successor.

        The method walks the ring in order and selects vnodes belonging to
        different physical nodes, skipping additional vnodes from the same node.
        This ensures that replicas are placed on distinct nodes.
        """

        result: list[VNode] = [vnode]
        seen = {vnode.node_id}

        for successor in self.iter_from(vnode):
            if successor.node_id not in seen:
                result.append(successor)
                seen.add(successor.node_id)
            if len(result) == replication_factor:
                break

        return result

    def __getitem__(self, i: int) -> VNode:
        return self._vnodes[i]

    def __len__(self) -> int:
        return len(self._vnodes)

    def __iter__(self) -> Iterator[VNode]:
        return iter(self._vnodes)

    @staticmethod
    def _merge_sorted(a: list[VNode], b: list[VNode]) -> list[VNode]:
        """
        Merge two sorted lists of (token, node_id).
        Equivalent to a manual merge step in merge sort.
        """
        i = j = 0
        merged: list[VNode] = []
        len_a, len_b = len(a), len(b)

        while i < len_a and j < len_b:
            if a[i].token <= b[j].token:
                merged.append(a[i])
                i += 1
            else:
                merged.append(b[j])
                j += 1

        # Append remaining items
        if i < len_a:
            merged.extend(a[i:])
        if j < len_b:
            merged.extend(b[j:])

        return merged
