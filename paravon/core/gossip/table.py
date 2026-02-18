import hashlib

from paravon.core.gossip.bucket import Bucket
from paravon.core.models.membership import Membership, MembershipChange, MembershipDiff
from paravon.core.ports.serializer import Serializer
from paravon.core.service.meta import NodeMetaManager


class BucketTable:
    """
    A partitioned membership table used for gossip-based state synchronization.

    The table is divided into a fixed number of buckets. Each membership is
    deterministically assigned to a bucket based on its node_id hash. Buckets
    maintain their own membership set and checksum, allowing efficient
    detection of divergence and incremental synchronization.

    The table also maintains a lightweight global index mapping
    node_id → bucket_id to support O(1) uniform random sampling of members.
    """
    def __init__(
        self,
        total_buckets: int,
        serializer: Serializer,
        meta_manager: NodeMetaManager,
        delta: int,
    ) -> None:
        self._total_buckets = total_buckets
        self._meta_manager = meta_manager
        self._delta = delta

        self.buckets: dict[str, Bucket] = {
            str(i): Bucket(str(i), serializer)
            for i in range(total_buckets)
        }

        # Global index: node_id: bucket_id
        self._views: dict[str, str] = {}

        # Cached checksums for incremental gossip
        self._checksums_cache: dict[str, int] | None = None
        self._dirty_global: bool = True

        self._max_inc = 0

    async def add_or_update(self, membership: Membership) -> None:
        """
        Insert or refresh a membership in the table.

        This method is the entry point for local updates: when a node
        learns something new about itself or another peer, the table
        absorbs that information, updates the appropriate bucket, and
        ensures that incarnation numbers remain consistent. Any change
        marks the global state as "dirty", signaling that checksums
        must be recomputed before the next gossip round.
        """
        self._track_incarnation(membership.incarnation)
        await self._ensure_incarnation()
        bucket_id = self.bucket_for(membership.node_id)
        bucket = self.buckets[bucket_id]
        bucket.add_or_update(membership)
        self._views[membership.node_id] = bucket_id
        self._mark_dirty()

    def bucket_for(self, peer_id: str) -> str:
        """
        Determine which bucket a given peer belongs to.

        The assignment is fully deterministic: hashing the peer_id
        produces a stable, uniform distribution across buckets. This
        ensures that the table remains balanced even as the cluster
        grows or shrinks.
        """
        h = int(hashlib.md5(peer_id.encode()).hexdigest(), 16)
        return str(h % self._total_buckets)

    def get_bucket_memberships(self, bucket_id: str) -> dict[str, dict]:
        """
        Retrieve the serialized view of a bucket’s memberships.

        This is typically used when preparing gossip payloads: the
        caller receives a compact, serializer-friendly representation
        of all known members in the specified bucket.
        """
        return self.buckets[bucket_id].serialize_memberships()

    def get_checksums(self) -> dict[str, int]:
        """
        Return the checksum of each bucket, recomputing them only when needed.

        The table keeps a cached snapshot of bucket checksums. As long
        as no bucket has changed, the cached version is reused. When
        the table is marked dirty, all checksums are recomputed to
        reflect the latest state before being cached again.
        """
        if not self._dirty_global and self._checksums_cache is not None:
            return self._checksums_cache

        checksums = {}
        for i in range(self._total_buckets):
            idx = str(i)
            checksums[idx] = self.buckets[idx].get_checksum()

        self._checksums_cache = checksums
        self._dirty_global = False
        return checksums

    def get_views(self) -> dict[str, str]:
        """
        Expose the global index mapping node_id → bucket_id.

        This index acts as a fast lookup table that allows the system
        to quickly locate any membership and to perform uniform random
        sampling across the entire cluster without scanning all buckets.
        """
        return self._views

    async def merge_bucket(
        self,
        bucket_id: str,
        memberships: list[Membership]
    ) -> MembershipDiff:
        """
        Merge a remote bucket into the local one and compute the resulting diff.

        During gossip, peers exchange bucket-level snapshots. This method
        takes the remote snapshot, aligns incarnation numbers, integrates
        new or updated memberships, and purges entries that have logically
        expired. The returned diff captures exactly what changed, allowing
        higher layers to react or propagate updates efficiently.
        """
        bucket = self.buckets[bucket_id]

        if memberships:
            await self._sync_incarnation(memberships)

        added, updated = await self._upsert_bucket(bucket, memberships)
        removed = await self._purge_bucket(bucket, memberships)
        diff = MembershipDiff(
            added=added,
            updated=updated,
            removed=removed,
            bucket_id=bucket_id
        )

        if diff.changed:
            bucket.dirty = True
            self._mark_dirty()

        return diff

    async def remove(self, node_id: str) -> None:
        """
        Remove a membership from the table, if it exists.

        This method is used when a node transitions into a removal phase
        or when the system decides that a membership should no longer be
        tracked. The removal is applied to the appropriate bucket and the
        global index is updated accordingly.
        """
        bucket_id = self.bucket_for(node_id)
        bucket = self.buckets[bucket_id]

        if node_id in bucket.memberships:
            await self._ensure_incarnation()
            self._delete_membership(bucket, node_id)
            bucket.dirty = True
            self._mark_dirty()

    def _delete_membership(self, bucket: Bucket, node_id: str) -> Membership | None:
        local = bucket.memberships.pop(node_id, None)
        if local is not None:
            self._views.pop(node_id, None)
        return local

    async def _ensure_incarnation(self) -> None:
        local = await self._meta_manager.get_membership()
        if local.is_remove_phase():
            return

        target = max(local.incarnation, self._max_inc) + 1
        await self._meta_manager.set_incarnation(target)
        await self._refresh_local_membership()
        self._max_inc = target

    async def _logically_expired(self, membership: Membership) -> bool:
        """
        Determine whether a membership is logically expired.

        A membership is expired if the local incarnation exceeds its
        incarnation by more than the configured delta.
        """
        local = await self._meta_manager.get_membership()
        return local.incarnation > membership.incarnation + self._delta

    def _mark_dirty(self) -> None:
        self._dirty_global = True

    async def _refresh_local_membership(self):
        local = await self._meta_manager.get_membership()
        bucket_id = self.bucket_for(local.node_id)
        bucket = self.buckets[bucket_id]

        if local.node_id in bucket.memberships:
            bucket.add_or_update(local)
            self._views[local.node_id] = bucket_id
            self._mark_dirty()

    async def _sync_incarnation(self, memberships: list[Membership]) -> None:
        local = await self._meta_manager.get_membership()
        if local.is_remove_phase():
            return

        max_remote = max(memberships, key=lambda m: m.incarnation).incarnation
        self._track_incarnation(max_remote)
        if max_remote > local.incarnation:
            await self._meta_manager.set_incarnation(max_remote)
            await self._refresh_local_membership()

    def _track_incarnation(self, inc: int) -> None:
        if inc > self._max_inc:
            self._max_inc = inc

    async def _upsert_bucket(
        self,
        bucket: Bucket,
        memberships: list[Membership]
    ) -> tuple[list[Membership], list[MembershipChange]]:
        added = []
        updated: list[MembershipChange] = []

        for m in memberships:
            self._track_incarnation(m.incarnation)

            if await self._logically_expired(m):
                if m.is_remove_phase():
                    await self.remove(m.node_id)
                continue

            local = bucket.memberships.get(m.node_id)

            if local is None:
                bucket.add_or_update(m)
                self._views[m.node_id] = bucket.bucket_id
                added.append(m)
                continue

            if m.is_newer_than(local):
                bucket.add_or_update(m)
                self._views[m.node_id] = bucket.bucket_id
                updated.append(MembershipChange(before=local, after=m))

        return added, updated

    async def _purge_bucket(
        self,
        bucket: Bucket,
        memberships: list[Membership]
    ) -> list[Membership]:
        remote_ids = {m.node_id for m in memberships}
        local_ids = set(bucket.memberships.keys())
        removed = []

        for node_id in local_ids - remote_ids:
            local = bucket.memberships[node_id]

            if not local.is_remove_phase():
                continue

            if await self._logically_expired(local):
                deleted = self._delete_membership(bucket, node_id)
                if deleted is not None:
                    removed.append(deleted)

        return removed
