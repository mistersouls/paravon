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

    async def add_or_update(self, membership: Membership) -> None:
        """
        Insert or update a membership originating from the local node.

        This operation bumps the local incarnation to ensure monotonicity,
        updates the appropriate bucket, and refreshes the global index.
        """
        await self._meta_manager.bump_incarnation()

        bucket_id = self.bucket_for(membership.node_id)
        bucket = self.buckets[bucket_id]
        bucket.add_or_update(membership)
        self._views[membership.node_id] = bucket_id
        self._mark_dirty()

    def bucket_for(self, peer_id: str) -> str:
        """
        Compute the bucket identifier for a given peer_id.

        The mapping is stable and uniform due to MD5 hashing. This ensures
        even distribution of memberships across buckets.
        """
        h = int(hashlib.md5(peer_id.encode()).hexdigest(), 16)
        return str(h % self._total_buckets)

    def compute_missing_buckets(self, remote_checksums: dict[str, int]) -> list[str]:
        """
        Compare local and remote checksums to identify divergent buckets.
        """
        local = self.get_checksums()
        return [
            bucket_id
            for bucket_id, remote_crc in remote_checksums.items()
            if local.get(bucket_id) != remote_crc
        ]

    def get_bucket_memberships(self, bucket_id: str) -> dict[str, dict]:
        """
        Serialize all memberships stored in a given bucket.
        """
        return self.buckets[bucket_id].serialize_memberships()

    def get_checksums(self) -> dict[str, int]:
        """
        Compute or return cached checksums for all buckets.

        The checksum vector is used during gossip to detect which buckets
        differ between peers. Empty buckets produce a stable checksum.
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
        return self._views

    async def merge_bucket(
        self,
        bucket_id: str,
        memberships: list[Membership]
    ) -> MembershipDiff:
        """
        Merge a remote bucket into the local table.

        The merge is monotonic and conflict-free:
        - newer epochs replace older ones
        - logically expired memberships are ignored
        - removed memberships are purged only after TTL expiration
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
        Remove a membership originating from the local node.

        This marks the membership as deleted, bumps the local incarnation,
        and removes it from both the bucket and the global index.
        """
        bucket_id = self.bucket_for(node_id)
        bucket = self.buckets[bucket_id]

        if node_id in bucket.memberships:
            await self._meta_manager.bump_incarnation()
            del bucket.memberships[node_id]
            del self._views[node_id]
            bucket.dirty = True
            self._mark_dirty()

    async def _sync_incarnation(self, memberships: list[Membership]) -> None:
        greater_membership = max(memberships, key=lambda m: m.incarnation)
        local = await self._meta_manager.get_membership()
        if greater_membership.incarnation > local.incarnation:
            await self._meta_manager.set_incarnation(greater_membership.incarnation)

    async def _current_incarnation(self) -> int:
        membership = await self._meta_manager.get_membership()
        return membership.incarnation

    async def _logically_expired(self, membership: Membership) -> bool:
        """
        Determine whether a membership is logically expired.

        A membership is expired if the local incarnation exceeds its
        incarnation by more than the configured delta.
        """
        local_inc = await self._current_incarnation()
        return local_inc > membership.incarnation + self._delta

    def _mark_dirty(self) -> None:
        self._dirty_global = True

    async def _upsert_bucket(
        self,
        bucket: Bucket,
        memberships: list[Membership]
    ) -> tuple[list[Membership], list[MembershipChange]]:
        added = []
        updated: list[MembershipChange] = []

        for m in memberships:
            if await self._logically_expired(m):
                continue

            local = bucket.memberships.get(m.node_id)

            if local is None:
                bucket.add_or_update(m)
                self._views[m.node_id] = bucket.bucket_id
                added.append(m)
                continue

            if m.epoch > local.epoch:
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
                del bucket.memberships[node_id]
                del self._views[node_id]
                removed.append(local)

        return removed
