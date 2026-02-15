import asyncio
import logging
import random

from paravon.core.gossip.table import BucketTable
from paravon.core.helpers.lock import RWLock
from paravon.core.models.membership import Membership, MembershipDiff
from paravon.core.ports.serializer import Serializer
from paravon.core.service.meta import NodeMetaManager
from paravon.core.space.ring import Ring
from paravon.core.space.vnode import VNode


class TopologyManager:
    _TOTAL_BUCKETS = 128
    _INCARNATION_DELTA = 5

    def __init__(
        self,
        meta_manager: NodeMetaManager,
        serializer: Serializer,
        # spawner: TaskSpawner
    ) -> None:
        self._meta_manager = meta_manager
        self._serializer = serializer
        self._table = BucketTable(
            total_buckets=self._TOTAL_BUCKETS,
            serializer=serializer,
            meta_manager=meta_manager,
            delta=self._INCARNATION_DELTA
        )
        self._ring = Ring()
        self._rwlock = RWLock()
        # self._spawner = spawner
        self._logger = logging.getLogger("core.service.topology")

    async def add_membership(self, membership: Membership) -> None:
        async with self._rwlock.write():
            await self._table.add_or_update(membership)
            vnodes = VNode.vnodes_for(membership.node_id, membership.tokens)
            self._ring = self._ring.add_vnodes(vnodes)

    async def apply_bucket(
        self,
        bucket_id: str,
        memberships: list[Membership]
    ) -> None:
        async with self._rwlock.write():
            diff = await self._table.merge_bucket(bucket_id, memberships)
            if diff.changed:
                await asyncio.to_thread(self._update_ring, diff)

    # async def apply_checksums(self, data: dict) -> Message:
    #     source = data["source"]
    #     remote_checksums = data["checksums"]
    #     local = self._table.get_checksums()
    #     for bucket_id, checksum in remote_checksums.items():
    #         if local.get(bucket_id) != checksum:
    #             m = {
    #                 "peer": source["peer_address"],
    #                 "bucket_id": bucket_id,
    #                 "checksum": checksum,
    #             }

    async def get_checksums(self):
        async with self._rwlock.read():
            return self._table.get_checksums()

    async def get_ring(self) -> Ring:
        async with self._rwlock.read():
            return self._ring

    async def pick_random_membership(self) -> Membership | None:
        """
        Return a uniformly random membership from the table.

        This uses the global `views` index to achieve O(1) uniform sampling
        across all known memberships, independent of bucket distribution.
        """
        async with self._rwlock.read():
            views = self._table.get_views()
            if not views:
                return None

            node_id = random.choice(list(views.keys()))
            bucket_id = self._table.bucket_for(node_id)
            return self._table.buckets[bucket_id].memberships[node_id]

    async def remove_membership(self, membership: Membership) -> None:
        async with self._rwlock.write():
            await self._table.remove(membership.node_id)
            self._ring = self._ring.drop_nodes({membership.node_id})

    async def restore(
        self,
        memberships: list[Membership],
        *,
        excludes: list[str] | None = None
    ) -> None:
        excludes = excludes or []
        table = BucketTable(
            total_buckets=self._TOTAL_BUCKETS,
            serializer=self._serializer,
            meta_manager=self._meta_manager,
            delta=self._INCARNATION_DELTA
        )
        vnodes = []
        async with self._rwlock.write():
            for membership in memberships:
                if membership.node_id not in excludes:
                    await table.add_or_update(membership)
                    vnodes.extend(VNode.vnodes_for(membership.node_id, membership.tokens))

            self._table = table
            self._ring = Ring(vnodes)

    def _update_ring(self, diff: MembershipDiff) -> None:
        ring = self._ring

        # 1. Remove nodes that disappeared
        removed_node_ids = {m.node_id for m in diff.removed}
        if removed_node_ids:
            ring = ring.drop_nodes(removed_node_ids)

        # 2. Apply updates (replace tokens)
        for change in diff.updated:
            before = change.before
            after = change.after

            # Remove old tokens
            ring = ring.drop_nodes({before.node_id})

            # Add new tokens
            ring = ring.add_vnodes(VNode.vnodes_for(after.node_id, after.tokens))

        # 3. Add new nodes
        for membership in diff.added:
            ring = ring.add_vnodes(
                VNode.vnodes_for(membership.node_id, membership.tokens)
            )

        self._ring = ring

        self._logger.info(
            f"Ring updated from bucket {diff.bucket_id}: "
            f"{len(diff.added)} added, "
            f"{len(diff.removed)} removed, "
            f"{len(diff.updated)} updated"
        )
