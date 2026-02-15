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
    """
    The TopologyManager orchestrates the evolving shape of the cluster.

    It acts as the conductor of two complementary structures:
    - a BucketTable, which tracks membership and drives gossip convergence,
    - a consistent-hash Ring, which maps tokens to nodes.

    Together, they form the living topology of the system: nodes join,
    leave, update their tokens, and the manager ensures that every change
    is reflected coherently across the cluster.
    """
    _TOTAL_BUCKETS = 128
    _INCARNATION_DELTA = 5

    def __init__(
        self,
        meta_manager: NodeMetaManager,
        serializer: Serializer,
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
        self._logger = logging.getLogger("core.service.topology")

    async def add_membership(self, membership: Membership) -> None:
        """
        Register or update a membership and weave it into the ring.

        When a node announces itself—or refreshes its state—this method
        becomes the gateway through which it enters the cluster’s topology.
        The membership is first committed to the bucket table, ensuring
        it participates in gossip. Then, its virtual nodes are added to
        the ring, allowing it to take ownership of its share of the keyspace.
        """
        async with self._rwlock.write():
            await self._table.add_or_update(membership)
            vnodes = VNode.vnodes_for(membership.node_id, membership.tokens)
            self._ring = self._ring.add_vnodes(vnodes)

    async def apply_bucket(
        self,
        bucket_id: str,
        memberships: list[Membership]
    ) -> None:
        """
        Integrate a remote bucket snapshot into the local topology.

        During gossip exchanges, peers send partial views of their state.
        This method merges such a snapshot into the local bucket table,
        producing a diff that captures what changed. If the diff is not
        empty, the ring is updated accordingly—removing vanished nodes,
        refreshing updated ones, and adding newcomers. In essence, this
        method is where remote knowledge reshapes the local topology.
        """
        async with self._rwlock.write():
            diff = await self._table.merge_bucket(bucket_id, memberships)
            if diff.changed:
                await asyncio.to_thread(self._update_ring, diff)

    async def get_bucket_memberships(self, bucket_id: str) -> dict[str, Membership]:
        """
        Retrieve the raw membership objects stored in a specific bucket.

        This is a low-level view intended for inspection or debugging.
        It exposes the exact state of the bucket at the moment of the call,
        without serialization or transformation.
        """
        async with self._rwlock.read():
            memberships = self._table.buckets[bucket_id].memberships
            return memberships.copy()

    async def get_checksums(self) -> dict[str, int]:
        """
        Return the current checksum map of the bucket table.

        These checksums act as compact fingerprints of each bucket’s state.
        They allow gossip peers to quickly detect divergence and decide
        whether a deeper synchronization is needed.
        """
        async with self._rwlock.read():
            return self._table.get_checksums()

    async def get_ring(self) -> Ring:
        """
        Return the current consistent-hash ring.

        The ring is the backbone of data distribution. This method provides
        a snapshot of the structure that determines which node owns which
        portion of the keyspace.
        """
        async with self._rwlock.read():
            return self._ring

    async def pick_random_membership(self) -> Membership | None:
        """
        Select a random membership from the cluster.

        This method is often used by gossip protocols that rely on random
        peer sampling. It draws uniformly from the global membership index,
        ensuring every node has an equal chance of being selected.
        """
        async with self._rwlock.read():
            views = self._table.get_views()
            if not views:
                return None

            node_id = random.choice(list(views.keys()))
            bucket_id = self._table.bucket_for(node_id)
            return self._table.buckets[bucket_id].memberships[node_id]

    async def remove_membership(self, membership: Membership) -> None:
        """
        Remove a membership from both the bucket table and the ring.

        When a node leaves—or is deemed dead—this method ensures that it
        disappears cleanly from the topology. Its membership is removed
        from the table, and its virtual nodes are dropped from the ring,
        freeing its portion of the keyspace for redistribution.
        """
        async with self._rwlock.write():
            await self._table.remove(membership.node_id)
            self._ring = self._ring.drop_nodes({membership.node_id})

    async def restore(
        self,
        memberships: list[Membership],
        *,
        excludes: list[str] | None = None
    ) -> None:
        """
        Rebuild the entire topology from a list of memberships.

        This method is typically used during recovery or bootstrap. It
        constructs a fresh bucket table and ring from the provided
        memberships, skipping any nodes explicitly excluded. Once the
        reconstruction is complete, the manager atomically swaps in the
        new structures, effectively resetting the cluster’s topology.
        """
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
                    self._logger.debug(
                        f"Registering membership {membership.node_id}"
                    )
                    await table.add_or_update(membership)
                    vnodes.extend(
                        VNode.vnodes_for(membership.node_id, membership.tokens)
                    )

            self._table = table
            self._ring = Ring(vnodes)
            self._logger.info(f"Ring updated with {len(vnodes)} vnodes")

    def _update_ring(self, diff: MembershipDiff) -> None:
        """
        Apply a membership diff to the ring.

        This internal method translates the abstract changes captured in
        a MembershipDiff into concrete modifications of the consistent-hash
        ring. Nodes that disappeared are removed, updated nodes have their
        tokens replaced, and new nodes are woven into the ring. The result
        is a ring that faithfully reflects the latest cluster state.
        """
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
