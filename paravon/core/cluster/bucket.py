import zlib
from paravon.core.models.membership import Membership
from paravon.core.ports.serializer import Serializer


class Bucket:
    """
    A container for memberships belonging to a specific partition of the
    membership table.

    Each bucket maintains:
    - a mapping of node_id → Membership
    - a checksum representing the serialized state of the bucket
    - a dirty flag indicating whether the checksum must be recomputed

    Buckets are designed to be lightweight and deterministic. They do not
    implement any merge logic themselves; all conflict resolution is handled
    by the BucketTable.
    """

    def __init__(self, bucket_id: str, serializer: Serializer) -> None:
        self.bucket_id = bucket_id
        self._serializer = serializer
        self.memberships: dict[str, Membership] = {}
        self.checksum: int = 0
        self.dirty: bool = True

    def add_or_update(self, membership: Membership) -> None:
        """
        Insert or update a membership in the bucket.

        This operation marks the bucket as dirty so that the checksum will
        be recomputed on the next call to `get_checksum()`.
        """
        self.memberships[membership.node_id] = membership
        self.dirty = True

    def get_checksum(self) -> int:
        """
        Return the current checksum of the bucket.

        If the bucket is marked as dirty, the checksum is recomputed before
        being returned. Otherwise, the cached value is used.
        """
        if self.dirty:
            self.recompute_checksum()
        return self.checksum

    def recompute_checksum(self) -> None:
        """
        Recompute the checksum of the bucket.

        The checksum is computed by:
        - sorting memberships by node_id for deterministic ordering
        - serializing each membership
        - feeding the serialized bytes into a CRC32 accumulator

        This method updates the stored checksum and clears the dirty flag.
        """
        items = sorted(self.memberships.values(), key=lambda m: m.node_id)

        crc = 0
        for membership in items:
            raw = self._serializer.serialize(membership.to_dict())
            crc = zlib.crc32(raw, crc)

        self.checksum = crc
        self.dirty = False

    def serialize_memberships(self) -> dict[str, dict]:
        """
        Serialize all memberships stored in the bucket.

        Returns
        -------
        dict[str, dict]
            Mapping node_id → serialized membership dictionary.
        """
        return {
            node_id: membership.to_dict()
            for node_id, membership in self.memberships.items()
        }
