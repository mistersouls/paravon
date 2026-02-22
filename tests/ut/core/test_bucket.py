import pytest
import zlib
import json

from paravon.core.gossip.bucket import Bucket
from tests.utils import make_member


@pytest.fixture
def bucket(serializer):
    return Bucket(bucket_id="3", serializer=serializer)


@pytest.mark.ut
def test_add_or_update_inserts_membership(bucket):
    m = make_member("node-1")
    bucket.add_or_update(m)

    assert "node-1" in bucket.memberships
    assert bucket.memberships["node-1"] is m
    assert bucket.dirty is True


@pytest.mark.ut
def test_add_or_update_overwrites_existing(bucket):
    m1 = make_member("node-1", epoch=1)
    m2 = make_member("node-1", epoch=5)

    bucket.add_or_update(m1)
    bucket.add_or_update(m2)

    assert bucket.memberships["node-1"].epoch == 5
    assert bucket.dirty is True


@pytest.mark.ut
def test_recompute_checksum_empty_bucket(bucket):
    bucket.recompute_checksum()
    assert bucket.checksum == 0
    assert bucket.dirty is False


@pytest.mark.ut
def test_recompute_checksum_non_empty(bucket, serializer):
    m1 = make_member("a")
    m2 = make_member("b")

    bucket.add_or_update(m1)
    bucket.add_or_update(m2)

    bucket.recompute_checksum()

    expected = 0
    for m in sorted([m1, m2], key=lambda mi: mi.node_id):
        raw = serializer.serialize(m.to_dict())
        expected = zlib.crc32(raw, expected)

    assert bucket.checksum == expected
    assert bucket.dirty is False


@pytest.mark.ut
def test_checksum_cached(bucket):
    m = make_member("node-1")
    bucket.add_or_update(m)

    c1 = bucket.get_checksum()
    c2 = bucket.get_checksum()

    assert c1 == c2
    assert bucket.dirty is False


@pytest.mark.ut
def test_checksum_recomputed_when_dirty(bucket):
    m1 = make_member("node-1")
    bucket.add_or_update(m1)

    c1 = bucket.get_checksum()

    # Modify bucket → dirty
    m2 = make_member("node-2")
    bucket.add_or_update(m2)

    c2 = bucket.get_checksum()

    assert c1 != c2
    assert bucket.dirty is False


@pytest.mark.ut
def test_checksum_deterministic_order(bucket, serializer):
    m1 = make_member("b")
    m2 = make_member("a")

    bucket.add_or_update(m1)
    bucket.add_or_update(m2)

    c1 = bucket.get_checksum()

    # Recreate bucket in different insertion order
    bucket2 = Bucket("3", serializer)
    bucket2.add_or_update(m2)
    bucket2.add_or_update(m1)

    c2 = bucket2.get_checksum()

    assert c1 == c2


@pytest.mark.ut
def test_serialize_memberships(bucket):
    m1 = make_member("node-1")
    m2 = make_member("node-2")

    bucket.add_or_update(m1)
    bucket.add_or_update(m2)

    serialized = bucket.serialize_memberships()

    assert serialized["node-1"] == m1.to_dict()
    assert serialized["node-2"] == m2.to_dict()
    assert len(serialized) == 2


@pytest.mark.ut
def test_serialize_memberships_empty(bucket):
    assert bucket.serialize_memberships() == {}
