import pytest

from paravon.core.space.ring import Ring
from paravon.core.space.vnode import VNode


@pytest.mark.ut
def test_ring_initial_sorting():
    vnodes = [
        VNode(node_id="B", token=200),
        VNode(node_id="A", token=100),
        VNode(node_id="C", token=300),
    ]
    ring = Ring(vnodes)

    assert [v.token for v in ring] == [100, 200, 300]
    assert [v.node_id for v in ring] == ["A", "B", "C"]


@pytest.mark.ut
def test_find_successor_basic():
    vnodes = [
        VNode(node_id="A", token=100),
        VNode(node_id="B", token=200),
        VNode(node_id="C", token=300),
    ]
    ring = Ring(vnodes)

    assert ring.find_successor(50).node_id == "A"
    assert ring.find_successor(100).node_id == "B"
    assert ring.find_successor(150).node_id == "B"
    assert ring.find_successor(250).node_id == "C"


@pytest.mark.ut
def test_find_successor_wrap_around():
    vnodes = [
        VNode(node_id="A", token=100),
        VNode(node_id="B", token=200),
        VNode(node_id="C", token=300),
    ]
    ring = Ring(vnodes)

    # Token beyond the last vnode should wrap to the first
    assert ring.find_successor(400).node_id == "A"


@pytest.mark.ut
def test_add_vnodes_returns_new_ring_and_preserves_order():
    base = Ring([
        VNode(node_id="A", token=100),
        VNode(node_id="C", token=300),
    ])

    new = base.add_vnodes([
        VNode(node_id="B", token=200),
        VNode(node_id="D", token=400),
    ])

    # Immutability: original ring unchanged
    assert [v.node_id for v in base] == ["A", "C"]

    # New ring is correctly merged and sorted
    assert [v.token for v in new] == [100, 200, 300, 400]
    assert [v.node_id for v in new] == ["A", "B", "C", "D"]


@pytest.mark.ut
def test_drop_nodes_removes_all_vnodes_for_given_node_ids():
    vnodes = [
        VNode(node_id="A", token=100),
        VNode(node_id="B", token=200),
        VNode(node_id="A", token=300),
        VNode(node_id="C", token=400),
    ]
    ring = Ring(vnodes)

    new = ring.drop_nodes({"A", "C"})

    assert [v.node_id for v in new] == ["B"]
    assert [v.token for v in new] == [200]


@pytest.mark.ut
def test_iter_from_wraps_around():
    vnodes = [
        VNode(node_id="A", token=100),
        VNode(node_id="B", token=200),
        VNode(node_id="C", token=300),
    ]
    ring = Ring(vnodes)

    start = vnodes[1]  # B
    order = [v.node_id for v in ring.iter_from(start)]

    assert order == ["B", "C", "A"]


@pytest.mark.ut
def test_preference_list_distinct_nodes_and_respects_replication_factor():
    vnodes = [
        VNode(node_id="A", token=100),
        VNode(node_id="A", token=150),
        VNode(node_id="B", token=200),
        VNode(node_id="C", token=300),
    ]
    ring = Ring(vnodes)

    start = vnodes[0]  # A@100
    prefs = ring.preference_list(start, replication_factor=3)

    # Should contain 3 vnodes on distinct node_ids
    assert len(prefs) == 3
    assert {v.node_id for v in prefs} == {"A", "B", "C"}


@pytest.mark.ut
def test_len_and_getitem():
    vnodes = [
        VNode(node_id="A", token=100),
        VNode(node_id="B", token=200),
    ]
    ring = Ring(vnodes)

    assert len(ring) == 2
    assert ring[0].node_id == "A"
    assert ring[1].node_id == "B"


@pytest.mark.ut
def test_merge_sorted_maintains_order():
    a = [
        VNode(node_id="A", token=100),
        VNode(node_id="C", token=300),
    ]
    b = [
        VNode(node_id="B", token=200),
        VNode(node_id="D", token=400),
    ]

    merged = Ring._merge_sorted(a, b)

    assert [v.token for v in merged] == [100, 200, 300, 400]
    assert [v.node_id for v in merged] == ["A", "B", "C", "D"]
