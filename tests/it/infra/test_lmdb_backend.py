import pytest

from paravon.core.storage.codec import KeyCodec
from paravon.infra.lmdb_storage.backend import LMDBBackend


def setup_backend(tmp_path):
    backend = LMDBBackend(path=str(tmp_path), map_size=1 << 16)
    ks = b"a"
    return backend, ks


def insert(backend, ks, items):
    for k, v in items:
        backend.put(ks, k, v)


@pytest.mark.it
def test_basic_scan(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"a", b"1"),
        (b"c", b"3"),
        (b"d", b"4"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks)
    assert out == items


@pytest.mark.it
def test_scan_empty_db(tmp_path):
    backend, ks = setup_backend(tmp_path)
    out = backend.scan(ks)
    assert out == []


@pytest.mark.it
def test_scan_with_start(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"bar1", b"1"), (b"bar2", b"2"), (b"bar_a", b"3"),
        (b"foo1", b"1"), (b"foo2", b"2"), (b"foo_z", b"4"), (b"foo_a", b"3"),
        (b"toto1", b"1"), (b"toto2", b"2"), (b"toto_a", b"3"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, start=b"foo")
    expected = [
        (b"foo1", b"1"),
        (b"foo2", b"2"),
        (b"foo_a", b"3"),
        (b"foo_z", b"4"),
        (b"toto1", b"1"),
        (b"toto2", b"2"),
        (b"toto_a", b"3")
    ]
    assert out == expected

    # empty prefix
    out = backend.scan(ks, start=b"")
    assert out == sorted(items)


@pytest.mark.it
def test_scan_with_start_empty_db(tmp_path):
    backend, ks = setup_backend(tmp_path)
    out = backend.scan(ks, start=b"foo")
    assert out == []


@pytest.mark.it
def test_scan_start_after_all_forward(tmp_path):
    backend, ks = setup_backend(tmp_path)
    insert(backend, ks, [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")])

    out = backend.scan(ks, start=b"z")
    assert out == []


@pytest.mark.it
def test_scan_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"a", b"1"),
        (b"c", b"3"),
        (b"d", b"4"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, reverse=True)
    assert out == sorted(items, reverse=True)


@pytest.mark.it
def test_scan_reverse_empty_db(tmp_path):
    backend, ks = setup_backend(tmp_path)
    out = backend.scan(ks, reverse=True)
    assert out == []


@pytest.mark.it
def test_scan_with_start_and_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"bar1", b"1"), (b"bar2", b"2"), (b"bar_a", b"3"),
        (b"foo1", b"1"), (b"foo2", b"2"), (b"foo_z", b"4"), (b"foo_a", b"3"),
        (b"toto1", b"1"), (b"toto2", b"2"), (b"toto_a", b"3"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, start=b"foo", reverse=True)
    expected = [
        (b"foo_z", b"4"),
        (b"foo_a", b"3"),
        (b"foo2", b"2"),
        (b"foo1", b"1"),
        (b"bar_a", b"3"),
        (b"bar2", b"2"),
        (b"bar1", b"1")
    ]
    assert out == expected

    # empty prefix
    out = backend.scan(ks, start=b"", reverse=True)
    assert out == sorted(items, reverse=True)


@pytest.mark.it
def test_scan_start_after_all_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")]
    insert(backend, ks, items)

    out = backend.scan(ks, start=b"z", reverse=True)
    assert out == sorted(items, reverse=True)


@pytest.mark.it
def test_scan_start_before_all_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    insert(backend, ks, [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")])

    out = backend.scan(ks, start=b"\x00", reverse=True)
    assert out == []


@pytest.mark.it
def test_scan_start_between_forward(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")]
    insert(backend, ks, items)

    out = backend.scan(ks, start=b"b")
    assert out == [(b"c", b"3"), (b"d", b"4")]


@pytest.mark.it
def test_scan_start_between_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")]
    insert(backend, ks, items)

    out = backend.scan(ks, start=b"b", reverse=True)
    assert out == [(b"a", b"1")]


@pytest.mark.it
def test_scan_limit_forward(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")]
    insert(backend, ks, items)

    out = backend.scan(ks, limit=2)
    assert out == items[:2]


@pytest.mark.it
def test_scan_limit_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"a", b"1"), (b"c", b"3"), (b"d", b"4")]
    insert(backend, ks, items)

    out = backend.scan(ks, reverse=True, limit=2)
    assert out == sorted(items, reverse=True)[:2]


@pytest.mark.it
def test_pagination_forward(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ]
    insert(backend, ks, items)

    page_size = 2

    # Page 1
    p1 = backend.scan(ks, start=None, limit=page_size)
    assert p1 == items[:2]

    # Page 2
    last_key_p1 = p1[-1][0]
    start_p1 = KeyCodec.increment_key(last_key_p1)
    p2 = backend.scan(ks, start=start_p1, limit=page_size)
    assert p2 == items[2:4]

    # Page 3
    last_key_p2 = p2[-1][0]
    start_p2 = KeyCodec.increment_key(last_key_p2)
    p3 = backend.scan(ks, start=start_p2, limit=page_size)
    assert p3 == items[4:]


@pytest.mark.it
def test_pagination_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ]
    insert(backend, ks, items)

    page_size = 2
    rev_items = list(reversed(items))

    # Page 1
    p1 = backend.scan(ks, start=None, limit=page_size, reverse=True)
    assert p1 == rev_items[:2]   # [e, d]

    # Page 2
    last_key_p1 = p1[-1][0]      # d
    start_p1 = KeyCodec.decrement_key(last_key_p1)
    p2 = backend.scan(ks, start=start_p1, limit=page_size, reverse=True)
    assert p2 == rev_items[2:4]  # [c, b]

    # Page 3
    last_key_p2 = p2[-1][0]      # b
    start_p2 = KeyCodec.decrement_key(last_key_p2)
    p3 = backend.scan(ks, start=start_p2, limit=page_size, reverse=True)
    assert p3 == rev_items[4:]   # [a]


@pytest.mark.it
def test_get_latest_version(tmp_path):
    backend, ks = setup_backend(tmp_path)

    items = [
        (b"a1", b"v1"),
        (b"a2", b"v2"),
        (b"a3", b"v3"),
        (b"b1", b"w1"),
        (b"b2", b"w2"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, start=b"a", reverse=True, limit=1)

    assert out == [(b"a3", b"v3")]


@pytest.mark.it
def test_scan_prefix_basic(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
        (b"order:1", b"10"),
        (b"order:2", b"20"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"user:")
    assert out == [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
    ]


@pytest.mark.it
def test_scan_prefix_with_start_forward(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
        (b"user:4", b"4"),
        (b"zzz", b"999"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"user:", start=b"user:2")
    assert out == [
        (b"user:2", b"2"),
        (b"user:3", b"3"),
        (b"user:4", b"4"),
    ]


@pytest.mark.it
def test_scan_prefix_with_start_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
        (b"user:4", b"4"),
        (b"zzz", b"999"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"user:", start=b"user:3", reverse=True)
    assert out == [
        (b"user:3", b"3"),
        (b"user:2", b"2"),
        (b"user:1", b"1"),
    ]


@pytest.mark.it
def test_scan_prefix_limit(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"user:", limit=2)
    assert out == items[:2]


@pytest.mark.it
def test_scan_prefix_pagination_forward(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
        (b"user:4", b"4"),
    ]
    insert(backend, ks, items)

    # Page 1
    p1 = backend.scan(ks, prefix=b"user:", start=None, limit=2)
    assert p1 == items[:2]

    # Page 2
    start2 = KeyCodec.increment_key(p1[-1][0])
    p2 = backend.scan(ks, prefix=b"user:", start=start2, limit=2)
    assert p2 == items[2:]


@pytest.mark.it
def test_scan_prefix_pagination_reverse(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
        (b"user:4", b"4"),
    ]
    insert(backend, ks, items)

    rev = list(reversed(items))

    # Page 1
    p1 = backend.scan(ks, prefix=b"user:", start=None, limit=2, reverse=True)
    assert p1 == rev[:2]

    # Page 2
    start2 = KeyCodec.decrement_key(p1[-1][0])
    p2 = backend.scan(ks, prefix=b"user:", start=start2, limit=2, reverse=True)
    assert p2 == rev[2:]


@pytest.mark.it
def test_scan_prefix_not_found(tmp_path):
    backend, ks = setup_backend(tmp_path)
    insert(backend, ks, [(b"user:1", b"1"), (b"user:2", b"2")])

    out = backend.scan(ks, prefix=b"order:")
    assert out == []


@pytest.mark.it
def test_scan_prefix_start_outside(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"user:1", b"1"), (b"user:2", b"2")]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"user:", start=b"zzz")
    assert out == []


@pytest.mark.it
def test_scan_prefix_empty(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"")
    assert out == items


@pytest.mark.it
def test_scan_prefix_start_equals_prefix(tmp_path):
    backend, ks = setup_backend(tmp_path)
    items = [
        (b"user:1", b"1"),
        (b"user:2", b"2"),
        (b"user:3", b"3"),
    ]
    insert(backend, ks, items)

    out = backend.scan(ks, prefix=b"user:", start=b"user:")
    assert out == items


