import pytest
from paravon.core.space.hashspace import HashSpace


@pytest.mark.ut
def test_hash_is_deterministic():
    v = b"hello"
    assert HashSpace.hash(v) == HashSpace.hash(v)


@pytest.mark.ut
def test_hash_output_is_128_bits():
    h = HashSpace.hash(b"abc")
    assert 0 <= h < HashSpace.MAX
    assert h.bit_length() <= 128


@pytest.mark.ut
def test_hash_distinguishes_simple_inputs():
    # Not a cryptographic test, just ensuring no trivial collisions
    assert HashSpace.hash(b"a") != HashSpace.hash(b"b")
    assert HashSpace.hash(b"abc") != HashSpace.hash(b"abcd")


@pytest.mark.ut
def test_add_without_wrap():
    assert HashSpace.add(10, 5) == 15


@pytest.mark.ut
def test_add_with_wrap():
    x = HashSpace.MAX - 3
    assert HashSpace.add(x, 10) == 7  # wrap-around modulo 2^128


@pytest.mark.ut
def test_add_zero():
    assert HashSpace.add(123456, 0) == 123456


@pytest.mark.ut
def test_interval_normal_inside():
    assert HashSpace.in_interval(5, 3, 10) is True


@pytest.mark.ut
def test_interval_normal_outside():
    assert HashSpace.in_interval(2, 3, 10) is False


@pytest.mark.ut
def test_interval_wrapped_inside():
    assert HashSpace.in_interval(12, 10, 3) is True
    assert HashSpace.in_interval(1, 10, 3) is True


@pytest.mark.ut
def test_interval_wrapped_outside():
    assert HashSpace.in_interval(5, 10, 3) is False


@pytest.mark.ut
def test_interval_edge_cases():
    assert HashSpace.in_interval(10, 3, 10) is True
    assert HashSpace.in_interval(3, 3, 10) is False


@pytest.mark.ut
def test_token_is_deterministic():
    t1 = HashSpace.token("node-1", 0)
    t2 = HashSpace.token("node-1", 0)
    assert t1 == t2


@pytest.mark.ut
def test_token_changes_with_index():
    t1 = HashSpace.token("node-1", 0)
    t2 = HashSpace.token("node-1", 1)
    assert t1 != t2


@pytest.mark.ut
def test_token_changes_with_label():
    t1 = HashSpace.token("node-1", 0)
    t2 = HashSpace.token("node-2", 0)
    assert t1 != t2


@pytest.mark.ut
def test_random_token_is_in_range():
    t = HashSpace.random_token("node-1")
    assert 0 <= t < HashSpace.MAX


@pytest.mark.ut
def test_random_token_is_not_deterministic():
    t1 = HashSpace.random_token("node-1")
    t2 = HashSpace.random_token("node-1")
    assert t1 != t2


@pytest.mark.ut
def test_generate_tokens_count():
    tokens = list(HashSpace.generate_tokens("node-1", 5))
    assert len(tokens) == 5


@pytest.mark.ut
def test_generate_tokens_are_deterministic():
    t1 = list(HashSpace.generate_tokens("node-1", 5))
    t2 = list(HashSpace.generate_tokens("node-1", 5))
    assert t1 == t2


@pytest.mark.ut
def test_generate_tokens_are_unique():
    tokens = list(HashSpace.generate_tokens("node-1", 100))
    assert len(tokens) == len(set(tokens))
