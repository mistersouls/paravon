import pytest
from paravon.core.throttling.backoff import ExponentialBackoff


@pytest.mark.ut
def test_initial_state():
    b = ExponentialBackoff(initial=0.5, factor=2.0, maximum=10.0, jitter=0)
    assert b._current == 0.5


@pytest.mark.ut
def test_next_delay_no_jitter():
    b = ExponentialBackoff(initial=1.0, factor=2.0, maximum=10.0, jitter=0)

    d1 = b.next_delay()
    assert d1 == 1.0
    assert b._current == 2.0

    d2 = b.next_delay()
    assert d2 == 2.0
    assert b._current == 4.0

    d3 = b.next_delay()
    assert d3 == 4.0
    assert b._current == 8.0


@pytest.mark.ut
def test_next_delay_with_jitter():
    b = ExponentialBackoff(initial=1.0, factor=2.0, maximum=10.0, jitter=1.0)

    d = b.next_delay()
    # delay = current + random.uniform(0, jitter)
    assert 1.0 <= d <= 2.0
    assert b._current == 2.0


@pytest.mark.ut
def test_maximum_cap():
    b = ExponentialBackoff(initial=5.0, factor=3.0, maximum=10.0, jitter=0)

    d1 = b.next_delay()
    assert d1 == 5.0
    assert b._current == 10.0  # capped

    d2 = b.next_delay()
    assert d2 == 10.0
    assert b._current == 10.0  # stays capped


@pytest.mark.ut
def test_reset():
    b = ExponentialBackoff(initial=1.0, factor=2.0, maximum=10.0, jitter=0)

    b.next_delay()  # 1 -> 2
    b.next_delay()  # 2 -> 4
    assert b._current == 4.0

    b.reset()
    assert b._current == 1.0


@pytest.mark.ut
def test_multiple_calls_progression():
    b = ExponentialBackoff(initial=0.5, factor=2.0, maximum=5.0, jitter=0)

    delays = [b.next_delay() for _ in range(5)]
    assert delays == [0.5, 1.0, 2.0, 4.0, 5.0]  # capped at 5.0
    assert b._current == 5.0
