import pytest
from unittest.mock import patch

from paravon.core.throttling.cubic import CubicRateController


def make_controller(
    base=1.0,
    min_rate=0.1,
    max_rate=10.0,
    constant=0.02,
    beta=0.7
):
    return CubicRateController(
        base_rate=base,
        min_rate=min_rate,
        max_rate=max_rate,
        constant=constant,
        beta=beta,
    )


@pytest.mark.ut
def test_initial_rate():
    c = make_controller(base=2.0)
    assert c.rate == 2.0
    assert c._w_max == 2.0
    assert c._t_loss is None


@pytest.mark.ut
def test_on_error_backoff():
    c = make_controller(base=5.0, min_rate=1.0, beta=0.5)

    with patch("time.monotonic", return_value=100.0):
        c.on_error()

    # multiplicative decrease
    assert c.rate == 2.5
    # w_max updated
    assert c._w_max == 5.0
    # t_loss set
    assert c._t_loss == 100.0
    # K recomputed
    assert c._k > 0


@pytest.mark.ut
def test_on_error_clamped_to_min():
    c = make_controller(base=0.2, min_rate=0.1, beta=0.1)

    with patch("time.monotonic", return_value=50.0):
        c.on_error()

    assert c.rate == 0.1  # clamped to min_rate


@pytest.mark.ut
def test_on_success_linear_before_loss():
    c = make_controller(base=1.0, max_rate=10.0)

    # No loss yet -> linear increase (rate * 1.05)
    c.on_success()
    assert pytest.approx(c.rate, rel=1e-6) == 1.05

    c.on_success()
    assert pytest.approx(c.rate, rel=1e-6) == 1.1025


@pytest.mark.ut
def test_on_success_linear_clamped_to_max():
    c = make_controller(base=9.9, max_rate=10.0)

    c.on_success()
    assert c.rate == 10.0  # clamped


@pytest.mark.ut
def test_on_success_cubic_growth():
    c = make_controller(base=5.0, max_rate=10.0)

    # Simulate a loss at t=100
    with patch("time.monotonic", return_value=100.0):
        c.on_error()

    # Now simulate time passing
    with patch("time.monotonic", return_value=101.0):
        c.on_success()

    # CUBIC formula:
    # w = C * (t - K)^3 + Wmax
    t = 1.0  # 101 - 100
    w = c._C * (t - c._k) ** 3 + c._w_max

    assert pytest.approx(c.rate, rel=1e-6) == w


@pytest.mark.ut
def test_on_success_cubic_clamped_to_bounds():
    c = make_controller(base=5.0, min_rate=1.0, max_rate=6.0)

    # Loss at t=0
    with patch("time.monotonic", return_value=0.0):
        c.on_error()

    # Large time -> cubic growth would exceed max
    with patch("time.monotonic", return_value=999.0):
        c.on_success()

    assert c.rate == 6.0  # clamped to max


@pytest.mark.ut
def test_multiple_success_then_error_cycle():
    c = make_controller(base=1.0, max_rate=10.0)

    # Grow linearly
    for _ in range(5):
        c.on_success()

    assert c.rate > 1.0

    # Trigger loss
    with patch("time.monotonic", return_value=50.0):
        c.on_error()

    assert c.rate < c._w_max  # multiplicative decrease

    # Grow again via cubic
    with patch("time.monotonic", return_value=51.0):
        c.on_success()

    assert c.rate >= c._min
