import pytest
from unittest.mock import MagicMock

from paravon.core.throttling.cubic import CubicRateLimiter


@pytest.mark.ut
def test_delay_computed_from_rate():
    controller = MagicMock()
    controller.rate = 2.0  # 2 ops/sec → 0.5s delay

    limiter = CubicRateLimiter(controller)

    assert limiter.delay == 0.5


@pytest.mark.ut
def test_delay_updates_when_rate_changes():
    controller = MagicMock()
    controller.rate = 4.0  # 4 ops/sec → 0.25s delay

    limiter = CubicRateLimiter(controller)
    assert limiter.delay == 0.25

    controller.rate = 1.0  # 1 op/sec → 1s delay
    assert limiter.delay == 1.0


@pytest.mark.ut
def test_on_success_calls_controller():
    controller = MagicMock()
    limiter = CubicRateLimiter(controller)

    limiter.on_success()
    controller.on_success.assert_called_once()


@pytest.mark.ut
def test_on_error_calls_controller():
    controller = MagicMock()
    limiter = CubicRateLimiter(controller)

    limiter.on_error()
    controller.on_error.assert_called_once()
