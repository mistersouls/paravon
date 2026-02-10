import time
from typing import Optional


class CubicRateController:
    """
    Generic CUBIC controller that manages a *rate* (units per second),
    not a time interval. This makes it reusable for gossip, replication,
    heartbeats, or any adaptive control loop.

    rate(t) is always clamped between min_rate and max_rate.
    """
    def __init__(
        self,
        base_rate: float = 1.0,
        min_rate: float = 0.1,
        max_rate: float = 1.0,
        constant: float = 0.02,
        beta: float = 0.7,
    ) -> None:

        # Bounds
        self._min = min_rate
        self._max = max_rate

        # CUBIC parameters
        self._C = constant
        self._beta = beta

        # Current rate
        self._rate = base_rate

        # Wmax = rate at last congestion event
        self._w_max = base_rate

        # K = cubic zero point
        self._k = 0.0

        # Timestamp of last loss
        self._t_loss: Optional[float] = None

    @property
    def rate(self) -> float:
        """Current rate (units per second)."""
        return self._rate

    def _compute_k(self) -> None:
        """
        K = cubic_root( Wmax * (1 - beta) / C )
        """
        self._k = ((self._w_max * (1 - self._beta)) / self._C) ** (1.0 / 3.0)

    def on_error(self) -> None:
        """
        Congestion signal → multiplicative decrease.
        """
        self._w_max = self._rate
        self._t_loss = time.monotonic()
        self._compute_k()

        # Backoff
        self._rate = max(self._min, self._rate * self._beta)

    def on_success(self) -> None:
        """
        Success → follow the CUBIC growth curve.
        """
        if self._t_loss is None:
            # No loss yet → gentle linear increase
            self._rate = min(self._max, self._rate * 1.05)
            return

        t = time.monotonic() - self._t_loss

        # CUBIC growth
        w = self._C * (t - self._k) ** 3 + self._w_max

        # Clamp
        self._rate = min(self._max, max(self._min, w))


class CubicRateLimiter:
    """
    A rate limiter based on the CUBIC congestion control algorithm.

    This limiter delegates all rate adaptation logic to a `CubicRateController`,
    which adjusts the allowed rate based on success/error feedback. The limiter
    simply converts the controller's rate into a sleep interval and enforces it.

    This design mirrors TCP CUBIC behavior:
    - successes gradually increase throughput
    - errors trigger multiplicative backoff
    - the delay dynamically adapts to network or system conditions
    """

    def __init__(self, controller: CubicRateController):
        self.controller = controller

    @property
    def delay(self) -> float:
        """
        Return the current delay (in seconds) before the next allowed operation.

        The delay is computed as the inverse of the current rate:
            delay = 1 / rate
        """
        rate = self.controller.rate
        return 1.0 / rate

    def on_success(self) -> None:
        """
        Notify the controller that the last operation succeeded.

        This allows the CUBIC algorithm to increase the rate accordingly.
        """
        self.controller.on_success()

    def on_error(self) -> None:
        """
        Notify the controller that the last operation failed.

        This triggers a backoff in the CUBIC controller, reducing the rate.
        """
        self.controller.on_error()
