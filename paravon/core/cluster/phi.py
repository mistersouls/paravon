import time
import math
from collections import deque


class FailureDetector:
    """
    φ-accrual failure detector (Hayashibara et al. 2004).
    """

    def __init__(
        self,
        threshold: float = 2.0,
        window_size: int = 50,
        min_interval: float = 1e-3,
        max_interval: float = 60.0,
        phi_cap: float = 50.0,
    ):
        self._threshold = threshold
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._phi_cap = phi_cap
        self._arrival_times = deque(maxlen=window_size)

    @property
    def total_samples(self) -> int:
        return len(self._arrival_times)

    def record_heartbeat(self, timestamp: float | None = None) -> None:
        self._arrival_times.append(timestamp or time.time())

    def compute_phi(self, now: float | None = None) -> float:
        if not self._arrival_times:
            return float("inf")

        if now is None:
            now = time.time()

        times = list(self._arrival_times)
        inter_arrivals = [
            min(max(t2 - t1, self._min_interval), self._max_interval)
            for t1, t2 in zip(times, times[1:])
        ]

        if not inter_arrivals:
            return 0.0

        mean = sum(inter_arrivals) / len(inter_arrivals)
        mean = max(mean, 1e-6)

        last = self._arrival_times[-1]
        delta = now - last

        if delta <= 0:
            return 0.0

        x = -delta / mean

        if x < -self._phi_cap:
            return self._phi_cap

        survival = math.exp(x)
        survival = max(survival, 1e-15)

        return -math.log10(survival)

    def is_suspect(
        self,
        now: float | None = None,
        min_samples: int | None = None
    ) -> bool:
        if min_samples and min_samples > self.total_samples:
            return True
        return self.compute_phi(now) >= self._threshold

    def is_alive(
        self,
        now: float | None = None,
        min_samples: int | None = None
    ) -> bool:
        return not self.is_suspect(now=now, min_samples=min_samples)

    def reset(self) -> None:
        self._arrival_times.clear()
