import random
from dataclasses import dataclass


@dataclass
class ExponentialBackoff:
    """
    A simple exponential backoff mechanism with optional jitter.

    This utility is typically used to delay retries after failures in
    distributed systems, network calls, or asynchronous operations.
    The delay grows exponentially according to:

        next_delay = min(current * factor, maximum) + jitter

    The jitter component helps avoid thundering-herd effects by adding
    randomness to the delay, preventing multiple clients from retrying
    in lockstep.
    """

    initial: float = 0.5
    """Initial delay (in seconds) before the first retry."""

    maximum: float = 30.0
    """Maximum allowed delay (in seconds)."""

    factor: float = 2.0
    """Multiplicative factor applied to the delay after each retry."""

    jitter: float = 1.2
    """Maximum random jitter added to each delay."""

    _current: float = None
    """Internal state tracking the current delay."""

    def __post_init__(self):
        """
        Initialize the internal delay state.

        This ensures that the first call to `next_delay()` returns the
        initial delay (plus jitter), and subsequent calls grow exponentially.
        """
        self._current = self.initial

    def next_delay(self) -> float:
        """
        Compute and return the next backoff delay.

        The delay is computed as:
            delay = current_delay + random_jitter
            current_delay = min(current_delay * factor, maximum)
        """
        delay = self._current

        # Increase delay for next call
        self._current = min(self._current * self.factor, self.maximum)

        # Add jitter if enabled
        if self.jitter > 0:
            delay += random.uniform(0, self.jitter)

        return delay

    def reset(self):
        """
        Reset the backoff delay to its initial value.

        This is typically called after a successful operation, so that
        the next retry (if needed) starts from the minimum delay again.
        """
        self._current = self.initial
