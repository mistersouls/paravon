from typing import Protocol


class RateLimiter(Protocol):
    """
    Interface for rate limiters used to control the pacing of asynchronous operations.

    A rate limiter exposes:
    - a delay (seconds between operations)
    - feedback hooks for success and error events
    """

    @property
    def delay(self) -> float:
        """Return the current delay (in seconds) before the next allowed operation."""

    def on_success(self) -> None:
        """Notify the limiter that the last operation succeeded."""

    def on_error(self) -> None:
        """Notify the limiter that the last operation failed."""
