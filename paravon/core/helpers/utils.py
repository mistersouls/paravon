import asyncio
import contextlib
import functools
import importlib
import logging
import pkgutil
import sys
import signal
import threading
from collections.abc import Callable
from types import FrameType
from typing import Generator

SHUTDOWN_SIGNALS = (
    signal.SIGINT,
    signal.SIGTERM,
)

if sys.platform == "win32":
    SHUTDOWN_SIGNALS += (signal.SIGBREAK,)


@contextlib.contextmanager
def setup_signal_handler() -> Generator[asyncio.Event, None, None]:
    stop_event = asyncio.Event()

    if threading.current_thread() is not threading.main_thread():
        yield stop_event
        return

    captured_signals: list[signal.Signals | int] = []

    def handle(sig: int, frame: FrameType) -> None:
        captured_signals.append(sig)
        stop_event.set()

    # Install temporary handlers
    original_handlers = {
        sig: signal.signal(sig, handle)
        for sig in SHUTDOWN_SIGNALS
    }

    try:
        yield stop_event
    finally:
        # Restore original handlers
        for sig, old in original_handlers.items():
            signal.signal(sig, old)

        # Now replay signals with the real handler
        for sig in reversed(captured_signals):
            if original_handlers[sig] is not handle:
                signal.raise_signal(sig)


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)-8s [%(name)s:%(funcName)s] : %(message)s',
    )


def scan(package: str):
    """
    Decorator that triggers a component scan when the decorated function
    is imported.
    """
    def decorator(func: Callable):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Execute the scan BEFORE calling the function
            py_package = importlib.import_module(package)

            for module_info in pkgutil.iter_modules(py_package.__path__):
                module_name = f"{package}.{module_info.name}"
                importlib.import_module(module_name)

            # The function itself is usually a no-op, but we call it anyway
            return func(*args, **kwargs)

        return wrapper

    return decorator
