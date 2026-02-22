import functools
import logging
from typing import Callable, Awaitable, Mapping, Any

from paravon.core.models.message import Message


RouteHandler = Callable[[Mapping[str, Any]], Awaitable[Message]]


class Router:
    """
    A minimal message‑routing component used by RoutedApplication.

    The Router maps message types (strings) to asynchronous handler functions.
    Each handler is a coroutine accepting a mapping of input data and returning
    a `Message` instance.

    Handlers are registered exactly once per method. Attempting to register a
    second handler for the same method raises a RuntimeError.

    This component does not perform any validation, transformation, or
    dispatching logic by itself; it only stores and resolves handlers. The
    dispatch loop is implemented by `RoutedApplication`.
    """

    def __init__(self) -> None:
        self._routes: dict[str, RouteHandler] = {}
        self._logger = logging.getLogger("core.routing.router")

    def request(self, method: str) -> Callable[[RouteHandler], RouteHandler]:
        def decorator(func: RouteHandler) -> RouteHandler:
            if method in self._routes:
                raise RuntimeError(f"Handler already registered for '{method}'")

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):  # keep it for later
                return await func(*args, **kwargs)

            self._routes[method] = wrapper
            return wrapper

        return decorator

    def resolve(self, method: str) -> RouteHandler | None:
        return self._routes.get(method)

    def routes(self) -> dict[str, RouteHandler]:
        return dict(self._routes)
