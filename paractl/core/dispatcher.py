import argparse
import functools
from typing import Any, Protocol

from paractl.core.client import ParavonClient
from paractl.core.loader import ParaConfLoader
from paractl.core.ports.render import Renderer
from paravon.core.models.message import Message


class CommandHandler(Protocol):
    def __call__(
        self,
        client: ParavonClient,
        loader: ParaConfLoader,
        namespace: argparse.Namespace,
    ) -> Message:
        ...


class CommandDispatcher:
    def __init__(self) -> None:
        self._commands: dict[tuple[str, ...], CommandHandler] = {}

    def dispatch(
        self,
        *arguments: str,
        client: ParavonClient,
        loader: ParaConfLoader,
        namespace: argparse.Namespace
    ) -> Message:
        command = self._commands.get(arguments)
        if command is None:
            raise RuntimeError(f"Unknown '{" ".join(arguments)}' Command")
        return command(client, loader, namespace)

    def command(self, *arguments: str):
        def decorator(func: CommandHandler):

            @functools.wraps(func)
            def wrapper(
                client: ParavonClient,
                loader: ParaConfLoader,
                namespace: argparse.Namespace,
            ) -> Message:
                return func(client, loader, namespace)

            self._commands[arguments] = wrapper

            return wrapper

        return decorator
