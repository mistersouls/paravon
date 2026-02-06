import argparse

from paractl.bootstrap.deps import get_cli
from paractl.core.client import ParavonClient
from paractl.core.loader import ParaConfLoader
from paravon.core.models.message import Message

cli = get_cli()


@cli.command("config", "current-context")
def cmd_current_context(
    client: ParavonClient,
    loader: ParaConfLoader,
    namespace: argparse.Namespace
) -> Message:
    _ = client, namespace
    conf = loader.load()
    return Message(
        type="ok",
        data={"current_context": conf.current_context}
    )


@cli.command("config", "get-contexts")
def cmd_get_contexts(
    client: ParavonClient,
    loader: ParaConfLoader,
    namespace: argparse.Namespace
) -> Message:
    _ = client, namespace
    conf = loader.load()
    return Message(
        type="ok",
        data={"contexts": list(conf.contexts.keys())}
    )


@cli.command("config", "use-context")
def cmd_use_context(
    client: ParavonClient,
    loader: ParaConfLoader,
    namespace: argparse.Namespace
) -> Message:
    _ = client
    if not hasattr(namespace, "name"):
        raise ValueError(f"context name is required.")

    name = namespace.name
    conf = loader.load()
    if name not in conf.contexts:
        raise ValueError(f"Unknown context: {name}")

    conf.current_context = name
    loader.save(conf)
    return Message(
        type="ok",
        data={"message": f"Switched to context '{name}'"}
    )
