import argparse

from paractl.bootstrap.deps import get_cli
from paractl.core.client import ParavonClient
from paractl.core.loader import ParaConfLoader
from paravon.core.models.message import Message

cli = get_cli()


@cli.command("get")
def get(
    client: ParavonClient,
    _: ParaConfLoader,
    namespace: argparse.Namespace
) -> Message:
    if not hasattr(namespace, "key"):
        raise ValueError(f"key is required.")

    data = {"key": namespace.key}
    req = Message(type="get", data=data)
    return client.request(req)


@cli.command("put")
def put(
    client: ParavonClient,
    _: ParaConfLoader,
    namespace: argparse.Namespace
) -> Message:
    if not hasattr(namespace, "key"):
        raise ValueError(f"key is required.")
    if not hasattr(namespace, "value"):
        raise ValueError(f"value is required.")

    data = {
        "key": namespace.key,
        "value": namespace.value
    }
    req = Message(type="put", data=data)
    return client.request(req)


@cli.command("delete")
def delete(
    client: ParavonClient,
    _: ParaConfLoader,
    namespace: argparse.Namespace
) -> Message:
    if not hasattr(namespace, "key"):
        raise ValueError(f"key is required.")

    data = {"key": namespace.key}
    req = Message(type="delete", data=data)
    return client.request(req)
