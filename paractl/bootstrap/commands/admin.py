from paractl.bootstrap.deps import get_cli
from paractl.core.client import ParavonClient
from paravon.core.models.message import Message


cli = get_cli()


@cli.command("admin", "join")
def admin_join(client: ParavonClient, *_) -> Message:
    req = Message(type="join", data={})
    return client.request(req)


@cli.command("admin", "drain")
def admin_drain(client: ParavonClient, *_) -> Message:
    req = Message(type="drain", data={})
    return client.request(req)


@cli.command("admin", "remove")
def admin_remove(client: ParavonClient, *_) -> Message:
    req = Message(type="remove", data={})
    return client.request(req)
