from paravon.bootstrap.deps import get_peer_app, get_core
from paravon.core.models.message import Message


app = get_peer_app()


@app.request("join")
async def join(data: dict) -> Message:
    core = get_core()
    return await core.node.join()


@app.request("drain")
async def drain(data: dict) -> Message:
    core = get_core()
    return await core.node.drain()


@app.request("remove")
async def remove(data: dict) -> Message:
    core = get_core()
    return await core.node.remove()


@app.request("gossip/checksums")
async def gossip_checksums(data: dict) -> Message:
    core = get_core()
    return await core.node.apply_checksums(data)


@app.request("gossip/bucket")
async def gossip_bucket(data: dict) -> Message:
    core = get_core()
    return await core.node.apply_bucket(data)
