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


@app.request("status/node")
async def node_status(data: dict) -> Message:
    core = get_core()
    return await core.node.node_status()


@app.request("gossip/checksums")
async def gossip_checksums(data: dict) -> Message:
    core = get_core()
    return await core.node.apply_checksums(data)


@app.request("gossip/bucket")
async def gossip_bucket(data: dict) -> Message:
    core = get_core()
    return await core.node.apply_bucket(data)


@app.request("healthz")
async def healthz(data: dict) -> Message:
    core = get_core()
    node_info = await core.node.node_status()
    source = node_info.data["node_id"]
    return Message(type="healthz", data={"source": source})


@app.request("read")
async def read(data: dict) -> Message:
    core = get_core()
    return await core.storage.read(data)


@app.request("apply")
async def apply(data: dict) -> Message:
    core = get_core()
    return await core.storage.apply(data)


@app.request("forward/fetch")
async def forward_get(data: dict) -> Message:
    core = get_core()
    msg = await core.storage.get(data)
    if msg.type == "ok":
        return Message(type="forward/fetch", data=msg.data)
    return msg


@app.request("forward/write")
async def forward_put(data: dict) -> Message:
    core = get_core()
    value = data.get("value")
    if value is not None:
        msg = await core.storage.put(data)
    else:
        msg = await core.storage.delete(data)

    if msg.type == "ok":
        return Message(type="forward/write", data=msg.data)
    return msg
