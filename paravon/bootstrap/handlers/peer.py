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


@app.request("replica/get")
async def replica_get(data: dict) -> Message:
    core = get_core()
    return await core.storage.local_get(data)


@app.request("replica/put")
async def replica_put(data: dict) -> Message:
    core = get_core()
    return await core.storage.local_put(data)


@app.request("replica/delete")
async def replica_delete(data: dict) -> Message:
    core = get_core()
    return await core.storage.local_delete(data)


@app.request("forward/get")
async def forward_get(data: dict) -> Message:
    core = get_core()
    msg = await core.storage.get(data)
    if msg.type == "ok":
        return Message(type="forward/get", data=msg.data)
    return msg


@app.request("forward/put")
async def forward_put(data: dict) -> Message:
    core = get_core()
    msg = await core.storage.put(data)
    if msg.type == "ok":
        return Message(type="forward/put", data=msg.data)
    return msg


@app.request("forward/delete")
async def forward_delete(data: dict) -> Message:
    core = get_core()
    msg = await core.storage.delete(data)
    if msg.type == "ok":
        return Message(type="forward/delete", data=msg.data)
    return msg
