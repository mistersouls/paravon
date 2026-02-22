from paravon.bootstrap.deps import get_core, get_api_app
from paravon.core.models.message import Message


app = get_api_app()


@app.request("get")
async def get(data: dict) -> Message:
    core = get_core()
    return await core.storage.get(data)


@app.request("put")
async def put(data: dict) -> Message:
    core = get_core()
    return await core.storage.put(data)


@app.request("delete")
async def delete(data: dict) -> Message:
    core = get_core()
    return await core.storage.delete(data)
