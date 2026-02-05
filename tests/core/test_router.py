import pytest
from paravon.core.models.message import Message
from paravon.core.routing.router import Router


@pytest.mark.asyncio
async def test_router_register_and_resolve():
    router = Router()

    @router.request("ping")
    async def handle_ping(data):
        return Message(type="pong", data=data)

    handler = router.resolve("ping")
    assert handler is not None

    result = await handler({"x": 1})
    assert isinstance(result, Message)
    assert result.type == "pong"
    assert result.data == {"x": 1}


def test_router_duplicate_registration():
    router = Router()

    @router.request("ping")
    async def h1(data):
        ...

    with pytest.raises(RuntimeError):
        @router.request("ping")
        async def h2(data):
            ...


def test_router_routes_returns_copy():
    router = Router()

    @router.request("ping")
    async def h(data):
        ...

    routes = router.routes()
    assert "ping" in routes
    assert routes is not router._routes
