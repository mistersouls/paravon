import pytest

from paravon.core.models.message import Message
from paravon.core.routing.app import RoutedApplication
from tests.fake.fake_send_receive import FakeReceiveMessage, FakeSendMessage


@pytest.mark.ut
@pytest.mark.asyncio
async def test_routed_application_dispatch():
    app = RoutedApplication()

    @app.request("echo")
    async def handle_echo(data):
        return Message(type="echo", data=data)

    receive = FakeReceiveMessage([
        Message(type="echo", data={"x": 42}),
    ])
    send = FakeSendMessage()

    await app(receive, send)

    assert len(send.sent) == 1
    assert send.sent[0].type == "echo"
    assert send.sent[0].data["x"] == 42
    assert "request_id" in send.sent[0].data


@pytest.mark.ut
@pytest.mark.asyncio
async def test_routed_application_unknown_type():
    app = RoutedApplication()

    receive = FakeReceiveMessage([
        Message(type="nope", data={}),
    ])
    send = FakeSendMessage()

    await app(receive, send)

    assert len(send.sent) == 1
    assert send.sent[0].type == "ko"
    assert "Unknown message type" in send.sent[0].data["message"]


@pytest.mark.ut
@pytest.mark.asyncio
async def test_routed_application_handler_exception():
    app = RoutedApplication()

    @app.request("boom")
    async def handler_boom(data):
        raise ValueError("boom!")

    receive = FakeReceiveMessage([
        Message(type="boom", data={}),
    ])
    send = FakeSendMessage()

    await app(receive, send)

    assert len(send.sent) == 1
    assert send.sent[0].type == "ko"
    assert send.sent[0].data["message"] == "boom!"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_routed_application_handler_returns_none():
    app = RoutedApplication()

    @app.request("noop")
    async def handler_noop(data):
        return None

    receive = FakeReceiveMessage([
        Message(type="noop", data={}),
    ])
    send = FakeSendMessage()

    await app(receive, send)

    assert send.sent == [Message(type='ko', data={'message': 'Empty response'})]
