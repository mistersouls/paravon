from paravon.core.models.message import Message


class FakeClient:
    def __init__(self):
        self.sent: list[Message] = []

    async def send(self, message: Message):
        self.sent.append(message)


class FakeClientConnectionPool:
    def __init__(self):
        self.subscriptions: list[tuple[str, object]] = []
        self.clients: dict[str, FakeClient] = {}
        self.get_calls: list[str] = []

    def subscribe(self, msg_type: str, handler: object) -> None:
        self.subscriptions.append((msg_type, handler))

    async def get(self, node_id: str) -> FakeClient:
        self.get_calls.append(node_id)
        return self.clients[node_id]
