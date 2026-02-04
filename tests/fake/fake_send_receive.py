
class FakeReceiveMessage:
    def __init__(self, messages):
        self._messages = list(messages)

    async def __call__(self):
        if not self._messages:
            return None
        return self._messages.pop(0)


class FakeSendMessage:
    def __init__(self):
        self.sent = []

    async def __call__(self, msg):
        self.sent.append(msg)
