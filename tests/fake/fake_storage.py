from paravon.core.ports.storage import Storage


class FakeStorage(Storage):
    """
    A simple in-memory storage implementation for testing.
    It mimics the async get/put/delete interface of the real Storage.
    """

    def __init__(self):
        self._data = {}
        self.get_calls = 0

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        self.get_calls += 1
        return self._data.get((namespace, key))

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        self._data[(namespace, key)] = value

    async def delete(self, namespace: bytes, key: bytes) -> None:
        if (namespace, key) in self._data:
            del self._data[(namespace, key)]
