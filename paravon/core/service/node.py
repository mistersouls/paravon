from paravon.core.models.message import Message
from paravon.core.transport.server import MessageServer


class NodeService:
    def __init__(
        self,
        api_server: MessageServer
    ) -> None:
        self._api_server = api_server

    @property
    def standalone(self) -> bool:
        return True

    async def join(self) -> Message:
        ...

    async def drain(self) -> Message:
        ...

    async def remove(self) -> Message:
        ...

    async def wait_join(self) -> None:
        ...
