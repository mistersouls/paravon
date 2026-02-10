import asyncio

from paravon.core.models.message import Message
from paravon.core.models.membership import Membership
from paravon.core.service.meta import NodeMetaManager
from paravon.core.transport.server import MessageServer


class NodeService:
    def __init__(
        self,
        api_server: MessageServer,
        meta_manager: NodeMetaManager,
    ) -> None:
        self._api_server = api_server
        self._meta_manager = meta_manager
        self._join_done = asyncio.Event()

    async def join(self) -> Message:
        return Message(type="ko", data={"message": "Not implemented yet"})

    async def drain(self) -> Message:
        return Message(type="ko", data={"message": "Not implemented yet"})

    async def remove(self) -> Message:
        return Message(type="ko", data={"message": "Not implemented yet"})

    async def wait_join(self) -> None:
        await self._join_done.wait()

    async def recover_ring(self, membership: Membership) -> None:
        ...
