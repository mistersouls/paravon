import asyncio

from paravon.core.models.message import Message
from paravon.core.models.meta import Membership, NodePhase
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
        ...

    async def drain(self) -> Message:
        ...

    async def remove(self) -> Message:
        ...

    async def wait_join(self) -> None:
        await self._join_done.wait()

    async def recover_ring(self, membership: Membership) -> None:
        ...
