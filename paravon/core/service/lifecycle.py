import asyncio

from paravon.core.service.node import NodeService
from paravon.core.transport.server import MessageServer


class LifecycleService:
    def __init__(
        self,
        node_service: NodeService,
        api_server: MessageServer,
        peer_server: MessageServer,
    ) -> None:
        self._node_service = node_service
        self._api_server = api_server
        self._peer_server = peer_server

    @property
    def standalone(self) -> bool:
        return True

    async def start(self, stop_event) -> None:
        if self.standalone:
            ...
        else:
            await self.maybe_start(stop_event)

    async def stop(self) -> None:
        ...

    async def maybe_start(self, stop_event: asyncio.Event) -> None:
        ...

    async def live(self, stop_event: asyncio.Event) -> None:
        """join, drain, stop api_server ..."""
