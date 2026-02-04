from paravon.core.service.node import NodeService
from paravon.core.service.storage import StorageService


class ParaCore:
    def __init__(
        self,
        node_service: NodeService,
        storage_service: StorageService,
    ) -> None:
        self.node = node_service
        self.storage = storage_service
