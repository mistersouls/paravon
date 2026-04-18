from paravon.core.models.version import HLC


class FakeStorageService:
    def __init__(self, hlc: HLC | None = None):
        self._hlc = hlc or HLC.initial("node-1")
        self.last_hlc_calls: list[dict] = []
        self.apply_calls: list[dict] = []

    async def last_hlc_for(self, data: dict) -> HLC:
        self.last_hlc_calls.append(data)
        return self._hlc

    async def apply(self, data: dict):
        self.apply_calls.append(data)
