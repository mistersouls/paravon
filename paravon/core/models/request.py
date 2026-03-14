import asyncio
from dataclasses import dataclass, field

from paravon.core.models.message import Message
from paravon.core.models.version import HLC


@dataclass
class Request:
    request_id: str
    key: bytes
    quorum: int
    timeout: float


@dataclass
class GetRequest(Request):
    pass


@dataclass
class PutRequest(Request):
    value: bytes


@dataclass
class DeleteRequest(Request):
    pass


@dataclass(frozen=True, slots=True)
class ReplicaSet:
    remotes: tuple[str, ...]
    local: str | None = None

    @property
    def candidates(self) -> tuple[str, ...]:
        if self.local:
            return (self.local,) + self.remotes
        return self.remotes

    @property
    def primary(self) -> str:
        """Return the primary node responsible for this partition."""
        if self.remotes:
            return self.remotes[0]
        return self.local

    def has_local(self) -> bool:
        return self.local is not None


@dataclass
class RequestContext:
    request_id: str
    quorum: int
    replicas: ReplicaSet
    future: asyncio.Future[Message]
    responses: list[dict] = field(default_factory=list)
    failures: int = 0
    timeout: float | None = None


@dataclass(frozen=True, slots=True)
class PutData:
    items: list[tuple[bytes, bytes, bytes]]
    data_key: bytes
    index_key: bytes
    hlc: HLC
