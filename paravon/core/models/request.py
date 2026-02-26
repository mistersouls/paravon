import asyncio
from dataclasses import dataclass, field

from paravon.core.models.message import Message


@dataclass
class WriteRequest:
    request_id: str
    key: bytes
    quorum_write: int
    timeout: float


@dataclass
class PutRequest(WriteRequest):
    value: bytes


@dataclass
class DeleteRequest(WriteRequest):
    pass


@dataclass
class GetRequest:
    request_id: str
    key: bytes
    quorum_read: int
    timeout: float


@dataclass
class RequestContext:
    request_id: str
    replicas: list[str]
    quorum: int
    future: asyncio.Future[Message]
    responses: list[dict] = field(default_factory=list)
    failures: int = 0
