from typing import Protocol, Iterable

from paravon.core.models.version import ValueVersion


class ConflictResolver(Protocol):
    def resolve(self, versions: Iterable[ValueVersion]) -> ValueVersion | None:
        ...
