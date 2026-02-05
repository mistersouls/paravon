from dataclasses import dataclass, asdict
from enum import StrEnum
from typing import Any


class NodePhase(StrEnum):
    idle = "idle"
    joining = "joining"
    draining = "draining"
    ready = "ready"


@dataclass
class Membership:
    node_id: str
    phase: NodePhase

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class NodeMeta:
    node_id: str
    phase: NodePhase = "idle"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> NodeMeta:
        if "node_id" not in data:
            raise KeyError("Missing 'node_id' key")
        return cls(
            node_id=data["node_id"],
            phase=NodePhase(data.get("phase", "idle")),
        )
