import json

import yaml

from paractl.core.ports.render import Renderer
from paravon.core.ports.serializer import Serializer


class JsonRenderer(Renderer):
    def render(self, data: dict) -> str:
        return json.dumps(data, indent=2, sort_keys=False)


class YamlRenderer(Renderer):
    def __init__(self, serializer: Serializer) -> None:
        self._serializer = serializer

    def render(self, data: dict) -> str:
        normalized = self._normalize(data)
        return yaml.safe_dump(normalized, sort_keys=False)

    def _normalize(self, obj):
        if isinstance(obj, bytes):
            return self._serializer.deserialize(obj)

        if isinstance(obj, dict):
            return {self._normalize(k): self._normalize(v) for k, v in obj.items()}

        if isinstance(obj, (list, tuple)):
            return [self._normalize(x) for x in obj]

        return obj
