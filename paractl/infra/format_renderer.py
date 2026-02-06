import json

import yaml

from paractl.core.ports.render import Renderer


class JsonRenderer(Renderer):
    def render(self, data: dict) -> str:
        return json.dumps(data, indent=2, sort_keys=False)


class YamlRenderer(Renderer):
    def render(self, data: dict) -> str:
        return yaml.safe_dump(data, sort_keys=False)
