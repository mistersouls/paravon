from pathlib import Path
import os
import yaml

from paractl.core.model import ParaConf


class ParaConfLoader:
    """
    Loads and saves the Paravon CLI configuration file (paraconf.yaml).

    Resolution order for the config path:
      1. Explicit --paraconf argument
      2. PARACONF environment variable
      3. Default: ~/.para/paraconf.yaml
    """

    DEFAULT_PATH = "~/.para/paraconf.yaml"

    def __init__(self, cli_path: str | None = None):
        if cli_path:
            self.path = Path(cli_path).expanduser()
            return

        env_path = os.environ.get("PARACONF")
        if env_path:
            self.path = Path(env_path).expanduser()
            return

        self.path = Path(self.DEFAULT_PATH).expanduser()

    def load(self) -> ParaConf:
        if not self.path.exists():
            raise FileNotFoundError(f"paraconf not found: {self.path}")

        data = yaml.safe_load(self.path.read_text())
        try:
            return ParaConf.from_dict(data)
        except Exception:   # noqa
            print(f"paraconf format is invalid: {self.path.absolute()}")
            exit(1)

    def save(self, conf: ParaConf) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        yaml_str = yaml.safe_dump(conf.to_dict(), sort_keys=False)
        self.path.write_text(yaml_str)
