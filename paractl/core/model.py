from dataclasses import dataclass


@dataclass
class TLSConfig:
    ca: str
    cert: str
    key: str


@dataclass
class ClientConfig:
    client_id: str
    timeout: str


@dataclass
class ContextConfig:
    server: str
    client: ClientConfig
    tls: TLSConfig | None = None


@dataclass
class ParaConf:
    current_context: str
    contexts: dict[str, ContextConfig]

    @staticmethod
    def from_dict(data: dict) -> ParaConf:
        contexts = {}
        for name, ctx in data.get("contexts", {}).items():
            tls = ctx["tls"]
            client = ctx["client"]
            contexts[name] = ContextConfig(
                server=ctx["server"],
                tls=TLSConfig(
                    ca=tls["ca"],
                    cert=tls["cert"],
                    key=tls["key"],
                ),
                client=ClientConfig(
                    client_id=client["client_id"],
                    timeout=client["timeout"],
                ),
            )
        return ParaConf(
            current_context=data["current-context"],
            contexts=contexts,
        )

    def to_dict(self) -> dict:
        return {
            "current-context": self.current_context,
            "contexts": {
                name: {
                    "server": ctx.server,
                    "tls": {
                        "ca": ctx.tls.ca,
                        "cert": ctx.tls.cert,
                        "key": ctx.tls.key,
                    },
                    "client": {
                        "client_id": ctx.client.client_id,
                        "timeout": ctx.client.timeout,
                    },
                }
                for name, ctx in self.contexts.items()
            },
        }
