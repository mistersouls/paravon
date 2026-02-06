from paractl.core.model import ParaConf, ContextConfig, TLSConfig, ClientConfig


def parse_timeout(s: str) -> float:
    # Very simple parser: supports "<number>s" or "<number>ms"
    s = s.strip().lower()
    if s.endswith("ms"):
        return float(s[:-2]) / 1000.0
    if s.endswith("s"):
        return float(s[:-1])
    return float(s)


def resolve_context(
    conf: ParaConf,
    context_override: str | None,
    server_override: str | None
) -> tuple[str, ContextConfig]:
    ctx_name = context_override or conf.current_context
    if ctx_name not in conf.contexts:
        print(f"Warning: unknown context '{ctx_name}', using fallback context.")
        fallback = ContextConfig(
            server=server_override or "localhost:2000",
            tls=None,
            client=ClientConfig(client_id="zero", timeout="")
        )
        return "", fallback

    ctx = conf.contexts[ctx_name]
    if server_override:
        # shallow copy
        ctx = ContextConfig(
            server=server_override,
            tls=ctx.tls,
            client=ctx.client,
        )

    return ctx_name, ctx
