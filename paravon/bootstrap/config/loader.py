import argparse
import os
from functools import lru_cache
from pathlib import Path


@lru_cache
def get_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="paranode",
        description=(
            "Start a Paravon node.\n\n"
            "Paravon is a distributed key-value engine designed for strong "
            "consistency, deterministic convergence, and high performance "
            "across unreliable networks."
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to a Paravon configuration file"
    )

    parser.add_argument(
        "-l", "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help=(
            "Logging verbosity for the node.\n"
            "Choose among: DEBUG, INFO, WARNING, ERROR, CRITICAL.\n\n"
            "DEBUG    → verbose output, useful for development and tracing.\n"
            "INFO     → standard operational logs (default).\n"
            "WARNING  → only warnings and errors.\n"
            "ERROR    → only errors.\n"
            "CRITICAL → only critical failures.\n\n"
            "Example:\n"
            "  --log-level DEBUG"
        ),
    )

    return parser.parse_args()


@lru_cache
def get_configfile() -> Path:
    args = get_cli_args()

    # Priority: CLI > ENV > default file in current working directory
    raw = args.config or os.getenv("PARANODECONFIG")

    if raw is None:
        file = Path.cwd() / "paranode.yaml"
    else:
        file = Path(raw)

    if not file.is_file():
        raise SystemExit(
            f"[config] Configuration file not found: '{file}'.\n"
            "  - Use --config <file.yaml>\n"
            "  - Or set the PARANODECONFIG environment variable\n"
            "  - Or place a 'paranode.yaml' file in the current working directory."
        )

    return file
