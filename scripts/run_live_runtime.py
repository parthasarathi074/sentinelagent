"""Run SentinelAgent pipeline and live dashboard in one local process."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from sentinelagent.models import AgentRecord  # noqa: E402
from sentinelagent.registry import AgentRegistry  # noqa: E402
from sentinelagent.runtime_server import LocalSentinelRuntime, create_runtime_server  # noqa: E402


def _string_set(value: Any, field: str, index: int) -> set[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ValueError(f"agents[{index}].{field} must be a list of non-empty strings")
    return set(value)


def seed_missing_agents(registry: AgentRegistry, config_path: Path) -> int:
    """Register only configured agents that don't already exist; never overwrite."""
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)
    if not isinstance(config, dict) or not isinstance(config.get("agents"), list):
        raise ValueError("Config must be an object with an 'agents' list")

    inserted = 0
    seen_runtime_ids: set[str] = set()
    for index, item in enumerate(config["agents"]):
        if not isinstance(item, dict):
            raise ValueError(f"agents[{index}] must be an object")
        allowed_fields = {
            "logical_agent_id", "runtime_agent_id", "role", "version",
            "permissions", "allowed_targets", "activate_on_first_registration",
        }
        extras = set(item) - allowed_fields
        if extras:
            raise ValueError(f"agents[{index}] has unsupported fields: {sorted(extras)}")
        for field in ("logical_agent_id", "runtime_agent_id", "role", "version"):
            value = item.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"agents[{index}].{field} must be a non-empty string")

        runtime_id = item["runtime_agent_id"]
        if runtime_id in seen_runtime_ids:
            raise ValueError(f"Duplicate runtime_agent_id in config: {runtime_id}")
        seen_runtime_ids.add(runtime_id)

        try:
            registry.get(runtime_id)
        except KeyError:
            record = AgentRecord(
                logical_agent_id=item["logical_agent_id"],
                runtime_agent_id=runtime_id,
                role=item["role"],
                version=item["version"],
                permissions=_string_set(item.get("permissions"), "permissions", index),
                allowed_targets=_string_set(item.get("allowed_targets"), "allowed_targets", index),
            )
            registry.register(record)
            if item.get("activate_on_first_registration", True):
                registry.activate(runtime_id)
            inserted += 1
        else:
            # Existing records are deliberately left untouched, even if the
            # example config differs. This runner never rewrites registry state.
            logging.info("Keeping existing runtime registry record: %s", runtime_id)
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("SENTINEL_DASHBOARD_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SENTINEL_DASHBOARD_PORT", "8765")))
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-wait-seconds", type=float, default=1.0)
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(os.getenv("SENTINEL_STATE_DIR", str(Path.home() / ".sentinelagent" / "runtime"))),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "config" / "local-agents.example.json",
        help="JSON file used only to register missing local agents",
    )
    args = parser.parse_args()
    if args.batch_size <= 0 or args.max_wait_seconds <= 0:
        parser.error("--batch-size and --max-wait-seconds must be greater than zero")
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        logging.warning("Non-loopback bind requested. Add authentication/TLS before network exposure.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args.state_dir.mkdir(parents=True, exist_ok=True)
    registry = AgentRegistry(database_path=args.state_dir / "agent-registry.sqlite3")
    server = None
    try:
        inserted = seed_missing_agents(registry, args.config.resolve())
        logging.info("Loaded registry; newly registered local agents=%d", inserted)
        runtime = LocalSentinelRuntime(
            registry,
            batch_size=args.batch_size,
            max_wait_seconds=args.max_wait_seconds,
        )
        server = create_runtime_server(runtime, host=args.host, port=args.port)
        logging.info("Live SentinelAgent runtime listening on http://%s:%d", args.host, server.server_address[1])
        logging.info("Dashboard: http://%s:%d/ | Submit edge events: POST /api/edge-requests", args.host, server.server_address[1])
        logging.info("Registry database: %s", args.state_dir / "agent-registry.sqlite3")
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        logging.info("Shutdown requested")
    finally:
        if server is not None:
            server.server_close()
        registry.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
