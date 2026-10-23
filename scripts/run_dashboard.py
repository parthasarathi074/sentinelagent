"""Run a read-only demo dashboard. Replace demo_snapshot with real application wiring."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from sentinelagent.dashboard import create_server  # noqa: E402


def demo_snapshot() -> dict:
    # This deliberately does not fabricate live agent state. It makes the API
    # reachable before an application-specific snapshot provider is configured.
    return {"status": "demo", "message": "Connect a live AnalysisPipeline snapshot provider to expose runtime data.",
            "metrics": {}, "agent_status": [], "security_events": [], "audit_trail": []}


if __name__ == "__main__":
    host = os.getenv("SENTINEL_DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("SENTINEL_DASHBOARD_PORT", "8765"))
    server = create_server(demo_snapshot, host, port)
    print(f"SentinelAgent dashboard API listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
