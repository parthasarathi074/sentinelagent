import json
from urllib.request import urlopen
from urllib.error import HTTPError

from sentinelagent.dashboard import create_server


def test_dashboard_health_and_snapshot_endpoints():
    server = create_server(
        lambda: {
            "metrics": {
                "processed": 4,
                "failed": 0,
                "retried": 0,
                "quarantined": 0,
                "last_batch_duration_seconds": 0.125,
            },
            "risk_state": "NORMAL",
        },
        port=0,
    )
    try:
        server_thread = __import__("threading").Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        base = f"http://127.0.0.1:{server.server_address[1]}"
        with urlopen(base + "/health") as response:
            assert json.load(response)["status"] == "ok"
        with urlopen(base + "/") as response:
            assert "SentinelAgent" in response.read().decode("utf-8")
        with urlopen(base + "/api/metrics") as response:
            metrics = json.load(response)["metrics"]
            assert metrics["processed"] == 4
            assert metrics["last_batch_duration_seconds"] == 0.125
        with urlopen(base + "/api/security") as response:
            assert json.load(response)["risk_state"] == "NORMAL"
        try:
            urlopen(base + "/missing")
        except HTTPError as exc:
            assert exc.code == 404
        else:
            raise AssertionError("unknown path should return 404")
    finally:
        server.shutdown()
        server.server_close()
