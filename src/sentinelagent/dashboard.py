"""Read-only HTTP dashboard/API for SentinelAgent operational snapshots.

This is a lightweight local dashboard foundation, not an internet-facing
production control plane. Put it behind authentication/TLS before deployment.
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse

SnapshotProvider = Callable[[], dict[str, Any]]


def make_handler(snapshot_provider: SnapshotProvider):
    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "SentinelAgentDashboard/0.1"

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, default=str, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            path = urlparse(self.path).path
            if path == "/health":
                self._send(200, {"status": "ok", "service": "sentinelagent-dashboard"})
                return
            if path == "/":
                page = """<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>SentinelAgent</title><style>body{font:16px system-ui;margin:0;background:#0b1220;color:#e5e7eb}header{padding:24px 5vw;background:#111c30;border-bottom:1px solid #27364f}main{padding:24px 5vw}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px}.card{background:#111c30;border:1px solid #27364f;border-radius:12px;padding:18px;min-height:90px}h1{margin:0}h2{font-size:16px;color:#9fb3cc;margin-top:0}pre{white-space:pre-wrap;overflow-wrap:anywhere;color:#cbd5e1}.muted{color:#9fb3cc}</style></head><body><header><h1>SentinelAgent</h1><p class='muted'>Runtime security overview · read-only</p></header><main><div class='grid'><section class='card'><h2>Pipeline metrics</h2><pre id='metrics'>Loading…</pre></section><section class='card'><h2>Agent status</h2><pre id='agents'>Loading…</pre></section><section class='card'><h2>Risk & enforcement</h2><pre id='security'>Loading…</pre></section><section class='card'><h2>Audit trail</h2><pre id='audit'>Loading…</pre></section></div><p class='muted' id='status'>Connecting to local snapshot provider…</p></main><script>async function load(){try{const r=await fetch('/api/snapshot',{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);const d=await r.json();const put=(id,v)=>document.getElementById(id).textContent=JSON.stringify(v??[],null,2);put('metrics',d.metrics);put('agents',d.agent_status);put('security',{risk_state:d.risk_state,suspicious_activity:d.suspicious_activity,security_events:d.security_events,quarantine_events:d.quarantine_events,replacement_events:d.replacement_events,recovery_state:d.recovery_state});put('audit',d.audit_trail);document.getElementById('status').textContent='Snapshot refreshed '+new Date().toLocaleTimeString()}catch(e){document.getElementById('status').textContent='Snapshot unavailable: '+e.message}}load();setInterval(load,5000)</script></body></html>"""
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page.encode("utf-8"))))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(page.encode("utf-8"))
                return
            if path not in {"/api/snapshot", "/api/metrics", "/api/security"}:
                self._send(404, {"error": "not_found"})
                return
            try:
                snapshot = snapshot_provider()
                if not isinstance(snapshot, dict):
                    raise TypeError("snapshot provider must return a dictionary")
                if path == "/api/metrics":
                    result = {"metrics": snapshot.get("metrics", {})}
                elif path == "/api/security":
                    result = {key: snapshot.get(key) for key in (
                        "agent_status", "suspicious_activity", "security_events", "interaction_relationships",
                        "risk_state", "quarantine_events", "replacement_events", "recovery_state", "audit_trail")
                              if key in snapshot}
                else:
                    result = snapshot
                self._send(200, result)
            except Exception:
                self._send(500, {"error": "snapshot_unavailable"})

        def log_message(self, fmt: str, *args) -> None:
            # Avoid writing request contents or sensitive payloads to stdout.
            return

    return DashboardHandler


def create_server(snapshot_provider: SnapshotProvider, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    if not (0 <= port <= 65535):
        raise ValueError("port must be between 0 and 65535")
    return ThreadingHTTPServer((host, port), make_handler(snapshot_provider))
