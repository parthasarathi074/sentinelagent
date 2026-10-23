# Local live SentinelAgent runtime

`py -3.14 .\scripts\run_live_runtime.py` starts the existing edge, analysis,
policy, recovery, registry, and read-only dashboard objects inside one process.
The registry is persisted to SQLite under `%USERPROFILE%\.sentinelagent\runtime`
by default. Override its location with `SENTINEL_STATE_DIR` or `--state-dir`.
Only agents missing from the registry are seeded from
`config/local-agents.example.json`; existing records are not replaced or
reactivated by this script.

Open `http://127.0.0.1:8765` for the dashboard. `GET /health`,
`GET /api/snapshot`, `GET /api/metrics`, and `GET /api/security` remain
available. The runtime also accepts a local `POST /api/edge-requests` request
containing the required `EdgeRequest` fields (request ID, trace ID, edge ID,
source and target runtime IDs, action, requested permission; optional resource
ID, payload hash, timestamp, protocol version). It returns the SentinelEdge
ALLOW/DENY decision immediately. The queued event is analyzed by the same
pipeline according to its configured micro-batch size/time window, and the
updated metrics/risk/policy state appears in the dashboard on refresh.

PowerShell example (leave the runtime running in another terminal):

```powershell
$body = @{
  request_id = "req-local-001"
  trace_id = "trace-local-001"
  edge_id = "edge-agent-A-agent-B"
  source_runtime_id = "runtime-A-001"
  target_runtime_id = "runtime-B-001"
  action = "SEND_MESSAGE"
  requested_permission = "message:send"
  resource_id = "record-001"
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8765/api/edge-requests" `
  -ContentType "application/json" -Body $body
```

The local runtime intentionally uses the project's in-memory `EventStream` for
simple development and runs the HTTP server in a single thread so SQLite-backed
registry operations stay on their creating thread. It is **not** a production
Kafka runtime, and in-memory queued events can be lost on process exit. A
production deployment still needs authenticated API access, TLS, robust
concurrency controls, Kafka/broker integration, durable audit storage, and a
proper lifecycle/supervision strategy. Keep the host on loopback unless those
controls are provided. Do not use the sample agent IDs as real credentials.
