# Live dashboard integration

The basic `scripts/run_dashboard.py` launcher is intentionally a demo because
it constructs its own placeholder data. A dashboard in a separate process
cannot inspect another process's in-memory pipeline objects.

Use `sentinelagent.live_dashboard.LiveDashboardSnapshotProvider` from the same
Python process that creates the real application `AnalysisPipeline` and
`AgentRegistry`:

```python
from sentinelagent.analysis_pipeline import AnalysisPipeline
from sentinelagent.dashboard import create_server
from sentinelagent.live_dashboard import LiveDashboardSnapshotProvider
from sentinelagent.registry import AgentRegistry

registry = AgentRegistry()  # use the application's already-configured registry
pipeline = AnalysisPipeline(registry=registry)  # use the application's pipeline
provider = LiveDashboardSnapshotProvider(pipeline, registry)
server = create_server(provider, host="127.0.0.1", port=8765)
server.serve_forever()
```

**Important:** the example above illustrates the required object relationship.
In an existing application, do not create a second registry or pipeline just
for the dashboard. Pass the same instances that handle real agent requests
and events. If the application already owns its server lifecycle, attach the
provider to that lifecycle rather than starting another server.

The provider exposes current metrics, registered runtime identities and
statuses, current cumulative risk evidence, policy decisions, replacement and
quarantine relationships, recovery records, and pending event count when the
stream has a `size()` method. It deliberately omits credential references.

The current package does not retain a general-purpose immutable audit log of
all past security events. Therefore `audit_trail` is empty unless the caller
supplies an `audit_provider` that returns a list of audit dictionaries. The
`security_events`, `quarantine_events`, and `replacement_events` fields are
current decision/state summaries, not historical audit records. Connect a
separately persisted audit source before treating them as compliance evidence.

Keep the listener bound to `127.0.0.1` for local use. Before exposing it to a
network, add authentication, authorization, TLS, and deployment-specific
request logging/rate limits.
