# Persistent SentinelAgent Audit Trail

## Start the audited local runtime

Use the new runner instead of `scripts/run_live_runtime.py` when you want
request authorization outcomes stored durably:

```powershell
py -3.14 .\scripts\run_audited_runtime.py
```

The default state directory is `%USERPROFILE%\.sentinelagent\runtime`. The
runner uses separate SQLite files for `agent-registry.sqlite3` and
`audit-trail.sqlite3`. You can override the directory with
`--state-dir C:\path\to\state` and configure host/port as with the regular
runner. Keep it bound to loopback unless authentication and TLS are provided.

## What is recorded

For successfully parsed edge requests, the trail records an allowlisted subset
of metadata and the resulting `ALLOW`, `DENY`, `RESTRICT`, or `THROTTLE`
decision. It also records request-processing exception types without exception
messages. It does not persist raw request bodies, credentials, or arbitrary
payload fields. Dashboard snapshots expose recent records through `audit_trail`
and set `audit_trail_configured` to `true`.

Each entry is chained to the previous SHA-256 hash. `verify_integrity()` checks
all links and hashes at startup and can be called for verification. A chain
break prevents the audited runtime from starting, rather than silently treating
a corrupted trail as valid. Hash chaining is tamper-evident, not immutable
storage against an administrator who can rewrite the whole database; restrict
file permissions and back up the audit file off-host for stronger assurance.

## Scope and production boundary

This covers the local HTTP edge-request authorization path. It is not a
replacement for source-side audit exports, Kafka broker audit logs, OPA
decision logs, or data-lake access records. It does not yet audit malformed
HTTP bodies rejected before `submit_edge_request`, nor does it guarantee
atomicity between the existing in-memory event queue and the SQLite write. If
the audit database becomes unavailable after the pipeline has queued a request,
the endpoint can return an error even though queueing already occurred. A
production design should use a transactional outbox or a broker with durable
acknowledgement and a defined audit-write failure policy.

Configure retention, access controls, off-host backups, and alerting according
to your requirements before production use.
