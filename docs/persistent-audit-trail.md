# Persistent SentinelAgent Audit Trail

## 1. Overview

SentinelAgent provides persistent audit history for security decisions and event-processing lifecycle activity.

The audited local runtime combines the registry, persistent event stream, and audit storage using SQLite-backed state.

The audit trail is intended to make important prototype security and processing operations inspectable across process restarts.

## 2. Starting the audited runtime

From the project root, run:

```powershell
.\.venv\Scripts\python.exe .\scripts\run_audited_runtime.py
```

The runtime uses its configured local state directory. Refer to the runtime script and deployment documentation for supported configuration options.

Keep the local HTTP runtime bound to loopback unless an appropriately secured deployment has been implemented.

## 3. Audited activity

The persistent audit implementation records security decisions and supported processing lifecycle transitions.

The event-processing lifecycle includes:
- Batch started.
- Batch retried.
- Batch failed.
- Batch succeeded.
- Batch recovered after interruption.

Audit records are designed to retain safe metadata rather than arbitrary event payloads or credentials.

The implementation records error types rather than arbitrary exception messages where applicable.

## 4. Transactional outbox

The persistent event stream and audit history use coordinated SQLite transaction handling.

For event publication, the event and its required audit record are committed together.

If the required audit write fails, the transaction is rolled back rather than leaving the event published without its corresponding audit record.

Batch lifecycle transitions also use transactional handling so that a required lifecycle audit failure does not silently leave the corresponding state transition committed.

This is the transactional outbox behavior implemented by the persistent event-processing path.

## 5. Persistent processing and recovery

The persistent event stream stores pending events and processing state.

If processing is interrupted, pending work can be recovered after restart. Recovery is recorded in the audit history and preserves the relevant batch identity.

Successful acknowledgement does not remove the event from historical audit visibility.

Processing follows at-least-once semantics. A batch may be attempted again after interruption or failure.

The system does not guarantee exactly-once execution of external side effects. External operations should be designed to tolerate retries.

## 6. Audit integrity

The persistent audit implementation uses a SHA-256 hash chain to detect modifications to recorded audit history.

The chain can detect certain forms of record alteration or deletion when the chain is subsequently verified.

A hash chain is tamper-evident, not immutable. An administrator with sufficient access to the database and its contents may be able to rewrite the database and recompute the chain.

It should not be treated as a replacement for independently protected audit exports, access controls, or external immutable storage.

## 7. Security and privacy boundaries

Audit records should contain only the metadata needed to explain security and lifecycle decisions.

Do not store:
- Passwords.
- Authentication tokens.
- Private keys.
- Raw credentials.
- Unnecessary request bodies.
- Arbitrary sensitive payload content.

The audited runtime does not establish a complete organization-wide audit system. External data sources, external services, broker administration, and integrations require their own audit coverage.

Audit coverage depends on the request path. The runtime includes tests for safely persisting rejected HTTP request metadata. This should not be interpreted as proof that every possible failure before request submission is audited.

## 8. Current limitations

The persistent audit trail is a local prototype implementation.

It does not by itself provide:
- A centralized distributed audit service.
- External immutable audit storage.
- Production key management.
- Multi-node audit coordination.
- Complete audit coverage of every external service.
- Exactly-once external side effects.
- Production authentication and transport security for the dashboard.

## 9. Related documentation

- [Event Stream Architecture](event-stream-architecture.md)
- [Live Runtime](live-runtime.md)
- [Production Deployment](production-deployment.md)