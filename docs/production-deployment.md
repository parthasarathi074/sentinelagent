# Production deployment and remaining integration

SentinelAgent currently includes a Python analysis/enforcement prototype with in-memory and SQLite-backed event paths. Optional adapters added alongside the existing implementation provide Kafka transport, a read-only HTTP dashboard API, and an archive/recycle lifecycle. They are deliberately additive and do not silently replace the existing event stream or change current pipeline semantics.

## Optional integrations

Install optional extras when needed:

```powershell
python -m pip install -e ".[test]"
python -m pip install -r requirements-kafka.txt -r requirements-archive-s3.txt
```

- `kafka`: `confluent-kafka` producer/consumer transport. Use `edge_id` as the message key for partition affinity. Configure replication, topic retention, TLS/SASL, ACLs, and monitoring in Kafka itself.
- `archive-s3`: `boto3` S3-compatible object storage adapter. Configure credentials through the cloud provider's identity mechanism, not hard-coded secrets.
- Dashboard: `python scripts/run_dashboard.py`; default bind is `127.0.0.1:8765`. This is a demo/read-only API until a real snapshot provider is wired to `AnalysisPipeline` and the registry.

## Important boundary

The following are not claimed to be automatically configured by these adapters: LangGraph orchestration, OPA policy evaluation, FalkorDB graph persistence, Kafka cluster provisioning, cross-service credential rotation/revocation, multi-worker distributed coordination, or a production-authenticated dashboard. Those require deployment-specific wiring, dependencies, credentials, infrastructure and integration tests. The existing local `PolicyEngine` remains the policy implementation unless an explicit OPA adapter is added and integrated.

## Release checklist

- Run the existing suite and the new archive/dashboard/transport adapter tests.
- Run Kafka integration tests against a disposable secured broker (delivery, partition affinity, retries, duplicate delivery, offset commit behavior, and restart recovery).
- Test archive restore and purge against the actual provider, including lifecycle transitions, object lock/legal hold, denied IAM operations, and metadata DB backup/restore.
- Put dashboard behind authentication and TLS before exposing it beyond localhost; add authorization for any future mutating endpoint.
- Load-test increasing logical-agent counts and event throughput; record latency percentiles, CPU/memory, queue depth, error rates, quarantine latency and recovery time.
- Verify package install/build in a clean environment and CI on the supported Python versions.
