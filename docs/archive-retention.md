# Archive / recycle layer

SentinelAgent's archive layer provides a two-phase delete lifecycle for files and exported artifacts:

1. **Active system:** agents, Kafka, databases, checkpoints and data-lake files remain under their owning systems.
2. **Recycle/archive:** a delete request copies the artifact to a configured object backend, records a SHA-256 digest and retention deadline in SQLite metadata, then removes the source only after the archive record is committed.
3. **Restore:** an operator can restore an archived artifact before permanent deletion. The manager refuses to overwrite an existing destination and verifies the digest.
4. **Permanent deletion:** `permanent_delete()` enforces the retention deadline by default. `purge_expired()` is intended for a scheduled maintenance job. A provider's legal hold/object lock must remain authoritative.

## Storage categories

| Source | Recommended archive representation |
|---|---|
| Kafka | Configure Kafka tiered storage/retention for topic segments; use export artifacts only when message-level retention requirements demand it. The archive manager does not delete Kafka topics or offsets. |
| Agent logs | Rotate logs, then archive completed log files. Do not archive active log handles. |
| Checkpoints | Export versioned checkpoint files and metadata; preserve the exact runtime/graph version needed to restore. |
| Data lake | Use object versioning or immutable snapshots; archive overwritten/deleted object versions and record their original URI/version ID. |
| SQLite state / databases | Use a database-native consistent backup/snapshot before archiving. Do not copy a live database file blindly. |

## Backends and cost

- `FilesystemObjectBackend` is for local development and tests only.
- `S3ObjectBackend` uses the optional `boto3` dependency and can target S3-compatible object stores. Configure lifecycle rules to transition objects to a low-cost archive tier such as Glacier where appropriate.
- Cloud object storage, Glacier retrieval, Kafka clusters, and managed databases may incur charges. Estimate storage, requests, retrieval, egress, minimum storage duration, and deletion fees before deployment. No cloud resources are provisioned by this package.

## Safety requirements

- Keep the SQLite metadata database backed up and access-controlled; it is the archive catalog, not the object payload.
- Restrict permanent deletion to an authorized operator/service identity and retain audit events for archive, restore, and purge operations.
- Use provider IAM least privilege, encryption at rest/in transit, bucket versioning/object lock where required, and tested disaster recovery.
- Retention is not the same as a legal hold. Legal/compliance holds must block purge at the storage-provider layer too.
- Kafka is at-least-once in most practical consumer flows; use idempotent handlers and commit offsets only after successful processing.

## Example

```python
from sentinelagent.archive import ArchiveManager, FilesystemObjectBackend

manager = ArchiveManager(
    FilesystemObjectBackend("./var/recycle"),
    metadata_db="./var/archive-catalog.sqlite3",
)
record = manager.archive("./var/logs/agent-001.log", category="agent-logs", retention_days=90,
                         metadata={"runtime_id": "agent-001"})
# Restore before retention expires:
# manager.restore(record.archive_id, "./restored/agent-001.log")
# A scheduled job can call manager.purge_expired() after retention expires.
```
