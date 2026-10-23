"""Archive/recycle lifecycle for SentinelAgent artifacts.

The manager is backend-agnostic. The included filesystem backend is suitable for
local development; production deployments can implement ObjectBackend for S3,
Glacier-compatible storage, or another object store. Deletion is two-phase:
archive first, then restore or explicitly purge after retention.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Protocol


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value)


class ArchiveError(RuntimeError):
    """Base exception for archive lifecycle failures."""


class ArchiveNotFound(ArchiveError):
    """Raised when an archive record does not exist or is no longer restorable."""


class RetentionError(ArchiveError):
    """Raised when permanent deletion is requested before retention expires."""


class ObjectBackend(Protocol):
    """Minimal object-store contract used by ArchiveManager."""

    def put(self, key: str, source: Path) -> None: ...
    def restore(self, key: str, destination: Path) -> None: ...
    def delete(self, key: str) -> None: ...
    def exists(self, key: str) -> bool: ...


class FilesystemObjectBackend:
    """Filesystem object store for development and tests (not a cloud backup)."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        candidate = (self.root / key).resolve()
        if candidate == self.root or self.root not in candidate.parents:
            raise ValueError("Object key escapes the archive root")
        return candidate

    def put(self, key: str, source: Path) -> None:
        destination = self._path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    def restore(self, key: str, destination: Path) -> None:
        source = self._path(key)
        if not source.is_file():
            raise ArchiveNotFound(f"Archived object not found: {key}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    def delete(self, key: str) -> None:
        path = self._path(key)
        if path.exists():
            path.unlink()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()


class S3ObjectBackend:
    """S3-compatible object backend; requires the optional ``boto3`` package.

    Use an S3 lifecycle policy to transition objects to Glacier/deep archive.
    This adapter deliberately does not bypass Glacier restore delays or retention
    locks; callers must restore archived objects through the provider first.
    """

    def __init__(self, bucket: str, *, prefix: str = "sentinelagent-archive/", client=None) -> None:
        if not bucket.strip():
            raise ValueError("bucket must not be empty")
        if client is None:
            try:
                import boto3  # type: ignore[import-not-found]
            except ImportError as exc:
                raise RuntimeError("S3 support requires the optional dependency: pip install 'sentinelagent[archive-s3]'") from exc
            client = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self.client = client

    def _key(self, key: str) -> str:
        if key.startswith("/") or ".." in Path(key).parts:
            raise ValueError("Invalid object key")
        return self.prefix + key

    def put(self, key: str, source: Path) -> None:
        self.client.upload_file(str(source), self.bucket, self._key(key))

    def restore(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, self._key(key), str(destination))

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(key))

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = str(response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise


@dataclass(frozen=True)
class ArchiveRecord:
    archive_id: str
    original_path: str
    object_key: str
    category: str
    sha256: str
    size_bytes: int
    archived_at: str
    purge_after: str
    state: str
    metadata_json: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self) | {"metadata": json.loads(self.metadata_json)}


class ArchiveManager:
    """Archive artifacts, restore them, and enforce a two-phase delete workflow."""

    def __init__(self, backend: ObjectBackend, metadata_db: str | Path = "sentinelagent-archive.sqlite3") -> None:
        self.backend = backend
        self.metadata_db = str(metadata_db)
        Path(self.metadata_db).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS archive_records (
                archive_id TEXT PRIMARY KEY, original_path TEXT NOT NULL, object_key TEXT NOT NULL,
                category TEXT NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL,
                archived_at TEXT NOT NULL, purge_after TEXT NOT NULL, state TEXT NOT NULL,
                metadata_json TEXT NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_archive_state_purge ON archive_records(state, purge_after)")
            db.execute("""CREATE TABLE IF NOT EXISTS archive_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT, archive_id TEXT NOT NULL,
                action TEXT NOT NULL, occurred_at TEXT NOT NULL, details_json TEXT NOT NULL)""")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.metadata_db, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def _audit(self, archive_id: str, action: str, details: dict[str, object] | None = None) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO archive_audit(archive_id, action, occurred_at, details_json) VALUES (?, ?, ?, ?)",
                       (archive_id, action, _iso(_utcnow()), json.dumps(details or {}, sort_keys=True)))

    def audit_trail(self, archive_id: str | None = None) -> list[dict[str, object]]:
        sql = "SELECT * FROM archive_audit"
        params: tuple[object, ...] = ()
        if archive_id is not None:
            sql += " WHERE archive_id=?"
            params = (archive_id,)
        sql += " ORDER BY audit_id"
        with self._connect() as db:
            rows = db.execute(sql, params).fetchall()
        return [{"audit_id": row["audit_id"], "archive_id": row["archive_id"],
                 "action": row["action"], "occurred_at": row["occurred_at"],
                 "details": json.loads(row["details_json"])} for row in rows]

    @staticmethod
    def _digest(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def archive(self, source: str | Path, *, category: str, retention_days: int = 30,
                metadata: dict[str, object] | None = None, remove_source: bool = True) -> ArchiveRecord:
        """Copy an artifact into recycle storage and register it before unlinking source."""
        path = Path(source).resolve(strict=True)
        if not path.is_file():
            raise ValueError("Only regular files can be archived")
        if not category.strip():
            raise ValueError("category must not be empty")
        if retention_days < 0:
            raise ValueError("retention_days must be >= 0")
        archive_id = str(uuid.uuid4())
        key = f"{category.strip().replace('/', '_')}/{archive_id}/{path.name}"
        now = _utcnow()
        purge_after = now + timedelta(days=retention_days)
        sha256 = self._digest(path)
        self.backend.put(key, path)
        try:
            with self._connect() as db:
                db.execute("INSERT INTO archive_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    archive_id, str(path), key, category, sha256, path.stat().st_size,
                    _iso(now), _iso(purge_after), "ARCHIVED",
                    json.dumps(metadata or {}, sort_keys=True, separators=(",", ":"))))
        except Exception:
            self.backend.delete(key)
            raise
        self._audit(archive_id, "ARCHIVE", {"category": category, "sha256": sha256,
                                             "retention_days": retention_days, "source_removed": remove_source})
        if remove_source:
            path.unlink()
        record = self.get(archive_id)
        assert record is not None
        return record

    def get(self, archive_id: str) -> ArchiveRecord | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM archive_records WHERE archive_id=?", (archive_id,)).fetchone()
        return ArchiveRecord(**dict(row)) if row else None

    def list_records(self, *, state: str | None = None, category: str | None = None) -> list[ArchiveRecord]:
        sql, params = "SELECT * FROM archive_records WHERE 1=1", []
        if state:
            sql += " AND state=?"; params.append(state.upper())
        if category:
            sql += " AND category=?"; params.append(category)
        sql += " ORDER BY archived_at DESC"
        with self._connect() as db:
            rows = db.execute(sql, params).fetchall()
        return [ArchiveRecord(**dict(row)) for row in rows]

    def restore(self, archive_id: str, destination: str | Path | None = None) -> Path:
        record = self.get(archive_id)
        if record is None or record.state not in {"ARCHIVED", "RESTORED"}:
            raise ArchiveNotFound(f"Archive record is not restorable: {archive_id}")
        target = Path(destination).resolve() if destination is not None else Path(record.original_path)
        if target.exists():
            raise FileExistsError(f"Refusing to overwrite existing path: {target}")
        self.backend.restore(record.object_key, target)
        if self._digest(target) != record.sha256:
            target.unlink(missing_ok=True)
            raise ArchiveError("Restored object failed SHA-256 integrity verification")
        with self._connect() as db:
            db.execute("UPDATE archive_records SET state='RESTORED' WHERE archive_id=?", (archive_id,))
        self._audit(archive_id, "RESTORE", {"destination": str(target)})
        return target

    def permanent_delete(self, archive_id: str, *, now: datetime | None = None,
                         require_expired_retention: bool = True) -> None:
        """Permanently remove archived data only after retention has elapsed by default."""
        record = self.get(archive_id)
        if record is None:
            raise ArchiveNotFound(archive_id)
        if record.state == "PURGED":
            return
        if record.state not in {"ARCHIVED", "RESTORED"}:
            raise ArchiveError(f"Cannot purge archive in state {record.state}")
        current = now or _utcnow()
        if require_expired_retention and current < _parse(record.purge_after):
            raise RetentionError(f"Retention expires at {record.purge_after}")
        self.backend.delete(record.object_key)
        with self._connect() as db:
            db.execute("UPDATE archive_records SET state='PURGED' WHERE archive_id=?", (archive_id,))
        self._audit(archive_id, "PURGE", {"retention_enforced": require_expired_retention})

    def purge_expired(self, *, now: datetime | None = None, limit: int = 100) -> list[str]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        current = _iso(now or _utcnow())
        with self._connect() as db:
            rows = db.execute("SELECT archive_id FROM archive_records WHERE state='ARCHIVED' AND purge_after<=? ORDER BY purge_after LIMIT ?", (current, limit)).fetchall()
        purged: list[str] = []
        for row in rows:
            self.permanent_delete(row["archive_id"], now=_parse(current))
            purged.append(row["archive_id"])
        return purged


def archive_file(source: str | Path, archive_root: str | Path, metadata_db: str | Path, *,
                 category: str, retention_days: int = 30, metadata: dict[str, object] | None = None) -> ArchiveRecord:
    """Convenience helper using a local recycle directory."""
    return ArchiveManager(FilesystemObjectBackend(archive_root), metadata_db).archive(
        source, category=category, retention_days=retention_days, metadata=metadata)
