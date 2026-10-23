from datetime import datetime, timedelta, timezone

import pytest

from sentinelagent.archive import (
    ArchiveManager, ArchiveNotFound, FilesystemObjectBackend, RetentionError,
)


def manager(tmp_path):
    return ArchiveManager(FilesystemObjectBackend(tmp_path / "objects"), tmp_path / "catalog.sqlite3")


def test_archive_restore_preserves_bytes_and_metadata(tmp_path):
    source = tmp_path / "agent.log"
    source.write_bytes(b"security-event\n")
    archive = manager(tmp_path)
    record = archive.archive(source, category="agent-logs", retention_days=30,
                             metadata={"runtime_id": "runtime-1"})
    assert not source.exists()
    assert record.state == "ARCHIVED"
    assert record.to_dict()["metadata"]["runtime_id"] == "runtime-1"
    restored = archive.restore(record.archive_id, tmp_path / "restored.log")
    assert restored.read_bytes() == b"security-event\n"
    assert archive.get(record.archive_id).state == "RESTORED"
    assert [entry["action"] for entry in archive.audit_trail(record.archive_id)] == ["ARCHIVE", "RESTORE"]


def test_archive_refuses_overwrite_on_restore(tmp_path):
    source = tmp_path / "state.json"
    source.write_text('{"ok":true}')
    archive = manager(tmp_path)
    record = archive.archive(source, category="checkpoints")
    target = tmp_path / "already-there.json"
    target.write_text("keep")
    with pytest.raises(FileExistsError):
        archive.restore(record.archive_id, target)
    assert target.read_text() == "keep"


def test_purge_requires_expired_retention_and_is_idempotent(tmp_path):
    source = tmp_path / "old.txt"
    source.write_text("old")
    archive = manager(tmp_path)
    record = archive.archive(source, category="logs", retention_days=2)
    with pytest.raises(RetentionError):
        archive.permanent_delete(record.archive_id)
    assert archive.purge_expired(now=datetime.now(timezone.utc) + timedelta(days=3)) == [record.archive_id]
    archive.permanent_delete(record.archive_id)
    assert archive.get(record.archive_id).state == "PURGED"
    assert archive.audit_trail(record.archive_id)[-1]["action"] == "PURGE"


def test_unknown_archive_is_not_restorable(tmp_path):
    with pytest.raises(ArchiveNotFound):
        manager(tmp_path).restore("missing")


def test_archive_path_escape_is_rejected(tmp_path):
    backend = FilesystemObjectBackend(tmp_path / "objects")
    with pytest.raises(ValueError):
        backend.exists("../outside")
