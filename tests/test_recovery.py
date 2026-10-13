import json
import logging
import sqlite3

import pytest

from sentinelagent.analysis_pipeline import AnalysisPipeline
from sentinelagent.models import AgentRecord, AgentStatus
from sentinelagent.policy import PolicyAction, PolicyDecision
from sentinelagent.recovery import PipelineRecoveryManager, RecoveryState
from sentinelagent.registry import AgentRegistry


def set_stored_agent_field(
    registry: AgentRegistry,
    runtime_agent_id: str,
    field_name: str,
    value: object,
) -> None:
    """Alter internal registry state for negative tests only."""
    agents = object.__getattribute__(registry, "_agents")
    agent = agents[runtime_agent_id]
    setattr(agent, field_name, value)


def populate_registry(registry: AgentRegistry) -> tuple[AgentRecord, AgentRecord]:
    """Register an old runtime and create its replacement."""
    old = AgentRecord(
        logical_agent_id="validator-01",
        runtime_agent_id="runtime-validator-old",
        role="validator",
        version="1.0",
        permissions={"validate"},
        allowed_targets={"processor-01"},
        status=AgentStatus.ACTIVE,
        credentials_reference="credentials-old",
    )
    registry.register(old)
    registry.quarantine(old.runtime_agent_id)
    replacement = registry.create_replacement(
        quarantined_runtime_id=old.runtime_agent_id,
        new_runtime_id="runtime-validator-new",
        new_credentials_reference="credentials-new",
    )
    return old, replacement


def make_registry() -> tuple[AgentRegistry, AgentRecord, AgentRecord]:
    registry = AgentRegistry()
    old, replacement = populate_registry(registry)
    return registry, old, replacement


def make_pipeline_registry() -> tuple[AgentRegistry, AgentRecord]:
    registry = AgentRegistry()
    agent = AgentRecord(
        logical_agent_id="validator-01",
        runtime_agent_id="runtime-validator-01",
        role="validator",
        version="1.0",
        permissions={"validate"},
        allowed_targets={"processor-01"},
        status=AgentStatus.ACTIVE,
        credentials_reference="credentials-original",
    )
    registry.register(agent)
    return registry, agent


def test_recovery_verifies_and_records_success() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    result = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    assert result.state == RecoveryState.RECOVERED
    assert result.logical_agent_id == "validator-01"
    assert result.old_runtime_id == old.runtime_agent_id
    assert result.replacement_runtime_id == replacement.runtime_agent_id
    assert result.started_at is not None
    assert result.completed_at is not None
    assert result.attempt == 1


def test_recovery_logs_success(caplog: pytest.LogCaptureFixture) -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    with caplog.at_level(logging.INFO, logger="sentinelagent.recovery"):
        result = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    assert result.state == RecoveryState.RECOVERED
    assert any("recovery_started" in message for message in caplog.messages)
    assert any("recovery_finished" in message for message in caplog.messages)
    assert any(old.runtime_agent_id in message for message in caplog.messages)
    assert any(replacement.runtime_agent_id in message for message in caplog.messages)
    assert any("state=RECOVERED" in message for message in caplog.messages)


def test_recovery_logs_failure(caplog: pytest.LogCaptureFixture) -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry, replacement.runtime_agent_id, "status", AgentStatus.REGISTERED
    )
    manager = PipelineRecoveryManager(registry)

    with caplog.at_level(logging.WARNING, logger="sentinelagent.recovery"):
        result = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    assert result.state == RecoveryState.FAILED
    assert any("recovery_finished" in message for message in caplog.messages)
    assert any("state=FAILED" in message for message in caplog.messages)
    assert any("ACTIVE" in message for message in caplog.messages)


def test_recovery_logs_idempotent_success(caplog: pytest.LogCaptureFixture) -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)
    manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    with caplog.at_level(logging.INFO, logger="sentinelagent.recovery"):
        result = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    assert result.state == RecoveryState.RECOVERED
    assert result.attempt == 1
    assert any("recovery_skipped" in message for message in caplog.messages)
    assert any("reason=already_recovered" in message for message in caplog.messages)


def test_recovery_is_idempotent_after_success() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    first = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
    second = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    assert second is first
    assert second.state == RecoveryState.RECOVERED
    assert second.attempt == 1


def test_recovery_retries_after_repeated_failures() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)
    set_stored_agent_field(
        registry, replacement.runtime_agent_id, "status", AgentStatus.REGISTERED
    )

    first = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
    assert first.state == RecoveryState.FAILED
    assert first.attempt == 1
    assert first.completed_at is not None
    assert "ACTIVE" in (first.reason or "")

    second = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
    assert second is first
    assert second.state == RecoveryState.FAILED
    assert second.attempt == 2

    set_stored_agent_field(
        registry, replacement.runtime_agent_id, "status", AgentStatus.ACTIVE
    )
    third = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    assert third is first
    assert third.state == RecoveryState.RECOVERED
    assert third.attempt == 3
    assert third.completed_at is not None
    assert third.reason is None


def test_recovery_rejects_different_replacement_for_existing_recovery() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)
    manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    with pytest.raises(ValueError, match="different replacement"):
        manager.recover(old.runtime_agent_id, "runtime-validator-another")


def test_recovery_fails_if_old_runtime_is_not_quarantined() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(registry, old.runtime_agent_id, "status", AgentStatus.ACTIVE)
    result = PipelineRecoveryManager(registry).recover(
        old.runtime_agent_id, replacement.runtime_agent_id
    )
    assert result.state == RecoveryState.FAILED
    assert "QUARANTINED" in (result.reason or "")


def test_recovery_fails_if_replacement_is_not_active() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry, replacement.runtime_agent_id, "status", AgentStatus.REGISTERED
    )
    result = PipelineRecoveryManager(registry).recover(
        old.runtime_agent_id, replacement.runtime_agent_id
    )
    assert result.state == RecoveryState.FAILED
    assert "ACTIVE" in (result.reason or "")


def test_recovery_fails_for_wrong_logical_agent() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry, replacement.runtime_agent_id, "logical_agent_id", "different-agent"
    )
    result = PipelineRecoveryManager(registry).recover(
        old.runtime_agent_id, replacement.runtime_agent_id
    )
    assert result.state == RecoveryState.FAILED
    assert "different logical agents" in (result.reason or "")


def test_recovery_fails_when_relationship_is_inconsistent() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(registry, old.runtime_agent_id, "replaced_by", None)
    result = PipelineRecoveryManager(registry).recover(
        old.runtime_agent_id, replacement.runtime_agent_id
    )
    assert result.state == RecoveryState.FAILED
    assert "relationship" in (result.reason or "")


def test_recovery_requires_exactly_one_active_runtime() -> None:
    registry, old, replacement = make_registry()
    extra = AgentRecord(
        logical_agent_id=old.logical_agent_id,
        runtime_agent_id="runtime-validator-extra",
        role=old.role,
        version=old.version,
        permissions=set(old.permissions),
        allowed_targets=set(old.allowed_targets),
        status=AgentStatus.REGISTERED,
        credentials_reference="credentials-extra",
    )
    registry.register(extra)
    set_stored_agent_field(registry, extra.runtime_agent_id, "status", AgentStatus.ACTIVE)

    result = PipelineRecoveryManager(registry).recover(
        old.runtime_agent_id, replacement.runtime_agent_id
    )
    assert result.state == RecoveryState.FAILED
    assert "exactly one ACTIVE" in (result.reason or "")


def test_recovery_requires_fresh_credentials() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry,
        replacement.runtime_agent_id,
        "credentials_reference",
        old.credentials_reference,
    )
    result = PipelineRecoveryManager(registry).recover(
        old.runtime_agent_id, replacement.runtime_agent_id
    )
    assert result.state == RecoveryState.FAILED
    assert "different credentials" in (result.reason or "")


def test_recovery_snapshot_is_json_safe() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)
    manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)

    snapshot = manager.snapshot_json()
    assert snapshot[old.runtime_agent_id]["state"] == "RECOVERED"
    assert (
        snapshot[old.runtime_agent_id]["replacement_runtime_id"]
        == replacement.runtime_agent_id
    )


def test_pipeline_process_runs_recovery_after_replacement(monkeypatch) -> None:
    registry, agent = make_pipeline_registry()
    pipeline = AnalysisPipeline(registry=registry)
    decision = PolicyDecision(
        runtime_agent_id=agent.runtime_agent_id,
        risk_score=0.95,
        risk_level="HIGH",
        action=PolicyAction.QUARANTINE,
        reasons=["test recovery integration"],
    )
    monkeypatch.setattr(
        pipeline.policy_engine,
        "evaluate_analysis",
        lambda analysis_result: {agent.runtime_agent_id: decision},
    )

    result = pipeline.process([{}], batch_id="recovery-integration-test")
    replacement_id = result["replacement_runtime_ids"][agent.runtime_agent_id]
    recovery = result["recovery_results"][agent.runtime_agent_id]

    assert result["enforced_actions"][agent.runtime_agent_id] == PolicyAction.QUARANTINE.value
    assert recovery["state"] == "RECOVERED"
    assert recovery["replacement_runtime_id"] == replacement_id
    assert registry.get(agent.runtime_agent_id).status == AgentStatus.QUARANTINED
    assert registry.get(replacement_id).status == AgentStatus.ACTIVE

    active_runtimes = [
        runtime
        for runtime in registry.all_agents()
        if runtime.logical_agent_id == agent.logical_agent_id
        and runtime.status == AgentStatus.ACTIVE
    ]
    assert len(active_runtimes) == 1
    assert active_runtimes[0].runtime_agent_id == replacement_id


def test_pipeline_snapshot_exposes_recovery_results() -> None:
    registry, agent = make_pipeline_registry()
    pipeline = AnalysisPipeline(registry=registry)
    decision = PolicyDecision(
        runtime_agent_id=agent.runtime_agent_id,
        risk_score=0.95,
        risk_level="HIGH",
        action=PolicyAction.QUARANTINE,
        reasons=["test recovery snapshot"],
    )
    pipeline.policy_enforcer.enforce({agent.runtime_agent_id: decision})
    replacement_id = pipeline.policy_enforcer.replacement_runtime_ids[agent.runtime_agent_id]

    pipeline.recovery_manager.recover(
        old_runtime_id=agent.runtime_agent_id,
        replacement_runtime_id=replacement_id,
    )
    snapshot = pipeline.snapshot()
    assert snapshot["recovery_results"][agent.runtime_agent_id]["state"] == "RECOVERED"


def test_recovery_state_persists_after_manager_restart(tmp_path) -> None:
    registry_path = tmp_path / "registry.db"
    recovery_path = tmp_path / "recovery.db"

    registry = AgentRegistry(database_path=registry_path)
    old, replacement = populate_registry(registry)
    manager = PipelineRecoveryManager(registry, database_path=recovery_path)

    first = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
    assert first.state == RecoveryState.RECOVERED
    assert first.attempt == 1
    started_at = first.started_at
    completed_at = first.completed_at
    manager.close()
    registry.close()

    registry = AgentRegistry(database_path=registry_path)
    manager = PipelineRecoveryManager(registry, database_path=recovery_path)
    try:
        restored = manager.get(old.runtime_agent_id)
        assert restored is not None
        assert restored.state == RecoveryState.RECOVERED
        assert restored.attempt == 1
        assert restored.started_at == started_at
        assert restored.completed_at == completed_at

        repeated = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
        assert repeated.state == RecoveryState.RECOVERED
        assert repeated.attempt == 1
    finally:
        manager.close()
        registry.close()


def test_interrupted_recovery_can_resume_after_restart(tmp_path, monkeypatch) -> None:
    registry_path = tmp_path / "registry.db"
    recovery_path = tmp_path / "recovery.db"

    registry = AgentRegistry(database_path=registry_path)
    old, replacement = populate_registry(registry)
    manager = PipelineRecoveryManager(registry, database_path=recovery_path)

    def interrupt_verification(**kwargs):
        raise RuntimeError("simulated interruption")

    try:
        monkeypatch.setattr(manager, "_verify_recovery", interrupt_verification)
        with pytest.raises(RuntimeError, match="simulated interruption"):
            manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
    finally:
        manager.close()
        registry.close()

    registry = AgentRegistry(database_path=registry_path)
    manager = PipelineRecoveryManager(registry, database_path=recovery_path)
    try:
        restored = manager.get(old.runtime_agent_id)
        assert restored is not None
        assert restored.state == RecoveryState.RECOVERING
        assert restored.attempt == 1

        resumed = manager.recover(old.runtime_agent_id, replacement.runtime_agent_id)
        assert resumed.state == RecoveryState.RECOVERED
        assert resumed.attempt == 2
        assert resumed.completed_at is not None
    finally:
        manager.close()
        registry.close()


def test_initial_checkpoint_failure_does_not_publish_record(
    tmp_path, monkeypatch
) -> None:
    registry, old, replacement = make_registry()
    database_path = tmp_path / "recovery.db"
    manager = PipelineRecoveryManager(registry, database_path=database_path)

    def fail_save(record) -> None:
        raise sqlite3.OperationalError("simulated initial write failure")

    monkeypatch.setattr(manager, "_save_record", fail_save)

    try:
        with pytest.raises(
            sqlite3.OperationalError,
            match="simulated initial write failure",
        ):
            manager.recover(
                old.runtime_agent_id,
                replacement.runtime_agent_id,
            )

        assert manager.get(old.runtime_agent_id) is None

        connection = sqlite3.connect(database_path)
        try:
            row = connection.execute(
                """
                SELECT record_json
                FROM recovery_records
                WHERE old_runtime_id = ?
                """,
                (old.runtime_agent_id,),
            ).fetchone()
            assert row is None
        finally:
            connection.close()
    finally:
        manager.close()
        registry.close()


def test_final_checkpoint_failure_leaves_recovering_state(
    tmp_path, monkeypatch
) -> None:
    registry_path = tmp_path / "registry.db"
    recovery_path = tmp_path / "recovery.db"

    registry = AgentRegistry(database_path=registry_path)
    old, replacement = populate_registry(registry)
    manager = PipelineRecoveryManager(registry, database_path=recovery_path)

    original_save = manager._save_record
    save_calls = 0

    def fail_final_save(record) -> None:
        nonlocal save_calls
        save_calls += 1

        if save_calls == 2:
            raise sqlite3.OperationalError("simulated final write failure")

        original_save(record)

    monkeypatch.setattr(manager, "_save_record", fail_final_save)

    try:
        with pytest.raises(
            sqlite3.OperationalError,
            match="simulated final write failure",
        ):
            manager.recover(
                old.runtime_agent_id,
                replacement.runtime_agent_id,
            )

        stored = manager.get(old.runtime_agent_id)
        assert stored is not None
        assert stored.state == RecoveryState.RECOVERING
        assert stored.attempt == 1
    finally:
        manager.close()
        registry.close()

    restored_registry = AgentRegistry(database_path=registry_path)
    restored_manager = PipelineRecoveryManager(
        restored_registry,
        database_path=recovery_path,
    )
    try:
        persisted = restored_manager.get(old.runtime_agent_id)
        assert persisted is not None
        assert persisted.state == RecoveryState.RECOVERING
        assert persisted.attempt == 1

        connection = sqlite3.connect(recovery_path)
        try:
            row = connection.execute(
                """
                SELECT record_json
                FROM recovery_records
                WHERE old_runtime_id = ?
                """,
                (old.runtime_agent_id,),
            ).fetchone()
            assert row is not None
            record_data = json.loads(row[0])
            assert record_data["state"] == RecoveryState.RECOVERING.value
            assert record_data["attempt"] == 1
        finally:
            connection.close()
    finally:
        restored_manager.close()
        restored_registry.close()
