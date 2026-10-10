import pytest

from sentinelagent.analysis_pipeline import AnalysisPipeline
from sentinelagent.models import AgentRecord, AgentStatus
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


def make_registry() -> tuple[AgentRegistry, AgentRecord, AgentRecord]:
    registry = AgentRegistry()

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

    return registry, old, replacement


def test_recovery_verifies_and_records_success() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    assert result.state == RecoveryState.RECOVERED
    assert result.logical_agent_id == "validator-01"
    assert result.old_runtime_id == old.runtime_agent_id
    assert result.replacement_runtime_id == replacement.runtime_agent_id
    assert result.started_at is not None
    assert result.completed_at is not None
    assert result.attempt == 1


def test_recovery_is_idempotent_after_success() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    first = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )
    second = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    assert second is first
    assert second.state == RecoveryState.RECOVERED
    assert second.attempt == 1


def test_recovery_rejects_different_replacement_for_existing_recovery() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    with pytest.raises(ValueError, match="different replacement"):
        manager.recover(
            old.runtime_agent_id,
            "runtime-validator-another",
        )


def test_recovery_fails_if_old_runtime_is_not_quarantined() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry,
        old.runtime_agent_id,
        "status",
        AgentStatus.ACTIVE,
    )

    manager = PipelineRecoveryManager(registry)
    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    assert result.state == RecoveryState.FAILED
    assert "QUARANTINED" in (result.reason or "")


def test_recovery_fails_if_replacement_is_not_active() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry,
        replacement.runtime_agent_id,
        "status",
        AgentStatus.REGISTERED,
    )

    manager = PipelineRecoveryManager(registry)
    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    assert result.state == RecoveryState.FAILED
    assert "ACTIVE" in (result.reason or "")


def test_recovery_fails_for_wrong_logical_agent() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry,
        replacement.runtime_agent_id,
        "logical_agent_id",
        "different-agent",
    )

    manager = PipelineRecoveryManager(registry)
    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    assert result.state == RecoveryState.FAILED
    assert "different logical agents" in (result.reason or "")


def test_recovery_fails_when_relationship_is_inconsistent() -> None:
    registry, old, replacement = make_registry()
    set_stored_agent_field(
        registry,
        old.runtime_agent_id,
        "replaced_by",
        None,
    )

    manager = PipelineRecoveryManager(registry)
    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
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
    set_stored_agent_field(
        registry,
        extra.runtime_agent_id,
        "status",
        AgentStatus.ACTIVE,
    )

    manager = PipelineRecoveryManager(registry)
    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
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

    manager = PipelineRecoveryManager(registry)
    result = manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    assert result.state == RecoveryState.FAILED
    assert "different credentials" in (result.reason or "")


def test_recovery_snapshot_is_json_safe() -> None:
    registry, old, replacement = make_registry()
    manager = PipelineRecoveryManager(registry)

    manager.recover(
        old.runtime_agent_id,
        replacement.runtime_agent_id,
    )

    snapshot = manager.snapshot_json()

    assert snapshot[old.runtime_agent_id]["state"] == "RECOVERED"
    assert (
        snapshot[old.runtime_agent_id]["replacement_runtime_id"]
        == replacement.runtime_agent_id
    )


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


def test_pipeline_process_runs_recovery_after_replacement(
    monkeypatch,
) -> None:
    registry, agent = make_pipeline_registry()
    pipeline = AnalysisPipeline(registry=registry)

    from sentinelagent.policy import PolicyAction, PolicyDecision

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
        lambda analysis_result: {
            agent.runtime_agent_id: decision,
        },
    )

    result = pipeline.process(
        [{}],
        batch_id="recovery-integration-test",
    )

    replacement_id = result["replacement_runtime_ids"][
        agent.runtime_agent_id
    ]
    recovery = result["recovery_results"][agent.runtime_agent_id]

    assert result["enforced_actions"][agent.runtime_agent_id] == (
        PolicyAction.QUARANTINE.value
    )
    assert recovery["state"] == "RECOVERED"
    assert recovery["replacement_runtime_id"] == replacement_id

    assert registry.get(agent.runtime_agent_id).status == (
        AgentStatus.QUARANTINED
    )
    assert registry.get(replacement_id).status == AgentStatus.ACTIVE

    active_runtimes = [
        runtime
        for runtime in registry.all_agents()
        if (
            runtime.logical_agent_id == agent.logical_agent_id
            and runtime.status == AgentStatus.ACTIVE
        )
    ]

    assert len(active_runtimes) == 1
    assert active_runtimes[0].runtime_agent_id == replacement_id


def test_pipeline_snapshot_exposes_recovery_results() -> None:
    registry, agent = make_pipeline_registry()
    pipeline = AnalysisPipeline(registry=registry)

    from sentinelagent.policy import PolicyAction, PolicyDecision

    decision = PolicyDecision(
        runtime_agent_id=agent.runtime_agent_id,
        risk_score=0.95,
        risk_level="HIGH",
        action=PolicyAction.QUARANTINE,
        reasons=["test recovery snapshot"],
    )

    pipeline.policy_enforcer.enforce(
        {agent.runtime_agent_id: decision}
    )

    replacement_id = pipeline.policy_enforcer.replacement_runtime_ids[
        agent.runtime_agent_id
    ]

    pipeline.recovery_manager.recover(
        old_runtime_id=agent.runtime_agent_id,
        replacement_runtime_id=replacement_id,
    )

    snapshot = pipeline.snapshot()

    assert (
        snapshot["recovery_results"][agent.runtime_agent_id]["state"]
        == "RECOVERED"
    )
