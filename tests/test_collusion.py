from sentinelagent.collusion import (
    COLLUSION, HIGH_RISK, NORMAL, SUSPICIOUS, CollusionAssessor,
)


def test_ordinary_traffic_is_normal():
    result = CollusionAssessor().assess({"a": {"b": 1}, "b": {"c": 1}})
    assert result["state"] == NORMAL
    assert result["evidence"]["repeated_interactions"] == []


def test_repetition_alone_is_suspicious_not_collusion():
    result = CollusionAssessor().assess({"a": {"b": 3}})
    assert result["state"] == SUSPICIOUS
    assert "REPEATED_INTERACTIONS_OBSERVED_NOT_PROOF_OF_COLLUSION" in result["reasons"]


def test_repeated_shared_target_group_is_high_risk():
    result = CollusionAssessor().assess({"a": {"x": 2}, "b": {"x": 4}})
    assert result["state"] == HIGH_RISK
    assert result["evidence"]["shared_target_groups"][0]["member_runtime_ids"] == ["a", "b"]


def test_group_plus_reciprocal_repeats_is_collusion_label():
    result = CollusionAssessor().assess({
        "a": {"x": 2, "b": 2}, "b": {"x": 2, "a": 3}
    })
    assert result["state"] == COLLUSION
    assert "SHARED_TARGET_AND_RECIPROCAL_INTERACTION_SIGNALS_COMBINED" in result["reasons"]


def test_malformed_inputs_are_ignored_safely():
    result = CollusionAssessor().assess({
        "": {"b": 3}, "a": None, "b": {"c": True, "d": -1, None: 2, "e": "2"}
    })
    assert result["state"] == NORMAL
    assert result["evidence"]["repeated_interactions"] == []


def test_graph_snapshot_input_supported():
    result = CollusionAssessor().assess({"edges": [
        {"source_runtime_id": "a", "target_runtime_id": "b", "interaction_count": 2}
    ]})
    assert result["state"] == SUSPICIOUS
