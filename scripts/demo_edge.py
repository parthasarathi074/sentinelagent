import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

sys.path.insert(0, str(SRC))

from sentinelagent.edge import SentinelEdge
from sentinelagent.models import AgentRecord, EdgeRequest
from sentinelagent.registry import AgentRegistry


def main() -> None:
    registry = AgentRegistry()

    registry.register(
        AgentRecord(
            logical_agent_id="agent-A",
            runtime_agent_id="runtime-A-001",
            role="ingest",
            version="1.0",
            permissions={"message:send"},
            allowed_targets={"agent-B"},
            credentials_reference="cred-A-001",
        )
    )

    registry.register(
        AgentRecord(
            logical_agent_id="agent-B",
            runtime_agent_id="runtime-B-001",
            role="validator",
            version="1.0",
            permissions={"validation:execute"},
            allowed_targets={"agent-A"},
            credentials_reference="cred-B-001",
        )
    )

    registry.activate("runtime-A-001")
    registry.activate("runtime-B-001")

    events = []

    edge = SentinelEdge(
        registry=registry,
        event_sink=events.append,
    )

    print("\n1. Normal authorized communication")

    request = EdgeRequest(
        request_id="req-001",
        trace_id="trace-001",
        edge_id="edge-agent-A-agent-B",
        source_runtime_id="runtime-A-001",
        target_runtime_id="runtime-B-001",
        action="SEND_MESSAGE",
        requested_permission="message:send",
        resource_id="record-001",
    )

    result = edge.handle(request)

    print("Decision:", result.decision.value)
    print("Reason:", result.reason)

    print("\n2. Unauthorized action")

    bad_request = EdgeRequest(
        request_id="req-002",
        trace_id="trace-002",
        edge_id="edge-agent-A-agent-B",
        source_runtime_id="runtime-A-001",
        target_runtime_id="runtime-B-001",
        action="MODIFY_DATA",
        requested_permission="data:write",
        resource_id="record-001",
    )

    result = edge.handle(bad_request)

    print("Decision:", result.decision.value)
    print("Reason:", result.reason)

    print("\n3. Quarantine Agent B")

    registry.quarantine("runtime-B-001")

    quarantine_request = EdgeRequest(
        request_id="req-003",
        trace_id="trace-003",
        edge_id="edge-agent-A-agent-B",
        source_runtime_id="runtime-A-001",
        target_runtime_id="runtime-B-001",
        action="SEND_MESSAGE",
        requested_permission="message:send",
        resource_id="record-001",
    )

    result = edge.handle(quarantine_request)

    print("Decision:", result.decision.value)
    print("Reason:", result.reason)

    print("\n4. Create clean replacement")

    replacement = registry.create_replacement(
        quarantined_runtime_id="runtime-B-001",
        new_runtime_id="runtime-B-002",
        new_credentials_reference="cred-B-002",
    )

    print("Old runtime:", "runtime-B-001")
    print("Old status:", registry.get("runtime-B-001").status.value)

    print("New runtime:", replacement.runtime_agent_id)
    print("New status:", replacement.status.value)
    print("Replacement for:", replacement.replacement_for)

    print("\n5. Communication with replacement")

    replacement_request = EdgeRequest(
        request_id="req-004",
        trace_id="trace-004",
        edge_id="edge-agent-A-agent-B",
        source_runtime_id="runtime-A-001",
        target_runtime_id="runtime-B-002",
        action="SEND_MESSAGE",
        requested_permission="message:send",
        resource_id="record-001",
    )

    # The original A->B edge should now target the current runtime.
    replacement_request.edge_id = "edge-agent-A-agent-B"

    result = edge.handle(replacement_request)

    print("Decision:", result.decision.value)
    print("Reason:", result.reason)

    print("\nCollected security events:")

    for event in events:
        print(json.dumps(event, indent=2))


if __name__ == "__main__":
    main()