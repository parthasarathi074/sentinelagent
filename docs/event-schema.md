# SentinelAgent Event Schema

## 1. Purpose

This document defines the common event envelope used by SentinelAgent
to record agent interactions and security-relevant activity.

## 2. Common Event Fields

Every event should contain:

- event_id
- event_type
- schema_version
- timestamp
- edge_id where applicable
- source_agent
- target_agent where applicable
- action
- resource_id where applicable
- authorization result
- policy decision
- request_id
- trace_id

## 3. Agent Identity

The event should identify both:

- logical agent ID
- runtime agent ID

This allows SentinelAgent to distinguish stable pipeline roles from
specific runtime instances.

## 4. Event Types

Initial event types:

- INTER_AGENT_REQUEST
- INTER_AGENT_RESPONSE
- MESSAGE
- TOOL_CALL
- DATA_READ
- DATA_WRITE
- VALIDATION
- APPROVAL
- REJECTION
- AUTHORIZATION
- POLICY_VIOLATION
- AGENT_START
- AGENT_STOP
- AGENT_QUARANTINE
- AGENT_REPLACEMENT
- PIPELINE_RECOVERY

## 5. Authorization Record

The event should contain:

- requested_permission
- allowed
- policy_id
- decision_reason

## 6. Correlation

request_id identifies one request.

trace_id connects related events across the pipeline.

edge_id identifies the logical source-target communication edge.

## 7. Data Protection

Events should avoid unnecessary storage of sensitive raw payloads.

Possible representations include:

- payload_hash
- payload_reference
- resource_id
- metadata

## 8. Example

{
  "event_id": "evt-10001",
  "event_type": "INTER_AGENT_REQUEST",
  "schema_version": "1.0",

  "timestamp": "2026-09-27T10:00:00Z",

  "edge_id": "edge-agent-A-agent-B",

  "source_agent": {
    "logical_id": "agent-A",
    "runtime_id": "runtime-A-01"
  },

  "target_agent": {
    "logical_id": "agent-B",
    "runtime_id": "runtime-B-02"
  },

  "action": "VALIDATE_RECORD",
  "resource_id": "record-781",

  "authorization": {
    "requested_permission": "validation:execute",
    "allowed": true,
    "policy_id": "policy-validator-001",
    "decision_reason": "authorized"
  },

  "decision": "ALLOW",

  "correlation": {
    "request_id": "req-10001",
    "trace_id": "trace-50001"
  }
}

## 9. Partitioning Principle

Events associated with the same logical edge should be partitionable
by edge_id so that related interactions can be processed together
while independent edges can be processed in parallel.

## 10. Versioning

Schema versions must be recorded with events so that historical event
records remain interpretable after the system evolves.
