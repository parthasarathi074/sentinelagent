# Sentinel Edge Protocol

## 1. Purpose

The Sentinel Edge is the inline security checkpoint between two
logical agents. It observes, authenticates, authorizes, records,
and applies immediate policy decisions to inter-agent requests.

The Sentinel Edge is responsible for fast local enforcement.

Heavy behavioral, temporal, graph, and group-level collusion analysis
is performed asynchronously by the Sentinel Coordinator.

## 2. Communication Model

Every inter-agent request follows:

Agent A
    |
    v
Sentinel(A,B)
    |
    v
Agent B

The Sentinel Edge is logical. Multiple logical edges may be handled
by a shared scalable worker pool.

## 3. Request Processing

1. Receive the request.
2. Resolve the source agent identity.
3. Resolve the target agent identity.
4. Check source agent status.
5. Check target agent status.
6. Check requested permission.
7. Check communication policy.
8. Create a security event.
9. Apply fast-path policy/risk checks.
10. Produce an ALLOW, DENY, RESTRICT, or THROTTLE decision.
11. Forward the request if permitted.
12. Record the result.
13. Publish the event for asynchronous security analysis.

## 4. Request Envelope

Each request shall contain:

- request_id
- trace_id
- edge_id
- source logical agent ID
- source runtime agent ID
- target logical agent ID
- target runtime agent ID
- action type
- resource ID where applicable
- timestamp
- payload hash or payload reference
- requested permission
- protocol version

## 5. Fast-Path Decisions

### ALLOW

The request satisfies identity, permission, and immediate policy checks.

### DENY

The request violates identity, authorization, agent status, or mandatory
security policy.

### RESTRICT

The request may proceed under a restricted policy.

### THROTTLE

The request may proceed at a reduced rate.

## 6. Quarantine Behavior

If an agent is QUARANTINED:

- new requests from the agent are denied;
- requests targeting the quarantined runtime are denied;
- credentials are considered inactive;
- inter-agent communication is disabled;
- evidence and audit records are preserved.

## 7. Event Publication

Every significant interaction shall produce an event for the
asynchronous Sentinel analysis pipeline.

Events should be partitionable by edge_id.

## 8. Separation of Responsibilities

Sentinel Edge:

- identity verification
- authorization
- immediate policy enforcement
- event generation
- fast-path decisions

Sentinel Coordinator:

- behavioral analysis
- temporal analysis
- interaction graph analysis
- group-level analysis
- collusion assessment
- longer-window correlation

Policy Engine:

- maps security states and evidence to enforcement actions.

## 9. Security Logging

Security logs should not unnecessarily contain sensitive raw data.
Where possible, the system should record resource references,
payload hashes, and metadata rather than unrestricted payload contents.

## 10. Protocol Versioning

The event and request formats shall contain a protocol/schema version
so that future changes can be made without breaking historical data.

## 11. Design Principle

A single anomaly must not automatically result in a collusion decision.
Collusion assessment requires correlated evidence across interactions,
agents, and time.
