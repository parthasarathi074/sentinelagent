# SentinelAgent

SentinelAgent is a prototype security framework for multi-agent data pipelines. It explores how agent-to-agent communication can be monitored, analyzed, and governed using identity and permission checks, behavioral evidence, interaction graphs, policy enforcement, and recovery mechanisms.

The project is developed incrementally, emphasizing reproducible experiments, automated tests, and a clear distinction between implemented functionality and future goals.

## Project Status

**Current stage:** Prototype development

The repository contains components for agent registration and lifecycle management, inter-agent authorization, event telemetry, batch analysis, collusion assessment, policy enforcement, replacement, and recovery.

The project supports both in-memory event processing and SQLite-backed persistent event storage. It remains a development and research prototype, not a production-ready security platform.

## Current Capabilities

- **Agent registry:** Tracks logical and runtime agent identities, permissions, allowed targets, statuses, and replacement relationships.
- **Registry integrity:** Returns defensive copies of agent records to prevent callers from directly modifying authoritative registry state through returned objects.
- **Inter-agent authorization:** Checks agent status, requested permissions, and authorized targets.
- **Event telemetry:** Represents and processes inter-agent activity using structured events.
- **Persistent event storage:** Stores JSON-serializable events in SQLite with publication-order sequence numbers and persisted event status.
- **Batch processing:** Retrieves pending events in publication order and supports restoring failed batches for retry.
- **Interrupted-batch recovery:** On startup, returns events left in an unfinished batch to the pending queue.
- **Persistent audit history:** Retains successfully acknowledged events with a completed status and preserves their batch records for audit purposes.
- **Batch analysis:** Calculates interaction statistics and assigns risk levels based on observed behavior.
- **Interaction graphs:** Analyzes relationships and repeated interaction patterns among agents.
- **Collusion assessment (FR-11):** Classifies interaction evidence as `NORMAL`, `SUSPICIOUS`, `HIGH_RISK`, or `COLLUSION` and provides supporting evidence and reason codes. Repeated traffic alone is not proof of collusion. The `COLLUSION` label requires both a repeated shared-target group and reciprocal repeated interactions among group members. These labels do not independently trigger policy enforcement or quarantine.
- **Policy enforcement:** Applies the prototype's configured responses to assessed risk.
- **Quarantine and replacement:** Supports quarantining an agent runtime and creating a replacement runtime.
- **Recovery validation:** Checks important replacement and runtime-state invariants.
- **Automated tests:** Uses pytest to test registry, analysis, enforcement, replacement, recovery, event persistence, batch retry, and other implemented behavior.

These capabilities are implemented to varying levels of completeness. The source code and automated tests are the best references for the current behavior of each component.

## Persistent Event Processing: Guarantees and Limitations

The SQLite-backed event stream provides durable event storage and recovery of unfinished batches across restarts. Successfully acknowledged events remain stored for audit purposes.

Processing follows **at-least-once delivery semantics**, not exactly-once processing. If a handler performs its work but the process stops before the batch is acknowledged, the events may be processed again after recovery. Handlers should therefore be designed to tolerate retries when duplicate side effects would be harmful.

Persistent event storage does not, by itself, guarantee exactly-once side effects, concurrent processing safety, persistent quarantine state, or production-grade security.

## Planned Improvements

The following remain development goals or require further validation before they can be considered production capabilities:

- A realistic end-to-end multi-agent pipeline simulation
- More comprehensive temporal and coordinated-attack analysis
- Persistent quarantine and recovery state
- Integration with a real credential-rotation system
- Concurrent and scalable event processing
- A security audit dashboard
- Broader controlled-attack experiments and evaluation

## Repository Structure

```text
sentinelagent/
├── agents/       # Agent implementations
├── configs/      # Configuration files
├── detection/    # Detection and risk-analysis components
├── docs/         # Architecture and project documentation
├── enforcement/  # Policy, quarantine, replacement, and recovery
├── scripts/      # Development and experiment scripts
├── src/          # Core application code
├── tests/        # Automated tests
├── .gitattributes
├── .gitignore
└── README.md
```

This is a high-level overview of the repository structure. Other files may exist within the listed directories.

## Development Setup

Run commands from the repository root in PowerShell.

### Run the automated tests

```powershell
python -m pytest -q
```

The test suite verifies the behaviors covered by the existing tests. Passing tests do not guarantee that every attack scenario is detected or that the prototype is production-ready.

### Dependencies

The repository root currently has no `requirements.txt` file. Dependency installation instructions should be added after the project's actual dependencies and installation method have been verified.

## Development Principles

1. Build the system in small, reproducible stages.
2. Establish reliable telemetry before relying on detection results.
3. Introduce controlled attack scenarios and evaluate their effects.
4. Distinguish observed evidence from confirmed malicious behavior.
5. Validate enforcement, quarantine, and recovery through automated tests.
6. Document implemented behavior separately from planned capabilities.
7. Do not claim durability, scalability, or credential security beyond the properties actually implemented and tested.

## Project Scope

SentinelAgent is an evolving prototype for experimentation and research into multi-agent pipeline security. Security decisions and recovery behavior should be evaluated in a controlled environment before any production use.
