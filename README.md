# SentinelAgent

SentinelAgent is a prototype security framework for multi-agent data pipelines. It explores how agent-to-agent communication can be monitored, analyzed, and governed using identity and permission checks, behavioral evidence, interaction graphs, policy enforcement, and recovery mechanisms.

The project is being developed incrementally, with an emphasis on reproducible experiments, automated tests, and a clear distinction between implemented functionality and future goals.

## Project Status

**Current stage:** Prototype development

The repository contains working components for agent registration and lifecycle management, inter-agent authorization, event telemetry, batch analysis, collusion assessment, policy enforcement, replacement, and recovery.

The current implementation is primarily in-memory. SentinelAgent should be treated as a development and research prototype, not a production-ready security platform.

## Current Capabilities

- **Agent registry:** Tracks logical and runtime agent identities, permissions, allowed targets, statuses, and replacement relationships.
- **Registry integrity:** Returns defensive copies of agent records to prevent callers from directly modifying authoritative registry state through returned objects.
- **Inter-agent authorization:** Checks agent status, requested permissions, and authorized targets.
- **Event telemetry:** Represents and processes inter-agent activity using structured events and an in-memory event stream.
- **Batch analysis:** Calculates interaction statistics and assigns risk levels based on observed behavior.
- **Interaction graphs:** Analyzes relationships and repeated interaction patterns among agents.
- **Collusion assessment (FR-11):** Classifies interaction evidence as `NORMAL`, `SUSPICIOUS`, `HIGH_RISK`, or `COLLUSION` and provides supporting evidence and reason codes. Repeated traffic alone is not proof of collusion. The `COLLUSION` label requires both a repeated shared-target group and reciprocal repeated interactions among group members. These labels do not independently trigger policy enforcement or quarantine.
- **Policy enforcement:** Applies the prototype's configured responses to assessed risk.
- **Quarantine and replacement:** Supports quarantining an agent runtime and creating a replacement runtime.
- **Recovery validation:** Checks important replacement and runtime-state invariants.
- **Automated tests:** Uses pytest to test registry, analysis, enforcement, replacement, recovery, and other component behavior.

These capabilities are implemented to varying levels of completeness. The source code and automated tests are the best references for the current behavior of each component.

## Planned Improvements

The following remain development goals or require further validation before they can be considered production capabilities:

- A realistic end-to-end multi-agent pipeline simulation
- More comprehensive temporal and coordinated-attack analysis
- Durable event storage and persistent audit history
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

The repository root currently has no `requirements.txt` file. Therefore, this README does not provide a dependency-installation command. Dependency instructions should be added after the project's actual dependencies and installation method have been verified.

## Development Principles

1. Build the system in small, reproducible stages.
2. Establish reliable telemetry before relying on detection results.
3. Introduce controlled attack scenarios and evaluate their effects.
4. Distinguish observed evidence from confirmed malicious behavior.
5. Validate enforcement, quarantine, and recovery through automated tests.
6. Document implemented behavior separately from planned capabilities.
7. Do not claim durability, scalability, or credential security until those properties have been implemented and tested.

## Project Scope

SentinelAgent is an evolving prototype for experimentation and research into multi-agent pipeline security. Security decisions and recovery behavior should be evaluated in a controlled environment before any production use.