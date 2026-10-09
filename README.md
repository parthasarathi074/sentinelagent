# SentinelAgent

SentinelAgent is a security framework for multi-agent data pipelines.
It places logical security guards on agent-to-agent communication edges and combines behavioral, temporal, graph, policy, and data-impact evidence to detect coordinated malicious behavior. Detected rogue agents are quarantined, preserved for forensic analysis, and replaced with clean agent instances so the pipeline can recover.

## Project Status

Phase 0 / Step 1 — Repository initialization

## Planned system capabilities

- Multi-agent data pipeline simulation
- Inter-agent Sentinel Edge Guards
- Event-driven telemetry
- Micro-batch processing
- Behavioral anomaly detection
- Temporal coordination analysis
- Interaction-graph analysis
- Policy and permission checks
- Collusion risk assessment

### Collusion assessment (FR-11)

The observational collusion assessor classifies interaction evidence as `NORMAL`,
`SUSPICIOUS`, `HIGH_RISK`, or `COLLUSION` and returns the supporting evidence and
reason codes. Repeated traffic alone is classified as suspicious, not proof of
collusion. Repeated shared-target groups raise the assessment to high risk; the
`COLLUSION` label requires both a repeated shared-target group and reciprocal
repeated interactions among group members. Results are exposed in per-batch
analysis and the accumulated `AnalysisPipeline.snapshot()`. These labels do not
independently trigger policy enforcement or quarantine.
- Rogue-agent quarantine
- Clean-agent replacement
- Pipeline recovery
- Security audit dashboard

## Repository structure

```text
sentinelagent/
├── agents/       # Agent implementations
├── configs/      # Configuration files
├── detection/    # Detection and risk-analysis components
├── docs/         # Architecture, threat model, research notes, reports
├── enforcement/  # Policy, quarantine, replacement, and recovery logic
├── scripts/      # Development and experiment scripts
├── src/          # Shared application code
├── tests/        # Automated tests
├── .gitignore
└── README.md
```

## Development principle

Build the system in small, reproducible stages. Establish the normal pipeline first, introduce controlled attack scenarios second, and implement and evaluate detection only after reliable telemetry is available.
