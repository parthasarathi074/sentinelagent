# SentinelAgent Documentation

This directory contains architecture, implementation, runtime, deployment, and requirements documentation for SentinelAgent.

SentinelAgent is a prototype security framework for multi-agent data pipelines. Documentation distinguishes current prototype capabilities from optional integrations and future production requirements.

## Getting started

- [Main README](../README.md) – Project overview, capabilities, installation, and development commands.
- [Contributing](../CONTRIBUTING.md) – Development setup, checks, and contribution guidelines.

## Architecture and event processing

- [Event Stream Architecture](event-stream-architecture.md) – Event flow, event-stream implementations, micro-batch processing, and optional Kafka integration.
- [Event Schema](event-schema.md) – Security event structure and fields.
- [Sentinel Edge Protocol](sentinel-edge-protocol.md) – Edge communication and security context.
- [Agent Registry](agent-registry.md) – Runtime registration and lifecycle management.

## Runtime and monitoring

- [Live Runtime](live-runtime.md) – Starting and using the local runtime.
- [Live Dashboard Integration](live-dashboard-integration.md) – Dashboard data and integration details.
- [Persistent Audit Trail](persistent-audit-trail.md) – Durable audit records, transactional event publishing, and lifecycle auditing.

## Deployment and operations

- [Production Deployment](production-deployment.md) – Current deployment boundaries, optional adapters, and production readiness requirements.
- [Archive Retention](archive-retention.md) – Archive, restore, and retention behavior.

## Requirements

- [Project Requirements](requirements.md) – Functional, security, and non-functional requirements.

## Implementation status

The project includes prototype implementations for edge authorization, agent registration, event processing, persistent SQLite storage, audit history, batch analysis, risk assessment, policy enforcement, and recovery.

Some integrations and distributed deployment capabilities remain optional or planned. Refer to the relevant architecture and deployment documents before assuming that a component is active in a running environment.