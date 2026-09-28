# Agent Registry Specification

## 1. Purpose

The Agent Registry is the authoritative source of runtime agent
identity, logical role, status, permissions, lifecycle information,
and replacement history.

## 2. Logical vs Runtime Identity

### Logical Agent

Represents the stable pipeline role.

Example:

validator-01

### Runtime Agent

Represents the current execution instance.

Example:

runtime-validator-a231

A logical agent may have multiple runtime instances over its lifetime,
but only one runtime instance should normally be ACTIVE at a time.

## 3. Agent Record

Each agent record shall contain:

- logical_agent_id
- runtime_agent_id
- role
- version
- status
- permissions
- allowed_targets
- credentials_reference
- created_at
- activated_at
- quarantined_at
- replaced_at
- replacement_for

## 4. Agent Status

Supported lifecycle states:

- REGISTERED
- ACTIVE
- SUSPICIOUS
- QUARANTINED
- REPLACED

## 5. ACTIVE

An ACTIVE runtime may participate in the pipeline according to its
assigned permissions and communication policies.

## 6. SUSPICIOUS

A SUSPICIOUS runtime has elevated security attention and may be
restricted according to policy.

Suspicious status does not automatically mean confirmed collusion.

## 7. QUARANTINED

A QUARANTINED runtime:

- cannot communicate with active agents;
- cannot access protected pipeline resources;
- cannot perform new pipeline actions;
- cannot use active credentials;
- remains preserved for forensic investigation.

## 8. REPLACED

A REPLACED runtime is no longer active in the pipeline.

Its historical identity, evidence, and lifecycle information remain
available for investigation.

## 9. Replacement Rules

A replacement agent shall receive:

- a new runtime ID;
- new credentials;
- the expected logical role;
- expected permissions;
- clean initialization state.

Compromised credentials and arbitrary compromised runtime state shall
not be copied into the replacement.

## 10. Replacement Relationship

The registry shall maintain:

logical_agent_id = stable role

current_runtime_id = currently active instance

replacement_for = previous compromised runtime

Example:

logical_agent_id:
validator-01

previous runtime:
runtime-validator-9f72

current runtime:
runtime-validator-a231

## 11. Active Instance Rule

At most one runtime instance for a logical role should normally have
ACTIVE status.

Any replacement workflow must verify that duplicate active runtime
instances do not exist.

## 12. Permission Model

Each runtime inherits only the permissions assigned to its logical role
and approved policy.

Permissions should explicitly identify:

- allowed actions
- allowed resources
- allowed tools
- allowed communication targets

## 13. Quarantine Persistence

A quarantined runtime remains in the registry and forensic storage
until explicitly released by the test operator.

Quarantine is not deletion.

## 14. Lifecycle

REGISTERED
    |
    v
ACTIVE
    |
    v
SUSPICIOUS
    |
    v
QUARANTINED
    |
    v
REPLACED

## 15. Security Principle

Identity, role, permissions, and runtime status must be evaluated
before an agent is allowed to interact with another agent.
