from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utc_now() -> str:
    """Return a timezone-aware UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


class AgentStatus(str, Enum):
    REGISTERED = "REGISTERED"
    ACTIVE = "ACTIVE"
    SUSPICIOUS = "SUSPICIOUS"
    QUARANTINED = "QUARANTINED"


class EdgeDecision(str, Enum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    RESTRICT = "RESTRICT"
    THROTTLE = "THROTTLE"


@dataclass
class AgentRecord:
    logical_agent_id: str
    runtime_agent_id: str
    role: str
    version: str
    permissions: set[str]
    allowed_targets: set[str]

    status: AgentStatus = AgentStatus.REGISTERED

    credentials_reference: str | None = None

    created_at: str = field(default_factory=utc_now)
    activated_at: str | None = None
    quarantined_at: str | None = None

    replacement_for: str | None = None
    replaced_by: str | None = None


@dataclass
class EdgeRequest:
    request_id: str
    trace_id: str
    edge_id: str

    source_runtime_id: str
    target_runtime_id: str

    action: str
    requested_permission: str

    resource_id: str | None = None
    payload_hash: str | None = None

    timestamp: str = field(default_factory=utc_now)
    protocol_version: str = "1.0"


@dataclass
class EdgeResult:
    decision: EdgeDecision
    reason: str
    event: dict[str, Any]