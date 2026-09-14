from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict

from signalforge.remediation.models import RemediationActionKind
from signalforge.remediation.targets import ServiceTarget


class RemediationExecutionRequestedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    proposal_id: UUID
    action_kind: RemediationActionKind
    target: ServiceTarget


class RemediationExecutionRequested(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: Literal["remediation.execution.requested"] = (
        "remediation.execution.requested"
    )
    event_version: Literal[1] = 1
    occurred_at: AwareDatetime
    aggregate_id: UUID
    payload: RemediationExecutionRequestedPayload
