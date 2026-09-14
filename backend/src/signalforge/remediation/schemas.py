from typing import Annotated
from unicodedata import category
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator

from signalforge.remediation.models import RemediationActionKind

ServiceTarget = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?$",
    ),
]
ProposalReason = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=1000),
]
RejectionReason = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=1000),
]


class RemediationProposalCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: UUID
    action_kind: RemediationActionKind
    target: ServiceTarget
    reason: ProposalReason

    @field_validator("target", mode="before")
    @classmethod
    def reject_target_control_characters(cls, value: object) -> object:
        if isinstance(value, str) and any(
            category(character) == "Cc" for character in value
        ):
            raise ValueError("target must not contain control characters")
        return value


class RemediationProposalReject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rejection_reason: RejectionReason
