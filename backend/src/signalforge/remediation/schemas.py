from datetime import datetime
from typing import Annotated, Self
from unicodedata import category
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationProposalStatus,
)

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
TargetFilter = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]
Limit = Annotated[int, Field(ge=1, le=100)]
Offset = Annotated[int, Field(ge=0)]


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


class RemediationProposalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    action_kind: RemediationActionKind
    target: str
    reason: str
    status: RemediationProposalStatus
    proposed_by_user_id: UUID
    approved_by_user_id: UUID | None
    approved_at: datetime | None
    rejected_by_user_id: UUID | None
    rejected_at: datetime | None
    rejection_reason: str | None
    created_at: datetime
    updated_at: datetime


class RemediationProposalListQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: UUID | None = None
    status: RemediationProposalStatus | None = None
    action_kind: RemediationActionKind | None = None
    target: TargetFilter | None = None
    proposed_by_user_id: UUID | None = None
    created_from: AwareDatetime | None = None
    created_to: AwareDatetime | None = None
    limit: Limit = 20
    offset: Offset = 0

    @model_validator(mode="after")
    def validate_created_range(self) -> Self:
        if (
            self.created_from is not None
            and self.created_to is not None
            and self.created_from > self.created_to
        ):
            raise ValueError("created_from must not be after created_to")
        return self


class RemediationProposalListResponse(BaseModel):
    items: list[RemediationProposalResponse]
    limit: int
    offset: int
    total: int
