from datetime import datetime
from typing import Annotated, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from signalforge.incidents.models import IncidentSeverity, IncidentStatus

Source = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]
Title = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]
Description = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2000),
]
SourceFilter = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]
Limit = Annotated[int, Field(ge=1, le=100)]
Offset = Annotated[int, Field(ge=0)]


class IncidentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Source
    title: Title
    description: Description | None = None
    severity: IncidentSeverity
    occurred_at: AwareDatetime


class IncidentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    source: str
    title: str
    description: str | None
    severity: IncidentSeverity
    status: IncidentStatus
    acknowledged_at: datetime | None
    acknowledged_by_user_id: UUID | None
    resolved_at: datetime | None
    resolved_by_user_id: UUID | None
    occurred_at: datetime
    created_at: datetime
    updated_at: datetime


class IncidentListQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: IncidentStatus | None = None
    severity: IncidentSeverity | None = None
    source: SourceFilter | None = None
    occurred_from: AwareDatetime | None = None
    occurred_to: AwareDatetime | None = None
    limit: Limit = 20
    offset: Offset = 0

    @model_validator(mode="after")
    def validate_occurred_range(self) -> Self:
        if (
            self.occurred_from is not None
            and self.occurred_to is not None
            and self.occurred_from > self.occurred_to
        ):
            raise ValueError("occurred_from must not be after occurred_to")
        return self


class IncidentListResponse(BaseModel):
    items: list[IncidentResponse]
    limit: int
    offset: int
    total: int
