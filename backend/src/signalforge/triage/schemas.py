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

from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority

SourceFilter = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]
Limit = Annotated[int, Field(ge=1, le=100)]
Offset = Annotated[int, Field(ge=0)]


class TriageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    incident_id: UUID
    event_id: UUID
    source: str
    original_severity: IncidentSeverity
    priority: TriagePriority
    requires_human_review: bool
    created_at: datetime


class TriageListQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority: TriagePriority | None = None
    original_severity: IncidentSeverity | None = None
    requires_human_review: bool | None = None
    source: SourceFilter | None = None
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


class TriageListResponse(BaseModel):
    items: list[TriageResponse]
    limit: int
    offset: int
    total: int
