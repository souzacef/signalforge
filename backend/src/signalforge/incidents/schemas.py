from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, StringConstraints

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
