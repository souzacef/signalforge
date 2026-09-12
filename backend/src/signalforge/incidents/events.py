from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict

from signalforge.incidents.models import IncidentSeverity


class IncidentCreatedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str
    title: str
    description: str | None
    severity: IncidentSeverity
    incident_occurred_at: AwareDatetime


class IncidentCreated(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: Literal["incident.created"] = "incident.created"
    event_version: Literal[1] = 1
    occurred_at: AwareDatetime
    aggregate_id: UUID
    payload: IncidentCreatedPayload
