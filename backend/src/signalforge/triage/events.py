from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict

from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority


class TriageEnrichmentRequestedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    trigger_event_id: UUID
    source: str
    title: str
    description: str | None
    original_severity: IncidentSeverity
    priority: TriagePriority
    requires_human_review: bool
    incident_occurred_at: AwareDatetime


class TriageEnrichmentRequested(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: Literal["triage.enrichment.requested"] = "triage.enrichment.requested"
    event_version: Literal[1] = 1
    occurred_at: AwareDatetime
    aggregate_id: UUID
    payload: TriageEnrichmentRequestedPayload
