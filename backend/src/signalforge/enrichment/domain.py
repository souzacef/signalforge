from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority

BoundedText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=500),
]


class EnrichmentCategory(StrEnum):
    AVAILABILITY = "availability"
    PERFORMANCE = "performance"
    SECURITY = "security"
    CAPACITY = "capacity"
    DEPENDENCY = "dependency"
    DEPLOYMENT = "deployment"
    DATA = "data"
    UNKNOWN = "unknown"


class EnrichmentInput(BaseModel):
    """Validated durable snapshot supplied to an enrichment provider."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    incident_id: UUID
    source: str
    title: str
    description: str | None
    original_severity: IncidentSeverity
    priority: TriagePriority
    requires_human_review: bool
    incident_occurred_at: AwareDatetime


class EnrichmentResult(BaseModel):
    """Strict advisory result accepted from any enrichment provider."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    summary: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=1000),
    ]
    category: EnrichmentCategory
    suspected_component: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, min_length=1, max_length=200),
        ]
        | None
    ) = None
    investigation_steps: Annotated[list[BoundedText], Field(min_length=1, max_length=5)]
