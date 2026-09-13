"""Read-only schemas for persisted advisory enrichment snapshots."""

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

from signalforge.enrichment.domain import EnrichmentCategory

ProviderFilter = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=50)
]
ModelFilter = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)
]
Limit = Annotated[int, Field(ge=1, le=100)]
Offset = Annotated[int, Field(ge=0)]


class EnrichmentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid")

    request_event_id: UUID
    incident_id: UUID
    trigger_event_id: UUID
    provider: str
    model: str
    summary: str
    category: EnrichmentCategory
    suspected_component: str | None
    investigation_steps: list[str]
    created_at: datetime


class EnrichmentListQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: EnrichmentCategory | None = None
    provider: ProviderFilter | None = None
    model: ModelFilter | None = None
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


class GlobalEnrichmentListQuery(EnrichmentListQuery):
    incident_id: UUID | None = None


class EnrichmentListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[EnrichmentResponse]
    total: int
    limit: int
    offset: int
