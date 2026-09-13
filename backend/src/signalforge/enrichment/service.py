"""Read persisted enrichment snapshots without invoking the provider."""

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from signalforge.enrichment.models import TriageEnrichment
from signalforge.enrichment.schemas import EnrichmentListQuery
from signalforge.incidents import service as incident_service
from signalforge.incidents.errors import IncidentNotFoundError


async def require_incident(session: AsyncSession, incident_id: UUID) -> None:
    if await incident_service.get_incident(session, incident_id) is None:
        raise IncidentNotFoundError


def _enrichment_filters(
    query: EnrichmentListQuery,
    incident_id: UUID | None,
) -> tuple[ColumnElement[bool], ...]:
    filters: list[ColumnElement[bool]] = []
    if incident_id is not None:
        filters.append(TriageEnrichment.incident_id == incident_id)
    if query.category is not None:
        filters.append(TriageEnrichment.category == query.category)
    if query.provider is not None:
        filters.append(TriageEnrichment.provider == query.provider)
    if query.model is not None:
        filters.append(TriageEnrichment.model == query.model)
    if query.created_from is not None:
        filters.append(TriageEnrichment.created_at >= query.created_from)
    if query.created_to is not None:
        filters.append(TriageEnrichment.created_at <= query.created_to)
    return tuple(filters)


async def list_enrichments(
    session: AsyncSession,
    query: EnrichmentListQuery,
    *,
    incident_id: UUID | None = None,
) -> tuple[list[TriageEnrichment], int]:
    filters = _enrichment_filters(query, incident_id)
    total = await session.scalar(
        select(func.count()).select_from(TriageEnrichment).where(*filters)
    )
    enrichments = await session.scalars(
        select(TriageEnrichment)
        .where(*filters)
        .order_by(
            TriageEnrichment.created_at.desc(),
            TriageEnrichment.request_event_id.desc(),
        )
        .limit(query.limit)
        .offset(query.offset)
    )
    return list(enrichments), total or 0
