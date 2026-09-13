from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from signalforge.incidents import service as incident_service
from signalforge.incidents.errors import IncidentNotFoundError
from signalforge.triage.errors import TriageNotFoundError
from signalforge.triage.models import IncidentTriage
from signalforge.triage.schemas import TriageListQuery


async def get_incident_triage(
    session: AsyncSession,
    incident_id: UUID,
) -> IncidentTriage:
    if await incident_service.get_incident(session, incident_id) is None:
        raise IncidentNotFoundError

    triage = await session.get(IncidentTriage, incident_id)
    if triage is None:
        raise TriageNotFoundError
    return triage


def _triage_filters(query: TriageListQuery) -> tuple[ColumnElement[bool], ...]:
    filters: list[ColumnElement[bool]] = []
    if query.priority is not None:
        filters.append(IncidentTriage.priority == query.priority)
    if query.original_severity is not None:
        filters.append(IncidentTriage.original_severity == query.original_severity)
    if query.requires_human_review is not None:
        filters.append(
            IncidentTriage.requires_human_review == query.requires_human_review
        )
    if query.source is not None:
        filters.append(IncidentTriage.source == query.source)
    if query.created_from is not None:
        filters.append(IncidentTriage.created_at >= query.created_from)
    if query.created_to is not None:
        filters.append(IncidentTriage.created_at <= query.created_to)
    return tuple(filters)


async def list_triage(
    session: AsyncSession,
    query: TriageListQuery,
) -> tuple[list[IncidentTriage], int]:
    filters = _triage_filters(query)
    total = await session.scalar(
        select(func.count()).select_from(IncidentTriage).where(*filters)
    )
    triage = await session.scalars(
        select(IncidentTriage)
        .where(*filters)
        .order_by(IncidentTriage.created_at.desc(), IncidentTriage.incident_id.desc())
        .limit(query.limit)
        .offset(query.offset)
    )
    return list(triage), total or 0
