from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from signalforge.incidents.errors import (
    IncidentNotFoundError,
    InvalidIncidentTransitionError,
)
from signalforge.incidents.models import Incident, IncidentStatus
from signalforge.incidents.schemas import IncidentCreate, IncidentListQuery


async def create_incident(
    session: AsyncSession,
    incident_data: IncidentCreate,
) -> Incident:
    incident = Incident(**incident_data.model_dump())
    session.add(incident)
    await session.commit()
    await session.refresh(incident)
    return incident


async def get_incident(
    session: AsyncSession,
    incident_id: UUID,
) -> Incident | None:
    return await session.get(Incident, incident_id)


def _incident_filters(query: IncidentListQuery) -> tuple[ColumnElement[bool], ...]:
    filters: list[ColumnElement[bool]] = []
    if query.status is not None:
        filters.append(Incident.status == query.status)
    if query.severity is not None:
        filters.append(Incident.severity == query.severity)
    if query.source is not None:
        filters.append(Incident.source == query.source)
    if query.occurred_from is not None:
        filters.append(Incident.occurred_at >= query.occurred_from)
    if query.occurred_to is not None:
        filters.append(Incident.occurred_at <= query.occurred_to)
    return tuple(filters)


async def list_incidents(
    session: AsyncSession,
    query: IncidentListQuery,
) -> tuple[list[Incident], int]:
    filters = _incident_filters(query)
    total = await session.scalar(
        select(func.count()).select_from(Incident).where(*filters)
    )
    incidents = await session.scalars(
        select(Incident)
        .where(*filters)
        .order_by(Incident.occurred_at.desc(), Incident.id.desc())
        .limit(query.limit)
        .offset(query.offset)
    )
    return list(incidents), total or 0


async def _get_incident_for_update(
    session: AsyncSession,
    incident_id: UUID,
) -> Incident:
    result = await session.execute(
        select(Incident).where(Incident.id == incident_id).with_for_update()
    )
    incident = result.scalar_one_or_none()
    if incident is None:
        raise IncidentNotFoundError
    return incident


async def acknowledge_incident(
    session: AsyncSession,
    incident_id: UUID,
    actor_user_id: UUID,
) -> Incident:
    incident = await _get_incident_for_update(session, incident_id)
    if incident.status is not IncidentStatus.OPEN:
        raise InvalidIncidentTransitionError

    transitioned_at = datetime.now(UTC)
    incident.status = IncidentStatus.ACKNOWLEDGED
    incident.acknowledged_at = transitioned_at
    incident.acknowledged_by_user_id = actor_user_id
    incident.updated_at = transitioned_at
    await session.commit()
    await session.refresh(incident)
    return incident


async def resolve_incident(
    session: AsyncSession,
    incident_id: UUID,
    actor_user_id: UUID,
) -> Incident:
    incident = await _get_incident_for_update(session, incident_id)
    if incident.status is not IncidentStatus.ACKNOWLEDGED:
        raise InvalidIncidentTransitionError

    transitioned_at = datetime.now(UTC)
    incident.status = IncidentStatus.RESOLVED
    incident.resolved_at = transitioned_at
    incident.resolved_by_user_id = actor_user_id
    incident.updated_at = transitioned_at
    await session.commit()
    await session.refresh(incident)
    return incident
