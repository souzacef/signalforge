from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.incidents.errors import (
    IncidentNotFoundError,
    InvalidIncidentTransitionError,
)
from signalforge.incidents.models import Incident, IncidentStatus
from signalforge.incidents.schemas import IncidentCreate


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
