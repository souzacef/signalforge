from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.incidents.models import Incident
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
