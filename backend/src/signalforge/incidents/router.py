from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.db.session import get_session
from signalforge.incidents import service
from signalforge.incidents.models import Incident
from signalforge.incidents.schemas import IncidentCreate, IncidentResponse

router = APIRouter(prefix="/api/v1/incidents", tags=["incidents"])
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]


@router.post("", response_model=IncidentResponse, status_code=status.HTTP_201_CREATED)
async def create_incident(
    incident_data: IncidentCreate,
    session: DatabaseSession,
) -> Incident:
    return await service.create_incident(session, incident_data)


@router.get("/{incident_id}", response_model=IncidentResponse)
async def get_incident(
    incident_id: UUID,
    session: DatabaseSession,
) -> Incident:
    incident = await service.get_incident(session, incident_id)
    if incident is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found",
        )
    return incident
