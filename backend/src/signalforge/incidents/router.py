from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.authorization import require_roles
from signalforge.db.session import get_session
from signalforge.incidents import service
from signalforge.incidents.models import Incident
from signalforge.incidents.schemas import IncidentCreate, IncidentResponse
from signalforge.users.models import UserRole

router = APIRouter(prefix="/api/v1/incidents", tags=["incidents"])
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]


@router.post(
    "",
    response_model=IncidentResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.OPERATOR, UserRole.ADMIN))],
)
async def create_incident(
    incident_data: IncidentCreate,
    session: DatabaseSession,
) -> Incident:
    return await service.create_incident(session, incident_data)


@router.get(
    "/{incident_id}",
    response_model=IncidentResponse,
    dependencies=[
        Depends(
            require_roles(UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN),
        ),
    ],
)
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
