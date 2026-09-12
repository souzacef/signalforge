from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.authorization import require_roles
from signalforge.db.session import get_session
from signalforge.incidents import application, service
from signalforge.incidents.errors import (
    IncidentNotFoundError,
    InvalidIncidentTransitionError,
)
from signalforge.incidents.models import Incident
from signalforge.incidents.schemas import (
    IncidentCreate,
    IncidentListQuery,
    IncidentListResponse,
    IncidentResponse,
)
from signalforge.users.models import User, UserRole

router = APIRouter(prefix="/api/v1/incidents", tags=["incidents"])
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]
LifecycleActor = Annotated[
    User,
    Depends(require_roles(UserRole.OPERATOR, UserRole.ADMIN)),
]
IncidentQuery = Annotated[IncidentListQuery, Query()]


@router.post(
    "",
    response_model=IncidentResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.OPERATOR, UserRole.ADMIN))],
)
async def create_incident(
    incident_data: IncidentCreate,
    session: DatabaseSession,
) -> IncidentResponse:
    return await application.create_incident_with_event(session, incident_data)


@router.get(
    "",
    response_model=IncidentListResponse,
    dependencies=[
        Depends(
            require_roles(UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN),
        ),
    ],
)
async def list_incidents(
    query: IncidentQuery,
    session: DatabaseSession,
) -> IncidentListResponse:
    incidents, total = await service.list_incidents(session, query)
    return IncidentListResponse(
        items=[IncidentResponse.model_validate(incident) for incident in incidents],
        limit=query.limit,
        offset=query.offset,
        total=total,
    )


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


@router.post("/{incident_id}/acknowledge", response_model=IncidentResponse)
async def acknowledge_incident(
    incident_id: UUID,
    session: DatabaseSession,
    actor: LifecycleActor,
) -> Incident:
    try:
        return await service.acknowledge_incident(session, incident_id, actor.id)
    except IncidentNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found",
        ) from error
    except InvalidIncidentTransitionError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Invalid incident state transition",
        ) from error


@router.post("/{incident_id}/resolve", response_model=IncidentResponse)
async def resolve_incident(
    incident_id: UUID,
    session: DatabaseSession,
    actor: LifecycleActor,
) -> Incident:
    try:
        return await service.resolve_incident(session, incident_id, actor.id)
    except IncidentNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found",
        ) from error
    except InvalidIncidentTransitionError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Invalid incident state transition",
        ) from error
