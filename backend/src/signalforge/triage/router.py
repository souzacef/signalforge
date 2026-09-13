from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.authorization import require_roles
from signalforge.db.session import get_session
from signalforge.incidents.errors import IncidentNotFoundError
from signalforge.triage import service
from signalforge.triage.errors import TriageNotFoundError
from signalforge.triage.models import IncidentTriage
from signalforge.triage.schemas import (
    TriageListQuery,
    TriageListResponse,
    TriageResponse,
)
from signalforge.users.models import UserRole

router = APIRouter(
    prefix="/api/v1",
    tags=["triage"],
    dependencies=[
        Depends(
            require_roles(UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN),
        ),
    ],
)
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]
TriageQuery = Annotated[TriageListQuery, Query()]


@router.get(
    "/incidents/{incident_id}/triage",
    response_model=TriageResponse,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Incident or triage not found"}
    },
)
async def get_incident_triage(
    incident_id: UUID,
    session: DatabaseSession,
) -> IncidentTriage:
    try:
        return await service.get_incident_triage(session, incident_id)
    except IncidentNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found",
        ) from error
    except TriageNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident triage not found",
        ) from error


@router.get("/triage", response_model=TriageListResponse)
async def list_triage(
    query: TriageQuery,
    session: DatabaseSession,
) -> TriageListResponse:
    triage, total = await service.list_triage(session, query)
    return TriageListResponse(
        items=[TriageResponse.model_validate(item) for item in triage],
        limit=query.limit,
        offset=query.offset,
        total=total,
    )
