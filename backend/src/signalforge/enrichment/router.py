"""Authenticated, read-only advisory enrichment API."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.authorization import require_roles
from signalforge.db.session import get_session
from signalforge.enrichment import service
from signalforge.enrichment.models import TriageEnrichment
from signalforge.enrichment.schemas import (
    EnrichmentListQuery,
    EnrichmentListResponse,
    EnrichmentResponse,
    GlobalEnrichmentListQuery,
)
from signalforge.incidents.errors import IncidentNotFoundError
from signalforge.users.models import UserRole

router = APIRouter(
    prefix="/api/v1",
    tags=["enrichments"],
    dependencies=[
        Depends(require_roles(UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN)),
    ],
)
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]
ScopedQuery = Annotated[EnrichmentListQuery, Query()]
GlobalQuery = Annotated[GlobalEnrichmentListQuery, Query()]


def _response(
    rows: list[TriageEnrichment], total: int, query: EnrichmentListQuery
) -> EnrichmentListResponse:
    return EnrichmentListResponse(
        items=[EnrichmentResponse.model_validate(row) for row in rows],
        total=total,
        limit=query.limit,
        offset=query.offset,
    )


@router.get(
    "/incidents/{incident_id}/enrichments",
    response_model=EnrichmentListResponse,
    responses={status.HTTP_404_NOT_FOUND: {"description": "Incident not found"}},
)
async def list_incident_enrichments(
    incident_id: UUID,
    query: ScopedQuery,
    session: DatabaseSession,
) -> EnrichmentListResponse:
    try:
        await service.require_incident(session, incident_id)
    except IncidentNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found",
        ) from error
    rows, total = await service.list_enrichments(
        session, query, incident_id=incident_id
    )
    return _response(rows, total, query)


@router.get("/enrichments", response_model=EnrichmentListResponse)
async def list_enrichments(
    query: GlobalQuery,
    session: DatabaseSession,
) -> EnrichmentListResponse:
    rows, total = await service.list_enrichments(
        session, query, incident_id=query.incident_id
    )
    return _response(rows, total, query)
