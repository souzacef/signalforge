from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.db.session import get_session

router = APIRouter(prefix="/health", tags=["health"])
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]


class HealthResponse(BaseModel):
    status: str


@router.get("/live", response_model=HealthResponse)
async def liveness() -> HealthResponse:
    """Report process liveness without consulting external dependencies."""
    return HealthResponse(status="ok")


@router.get(
    "/ready",
    response_model=HealthResponse,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthResponse}},
)
async def readiness(session: DatabaseSession) -> HealthResponse | JSONResponse:
    """Report whether PostgreSQL is reachable without leaking failure details."""
    try:
        await session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        # Readiness is a protective boundary: database errors must not reach clients.
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable"},
        )

    return HealthResponse(status="ok")
