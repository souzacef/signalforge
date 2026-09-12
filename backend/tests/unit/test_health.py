from collections.abc import AsyncIterator
from socket import gaierror
from unittest.mock import AsyncMock

import pytest
from asyncpg import CannotConnectNowError
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.db.session import get_session


@pytest.mark.anyio
async def test_liveness_does_not_require_postgres(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    async def fail_if_called() -> AsyncIterator[AsyncSession]:
        raise AssertionError("liveness must not resolve a database session")
        yield  # pragma: no cover

    app.dependency_overrides[get_session] = fail_if_called

    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_readiness_sanitizes_database_failures(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = SQLAlchemyError(
        "postgresql+asyncpg://user:sensitive-value@database/internal"
    )

    async def failing_session() -> AsyncIterator[AsyncSession]:
        yield session

    app.dependency_overrides[get_session] = failing_session

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
    assert "sensitive-value" not in response.text


@pytest.mark.anyio
async def test_readiness_sanitizes_dns_resolution_failures(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = gaierror(
        -2,
        "database.internal: sensitive-name-resolution-detail",
    )

    async def failing_session() -> AsyncIterator[AsyncSession]:
        yield session

    app.dependency_overrides[get_session] = failing_session

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
    assert "sensitive-name-resolution-detail" not in response.text


@pytest.mark.anyio
async def test_readiness_sanitizes_asyncpg_connection_failures(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = CannotConnectNowError(
        "sensitive-database-startup-detail"
    )

    async def failing_session() -> AsyncIterator[AsyncSession]:
        yield session

    app.dependency_overrides[get_session] = failing_session

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
    assert "sensitive-database-startup-detail" not in response.text


@pytest.mark.anyio
async def test_readiness_does_not_swallow_programming_errors(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = ValueError("unrelated programming defect")

    async def failing_session() -> AsyncIterator[AsyncSession]:
        yield session

    app.dependency_overrides[get_session] = failing_session

    with pytest.raises(ValueError, match="unrelated programming defect"):
        await client.get("/health/ready")
