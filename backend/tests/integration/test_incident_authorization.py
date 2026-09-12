from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.users.models import User, UserRole
from tests.integration.factories import AuthHeadersFactory

INCIDENT_PAYLOAD: dict[str, Any] = {
    "source": "manual",
    "title": "Checkout API error rate increased",
    "description": "5xx error rate exceeded threshold",
    "severity": "high",
    "occurred_at": "2026-09-11T18:30:00Z",
}


async def persist_incident(
    session_factory: async_sessionmaker[AsyncSession],
) -> Incident:
    async with session_factory() as session:
        incident = Incident(
            source="manual",
            title="Checkout API error rate increased",
            description="5xx error rate exceeded threshold",
            severity=IncidentSeverity.HIGH,
            occurred_at=datetime(2026, 9, 11, 18, 30, tzinfo=UTC),
        )
        session.add(incident)
        await session.commit()
        await session.refresh(incident)
        return incident


async def incident_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(Incident)) or 0


@pytest.mark.integration
@pytest.mark.anyio
async def test_anonymous_incident_requests_are_unauthorized(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    incident = await persist_incident(database_session_factory)
    count_before_post = await incident_count(database_session_factory)

    get_response = await client.get(f"/api/v1/incidents/{incident.id}")
    post_response = await client.post(
        "/api/v1/incidents",
        json=INCIDENT_PAYLOAD,
    )

    assert get_response.status_code == 401
    assert get_response.headers["www-authenticate"] == "Bearer"
    assert post_response.status_code == 401
    assert post_response.headers["www-authenticate"] == "Bearer"
    assert await incident_count(database_session_factory) == count_before_post


@pytest.mark.integration
@pytest.mark.anyio
async def test_viewer_can_read_but_cannot_create_incidents(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    incident = await persist_incident(database_session_factory)
    headers, _ = await auth_headers_factory(UserRole.VIEWER)
    count_before_post = await incident_count(database_session_factory)

    get_response = await client.get(
        f"/api/v1/incidents/{incident.id}",
        headers=headers,
    )
    post_response = await client.post(
        "/api/v1/incidents",
        json=INCIDENT_PAYLOAD,
        headers=headers,
    )

    assert get_response.status_code == 200
    assert post_response.status_code == 403
    assert post_response.json() == {"detail": "Insufficient permissions"}
    assert await incident_count(database_session_factory) == count_before_post


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("role", [UserRole.OPERATOR, UserRole.ADMIN])
async def test_operator_and_admin_can_create_and_read_incidents(
    role: UserRole,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(role)

    create_response = await client.post(
        "/api/v1/incidents",
        json=INCIDENT_PAYLOAD,
        headers=headers,
    )
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["status"] == "open"

    get_response = await client.get(
        f"/api/v1/incidents/{created['id']}",
        headers=headers,
    )
    assert get_response.status_code == 200
    assert get_response.json() == created

    async with database_session_factory() as session:
        stored_incident = await session.get(Incident, UUID(created["id"]))
    assert stored_incident is not None


@pytest.mark.integration
@pytest.mark.anyio
async def test_role_change_applies_to_same_access_token(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, user = await auth_headers_factory(UserRole.OPERATOR)

    allowed_response = await client.post(
        "/api/v1/incidents",
        json=INCIDENT_PAYLOAD,
        headers=headers,
    )
    assert allowed_response.status_code == 201

    async with database_session_factory() as session:
        stored_user = await session.get(User, user.id)
        assert stored_user is not None
        stored_user.role = UserRole.VIEWER
        await session.commit()

    denied_response = await client.post(
        "/api/v1/incidents",
        json=INCIDENT_PAYLOAD,
        headers=headers,
    )
    get_response = await client.get(
        f"/api/v1/incidents/{allowed_response.json()['id']}",
        headers=headers,
    )

    assert denied_response.status_code == 403
    assert denied_response.json() == {"detail": "Insufficient permissions"}
    assert get_response.status_code == 200


@pytest.mark.integration
@pytest.mark.anyio
async def test_invalid_token_is_unauthorized_for_incidents(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.get(
        "/api/v1/incidents/00000000-0000-0000-0000-000000000000",
        headers={"Authorization": "Bearer not-a-jwt"},
    )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
