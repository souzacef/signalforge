from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.incidents.models import Incident, IncidentStatus
from signalforge.users.models import UserRole
from tests.integration.factories import AuthHeadersFactory

INCIDENT_PAYLOAD: dict[str, Any] = {
    "source": "manual",
    "title": "Checkout API error rate increased",
    "description": "5xx error rate exceeded threshold",
    "severity": "high",
    "occurred_at": "2026-09-11T18:30:00Z",
}
INVALID_TRANSITION_RESPONSE = {"detail": "Invalid incident state transition"}


async def create_open_incident(
    client: AsyncClient,
    headers: dict[str, str],
) -> dict[str, Any]:
    response = await client.post(
        "/api/v1/incidents",
        json=INCIDENT_PAYLOAD,
        headers=headers,
    )
    assert response.status_code == 201
    return response.json()


def parse_aware_timestamp(value: str | None) -> datetime:
    assert value is not None
    timestamp = datetime.fromisoformat(value)
    assert timestamp.tzinfo is not None
    return timestamp


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("role", [UserRole.OPERATOR, UserRole.ADMIN])
async def test_operator_and_admin_can_acknowledge_open_incident(
    role: UserRole,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, actor = await auth_headers_factory(role)
    created = await create_open_incident(client, headers)

    response = await client.post(
        f"/api/v1/incidents/{created['id']}/acknowledge",
        headers=headers,
    )

    assert response.status_code == 200
    body = response.json()
    acknowledged_at = parse_aware_timestamp(body["acknowledged_at"])
    assert body["status"] == "acknowledged"
    assert body["acknowledged_by_user_id"] == str(actor.id)
    assert body["updated_at"] == body["acknowledged_at"]
    assert body["resolved_at"] is None
    assert body["resolved_by_user_id"] is None

    async with database_session_factory() as session:
        stored_incident = await session.get(Incident, UUID(created["id"]))

    assert stored_incident is not None
    assert stored_incident.status is IncidentStatus.ACKNOWLEDGED
    assert stored_incident.acknowledged_at == acknowledged_at
    assert stored_incident.acknowledged_by_user_id == actor.id
    assert stored_incident.resolved_at is None
    assert stored_incident.resolved_by_user_id is None


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("role", [UserRole.OPERATOR, UserRole.ADMIN])
async def test_operator_and_admin_can_resolve_acknowledged_incident(
    role: UserRole,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, actor = await auth_headers_factory(role)
    created = await create_open_incident(client, headers)
    acknowledge_response = await client.post(
        f"/api/v1/incidents/{created['id']}/acknowledge",
        headers=headers,
    )
    assert acknowledge_response.status_code == 200
    acknowledged = acknowledge_response.json()

    response = await client.post(
        f"/api/v1/incidents/{created['id']}/resolve",
        headers=headers,
    )

    assert response.status_code == 200
    body = response.json()
    resolved_at = parse_aware_timestamp(body["resolved_at"])
    assert body["status"] == "resolved"
    assert body["resolved_by_user_id"] == str(actor.id)
    assert body["updated_at"] == body["resolved_at"]
    assert body["acknowledged_at"] == acknowledged["acknowledged_at"]
    assert body["acknowledged_by_user_id"] == acknowledged["acknowledged_by_user_id"]

    async with database_session_factory() as session:
        stored_incident = await session.get(Incident, UUID(created["id"]))

    assert stored_incident is not None
    assert stored_incident.status is IncidentStatus.RESOLVED
    assert stored_incident.resolved_at == resolved_at
    assert stored_incident.resolved_by_user_id == actor.id
    assert stored_incident.acknowledged_at == datetime.fromisoformat(
        acknowledged["acknowledged_at"]
    )


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("action", ["acknowledge", "resolve"])
async def test_viewer_cannot_run_lifecycle_actions(
    action: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    operator_headers, _ = await auth_headers_factory(UserRole.OPERATOR)
    viewer_headers, _ = await auth_headers_factory(UserRole.VIEWER)
    created = await create_open_incident(client, operator_headers)

    response = await client.post(
        f"/api/v1/incidents/{created['id']}/{action}",
        headers=viewer_headers,
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Insufficient permissions"}
    async with database_session_factory() as session:
        stored_incident = await session.get(Incident, UUID(created["id"]))
    assert stored_incident is not None
    assert stored_incident.status is IncidentStatus.OPEN


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("action", ["acknowledge", "resolve"])
async def test_anonymous_lifecycle_requests_are_unauthorized(
    action: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    created = await create_open_incident(client, operator_headers)

    response = await client.post(
        f"/api/v1/incidents/{created['id']}/{action}",
    )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("action", ["acknowledge", "resolve"])
async def test_lifecycle_action_returns_not_found_for_unknown_uuid(
    action: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    response = await client.post(
        f"/api/v1/incidents/{uuid4()}/{action}",
        headers=operator_headers,
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Incident not found"}


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("action", ["acknowledge", "resolve"])
async def test_lifecycle_action_rejects_malformed_uuid(
    action: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    response = await client.post(
        f"/api/v1/incidents/not-a-uuid/{action}",
        headers=operator_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_acknowledge_rejects_acknowledged_and_resolved_incidents(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    created = await create_open_incident(client, operator_headers)
    acknowledge_url = f"/api/v1/incidents/{created['id']}/acknowledge"
    resolve_url = f"/api/v1/incidents/{created['id']}/resolve"

    first_acknowledge = await client.post(acknowledge_url, headers=operator_headers)
    repeated_acknowledge = await client.post(
        acknowledge_url,
        headers=operator_headers,
    )
    resolve = await client.post(resolve_url, headers=operator_headers)
    resolved_acknowledge = await client.post(
        acknowledge_url,
        headers=operator_headers,
    )

    assert first_acknowledge.status_code == 200
    assert repeated_acknowledge.status_code == 409
    assert repeated_acknowledge.json() == INVALID_TRANSITION_RESPONSE
    assert resolve.status_code == 200
    assert resolved_acknowledge.status_code == 409
    assert resolved_acknowledge.json() == INVALID_TRANSITION_RESPONSE


@pytest.mark.integration
@pytest.mark.anyio
async def test_resolve_rejects_open_and_resolved_incidents(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    created = await create_open_incident(client, operator_headers)
    acknowledge_url = f"/api/v1/incidents/{created['id']}/acknowledge"
    resolve_url = f"/api/v1/incidents/{created['id']}/resolve"

    open_resolve = await client.post(resolve_url, headers=operator_headers)
    acknowledge = await client.post(acknowledge_url, headers=operator_headers)
    first_resolve = await client.post(resolve_url, headers=operator_headers)
    repeated_resolve = await client.post(resolve_url, headers=operator_headers)

    assert open_resolve.status_code == 409
    assert open_resolve.json() == INVALID_TRANSITION_RESPONSE
    assert acknowledge.status_code == 200
    assert first_resolve.status_code == 200
    assert repeated_resolve.status_code == 409
    assert repeated_resolve.json() == INVALID_TRANSITION_RESPONSE


@pytest.mark.integration
@pytest.mark.anyio
async def test_acknowledgement_and_resolution_record_different_actors(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    acknowledger_headers, acknowledger = await auth_headers_factory(UserRole.OPERATOR)
    resolver_headers, resolver = await auth_headers_factory(UserRole.OPERATOR)
    created = await create_open_incident(client, acknowledger_headers)

    acknowledge_response = await client.post(
        f"/api/v1/incidents/{created['id']}/acknowledge",
        headers=acknowledger_headers,
    )
    resolve_response = await client.post(
        f"/api/v1/incidents/{created['id']}/resolve",
        headers=resolver_headers,
    )

    assert acknowledge_response.status_code == 200
    assert resolve_response.status_code == 200
    resolved = resolve_response.json()
    assert resolved["acknowledged_by_user_id"] == str(acknowledger.id)
    assert resolved["resolved_by_user_id"] == str(resolver.id)

    async with database_session_factory() as session:
        stored_incident = await session.get(Incident, UUID(created["id"]))
    assert stored_incident is not None
    assert stored_incident.acknowledged_by_user_id == acknowledger.id
    assert stored_incident.resolved_by_user_id == resolver.id
