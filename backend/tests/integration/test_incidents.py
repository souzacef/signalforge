from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.incidents.models import Incident, IncidentStatus


@pytest.fixture
def incident_payload() -> dict[str, Any]:
    return {
        "source": " manual ",
        "title": " Checkout API error rate increased ",
        "description": " 5xx error rate exceeded threshold ",
        "severity": "high",
        "occurred_at": "2026-09-11T18:30:00Z",
    }


@pytest.mark.integration
@pytest.mark.anyio
async def test_create_incident_persists_open_incident(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
    operator_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/incidents",
        json=incident_payload,
        headers=operator_headers,
    )

    assert response.status_code == 201
    body = response.json()
    incident_id = UUID(body["id"])
    assert body["source"] == "manual"
    assert body["title"] == "Checkout API error rate increased"
    assert body["description"] == "5xx error rate exceeded threshold"
    assert body["severity"] == "high"
    assert body["status"] == "open"
    assert datetime.fromisoformat(body["occurred_at"]).tzinfo is not None
    assert datetime.fromisoformat(body["created_at"]).tzinfo is not None
    assert datetime.fromisoformat(body["updated_at"]).tzinfo is not None

    async with database_session_factory() as session:
        stored_incident = await session.get(Incident, incident_id)

    assert stored_incident is not None
    assert stored_incident.status is IncidentStatus.OPEN
    assert stored_incident.source == "manual"


@pytest.mark.integration
@pytest.mark.anyio
async def test_create_incident_rejects_invalid_severity(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
    operator_headers: dict[str, str],
) -> None:
    incident_payload["severity"] = "urgent"

    response = await client.post(
        "/api/v1/incidents",
        json=incident_payload,
        headers=operator_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("field", ["title", "source"])
async def test_create_incident_rejects_blank_required_text(
    field: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
    operator_headers: dict[str, str],
) -> None:
    incident_payload[field] = "   "

    response = await client.post(
        "/api/v1/incidents",
        json=incident_payload,
        headers=operator_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_create_incident_rejects_naive_occurred_at(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
    operator_headers: dict[str, str],
) -> None:
    incident_payload["occurred_at"] = "2026-09-11T18:30:00"

    response = await client.post(
        "/api/v1/incidents",
        json=incident_payload,
        headers=operator_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_create_incident_rejects_client_supplied_status(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
    operator_headers: dict[str, str],
) -> None:
    incident_payload["status"] = "resolved"

    response = await client.post(
        "/api/v1/incidents",
        json=incident_payload,
        headers=operator_headers,
    )

    assert response.status_code == 422
    assert any(
        error["loc"] == ["body", "status"] for error in response.json()["detail"]
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_get_incident_returns_created_incident(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
    operator_headers: dict[str, str],
) -> None:
    create_response = await client.post(
        "/api/v1/incidents",
        json=incident_payload,
        headers=operator_headers,
    )
    assert create_response.status_code == 201
    created_incident = create_response.json()

    response = await client.get(
        f"/api/v1/incidents/{created_incident['id']}",
        headers=operator_headers,
    )

    assert response.status_code == 200
    assert response.json() == created_incident


@pytest.mark.integration
@pytest.mark.anyio
async def test_get_incident_returns_not_found_for_unknown_uuid(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    response = await client.get(
        f"/api/v1/incidents/{uuid4()}",
        headers=operator_headers,
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Incident not found"}


@pytest.mark.integration
@pytest.mark.anyio
async def test_get_incident_rejects_malformed_uuid(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/incidents/not-a-uuid",
        headers=operator_headers,
    )

    assert response.status_code == 422
