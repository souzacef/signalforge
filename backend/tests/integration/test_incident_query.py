from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.incidents.models import (
    Incident,
    IncidentSeverity,
    IncidentStatus,
)
from signalforge.users.models import UserRole
from tests.integration.factories import AuthHeadersFactory


async def persist_incident(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    source: str = "manual",
    severity: IncidentSeverity = IncidentSeverity.MEDIUM,
    status: IncidentStatus = IncidentStatus.OPEN,
    occurred_at: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    incident_id: UUID | None = None,
) -> Incident:
    async with session_factory() as session:
        incident = Incident(
            id=incident_id or uuid4(),
            source=source,
            title=f"{source} incident",
            description=None,
            severity=severity,
            status=status,
            occurred_at=occurred_at,
        )
        session.add(incident)
        await session.commit()
        await session.refresh(incident)
        return incident


@pytest.mark.integration
@pytest.mark.anyio
async def test_viewer_lists_incidents_in_deterministic_default_order(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    older = await persist_incident(
        database_session_factory,
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        incident_id=UUID("00000000-0000-0000-0000-000000000003"),
    )
    lower_tie_id = await persist_incident(
        database_session_factory,
        occurred_at=datetime(2026, 1, 2, tzinfo=UTC),
        incident_id=UUID("00000000-0000-0000-0000-000000000001"),
    )
    higher_tie_id = await persist_incident(
        database_session_factory,
        occurred_at=datetime(2026, 1, 2, tzinfo=UTC),
        incident_id=UUID("00000000-0000-0000-0000-000000000002"),
    )
    headers, _ = await auth_headers_factory(UserRole.VIEWER)

    response = await client.get("/api/v1/incidents", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 20
    assert body["offset"] == 0
    assert body["total"] == 3
    assert [item["id"] for item in body["items"]] == [
        str(higher_tie_id.id),
        str(lower_tie_id.id),
        str(older.id),
    ]


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "role",
    [UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN],
)
async def test_all_authenticated_roles_can_list_incidents(
    role: UserRole,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(role)

    response = await client.get("/api/v1/incidents", headers=headers)

    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "headers",
    [None, {"Authorization": "Bearer not-a-jwt"}],
)
async def test_unauthenticated_callers_cannot_list_incidents(
    headers: dict[str, str] | None,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.get("/api/v1/incidents", headers=headers)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
async def test_status_filter_returns_only_matching_incidents(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    await persist_incident(database_session_factory, status=IncidentStatus.OPEN)
    acknowledged = await persist_incident(
        database_session_factory,
        status=IncidentStatus.ACKNOWLEDGED,
    )
    await persist_incident(database_session_factory, status=IncidentStatus.RESOLVED)

    response = await client.get(
        "/api/v1/incidents",
        params={"status": "acknowledged"},
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 20
    assert body["offset"] == 0
    assert body["total"] == 1
    assert [item["id"] for item in body["items"]] == [str(acknowledged.id)]


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("severity", list(IncidentSeverity))
async def test_severity_filter_returns_only_matching_incidents(
    severity: IncidentSeverity,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    incidents = {
        candidate: await persist_incident(
            database_session_factory,
            severity=candidate,
        )
        for candidate in IncidentSeverity
    }

    response = await client.get(
        "/api/v1/incidents",
        params={"severity": severity.value},
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["id"] for item in body["items"]] == [str(incidents[severity].id)]


@pytest.mark.integration
@pytest.mark.anyio
async def test_source_filter_trims_input_and_matches_exactly(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    await persist_incident(database_session_factory, source="alertmanager")
    manual = await persist_incident(database_session_factory, source="manual")
    await persist_incident(database_session_factory, source="Manual")
    await persist_incident(database_session_factory, source="github")

    response = await client.get(
        "/api/v1/incidents",
        params={"source": "  manual  "},
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["id"] for item in body["items"]] == [str(manual.id)]


@pytest.mark.integration
@pytest.mark.anyio
async def test_occurred_at_filters_are_inclusive(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    first_time = datetime(2026, 1, 1, tzinfo=UTC)
    middle_time = datetime(2026, 1, 2, tzinfo=UTC)
    last_time = datetime(2026, 1, 3, tzinfo=UTC)
    first = await persist_incident(database_session_factory, occurred_at=first_time)
    middle = await persist_incident(database_session_factory, occurred_at=middle_time)
    last = await persist_incident(database_session_factory, occurred_at=last_time)

    from_response = await client.get(
        "/api/v1/incidents",
        params={"occurred_from": middle_time.isoformat()},
        headers=viewer_headers,
    )
    to_response = await client.get(
        "/api/v1/incidents",
        params={"occurred_to": middle_time.isoformat()},
        headers=viewer_headers,
    )
    bounded_response = await client.get(
        "/api/v1/incidents",
        params={
            "occurred_from": middle_time.isoformat(),
            "occurred_to": middle_time.isoformat(),
        },
        headers=viewer_headers,
    )
    reversed_response = await client.get(
        "/api/v1/incidents",
        params={
            "occurred_from": last_time.isoformat(),
            "occurred_to": first_time.isoformat(),
        },
        headers=viewer_headers,
    )

    assert [item["id"] for item in from_response.json()["items"]] == [
        str(last.id),
        str(middle.id),
    ]
    assert [item["id"] for item in to_response.json()["items"]] == [
        str(middle.id),
        str(first.id),
    ]
    assert [item["id"] for item in bounded_response.json()["items"]] == [str(middle.id)]
    assert reversed_response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_offset_pagination_preserves_total_and_order(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    incidents = [
        await persist_incident(
            database_session_factory,
            occurred_at=datetime(2026, 1, day, tzinfo=UTC),
        )
        for day in range(1, 6)
    ]
    expected_ids = [str(incident.id) for incident in reversed(incidents)]

    responses = [
        await client.get(
            "/api/v1/incidents",
            params={"limit": 2, "offset": offset},
            headers=viewer_headers,
        )
        for offset in (0, 2, 4, 10)
    ]

    assert all(response.status_code == 200 for response in responses)
    bodies = [response.json() for response in responses]
    assert [body["total"] for body in bodies] == [5, 5, 5, 5]
    assert [body["limit"] for body in bodies] == [2, 2, 2, 2]
    assert [body["offset"] for body in bodies] == [0, 2, 4, 10]
    assert [[item["id"] for item in body["items"]] for body in bodies] == [
        expected_ids[:2],
        expected_ids[2:4],
        expected_ids[4:],
        [],
    ]


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "params",
    [
        {"status": "garbage"},
        {"severity": "garbage"},
        {"limit": "0"},
        {"limit": "101"},
        {"offset": "-1"},
        {"source": "   "},
        {"occurred_from": "2026-01-01T12:00:00"},
        {"occurred_to": "2026-01-01T12:00:00"},
    ],
)
async def test_invalid_query_parameters_return_unprocessable_entity(
    params: dict[str, str],
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/incidents",
        params=params,
        headers=viewer_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_unsupported_query_parameter_returns_unprocessable_entity(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/incidents",
        params={"unexpected": "value"},
        headers=viewer_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_combined_filters_use_and_semantics(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    target = await persist_incident(
        database_session_factory,
        source="alertmanager",
        severity=IncidentSeverity.CRITICAL,
        status=IncidentStatus.OPEN,
    )
    await persist_incident(
        database_session_factory,
        source="manual",
        severity=IncidentSeverity.CRITICAL,
        status=IncidentStatus.OPEN,
    )
    await persist_incident(
        database_session_factory,
        source="alertmanager",
        severity=IncidentSeverity.HIGH,
        status=IncidentStatus.OPEN,
    )
    await persist_incident(
        database_session_factory,
        source="alertmanager",
        severity=IncidentSeverity.CRITICAL,
        status=IncidentStatus.RESOLVED,
    )

    response = await client.get(
        "/api/v1/incidents",
        params={
            "status": "open",
            "severity": "critical",
            "source": "alertmanager",
        },
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["id"] for item in body["items"]] == [str(target.id)]


@pytest.mark.integration
@pytest.mark.anyio
async def test_filtered_total_counts_rows_before_pagination(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    for day in range(1, 5):
        await persist_incident(
            database_session_factory,
            status=IncidentStatus.OPEN,
            occurred_at=datetime(2026, 1, day, tzinfo=UTC),
        )
    await persist_incident(
        database_session_factory,
        status=IncidentStatus.RESOLVED,
    )

    response = await client.get(
        "/api/v1/incidents",
        params={"status": "open", "limit": 2},
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 2
    assert body["total"] == 4
