from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.triage.models import IncidentTriage, TriagePriority
from signalforge.users.models import UserRole
from tests.integration.factories import AuthHeadersFactory


async def persist_triage(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    incident_id: UUID | None = None,
    event_id: UUID | None = None,
    incident_source: str = "current-incident-source",
    incident_severity: IncidentSeverity = IncidentSeverity.MEDIUM,
    source: str = "event-snapshot-source",
    original_severity: IncidentSeverity = IncidentSeverity.MEDIUM,
    priority: TriagePriority = TriagePriority.P3,
    requires_human_review: bool = False,
    created_at: datetime = datetime(2026, 1, 1, tzinfo=UTC),
) -> IncidentTriage:
    async with session_factory() as session:
        incident = Incident(
            id=incident_id or uuid4(),
            source=incident_source,
            title="Persisted triage API probe",
            description=None,
            severity=incident_severity,
            occurred_at=datetime(2025, 12, 31, tzinfo=UTC),
        )
        session.add(incident)
        await session.flush()
        triage = IncidentTriage(
            incident_id=incident.id,
            event_id=event_id or uuid4(),
            source=source,
            original_severity=original_severity,
            priority=priority,
            requires_human_review=requires_human_review,
            created_at=created_at,
        )
        session.add(triage)
        await session.commit()
        await session.refresh(triage)
        return triage


async def persist_incident_without_triage(
    session_factory: async_sessionmaker[AsyncSession],
) -> Incident:
    async with session_factory() as session:
        incident = Incident(
            source="manual",
            title="Awaiting event consumption",
            description=None,
            severity=IncidentSeverity.LOW,
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        session.add(incident)
        await session.commit()
        await session.refresh(incident)
        return incident


async def persist_filter_records(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[str, IncidentTriage]:
    records = {
        "p1": await persist_triage(
            session_factory,
            source="pager",
            original_severity=IncidentSeverity.CRITICAL,
            priority=TriagePriority.P1,
            requires_human_review=True,
            created_at=datetime(2026, 1, 4, tzinfo=UTC),
        ),
        "p2": await persist_triage(
            session_factory,
            source="alertmanager",
            original_severity=IncidentSeverity.HIGH,
            priority=TriagePriority.P2,
            created_at=datetime(2026, 1, 3, tzinfo=UTC),
        ),
        "p3": await persist_triage(
            session_factory,
            source="pager",
            original_severity=IncidentSeverity.MEDIUM,
            priority=TriagePriority.P3,
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
        ),
        "p4": await persist_triage(
            session_factory,
            source="manual",
            original_severity=IncidentSeverity.LOW,
            priority=TriagePriority.P4,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    }
    return records


@pytest.mark.integration
@pytest.mark.anyio
async def test_viewer_fetches_exact_persisted_triage_snapshot(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    created_at = datetime(2026, 2, 3, 4, 5, tzinfo=UTC)
    triage = await persist_triage(
        database_session_factory,
        event_id=UUID("00000000-0000-0000-0000-000000000111"),
        incident_source="current-source",
        incident_severity=IncidentSeverity.LOW,
        source="event-source",
        original_severity=IncidentSeverity.HIGH,
        priority=TriagePriority.P2,
        created_at=created_at,
    )

    response = await client.get(
        f"/api/v1/incidents/{triage.incident_id}/triage",
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "incident_id",
        "event_id",
        "source",
        "original_severity",
        "priority",
        "requires_human_review",
        "created_at",
    }
    assert body["incident_id"] == str(triage.incident_id)
    assert body["event_id"] == str(triage.event_id)
    assert body["source"] == "event-source"
    assert body["original_severity"] == "high"
    assert body["priority"] == "P2"
    assert body["requires_human_review"] is False
    assert datetime.fromisoformat(body["created_at"]) == created_at


@pytest.mark.integration
@pytest.mark.anyio
async def test_unknown_incident_returns_safe_not_found(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        f"/api/v1/incidents/{uuid4()}/triage",
        headers=viewer_headers,
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Incident not found"}


@pytest.mark.integration
@pytest.mark.anyio
async def test_existing_incident_without_triage_returns_safe_not_found(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    incident = await persist_incident_without_triage(database_session_factory)

    response = await client.get(
        f"/api/v1/incidents/{incident.id}/triage",
        headers=viewer_headers,
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Incident triage not found"}


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("role", list(UserRole))
async def test_all_existing_roles_can_read_single_and_list_triage(
    role: UserRole,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    triage = await persist_triage(database_session_factory)
    headers, _ = await auth_headers_factory(role)

    single = await client.get(
        f"/api/v1/incidents/{triage.incident_id}/triage",
        headers=headers,
    )
    listed = await client.get("/api/v1/triage", headers=headers)

    assert single.status_code == 200
    assert listed.status_code == 200


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "headers",
    [None, {"Authorization": "Bearer not-a-jwt"}],
)
async def test_unauthenticated_callers_cannot_read_triage(
    headers: dict[str, str] | None,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    triage = await persist_triage(database_session_factory)

    single = await client.get(
        f"/api/v1/incidents/{triage.incident_id}/triage",
        headers=headers,
    )
    listed = await client.get("/api/v1/triage", headers=headers)

    assert single.status_code == 401
    assert listed.status_code == 401
    assert single.headers["www-authenticate"] == "Bearer"
    assert listed.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    ("params", "expected_keys"),
    [
        ({"priority": "P1"}, ["p1"]),
        ({"original_severity": "low"}, ["p4"]),
        ({"requires_human_review": "true"}, ["p1"]),
        ({"requires_human_review": "false"}, ["p2", "p3", "p4"]),
        ({"source": "  pager  "}, ["p1", "p3"]),
        ({"created_from": "2026-01-03T00:00:00Z"}, ["p1", "p2"]),
        ({"created_to": "2026-01-02T00:00:00Z"}, ["p3", "p4"]),
        (
            {"source": "pager", "requires_human_review": "false"},
            ["p3"],
        ),
    ],
)
async def test_list_filters_use_exact_inclusive_and_semantics(
    params: dict[str, str],
    expected_keys: list[str],
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    records = await persist_filter_records(database_session_factory)

    response = await client.get(
        "/api/v1/triage",
        params=params,
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == len(expected_keys)
    assert [item["incident_id"] for item in body["items"]] == [
        str(records[key].incident_id) for key in expected_keys
    ]


@pytest.mark.integration
@pytest.mark.anyio
async def test_list_order_is_created_at_then_incident_id_descending(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    older = await persist_triage(
        database_session_factory,
        incident_id=UUID("00000000-0000-0000-0000-000000000003"),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    lower_tie_id = await persist_triage(
        database_session_factory,
        incident_id=UUID("00000000-0000-0000-0000-000000000001"),
        created_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    higher_tie_id = await persist_triage(
        database_session_factory,
        incident_id=UUID("00000000-0000-0000-0000-000000000002"),
        created_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    response = await client.get("/api/v1/triage", headers=viewer_headers)

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 20
    assert body["offset"] == 0
    assert body["total"] == 3
    assert [item["incident_id"] for item in body["items"]] == [
        str(higher_tie_id.incident_id),
        str(lower_tie_id.incident_id),
        str(older.incident_id),
    ]


@pytest.mark.integration
@pytest.mark.anyio
async def test_limit_offset_pagination_is_stable_across_ties(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    tied = [
        await persist_triage(
            database_session_factory,
            incident_id=UUID(f"00000000-0000-0000-0000-{number:012d}"),
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        for number in range(1, 6)
    ]
    expected_ids = [str(item.incident_id) for item in reversed(tied)]

    default = await client.get("/api/v1/triage", headers=viewer_headers)
    first_page = await client.get(
        "/api/v1/triage",
        params={"limit": 2, "offset": 0},
        headers=viewer_headers,
    )
    second_page = await client.get(
        "/api/v1/triage",
        params={"limit": 2, "offset": 2},
        headers=viewer_headers,
    )

    assert (
        default.status_code == first_page.status_code == second_page.status_code == 200
    )
    assert default.json()["limit"] == 20
    assert default.json()["offset"] == 0
    assert [item["incident_id"] for item in default.json()["items"]] == expected_ids
    assert first_page.json()["total"] == second_page.json()["total"] == 5
    assert [item["incident_id"] for item in first_page.json()["items"]] == expected_ids[
        :2
    ]
    assert [
        item["incident_id"] for item in second_page.json()["items"]
    ] == expected_ids[2:4]


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "params",
    [
        {"priority": "P5"},
        {"original_severity": "urgent"},
        {"requires_human_review": "maybe"},
        {"source": "   "},
        {"limit": "0"},
        {"limit": "101"},
        {"offset": "-1"},
        {"created_from": "2026-01-01T00:00:00"},
        {"created_to": "2026-01-01T00:00:00"},
    ],
)
async def test_invalid_list_parameters_return_unprocessable_entity(
    params: dict[str, str],
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/triage",
        params=params,
        headers=viewer_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_reversed_created_range_returns_unprocessable_entity(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/triage",
        params={
            "created_from": "2026-01-02T00:00:00Z",
            "created_to": "2026-01-01T00:00:00Z",
        },
        headers=viewer_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_unsupported_list_parameter_returns_unprocessable_entity(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/triage",
        params={"unexpected": "value"},
        headers=viewer_headers,
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_openapi_describes_read_only_authenticated_triage_api(
    client: AsyncClient,
) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    document = response.json()
    single = document["paths"]["/api/v1/incidents/{incident_id}/triage"]
    listed = document["paths"]["/api/v1/triage"]
    assert set(single) == {"get"}
    assert set(listed) == {"get"}
    assert "404" in single["get"]["responses"]
    assert single["get"]["security"]
    assert listed["get"]["security"]
    assert {parameter["name"] for parameter in listed["get"]["parameters"]} == {
        "priority",
        "original_severity",
        "requires_human_review",
        "source",
        "created_from",
        "created_to",
        "limit",
        "offset",
    }
    assert document["components"]["schemas"]["TriagePriority"]["enum"] == [
        "P1",
        "P2",
        "P3",
        "P4",
    ]
