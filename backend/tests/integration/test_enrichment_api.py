from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.enrichment.domain import EnrichmentCategory
from signalforge.enrichment.models import TriageEnrichment
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.users.models import UserRole
from tests.integration.factories import AuthHeadersFactory


async def persist_incident(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    incident_id: UUID | None = None,
    source: str = "current-source",
    title: str = "Current Incident title",
) -> Incident:
    async with session_factory() as session:
        incident = Incident(
            id=incident_id or uuid4(),
            source=source,
            title=title,
            description="Current Incident description",
            severity=IncidentSeverity.MEDIUM,
            occurred_at=datetime(2025, 12, 31, tzinfo=UTC),
        )
        session.add(incident)
        await session.commit()
        await session.refresh(incident)
        return incident


async def persist_enrichment(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    incident_id: UUID,
    request_event_id: UUID | None = None,
    trigger_event_id: UUID | None = None,
    provider: str = "gemini",
    model: str = "gemini-test",
    summary: str = "Persisted advisory summary",
    category: EnrichmentCategory = EnrichmentCategory.PERFORMANCE,
    suspected_component: str | None = "checkout",
    investigation_steps: list[str] | None = None,
    created_at: datetime = datetime(2026, 1, 1, tzinfo=UTC),
) -> TriageEnrichment:
    async with session_factory() as session:
        enrichment = TriageEnrichment(
            request_event_id=request_event_id or uuid4(),
            incident_id=incident_id,
            trigger_event_id=trigger_event_id or uuid4(),
            provider=provider,
            model=model,
            summary=summary,
            category=category,
            suspected_component=suspected_component,
            investigation_steps=investigation_steps or ["Inspect persisted evidence."],
            created_at=created_at,
        )
        session.add(enrichment)
        await session.commit()
        await session.refresh(enrichment)
        return enrichment


async def seed_filter_records(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[dict[str, TriageEnrichment], Incident, Incident]:
    first_incident = await persist_incident(session_factory, source="first")
    second_incident = await persist_incident(session_factory, source="second")
    records = {
        "p1": await persist_enrichment(
            session_factory,
            incident_id=first_incident.id,
            request_event_id=UUID("00000000-0000-0000-0000-000000000004"),
            category=EnrichmentCategory.PERFORMANCE,
            provider="gemini",
            model="flash",
            created_at=datetime(2026, 1, 4, tzinfo=UTC),
        ),
        "p2": await persist_enrichment(
            session_factory,
            incident_id=first_incident.id,
            request_event_id=UUID("00000000-0000-0000-0000-000000000003"),
            category=EnrichmentCategory.SECURITY,
            provider="vertex",
            model="secure",
            created_at=datetime(2026, 1, 3, tzinfo=UTC),
        ),
        "p3": await persist_enrichment(
            session_factory,
            incident_id=second_incident.id,
            request_event_id=UUID("00000000-0000-0000-0000-000000000002"),
            category=EnrichmentCategory.PERFORMANCE,
            provider="gemini",
            model="pro",
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
        ),
        "p4": await persist_enrichment(
            session_factory,
            incident_id=second_incident.id,
            request_event_id=UUID("00000000-0000-0000-0000-000000000001"),
            category=EnrichmentCategory.CAPACITY,
            provider="other",
            model="flash",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    }
    return records, first_incident, second_incident


@pytest.mark.integration
@pytest.mark.anyio
async def test_incident_collection_returns_persisted_fields_order_and_pagination(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    incident = await persist_incident(database_session_factory)
    created_at = datetime(2026, 2, 1, tzinfo=UTC)
    records = [
        await persist_enrichment(
            database_session_factory,
            incident_id=incident.id,
            request_event_id=UUID(f"00000000-0000-0000-0000-{number:012d}"),
            trigger_event_id=UUID(f"10000000-0000-0000-0000-{number:012d}"),
            summary=f"Persisted summary {number}",
            investigation_steps=[f"Persisted step {number}"],
            created_at=created_at,
        )
        for number in range(1, 4)
    ]

    response = await client.get(
        f"/api/v1/incidents/{incident.id}/enrichments",
        params={"limit": 2, "offset": 1},
        headers=viewer_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert [item["request_event_id"] for item in body["items"]] == [
        str(records[1].request_event_id),
        str(records[0].request_event_id),
    ]
    item = body["items"][0]
    assert item == {
        "request_event_id": str(records[1].request_event_id),
        "incident_id": str(incident.id),
        "trigger_event_id": str(records[1].trigger_event_id),
        "provider": "gemini",
        "model": "gemini-test",
        "summary": "Persisted summary 2",
        "category": "performance",
        "suspected_component": "checkout",
        "investigation_steps": ["Persisted step 2"],
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
    }


@pytest.mark.integration
@pytest.mark.anyio
async def test_existing_incident_without_enrichment_returns_empty_collection(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    incident = await persist_incident(database_session_factory)

    response = await client.get(
        f"/api/v1/incidents/{incident.id}/enrichments",
        headers=viewer_headers,
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": 20, "offset": 0}


@pytest.mark.integration
@pytest.mark.anyio
async def test_missing_incident_returns_not_found(
    client: AsyncClient,
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        f"/api/v1/incidents/{uuid4()}/enrichments",
        headers=viewer_headers,
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Incident not found"}


@pytest.mark.integration
@pytest.mark.anyio
async def test_global_collection_lists_all_incidents_with_stable_pagination(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    records, _, _ = await seed_filter_records(database_session_factory)

    first = await client.get(
        "/api/v1/enrichments",
        params={"limit": 2},
        headers=viewer_headers,
    )
    second = await client.get(
        "/api/v1/enrichments",
        params={"limit": 2, "offset": 2},
        headers=viewer_headers,
    )

    assert first.status_code == second.status_code == 200
    assert first.json()["total"] == second.json()["total"] == 4
    assert [item["request_event_id"] for item in first.json()["items"]] == [
        str(records[key].request_event_id) for key in ("p1", "p2")
    ]
    assert [item["request_event_id"] for item in second.json()["items"]] == [
        str(records[key].request_event_id) for key in ("p3", "p4")
    ]


@pytest.mark.integration
@pytest.mark.anyio
async def test_global_filters_are_exact_trimmed_inclusive_and_composable(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    records, first_incident, _ = await seed_filter_records(database_session_factory)
    cases = [
        ({"incident_id": str(first_incident.id)}, ("p1", "p2")),
        ({"category": "performance"}, ("p1", "p3")),
        ({"provider": "  gemini  "}, ("p1", "p3")),
        ({"model": "  flash  "}, ("p1", "p4")),
        ({"created_from": "2026-01-03T00:00:00Z"}, ("p1", "p2")),
        ({"created_to": "2026-01-02T00:00:00Z"}, ("p3", "p4")),
        (
            {
                "category": "performance",
                "provider": "gemini",
                "model": "pro",
            },
            ("p3",),
        ),
        (
            {
                "created_from": "2026-01-02T00:00:00Z",
                "created_to": "2026-01-02T00:00:00Z",
            },
            ("p3",),
        ),
    ]

    for params, expected_keys in cases:
        response = await client.get(
            "/api/v1/enrichments", params=params, headers=viewer_headers
        )
        assert response.status_code == 200
        assert response.json()["total"] == len(expected_keys)
        assert [item["request_event_id"] for item in response.json()["items"]] == [
            str(records[key].request_event_id) for key in expected_keys
        ]

    scoped = await client.get(
        f"/api/v1/incidents/{first_incident.id}/enrichments",
        params={"category": "performance", "provider": " gemini "},
        headers=viewer_headers,
    )
    assert scoped.status_code == 200
    assert scoped.json()["total"] == 1
    assert scoped.json()["items"][0]["request_event_id"] == str(
        records["p1"].request_event_id
    )


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("role", list(UserRole))
async def test_all_roles_can_read_scoped_and_global_enrichments(
    role: UserRole,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    incident = await persist_incident(database_session_factory)
    await persist_enrichment(database_session_factory, incident_id=incident.id)
    headers, _ = await auth_headers_factory(role)

    scoped = await client.get(
        f"/api/v1/incidents/{incident.id}/enrichments", headers=headers
    )
    global_list = await client.get("/api/v1/enrichments", headers=headers)

    assert scoped.status_code == global_list.status_code == 200


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("headers", [None, {"Authorization": "Bearer invalid"}])
async def test_unauthenticated_callers_cannot_read_enrichments(
    headers: dict[str, str] | None,
    client: AsyncClient,
) -> None:
    scoped = await client.get(
        f"/api/v1/incidents/{uuid4()}/enrichments", headers=headers
    )
    global_list = await client.get("/api/v1/enrichments", headers=headers)

    assert scoped.status_code == global_list.status_code == 401
    assert scoped.headers["www-authenticate"] == "Bearer"
    assert global_list.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "params",
    [
        {"category": "invalid"},
        {"provider": "   "},
        {"model": "   "},
        {"created_from": "2026-01-01T00:00:00"},
        {"created_to": "2026-01-01T00:00:00"},
        {
            "created_from": "2026-01-02T00:00:00Z",
            "created_to": "2026-01-01T00:00:00Z",
        },
        {"limit": "0"},
        {"limit": "101"},
        {"offset": "-1"},
        {"unexpected": "value"},
    ],
)
async def test_invalid_query_parameters_return_unprocessable_entity(
    params: dict[str, str],
    client: AsyncClient,
    viewer_headers: dict[str, str],
) -> None:
    response = await client.get(
        "/api/v1/enrichments", params=params, headers=viewer_headers
    )

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_response_is_historical_snapshot_after_incident_changes(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    viewer_headers: dict[str, str],
) -> None:
    incident = await persist_incident(database_session_factory, title="Original title")
    enrichment = await persist_enrichment(
        database_session_factory,
        incident_id=incident.id,
        summary="Historical AI summary",
        category=EnrichmentCategory.SECURITY,
        suspected_component="identity",
        investigation_steps=["Review the historical authentication evidence."],
    )
    async with database_session_factory() as session:
        current = await session.get(Incident, incident.id)
        assert current is not None
        current.title = "Changed after enrichment"
        current.description = "Current state differs"
        current.severity = IncidentSeverity.CRITICAL
        await session.commit()

    response = await client.get(
        f"/api/v1/incidents/{incident.id}/enrichments",
        headers=viewer_headers,
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["request_event_id"] == str(enrichment.request_event_id)
    assert item["summary"] == "Historical AI summary"
    assert item["category"] == "security"
    assert item["suspected_component"] == "identity"
    assert item["investigation_steps"] == [
        "Review the historical authentication evidence."
    ]


@pytest.mark.integration
@pytest.mark.anyio
async def test_openapi_describes_read_only_authenticated_enrichment_api(
    client: AsyncClient,
) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    document = response.json()
    scoped = document["paths"]["/api/v1/incidents/{incident_id}/enrichments"]
    global_list = document["paths"]["/api/v1/enrichments"]
    assert set(scoped) == {"get"}
    assert set(global_list) == {"get"}
    assert "404" in scoped["get"]["responses"]
    assert scoped["get"]["security"]
    assert global_list["get"]["security"]
    assert {parameter["name"] for parameter in scoped["get"]["parameters"]} == {
        "incident_id",
        "category",
        "provider",
        "model",
        "created_from",
        "created_to",
        "limit",
        "offset",
    }
    assert {parameter["name"] for parameter in global_list["get"]["parameters"]} == {
        "incident_id",
        "category",
        "provider",
        "model",
        "created_from",
        "created_to",
        "limit",
        "offset",
    }
    properties = document["components"]["schemas"]["EnrichmentResponse"]["properties"]
    assert set(properties) == {
        "request_event_id",
        "incident_id",
        "trigger_event_id",
        "provider",
        "model",
        "summary",
        "category",
        "suspected_component",
        "investigation_steps",
        "created_at",
    }
    assert "gemini_api_key" not in str(document).lower()
