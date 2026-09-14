import json
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind
from sqlalchemy import event, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapper

from signalforge.core.config import TracingSettings
from signalforge.db.session import get_session
from signalforge.incidents import application, service
from signalforge.incidents.models import Incident
from signalforge.incidents.schemas import IncidentCreate
from signalforge.main import create_app
from signalforge.observability.tracing import (
    API_SERVICE_NAME,
    create_tracing_runtime,
)
from signalforge.outbox.models import OutboxEvent
from signalforge.users.models import UserRole
from tests.integration.factories import AuthHeadersFactory

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


@pytest.fixture
def incident_payload() -> dict[str, Any]:
    return {
        "source": " manual ",
        "title": " Checkout alert ",
        "description": " Database unreachable ",
        "severity": "high",
        "occurred_at": "2026-09-11T15:30:00-03:00",
    }


async def assert_no_creation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Incident)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.parametrize("description", [" Database unreachable ", None])
async def test_authenticated_creation_commits_incident_and_snapshot_once(
    description: str | None,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
    incident_payload: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incident_payload["description"] = description
    commits: list[AsyncSession] = []
    original_commit = AsyncSession.commit

    async def observe_commit(session: AsyncSession) -> None:
        commits.append(session)
        await original_commit(session)

    # Authentication uses the real shared session and triggers autobegin.
    monkeypatch.setattr(AsyncSession, "commit", observe_commit)
    response = await client.post(
        "/api/v1/incidents", json=incident_payload, headers=operator_headers
    )

    assert response.status_code == 201
    assert len(commits) == 1
    body = response.json()
    incident_id = UUID(body["id"])
    async with database_session_factory() as session:
        incident = (await session.scalars(select(Incident))).one()
        outbox = (await session.scalars(select(OutboxEvent))).one()

    assert incident.id == incident_id
    assert incident.created_at == datetime.fromisoformat(body["created_at"])
    assert incident.updated_at == datetime.fromisoformat(body["updated_at"])
    assert isinstance(outbox.id, UUID)
    assert outbox.id != incident.id
    assert outbox.aggregate_id == incident.id
    assert outbox.event_type == "incident.created"
    assert outbox.event_version == 1
    assert outbox.occurred_at == incident.created_at
    assert outbox.occurred_at != incident.occurred_at
    assert outbox.created_at.tzinfo is not None
    assert outbox.published_at is None
    assert outbox.attempt_count == 0
    assert outbox.next_attempt_at == outbox.created_at
    assert outbox.claim_token is None
    assert outbox.claimed_until is None
    assert outbox.last_error is None
    assert outbox.traceparent is None
    assert outbox.tracestate is None
    assert outbox.payload == {
        "source": "manual",
        "title": "Checkout alert",
        "description": description.strip() if description is not None else None,
        "severity": "high",
        "incident_occurred_at": "2026-09-11T15:30:00-03:00",
    }
    assert type(outbox.payload["severity"]) is str
    assert json.loads(json.dumps(outbox.payload)) == outbox.payload


@pytest.mark.parametrize(
    ("request_case", "expected_status"),
    [("anonymous", 401), ("viewer", 403), ("invalid", 422)],
)
async def test_rejected_creation_has_no_outbox_intent(
    request_case: str,
    expected_status: int,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    auth_headers_factory: AuthHeadersFactory,
    incident_payload: dict[str, Any],
) -> None:
    headers = {}
    if request_case != "anonymous":
        role = UserRole.VIEWER if request_case == "viewer" else UserRole.OPERATOR
        headers, _ = await auth_headers_factory(role)
    if request_case == "invalid":
        incident_payload["severity"] = "invalid-severity"

    response = await client.post(
        "/api/v1/incidents", json=incident_payload, headers=headers
    )

    assert response.status_code == expected_status
    await assert_no_creation(database_session_factory)


async def test_persistence_creation_flushes_defaults_but_caller_can_rollback(
    database_session_factory: async_sessionmaker[AsyncSession],
    incident_payload: dict[str, Any],
) -> None:
    async with database_session_factory() as session:
        incident = await service.create_incident(
            session, IncidentCreate.model_validate(incident_payload)
        )
        incident_id = incident.id
        assert isinstance(incident_id, UUID)
        assert incident.created_at.tzinfo is not None
        assert incident.updated_at.tzinfo is not None
        assert await session.scalar(select(Incident.id)) == incident_id
        await session.rollback()

    await assert_no_creation(database_session_factory)


@pytest.mark.parametrize(
    ("field", "invalid_value", "constraint"),
    [
        ("event_version", 0, "ck_outbox_events_event_version_positive"),
        ("event_type", " ", "ck_outbox_events_event_type_nonempty"),
        ("payload", [], "ck_outbox_events_payload_object"),
    ],
)
async def test_outbox_constraint_failure_rolls_back_flushed_incident(
    field: str,
    invalid_value: object,
    constraint: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
    incident_payload: dict[str, Any],
) -> None:
    flushed_incidents: list[UUID] = []

    def invalidate_outbox(
        mapper: Mapper[OutboxEvent], connection: Connection, target: OutboxEvent
    ) -> None:
        # Verify the first INSERT really reached PostgreSQL before failing the second.
        assert (
            connection.scalar(
                select(Incident.id).where(Incident.id == target.aggregate_id)
            )
            == target.aggregate_id
        )
        flushed_incidents.append(target.aggregate_id)
        setattr(target, field, invalid_value)

    event.listen(OutboxEvent, "before_insert", invalidate_outbox)
    try:
        with pytest.raises(IntegrityError, match=constraint):
            await client.post(
                "/api/v1/incidents", json=incident_payload, headers=operator_headers
            )
    finally:
        event.remove(OutboxEvent, "before_insert", invalidate_outbox)

    assert len(flushed_incidents) == 1
    await assert_no_creation(database_session_factory)


@pytest.mark.parametrize("failure_stage", ["event", "response"])
async def test_validation_failure_before_commit_rolls_back_creation(
    failure_stage: str,
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
    incident_payload: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validator = Mock(side_effect=ValueError("simulated contract validation failure"))
    if failure_stage == "event":
        monkeypatch.setattr(application, "IncidentCreated", validator)
    else:
        monkeypatch.setattr(application.IncidentResponse, "model_validate", validator)

    with pytest.raises(ValueError, match="simulated contract validation failure"):
        await client.post(
            "/api/v1/incidents", json=incident_payload, headers=operator_headers
        )

    validator.assert_called_once()
    await assert_no_creation(database_session_factory)


async def test_final_commit_failure_propagates_without_http_success(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
    incident_payload: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commit = AsyncMock(side_effect=SQLAlchemyError("simulated final commit failure"))
    monkeypatch.setattr(AsyncSession, "commit", commit)

    with pytest.raises(SQLAlchemyError, match="simulated final commit failure"):
        await client.post(
            "/api/v1/incidents", json=incident_payload, headers=operator_headers
        )

    commit.assert_awaited_once()
    await assert_no_creation(database_session_factory)


async def test_traced_api_creation_persists_server_context_outside_domain_json(
    database_session_factory: async_sessionmaker[AsyncSession],
    operator_headers: dict[str, str],
    incident_payload: dict[str, Any],
) -> None:
    exporter = InMemorySpanExporter()
    tracing = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
        ),
        service_name=API_SERVICE_NAME,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    traced_app = create_app(tracing=tracing)

    async def override_session() -> Any:
        async with database_session_factory() as session:
            yield session

    traced_app.dependency_overrides[get_session] = override_session
    trace_id = "0af7651916cd43dd8448eb211c80319c"
    remote_parent_id = "b7ad6b7169203331"
    transport = ASGITransport(app=traced_app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as traced:
            response = await traced.post(
                "/api/v1/incidents",
                json=incident_payload,
                headers={
                    **operator_headers,
                    "traceparent": f"00-{trace_id}-{remote_parent_id}-01",
                    "tracestate": "vendor=value",
                    "baggage": "private=must-not-persist",
                },
            )
        assert response.status_code == 201

        async with database_session_factory() as session:
            outbox = (
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.aggregate_id == UUID(response.json()["id"])
                    )
                )
            ).one()

        server_spans = [
            span
            for span in exporter.get_finished_spans()
            if span.kind is SpanKind.SERVER
        ]
        assert len(server_spans) == 1
        server_span = server_spans[0]
        assert server_span.context is not None
        assert server_span.context.trace_id == int(trace_id, 16)
        assert outbox.traceparent is not None
        parts = outbox.traceparent.split("-")
        assert parts[1] == trace_id
        assert parts[2] == f"{server_span.context.span_id:016x}"
        assert parts[3] == "01"
        assert outbox.tracestate == "vendor=value"
        assert len(outbox.traceparent) <= 512
        assert len(outbox.tracestate) <= 512
        serialized_payload = json.dumps(outbox.payload)
        assert "traceparent" not in serialized_payload
        assert "tracestate" not in serialized_payload
        assert "baggage" not in serialized_payload
    finally:
        traced_app.dependency_overrides.clear()
        tracing.shutdown()
