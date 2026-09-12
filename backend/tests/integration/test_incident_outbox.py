import json
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import event, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapper

from signalforge.incidents import application, service
from signalforge.incidents.models import Incident
from signalforge.incidents.schemas import IncidentCreate
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
