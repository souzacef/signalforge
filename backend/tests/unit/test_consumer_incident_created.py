import asyncio
import json
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aio_pika import IncomingMessage
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.consumers import incident_created
from signalforge.consumers.incident_created import (
    InvalidEventError,
    ProcessingResult,
    decode_event,
    handle_message,
)
from signalforge.db.errors import DatabaseTransportError

pytestmark = pytest.mark.anyio


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.ack_calls = 0
        self.reject_calls: list[bool] = []
        self.nack_calls: list[bool] = []
        self.ack_error: BaseException | None = None
        self.nack_error: BaseException | None = None

    async def ack(self) -> None:
        self.ack_calls += 1
        if self.ack_error is not None:
            raise self.ack_error

    async def reject(self, *, requeue: bool) -> None:
        self.reject_calls.append(requeue)

    async def nack(self, *, requeue: bool) -> None:
        self.nack_calls.append(requeue)
        if self.nack_error is not None:
            raise self.nack_error


def valid_body() -> bytes:
    return json.dumps(
        {
            "event_id": str(uuid4()),
            "event_type": "incident.created",
            "event_version": 1,
            "occurred_at": datetime(2026, 9, 13, tzinfo=UTC).isoformat(),
            "aggregate_id": str(uuid4()),
            "payload": {
                "source": "manual",
                "title": "Database latency",
                "description": None,
                "severity": "high",
                "incident_occurred_at": datetime(
                    2026, 9, 12, 23, tzinfo=UTC
                ).isoformat(),
            },
        }
    ).encode()


def with_change(body: bytes, **changes: object) -> bytes:
    data = json.loads(body)
    data.update(changes)
    return json.dumps(data).encode()


def message(body: bytes) -> tuple[IncomingMessage, FakeMessage]:
    fake = FakeMessage(body)
    return cast(IncomingMessage, fake), fake


def test_decode_valid_incident_created() -> None:
    decoded = decode_event(valid_body())

    assert decoded.event_type == "incident.created"
    assert decoded.event_version == 1
    assert decoded.payload.severity == "high"


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"\xff", "invalid_encoding"),
        (b"{", "invalid_json"),
        (
            with_change(valid_body(), event_type="incident.resolved"),
            "unsupported_type",
        ),
        (with_change(valid_body(), event_version=2), "unsupported_version"),
        (
            with_change(valid_body(), payload={"severity": "impossible"}),
            "invalid_event",
        ),
    ],
)
def test_decode_rejects_invalid_or_unsupported_content(
    body: bytes, reason: str
) -> None:
    with pytest.raises(InvalidEventError) as caught:
        decode_event(body)

    assert caught.value.reason == reason


@pytest.mark.parametrize(
    "result", [ProcessingResult.PROCESSED, ProcessingResult.DUPLICATE]
)
async def test_handler_acks_and_returns_processing_result(
    result: ProcessingResult,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    process = AsyncMock(return_value=result)
    monkeypatch.setattr(incident_created, "process_event", process)

    actual = await handle_message(
        incoming, cast(async_sessionmaker[AsyncSession], object())
    )

    assert actual is result
    assert fake.ack_calls == 1
    assert fake.reject_calls == []
    assert fake.nack_calls == []


async def test_ack_waits_until_processing_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    started = asyncio.Event()
    complete = asyncio.Event()

    async def process(*args: object) -> ProcessingResult:
        started.set()
        await complete.wait()
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(incident_created, "process_event", process)
    task = asyncio.create_task(
        handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    assert fake.ack_calls == 0

    complete.set()
    assert await asyncio.wait_for(task, timeout=1) is ProcessingResult.PROCESSED
    assert fake.ack_calls == 1


async def test_invalid_message_is_rejected_without_requeue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(b"{")
    process = AsyncMock()
    monkeypatch.setattr(incident_created, "process_event", process)

    with pytest.raises(InvalidEventError):
        await handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))

    process.assert_not_awaited()
    assert fake.reject_calls == [False]
    assert fake.ack_calls == 0
    assert fake.nack_calls == []


async def test_database_failure_is_nacked_for_requeue_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    secret = "postgresql://user:password-must-stay-private@host/database"
    process = AsyncMock(
        side_effect=OperationalError("unavailable", {}, OSError(secret))
    )
    monkeypatch.setattr(incident_created, "process_event", process)

    with pytest.raises(OperationalError):
        await handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))

    assert fake.nack_calls == [True]
    assert fake.ack_calls == 0
    assert fake.reject_calls == []


async def test_raw_database_transport_failure_is_nacked_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    secret = "postgresql://user:secret@database/private"
    monkeypatch.setattr(
        incident_created, "process_event", AsyncMock(side_effect=OSError(secret))
    )

    with pytest.raises(DatabaseTransportError) as caught:
        await handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))

    assert secret not in str(caught.value)
    assert fake.nack_calls == [True]
    assert fake.ack_calls == 0
    assert fake.reject_calls == []


async def test_nack_transport_failure_is_not_wrapped_as_database_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    fake.nack_error = OSError("RabbitMQ NACK failed")
    monkeypatch.setattr(
        incident_created,
        "process_event",
        AsyncMock(side_effect=OSError("database unavailable")),
    )

    with pytest.raises(OSError, match="RabbitMQ NACK failed") as caught:
        await handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))

    assert not isinstance(caught.value, DatabaseTransportError)
    assert fake.nack_calls == [True]
    assert fake.ack_calls == 0


async def test_programming_error_propagates_without_message_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    process = AsyncMock(side_effect=RuntimeError("programming defect"))
    monkeypatch.setattr(incident_created, "process_event", process)

    with pytest.raises(RuntimeError, match="programming defect"):
        await handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))

    assert fake.ack_calls == 0
    assert fake.reject_calls == []
    assert fake.nack_calls == []


async def test_ack_failure_after_processing_propagates_without_requeue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    fake.ack_error = OSError("broker unavailable")
    process = AsyncMock(return_value=ProcessingResult.PROCESSED)
    monkeypatch.setattr(incident_created, "process_event", process)

    with pytest.raises(OSError, match="broker unavailable"):
        await handle_message(incoming, cast(async_sessionmaker[AsyncSession], object()))

    process.assert_awaited_once()
    assert fake.ack_calls == 1
    assert fake.nack_calls == []


def test_custom_validation_error_does_not_expose_input_secrets() -> None:
    database_secret = "database-password-must-stay-private"
    rabbitmq_secret = "rabbit-password-must-stay-private"
    body = with_change(
        valid_body(),
        payload={
            "severity": "invalid",
            "database_url": database_secret,
            "rabbitmq_url": rabbitmq_secret,
        },
    )

    with pytest.raises(InvalidEventError) as caught:
        decode_event(body)

    rendered = str(caught.value)
    assert database_secret not in rendered
    assert rabbitmq_secret not in rendered
