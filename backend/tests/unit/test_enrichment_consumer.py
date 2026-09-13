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

from signalforge.enrichment import consumer
from signalforge.enrichment.consumer import (
    EnrichmentProcessingResult,
    InvalidEnrichmentEventError,
    decode_event,
    handle_message,
)
from signalforge.enrichment.domain import EnrichmentInput, EnrichmentResult
from signalforge.enrichment.provider import (
    EnrichmentProvider,
    PermanentEnrichmentError,
    ProviderFailureReason,
    TransientEnrichmentError,
)

pytestmark = pytest.mark.anyio


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.ack_calls = 0
        self.reject_calls: list[bool] = []
        self.nack_calls: list[bool] = []
        self.ack_error: BaseException | None = None

    async def ack(self) -> None:
        self.ack_calls += 1
        if self.ack_error is not None:
            raise self.ack_error

    async def reject(self, *, requeue: bool) -> None:
        self.reject_calls.append(requeue)

    async def nack(self, *, requeue: bool) -> None:
        self.nack_calls.append(requeue)


class UnusedProvider:
    provider_name = "unused"
    model_name = "unused"

    def __init__(self) -> None:
        self.calls = 0

    async def enrich(self, input: EnrichmentInput) -> EnrichmentResult:
        self.calls += 1
        raise AssertionError("provider must not be called")


def valid_body() -> bytes:
    return json.dumps(
        {
            "event_id": str(uuid4()),
            "event_type": "triage.enrichment.requested",
            "event_version": 1,
            "occurred_at": datetime(2026, 9, 13, 12, tzinfo=UTC).isoformat(),
            "aggregate_id": str(uuid4()),
            "payload": {
                "trigger_event_id": str(uuid4()),
                "source": "monitoring",
                "title": "Checkout latency",
                "description": None,
                "original_severity": "high",
                "priority": "P2",
                "requires_human_review": False,
                "incident_occurred_at": datetime(
                    2026, 9, 13, 11, tzinfo=UTC
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


def dependencies() -> tuple[
    EnrichmentProvider,
    async_sessionmaker[AsyncSession],
]:
    return cast(EnrichmentProvider, object()), cast(
        async_sessionmaker[AsyncSession], object()
    )


def test_decode_accepts_only_enrichment_request_v1() -> None:
    event = decode_event(valid_body())

    assert event.event_type == "triage.enrichment.requested"
    assert event.event_version == 1
    assert event.payload.priority == "P2"


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"\xff", "invalid_encoding"),
        (b"{", "invalid_json"),
        (with_change(valid_body(), event_type="incident.created"), "unsupported_type"),
        (with_change(valid_body(), event_version=2), "unsupported_version"),
        (with_change(valid_body(), payload={"priority": "P9"}), "invalid_event"),
    ],
)
def test_decode_rejects_invalid_or_unsupported_events(
    body: bytes,
    reason: str,
) -> None:
    with pytest.raises(InvalidEnrichmentEventError) as caught:
        decode_event(body)

    assert caught.value.reason == reason


async def test_invalid_event_rejects_without_provider_or_database_use() -> None:
    incoming, fake = message(b"{")
    provider = UnusedProvider()

    with pytest.raises(InvalidEnrichmentEventError):
        await handle_message(
            incoming,
            provider,
            cast(async_sessionmaker[AsyncSession], object()),
        )

    assert provider.calls == 0
    assert fake.reject_calls == [False]
    assert fake.nack_calls == []
    assert fake.ack_calls == 0


@pytest.mark.parametrize(
    "error",
    [
        TransientEnrichmentError(ProviderFailureReason.UNAVAILABLE),
        OperationalError("database unavailable", {}, OSError("private URL")),
    ],
)
async def test_transient_or_database_failure_is_nacked_for_requeue(
    error: BaseException,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    process = AsyncMock(side_effect=error)
    monkeypatch.setattr(consumer, "process_event", process)
    provider, session_factory = dependencies()

    with pytest.raises(type(error)):
        await handle_message(incoming, provider, session_factory)

    assert fake.nack_calls == [True]
    assert fake.reject_calls == []
    assert fake.ack_calls == 0


async def test_permanent_provider_failure_is_rejected_without_requeue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    process = AsyncMock(
        side_effect=PermanentEnrichmentError(ProviderFailureReason.REQUEST_REJECTED)
    )
    monkeypatch.setattr(consumer, "process_event", process)
    provider, session_factory = dependencies()

    with pytest.raises(PermanentEnrichmentError):
        await handle_message(incoming, provider, session_factory)

    assert fake.reject_calls == [False]
    assert fake.nack_calls == []
    assert fake.ack_calls == 0


async def test_invalid_provider_response_is_nacked_for_requeue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    process = AsyncMock(
        side_effect=TransientEnrichmentError(ProviderFailureReason.INVALID_RESPONSE)
    )
    monkeypatch.setattr(consumer, "process_event", process)
    provider, session_factory = dependencies()

    with pytest.raises(TransientEnrichmentError) as caught:
        await handle_message(incoming, provider, session_factory)

    assert caught.value.reason is ProviderFailureReason.INVALID_RESPONSE
    assert fake.nack_calls == [True]
    assert fake.reject_calls == []
    assert fake.ack_calls == 0


async def test_ack_waits_for_processing_and_returns_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    started = asyncio.Event()
    complete = asyncio.Event()

    async def process(*args: object) -> EnrichmentProcessingResult:
        started.set()
        await complete.wait()
        return EnrichmentProcessingResult.PROCESSED

    monkeypatch.setattr(consumer, "process_event", process)
    provider, session_factory = dependencies()
    task = asyncio.create_task(handle_message(incoming, provider, session_factory))
    await asyncio.wait_for(started.wait(), timeout=1)
    assert fake.ack_calls == 0

    complete.set()
    assert (
        await asyncio.wait_for(task, timeout=1) is EnrichmentProcessingResult.PROCESSED
    )
    assert fake.ack_calls == 1


async def test_ack_failure_propagates_after_success_without_nack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    fake.ack_error = OSError("broker unavailable")
    monkeypatch.setattr(
        consumer,
        "process_event",
        AsyncMock(return_value=EnrichmentProcessingResult.PROCESSED),
    )
    provider, session_factory = dependencies()

    with pytest.raises(OSError, match="broker unavailable"):
        await handle_message(incoming, provider, session_factory)

    assert fake.ack_calls == 1
    assert fake.nack_calls == []
    assert fake.reject_calls == []


async def test_unrelated_programming_error_has_no_message_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming, fake = message(valid_body())
    monkeypatch.setattr(
        consumer,
        "process_event",
        AsyncMock(side_effect=RuntimeError("programming defect")),
    )
    provider, session_factory = dependencies()

    with pytest.raises(RuntimeError, match="programming defect"):
        await handle_message(incoming, provider, session_factory)

    assert fake.ack_calls == 0
    assert fake.nack_calls == []
    assert fake.reject_calls == []
