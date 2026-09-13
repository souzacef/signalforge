import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest
from aio_pika import IncomingMessage
from sqlalchemy import delete, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Session

from signalforge.consumers.models import ProcessedEvent
from signalforge.db.session import engine
from signalforge.enrichment.consumer import (
    CONSUMER_NAME,
    EnrichmentProcessingResult,
    InvalidEnrichmentEventError,
    handle_message,
    process_event,
)
from signalforge.enrichment.domain import (
    EnrichmentCategory,
    EnrichmentInput,
    EnrichmentResult,
)
from signalforge.enrichment.models import TriageEnrichment
from signalforge.enrichment.provider import (
    ProviderFailureReason,
    TransientEnrichmentError,
)
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.triage.events import (
    TriageEnrichmentRequested,
    TriageEnrichmentRequestedPayload,
)
from signalforge.triage.models import TriagePriority

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)


class FakeProvider:
    provider_name = "fake-ai"
    model_name = "fake-structured-v1"

    def __init__(self) -> None:
        self.calls: list[EnrichmentInput] = []
        self.failure: BaseException | None = None
        self.barrier: asyncio.Barrier | None = None
        self.result = EnrichmentResult(
            summary="Checkout latency is consistent with a database bottleneck.",
            category=EnrichmentCategory.PERFORMANCE,
            suspected_component="checkout database",
            investigation_steps=[
                "Compare query latency with the incident occurrence time.",
                "Inspect connection-pool saturation for checkout requests.",
            ],
        )

    async def enrich(self, input: EnrichmentInput) -> EnrichmentResult:
        self.calls.append(input)
        if self.barrier is not None:
            await self.barrier.wait()
        if self.failure is not None:
            raise self.failure
        return self.result


class FakeMessage:
    def __init__(
        self,
        body: bytes,
        *,
        on_ack: Callable[[], None] | None = None,
        ack_error: BaseException | None = None,
    ) -> None:
        self.body = body
        self.on_ack = on_ack
        self.ack_error = ack_error
        self.ack_calls = 0
        self.nack_calls: list[bool] = []
        self.reject_calls: list[bool] = []

    async def ack(self) -> None:
        self.ack_calls += 1
        if self.on_ack is not None:
            self.on_ack()
        if self.ack_error is not None:
            raise self.ack_error

    async def nack(self, *, requeue: bool) -> None:
        self.nack_calls.append(requeue)

    async def reject(self, *, requeue: bool) -> None:
        self.reject_calls.append(requeue)


def incoming(message: FakeMessage) -> IncomingMessage:
    return cast(IncomingMessage, message)


@pytest.fixture
async def enrichment_event() -> AsyncIterator[TriageEnrichmentRequested]:
    event = TriageEnrichmentRequested(
        event_id=uuid4(),
        occurred_at=datetime(2026, 9, 13, 12, tzinfo=UTC),
        aggregate_id=uuid4(),
        payload=TriageEnrichmentRequestedPayload(
            trigger_event_id=uuid4(),
            source="monitoring",
            title="Checkout latency",
            description="P95 latency exceeded the service objective.",
            original_severity=IncidentSeverity.HIGH,
            priority=TriagePriority.P2,
            requires_human_review=False,
            incident_occurred_at=datetime(2026, 9, 13, 11, tzinfo=UTC),
        ),
    )
    async with sessions.begin() as session:
        session.add(
            Incident(
                id=event.aggregate_id,
                source=event.payload.source,
                title=event.payload.title,
                description=event.payload.description,
                severity=event.payload.original_severity,
                occurred_at=event.payload.incident_occurred_at,
            )
        )
    try:
        yield event
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(Incident).where(Incident.id == event.aggregate_id)
            )


@pytest.fixture(autouse=True)
async def clean_enrichment_effects() -> AsyncIterator[None]:
    async with sessions.begin() as session:
        await session.execute(delete(TriageEnrichment))
        await session.execute(
            delete(ProcessedEvent).where(ProcessedEvent.consumer_name == CONSUMER_NAME)
        )
    yield
    async with sessions.begin() as session:
        await session.execute(delete(TriageEnrichment))
        await session.execute(
            delete(ProcessedEvent).where(ProcessedEvent.consumer_name == CONSUMER_NAME)
        )


async def receipt_rows() -> list[ProcessedEvent]:
    async with sessions() as session:
        return list(
            await session.scalars(
                select(ProcessedEvent).where(
                    ProcessedEvent.consumer_name == CONSUMER_NAME
                )
            )
        )


async def enrichment_rows() -> list[TriageEnrichment]:
    async with sessions() as session:
        return list(await session.scalars(select(TriageEnrichment)))


async def test_first_processing_commits_receipt_and_result_before_ack(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    committed = False

    def mark_committed(session: Session) -> None:
        nonlocal committed
        committed = True

    def assert_committed() -> None:
        assert committed

    sa_event.listen(Session, "after_commit", mark_committed)
    message = FakeMessage(
        enrichment_event.model_dump_json().encode(), on_ack=assert_committed
    )
    try:
        result = await handle_message(incoming(message), provider, sessions)
    finally:
        sa_event.remove(Session, "after_commit", mark_committed)

    assert result is EnrichmentProcessingResult.PROCESSED
    assert message.ack_calls == 1
    assert message.nack_calls == message.reject_calls == []
    assert len(provider.calls) == 1
    assert provider.calls[0] == EnrichmentInput(
        incident_id=enrichment_event.aggregate_id,
        source=enrichment_event.payload.source,
        title=enrichment_event.payload.title,
        description=enrichment_event.payload.description,
        original_severity=enrichment_event.payload.original_severity,
        priority=enrichment_event.payload.priority,
        requires_human_review=enrichment_event.payload.requires_human_review,
        incident_occurred_at=enrichment_event.payload.incident_occurred_at,
    )

    receipts = await receipt_rows()
    enrichments = await enrichment_rows()
    assert len(receipts) == len(enrichments) == 1
    assert receipts[0].event_id == enrichment_event.event_id
    assert receipts[0].event_type == enrichment_event.event_type
    persisted = enrichments[0]
    assert persisted.request_event_id == enrichment_event.event_id
    assert persisted.incident_id == enrichment_event.aggregate_id
    assert persisted.trigger_event_id == enrichment_event.payload.trigger_event_id
    assert persisted.provider == provider.provider_name
    assert persisted.model == provider.model_name
    assert persisted.summary == provider.result.summary
    assert persisted.category is provider.result.category
    assert persisted.suspected_component == provider.result.suspected_component
    assert persisted.investigation_steps == provider.result.investigation_steps
    assert persisted.created_at.tzinfo is not None


async def test_committed_redelivery_is_duplicate_without_second_provider_call(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    first = await process_event(enrichment_event, provider, sessions)
    original = (await enrichment_rows())[0]
    snapshot = (
        original.request_event_id,
        original.summary,
        original.category,
        original.investigation_steps,
        original.created_at,
    )

    second = await process_event(enrichment_event, provider, sessions)

    assert first is EnrichmentProcessingResult.PROCESSED
    assert second is EnrichmentProcessingResult.DUPLICATE
    assert len(provider.calls) == 1
    assert len(await receipt_rows()) == len(await enrichment_rows()) == 1
    persisted = (await enrichment_rows())[0]
    assert (
        persisted.request_event_id,
        persisted.summary,
        persisted.category,
        persisted.investigation_steps,
        persisted.created_at,
    ) == snapshot


async def test_concurrent_duplicate_calls_may_both_invoke_provider_but_persist_once(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    provider.barrier = asyncio.Barrier(2)

    async def worker() -> tuple[int, EnrichmentProcessingResult]:
        async with engine.connect() as connection:
            pid = (await connection.execute(select(func.pg_backend_pid()))).scalar_one()
            await connection.rollback()
            factory = async_sessionmaker(bind=connection, expire_on_commit=False)
            return pid, await process_event(enrichment_event, provider, factory)

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker())
        second = group.create_task(worker())

    pid_a, result_a = first.result()
    pid_b, result_b = second.result()
    assert pid_a != pid_b
    assert {result_a, result_b} == {
        EnrichmentProcessingResult.PROCESSED,
        EnrichmentProcessingResult.DUPLICATE,
    }
    assert 1 <= len(provider.calls) <= 2
    assert len(await receipt_rows()) == len(await enrichment_rows()) == 1


async def test_ack_loss_redelivery_skips_provider_and_preserves_result(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    first = FakeMessage(
        enrichment_event.model_dump_json().encode(),
        ack_error=OSError("simulated broker connection loss"),
    )

    with pytest.raises(OSError, match="simulated broker connection loss"):
        await handle_message(incoming(first), provider, sessions)

    original = (await enrichment_rows())[0]
    snapshot = (original.summary, original.investigation_steps, original.created_at)
    redelivery = FakeMessage(enrichment_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), provider, sessions)

    assert result is EnrichmentProcessingResult.DUPLICATE
    assert len(provider.calls) == 1
    assert redelivery.ack_calls == 1
    persisted = (await enrichment_rows())[0]
    assert (
        persisted.summary,
        persisted.investigation_steps,
        persisted.created_at,
    ) == snapshot
    assert len(await receipt_rows()) == len(await enrichment_rows()) == 1


async def test_transient_provider_failure_requeues_without_rows_then_retries(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    provider.failure = TransientEnrichmentError(ProviderFailureReason.UNAVAILABLE)
    first = FakeMessage(enrichment_event.model_dump_json().encode())

    with pytest.raises(TransientEnrichmentError):
        await handle_message(incoming(first), provider, sessions)

    assert first.nack_calls == [True]
    assert first.ack_calls == 0
    assert await receipt_rows() == []
    assert await enrichment_rows() == []

    provider.failure = None
    redelivery = FakeMessage(enrichment_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), provider, sessions)

    assert result is EnrichmentProcessingResult.PROCESSED
    assert redelivery.ack_calls == 1
    assert len(provider.calls) == 2
    assert len(await receipt_rows()) == len(await enrichment_rows()) == 1


async def test_database_failure_rolls_back_receipt_and_result_then_retries(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    secret = "postgresql://user:password-must-stay-private@host/database"

    def fail_result(session: Session, *args: object) -> None:
        if any(isinstance(item, TriageEnrichment) for item in session.new):
            raise OperationalError("result unavailable", {}, OSError(secret))

    sa_event.listen(Session, "before_flush", fail_result)
    first = FakeMessage(enrichment_event.model_dump_json().encode())
    try:
        with pytest.raises(OperationalError):
            await handle_message(incoming(first), provider, sessions)
    finally:
        sa_event.remove(Session, "before_flush", fail_result)

    assert first.nack_calls == [True]
    assert first.ack_calls == 0
    assert await receipt_rows() == []
    assert await enrichment_rows() == []

    redelivery = FakeMessage(enrichment_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), provider, sessions)

    assert result is EnrichmentProcessingResult.PROCESSED
    assert redelivery.ack_calls == 1
    assert len(provider.calls) == 2
    assert len(await receipt_rows()) == len(await enrichment_rows()) == 1


async def test_invalid_provider_result_requeues_without_rows_then_retries(
    enrichment_event: TriageEnrichmentRequested,
) -> None:
    provider = FakeProvider()
    valid_result = provider.result
    provider.result = cast(EnrichmentResult, {"summary": "missing required data"})
    first = FakeMessage(enrichment_event.model_dump_json().encode())

    with pytest.raises(TransientEnrichmentError) as caught:
        await handle_message(incoming(first), provider, sessions)

    assert caught.value.reason is ProviderFailureReason.INVALID_RESPONSE
    assert first.nack_calls == [True]
    assert first.reject_calls == []
    assert first.ack_calls == 0
    assert len(provider.calls) == 1
    assert await receipt_rows() == []
    assert await enrichment_rows() == []

    provider.result = valid_result
    redelivery = FakeMessage(enrichment_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), provider, sessions)

    assert result is EnrichmentProcessingResult.PROCESSED
    assert redelivery.ack_calls == 1
    assert redelivery.nack_calls == redelivery.reject_calls == []
    assert len(provider.calls) == 2
    assert len(await receipt_rows()) == len(await enrichment_rows()) == 1


async def test_invalid_message_rejects_without_provider_or_database_rows() -> None:
    provider = FakeProvider()
    message = FakeMessage(b'{"event_type":"incident.created"}')

    with pytest.raises(InvalidEnrichmentEventError):
        await handle_message(incoming(message), provider, sessions)

    assert message.reject_calls == [False]
    assert message.nack_calls == []
    assert provider.calls == []
    assert await receipt_rows() == []
    assert await enrichment_rows() == []
