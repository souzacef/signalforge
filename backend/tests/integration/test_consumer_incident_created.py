import asyncio
import os
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from urllib.parse import quote
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, IncomingMessage, Message
from aio_pika.abc import AbstractQueue
from httpx import AsyncClient
from pamqp.commands import Basic
from sqlalchemy import delete, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Session
from yarl import URL

from signalforge.consumers.incident_created import (
    CONSUMER_NAME,
    InvalidEventError,
    ProcessingResult,
    handle_message,
    process_event,
)
from signalforge.consumers.models import ProcessedEvent
from signalforge.db.session import engine
from signalforge.incidents.events import IncidentCreated, IncidentCreatedPayload
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.outbox.models import OutboxEvent
from signalforge.outbox.rabbitmq import (
    ENRICHMENT_ROUTING_KEY,
    QUEUE_NAME,
    ROUTING_KEY,
    declare_topology,
)
from signalforge.triage.models import IncidentTriage, TriagePriority

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)


@dataclass(frozen=True, slots=True)
class Broker:
    url: URL
    api: AsyncClient
    vhost: str


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


@pytest.fixture
async def incident_event() -> AsyncIterator[IncidentCreated]:
    event = IncidentCreated(
        event_id=uuid4(),
        occurred_at=datetime(2026, 9, 13, 12, tzinfo=UTC),
        aggregate_id=uuid4(),
        payload=IncidentCreatedPayload(
            source="integration",
            title="Consumer idempotency probe",
            description="Process once",
            severity=IncidentSeverity.HIGH,
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
                severity=event.payload.severity,
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
async def clean_consumer_effects() -> AsyncIterator[None]:
    async with sessions.begin() as session:
        await session.execute(
            delete(OutboxEvent).where(OutboxEvent.event_type == ENRICHMENT_ROUTING_KEY)
        )
        await session.execute(delete(ProcessedEvent))
        await session.execute(delete(IncidentTriage))
    yield
    async with sessions.begin() as session:
        await session.execute(
            delete(OutboxEvent).where(OutboxEvent.event_type == ENRICHMENT_ROUTING_KEY)
        )
        await session.execute(delete(ProcessedEvent))
        await session.execute(delete(IncidentTriage))


@pytest.fixture
async def broker() -> AsyncIterator[Broker]:
    raw_url = os.environ.get("SIGNALFORGE_TEST_RABBITMQ_URL")
    if raw_url is None:
        pytest.skip("Set SIGNALFORGE_TEST_RABBITMQ_URL to run real RabbitMQ tests")
    url = URL(raw_url)
    if url.scheme not in {"amqp", "amqps"} or not url.path.startswith(
        "/signalforge_test_"
    ):
        pytest.fail("RabbitMQ test URL must name a signalforge_test_ vhost")
    management_url = os.environ.get("SIGNALFORGE_TEST_RABBITMQ_MANAGEMENT_URL")
    if management_url is None or not url.user or not url.password:
        pytest.fail("Provide test RabbitMQ credentials and its management URL")
    vhost = f"signalforge_test_{uuid4().hex}"
    async with AsyncClient(
        base_url=management_url, auth=(url.user, url.password), timeout=10
    ) as api:
        response = await api.get("/api/overview")
        response.raise_for_status()
        assert response.json()["rabbitmq_version"] == "4.3.5"
        (await api.put(f"/api/vhosts/{vhost}", json={})).raise_for_status()
        try:
            (
                await api.put(
                    f"/api/permissions/{vhost}/{quote(url.user, safe='')}",
                    json={"configure": ".*", "write": ".*", "read": ".*"},
                )
            ).raise_for_status()
            yield Broker(url.with_path(f"/{vhost}"), api, vhost)
        finally:
            response = await api.delete(f"/api/vhosts/{vhost}")
            assert response.status_code in {204, 404}


async def ledger_rows() -> list[ProcessedEvent]:
    async with sessions() as session:
        return list(await session.scalars(select(ProcessedEvent)))


async def triage_rows() -> list[IncidentTriage]:
    async with sessions() as session:
        return list(await session.scalars(select(IncidentTriage)))


async def enrichment_rows() -> list[OutboxEvent]:
    async with sessions() as session:
        return list(
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == ENRICHMENT_ROUTING_KEY
                )
            )
        )


def incoming(fake: FakeMessage) -> IncomingMessage:
    return cast(IncomingMessage, fake)


async def publish(
    exchange: aio_pika.abc.AbstractExchange,
    body: bytes,
    *,
    message_id: UUID,
) -> None:
    confirmation = await exchange.publish(
        Message(
            body,
            message_id=str(message_id),
            type=ROUTING_KEY,
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=ROUTING_KEY,
        mandatory=True,
    )
    assert isinstance(confirmation, Basic.Ack)


async def queue_has_no_ready_delivery(queue: AbstractQueue) -> bool:
    return await queue.get(no_ack=True, fail=False, timeout=0.2) is None


async def test_first_processing_atomically_creates_all_three_durable_effects(
    incident_event: IncidentCreated,
) -> None:
    result = await process_event(incident_event, sessions)

    assert result is ProcessingResult.PROCESSED
    receipts = await ledger_rows()
    triage = await triage_rows()
    enrichment = await enrichment_rows()
    assert len(receipts) == len(triage) == len(enrichment) == 1
    assert receipts[0].event_id == incident_event.event_id
    assert triage[0].event_id == incident_event.event_id
    assert triage[0].incident_id == incident_event.aggregate_id
    assert triage[0].source == incident_event.payload.source
    assert triage[0].original_severity is incident_event.payload.severity
    assert triage[0].priority is TriagePriority.P2
    assert triage[0].requires_human_review is False
    assert triage[0].created_at.tzinfo is not None

    request = enrichment[0]
    assert request.id != incident_event.event_id
    assert request.event_type == ENRICHMENT_ROUTING_KEY
    assert request.event_version == 1
    assert request.aggregate_id == incident_event.aggregate_id
    assert request.occurred_at.tzinfo is not None
    assert request.published_at is None
    assert request.claim_token is request.claimed_until is None
    request_payload = request.payload.copy()
    incident_occurred_at = request_payload.pop("incident_occurred_at")
    assert isinstance(incident_occurred_at, str)
    assert datetime.fromisoformat(incident_occurred_at) == (
        incident_event.payload.incident_occurred_at
    )
    assert request_payload == {
        "trigger_event_id": str(incident_event.event_id),
        "source": incident_event.payload.source,
        "title": incident_event.payload.title,
        "description": incident_event.payload.description,
        "original_severity": incident_event.payload.severity.value,
        "priority": TriagePriority.P2.value,
        "requires_human_review": False,
    }


async def test_duplicate_does_not_insert_or_mutate_triage(
    incident_event: IncidentCreated,
) -> None:
    assert await process_event(incident_event, sessions) is ProcessingResult.PROCESSED
    original = (await triage_rows())[0]
    snapshot = (
        original.incident_id,
        original.event_id,
        original.source,
        original.original_severity,
        original.priority,
        original.requires_human_review,
        original.created_at,
    )
    original_request = (await enrichment_rows())[0]
    request_snapshot = (
        original_request.id,
        original_request.event_type,
        original_request.event_version,
        original_request.aggregate_id,
        original_request.payload,
        original_request.occurred_at,
        original_request.created_at,
        original_request.published_at,
    )

    assert await process_event(incident_event, sessions) is ProcessingResult.DUPLICATE

    triage = await triage_rows()
    enrichment = await enrichment_rows()
    assert len(await ledger_rows()) == len(triage) == len(enrichment) == 1
    persisted = triage[0]
    assert (
        persisted.incident_id,
        persisted.event_id,
        persisted.source,
        persisted.original_severity,
        persisted.priority,
        persisted.requires_human_review,
        persisted.created_at,
    ) == snapshot
    persisted_request = enrichment[0]
    assert (
        persisted_request.id,
        persisted_request.event_type,
        persisted_request.event_version,
        persisted_request.aggregate_id,
        persisted_request.payload,
        persisted_request.occurred_at,
        persisted_request.created_at,
        persisted_request.published_at,
    ) == request_snapshot


async def test_concurrent_processing_atomically_deduplicates_on_real_postgres(
    incident_event: IncidentCreated,
) -> None:
    barrier = asyncio.Barrier(2)

    async def worker() -> tuple[int, ProcessingResult]:
        async with engine.connect() as connection:
            pid = (await connection.execute(select(func.pg_backend_pid()))).scalar_one()
            await connection.rollback()
            factory = async_sessionmaker(
                bind=connection,
                expire_on_commit=False,
            )
            await barrier.wait()
            return pid, await process_event(incident_event, factory)

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker())
        second = group.create_task(worker())

    pid_a, result_a = first.result()
    pid_b, result_b = second.result()
    assert pid_a != pid_b
    assert {result_a, result_b} == {
        ProcessingResult.PROCESSED,
        ProcessingResult.DUPLICATE,
    }
    rows = await ledger_rows()
    assert len(rows) == 1
    assert rows[0].consumer_name == CONSUMER_NAME
    assert rows[0].event_id == incident_event.event_id
    triage = await triage_rows()
    enrichment = await enrichment_rows()
    assert len(triage) == len(enrichment) == 1
    assert triage[0].event_id == incident_event.event_id
    assert enrichment[0].payload["trigger_event_id"] == str(incident_event.event_id)


async def test_triage_failure_rolls_back_receipt_and_retry_processes(
    incident_event: IncidentCreated,
) -> None:
    secret = "postgresql://user:password-must-stay-private@host/database"

    def fail_triage(session: Session, *args: object) -> None:
        if any(isinstance(item, IncidentTriage) for item in session.new):
            raise OperationalError("triage unavailable", {}, OSError(secret))

    sa_event.listen(Session, "before_flush", fail_triage)
    first = FakeMessage(incident_event.model_dump_json().encode())
    try:
        with pytest.raises(OperationalError):
            await handle_message(incoming(first), sessions)
    finally:
        sa_event.remove(Session, "before_flush", fail_triage)

    assert first.nack_calls == [True]
    assert first.ack_calls == 0
    assert await ledger_rows() == []
    assert await triage_rows() == []
    assert await enrichment_rows() == []

    redelivery = FakeMessage(incident_event.model_dump_json().encode())
    assert (
        await handle_message(incoming(redelivery), sessions)
        is ProcessingResult.PROCESSED
    )
    assert redelivery.ack_calls == 1
    assert len(await ledger_rows()) == 1
    assert len(await triage_rows()) == 1
    assert len(await enrichment_rows()) == 1


async def test_enrichment_outbox_failure_rolls_back_every_effect_then_retries(
    incident_event: IncidentCreated,
) -> None:
    secret = "postgresql://user:password-must-stay-private@host/database"

    def fail_enrichment_outbox(session: Session, *args: object) -> None:
        if any(isinstance(item, OutboxEvent) for item in session.new):
            raise OperationalError("outbox unavailable", {}, OSError(secret))

    sa_event.listen(Session, "before_flush", fail_enrichment_outbox)
    first = FakeMessage(incident_event.model_dump_json().encode())
    try:
        with pytest.raises(OperationalError):
            await handle_message(incoming(first), sessions)
    finally:
        sa_event.remove(Session, "before_flush", fail_enrichment_outbox)

    assert first.nack_calls == [True]
    assert first.ack_calls == 0
    assert await ledger_rows() == []
    assert await triage_rows() == []
    assert await enrichment_rows() == []

    redelivery = FakeMessage(incident_event.model_dump_json().encode())
    assert (
        await handle_message(incoming(redelivery), sessions)
        is ProcessingResult.PROCESSED
    )
    assert redelivery.ack_calls == 1
    assert len(await ledger_rows()) == 1
    assert len(await triage_rows()) == 1
    assert len(await enrichment_rows()) == 1


async def test_ack_observes_real_database_commit(
    incident_event: IncidentCreated,
) -> None:
    committed = False

    def mark_committed(session: Session) -> None:
        nonlocal committed
        committed = True

    def assert_committed() -> None:
        assert committed

    sa_event.listen(Session, "after_commit", mark_committed)
    fake = FakeMessage(
        incident_event.model_dump_json().encode(), on_ack=assert_committed
    )
    try:
        result = await handle_message(incoming(fake), sessions)
    finally:
        sa_event.remove(Session, "after_commit", mark_committed)

    assert result is ProcessingResult.PROCESSED
    assert fake.ack_calls == 1
    assert len(await ledger_rows()) == 1
    assert len(await triage_rows()) == 1
    assert len(await enrichment_rows()) == 1


async def test_ack_loss_then_redelivery_is_duplicate(
    incident_event: IncidentCreated,
) -> None:
    first = FakeMessage(
        incident_event.model_dump_json().encode(),
        ack_error=OSError("simulated broker connection loss"),
    )
    with pytest.raises(OSError, match="simulated broker connection loss"):
        await handle_message(incoming(first), sessions)

    assert first.ack_calls == 1
    assert len(await triage_rows()) == 1
    assert len(await ledger_rows()) == 1
    assert len(await enrichment_rows()) == 1

    redelivery = FakeMessage(incident_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), sessions)

    assert result is ProcessingResult.DUPLICATE
    assert redelivery.ack_calls == 1
    assert len(await ledger_rows()) == 1
    assert len(await triage_rows()) == 1
    assert len(await enrichment_rows()) == 1


async def test_failed_commit_requeues_and_later_redelivery_processes(
    incident_event: IncidentCreated,
) -> None:
    fail_commit = True
    secret = "postgresql://user:password-must-stay-private@host/database"

    def interrupt_commit(session: Session) -> None:
        if fail_commit:
            raise OperationalError("commit unavailable", {}, OSError(secret))

    sa_event.listen(Session, "before_commit", interrupt_commit)
    first = FakeMessage(incident_event.model_dump_json().encode())
    try:
        with pytest.raises(OperationalError):
            await handle_message(incoming(first), sessions)
    finally:
        fail_commit = False
        sa_event.remove(Session, "before_commit", interrupt_commit)

    assert first.nack_calls == [True]
    assert first.ack_calls == 0
    assert await triage_rows() == []
    assert await ledger_rows() == []
    assert await enrichment_rows() == []

    redelivery = FakeMessage(incident_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), sessions)

    assert result is ProcessingResult.PROCESSED
    assert redelivery.ack_calls == 1
    assert len(await ledger_rows()) == 1
    assert len(await triage_rows()) == 1
    assert len(await enrichment_rows()) == 1


async def test_real_rabbitmq_duplicate_deliveries_are_both_acked_once(
    broker: Broker,
    incident_event: IncidentCreated,
) -> None:
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    try:
        channel = await connection.channel()
        exchange = await declare_topology(channel, timeout=5)
        body = incident_event.model_dump_json().encode()
        await publish(exchange, body, message_id=incident_event.event_id)
        await publish(exchange, body, message_id=incident_event.event_id)
        queue = await channel.get_queue(QUEUE_NAME)

        first = await queue.get(no_ack=False, fail=True, timeout=5)
        second = await queue.get(no_ack=False, fail=True, timeout=5)
        assert await handle_message(first, sessions) is ProcessingResult.PROCESSED
        original_triage = (await triage_rows())[0]
        original_snapshot = (
            original_triage.incident_id,
            original_triage.event_id,
            original_triage.source,
            original_triage.original_severity,
            original_triage.priority,
            original_triage.requires_human_review,
            original_triage.created_at,
        )
        assert await handle_message(second, sessions) is ProcessingResult.DUPLICATE

        assert first.processed and second.processed
        assert await queue_has_no_ready_delivery(queue)
    finally:
        await connection.close()

    rows = await ledger_rows()
    triage = await triage_rows()
    enrichment = await enrichment_rows()
    assert len(triage) == len(enrichment) == 1
    assert triage[0].event_id == incident_event.event_id
    assert triage[0].incident_id == incident_event.aggregate_id
    assert (
        triage[0].incident_id,
        triage[0].event_id,
        triage[0].source,
        triage[0].original_severity,
        triage[0].priority,
        triage[0].requires_human_review,
        triage[0].created_at,
    ) == original_snapshot
    assert len(rows) == 1
    receipt = rows[0]
    assert receipt.consumer_name == CONSUMER_NAME
    assert receipt.event_id == incident_event.event_id
    assert receipt.event_type == incident_event.event_type
    assert receipt.event_version == incident_event.event_version
    assert receipt.aggregate_id == incident_event.aggregate_id
    assert receipt.processed_at.tzinfo is not None


async def test_real_malformed_message_is_rejected_without_requeue(
    broker: Broker,
) -> None:
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    try:
        channel = await connection.channel()
        exchange = await declare_topology(channel, timeout=5)
        message_id = uuid4()
        await publish(exchange, b"{", message_id=message_id)
        queue = await channel.get_queue(QUEUE_NAME)
        delivery = await queue.get(no_ack=False, fail=True, timeout=5)

        with pytest.raises(InvalidEventError) as caught:
            await handle_message(delivery, sessions)
        assert caught.value.reason == "invalid_json"

        assert delivery.processed
        assert await queue_has_no_ready_delivery(queue)
    finally:
        await connection.close()

    assert await ledger_rows() == []
    assert await triage_rows() == []
    assert await enrichment_rows() == []
