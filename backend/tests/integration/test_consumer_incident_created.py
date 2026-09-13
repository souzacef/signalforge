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
from signalforge.incidents.models import IncidentSeverity
from signalforge.outbox.rabbitmq import (
    QUEUE_NAME,
    ROUTING_KEY,
    declare_topology,
)

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
def incident_event() -> IncidentCreated:
    return IncidentCreated(
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


@pytest.fixture(autouse=True)
async def clean_processed_events() -> AsyncIterator[None]:
    async with sessions.begin() as session:
        await session.execute(delete(ProcessedEvent))
    yield
    async with sessions.begin() as session:
        await session.execute(delete(ProcessedEvent))


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
    assert len(await ledger_rows()) == 1

    redelivery = FakeMessage(incident_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), sessions)

    assert result is ProcessingResult.DUPLICATE
    assert redelivery.ack_calls == 1
    assert len(await ledger_rows()) == 1


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
    assert await ledger_rows() == []

    redelivery = FakeMessage(incident_event.model_dump_json().encode())
    result = await handle_message(incoming(redelivery), sessions)

    assert result is ProcessingResult.PROCESSED
    assert redelivery.ack_calls == 1
    assert len(await ledger_rows()) == 1


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
        assert await handle_message(second, sessions) is ProcessingResult.DUPLICATE

        assert first.processed and second.processed
        assert await queue_has_no_ready_delivery(queue)
    finally:
        await connection.close()

    rows = await ledger_rows()
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
