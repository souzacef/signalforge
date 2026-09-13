import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import aio_pika
import pytest
from httpx import AsyncClient
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker
from yarl import URL

from signalforge.db.session import engine
from signalforge.outbox import orchestrator
from signalforge.outbox.dispatching import (
    ClaimSnapshot,
    DispatchErrorCode,
    claim_pending,
    release_claim,
    settle_retry,
    settle_success,
)
from signalforge.outbox.models import OutboxEvent
from signalforge.outbox.orchestrator import (
    DispatchOutcome,
    OwnershipStatus,
    check_publish_ownership,
    dispatch_batch,
)
from signalforge.outbox.rabbitmq import (
    ENRICHMENT_QUEUE_NAME,
    ENRICHMENT_ROUTING_KEY,
    EXCHANGE_NAME,
    QUEUE_NAME,
    ROUTING_KEY,
    PublishFailedError,
    RabbitMQPublisher,
    declare_topology,
)

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)
type SeedEvents = Callable[[int], Awaitable[list[UUID]]]
LEASE = timedelta(minutes=5)
SETTLEMENT_BUDGET = timedelta(seconds=1)
IMMUTABLE_FIELDS = (
    "id",
    "event_type",
    "event_version",
    "aggregate_id",
    "payload",
    "occurred_at",
    "created_at",
)


@dataclass(frozen=True, slots=True)
class Broker:
    url: URL
    api: AsyncClient
    vhost: str


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


@pytest.fixture
async def publisher(broker: Broker) -> AsyncIterator[RabbitMQPublisher]:
    instance = await RabbitMQPublisher.connect(
        str(broker.url), timeout=10, cleanup_timeout=1
    )
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
async def seed_events() -> AsyncIterator[SeedEvents]:
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
    seeded_ids: list[UUID] = []

    async def seed(count: int) -> list[UUID]:
        ids = sorted(uuid4() for _ in range(count))
        seeded_ids.extend(ids)
        async with sessions.begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            for event_id in ids:
                session.add(
                    OutboxEvent(
                        id=event_id,
                        event_type="incident.created",
                        event_version=1,
                        aggregate_id=uuid4(),
                        payload={
                            "source": "manual",
                            "title": f"event {event_id}",
                            "description": None,
                            "severity": "high",
                            "incident_occurred_at": now.isoformat(),
                        },
                        occurred_at=now,
                    )
                )
        return ids

    try:
        yield seed
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.id.in_(seeded_ids))
            )


class RecordingPublisher:
    def __init__(self, error: PublishFailedError | None = None) -> None:
        self.error = error
        self.calls: list[UUID] = []
        self.active = 0
        self.maximum_active = 0
        self.is_retired = bool(error and error.ambiguous)

    async def publish(
        self,
        claim: ClaimSnapshot,
        *,
        timeout: float,  # noqa: ASYNC109 - matches the production publisher API
    ) -> None:
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            # NOWAIT proves the claim transaction ended before this network seam.
            async with sessions.begin() as session:
                row = await session.scalar(
                    select(OutboxEvent)
                    .where(OutboxEvent.id == claim.id)
                    .with_for_update(nowait=True)
                )
                assert row is not None
            self.calls.append(claim.id)
            await asyncio.sleep(0)
            if self.error is not None:
                raise self.error
        finally:
            self.active -= 1


async def read_event(event_id: UUID) -> dict[str, Any]:
    async with sessions() as session:
        return dict(
            (
                await session.execute(
                    select(OutboxEvent.__table__).where(OutboxEvent.id == event_id)
                )
            )
            .mappings()
            .one()
        )


async def make_due(event_id: UUID) -> None:
    async with sessions.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(next_attempt_at=func.clock_timestamp())
        )


async def seed_enrichment_request() -> UUID:
    event_id = uuid4()
    async with sessions.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        session.add(
            OutboxEvent(
                id=event_id,
                event_type=ENRICHMENT_ROUTING_KEY,
                event_version=1,
                aggregate_id=uuid4(),
                payload={
                    "trigger_event_id": str(uuid4()),
                    "source": "alertmanager",
                    "title": "Checkout latency increased",
                    "description": "p95 exceeded the threshold",
                    "original_severity": "critical",
                    "priority": "P1",
                    "requires_human_review": True,
                    "incident_occurred_at": "2026-09-11T15:30:00-03:00",
                },
                occurred_at=now,
            )
        )
    return event_id


async def probe(broker: Broker, queue_name: str = QUEUE_NAME):
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue(queue_name)
        return await queue.get(no_ack=True, fail=False, timeout=5)


def immutable(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row[field] for field in IMMUTABLE_FIELDS}


async def run_batch(
    publisher: Any, *, batch_limit: int = 10, publish_timeout: float = 2
):
    return await dispatch_batch(
        sessions,
        publisher,
        batch_limit=batch_limit,
        lease_duration=LEASE,
        publish_timeout=publish_timeout,
        settlement_budget=SETTLEMENT_BUDGET,
        jitter_source=lambda: 0.5,
    )


async def test_successful_real_one_shot_claims_publishes_and_settles(
    seed_events: SeedEvents, broker: Broker, publisher: RabbitMQPublisher
) -> None:
    (event_id,) = await seed_events(1)
    before = await read_event(event_id)
    result = await run_batch(publisher)
    assert result.claimed == result.published == 1
    assert result.retry_scheduled == result.ownership_lost == 0
    assert result.released == result.failed == 0
    assert not result.publisher_retired
    assert result.events[0].outcome is DispatchOutcome.PUBLISHED
    assert result.events[0].publication_confirmed
    message = await probe(broker)
    assert message.message_id == str(event_id)
    assert json.loads(message.body)["event_id"] == str(event_id)
    row = await read_event(event_id)
    assert row["published_at"] is not None
    assert row["claim_token"] is row["claimed_until"] is None
    assert row["attempt_count"] == 1
    assert immutable(row) == immutable(before)


async def test_real_enrichment_request_dispatches_and_settles(
    broker: Broker,
    publisher: RabbitMQPublisher,
) -> None:
    event_id = await seed_enrichment_request()
    try:
        before = await read_event(event_id)
        result = await run_batch(publisher)
        message = await probe(broker, ENRICHMENT_QUEUE_NAME)

        assert result.claimed == result.published == 1
        assert result.events[0].outcome is DispatchOutcome.PUBLISHED
        assert result.events[0].publication_confirmed
        assert message is not None
        body = json.loads(message.body)
        assert UUID(body["event_id"]) == event_id
        assert body["event_type"] == ENRICHMENT_ROUTING_KEY
        assert body["event_version"] == 1
        assert UUID(body["aggregate_id"]) == before["aggregate_id"]
        assert body["payload"] == before["payload"]
        assert datetime.fromisoformat(body["occurred_at"]) == before["occurred_at"]
        assert message.message_id == str(event_id)
        assert message.type == ENRICHMENT_ROUTING_KEY
        assert message.content_type == "application/json"
        assert message.content_encoding == "utf-8"
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT

        row = await read_event(event_id)
        assert row["published_at"] is not None
        assert row["claim_token"] is row["claimed_until"] is None
        assert row["attempt_count"] == 1
        assert immutable(row) == immutable(before)
        assert await probe(broker, QUEUE_NAME) is None
    finally:
        async with sessions.begin() as session:
            await session.execute(delete(OutboxEvent).where(OutboxEvent.id == event_id))


async def test_direct_exchange_isolates_incident_and_enrichment_queues(
    seed_events: SeedEvents,
    broker: Broker,
    publisher: RabbitMQPublisher,
) -> None:
    (incident_event_id,) = await seed_events(1)
    enrichment_event_id = await seed_enrichment_request()
    try:
        result = await run_batch(publisher)
        incident_message = await probe(broker, QUEUE_NAME)
        enrichment_message = await probe(broker, ENRICHMENT_QUEUE_NAME)

        assert result.claimed == result.published == 2
        assert incident_message is not None
        assert enrichment_message is not None
        assert incident_message.message_id == str(incident_event_id)
        assert incident_message.type == ROUTING_KEY
        assert json.loads(incident_message.body)["event_type"] == ROUTING_KEY
        assert enrichment_message.message_id == str(enrichment_event_id)
        assert enrichment_message.type == ENRICHMENT_ROUTING_KEY
        assert (
            json.loads(enrichment_message.body)["event_type"] == ENRICHMENT_ROUTING_KEY
        )
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.id == enrichment_event_id)
            )


async def test_preflight_rejects_reclaimed_owner_and_insufficient_budget(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    async with sessions.begin() as session:
        stale = (await claim_pending(session, batch_limit=1, lease_duration=LEASE))[0]
    async with sessions.begin() as session:
        first_check = await check_publish_ownership(
            session, stale, minimum_remaining=LEASE + timedelta(seconds=1)
        )
        assert first_check is OwnershipStatus.LOST
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(claimed_until=func.clock_timestamp() - timedelta(seconds=1))
        )
    publisher = RecordingPublisher()
    if first_check is OwnershipStatus.OWNED:
        await publisher.publish(stale, timeout=1)
    assert publisher.calls == []
    async with sessions.begin() as session:
        replacement = (
            await claim_pending(session, batch_limit=1, lease_duration=LEASE)
        )[0]
    assert replacement.claim_token != stale.claim_token
    async with sessions.begin() as session:
        assert (
            await check_publish_ownership(
                session, stale, minimum_remaining=SETTLEMENT_BUDGET
            )
            is OwnershipStatus.LOST
        )
        assert not await settle_success(
            session, event_id=event_id, claim_token=stale.claim_token
        )
        assert not await settle_retry(
            session,
            event_id=event_id,
            claim_token=stale.claim_token,
            delay=timedelta(seconds=2),
            error_code=DispatchErrorCode.DISPATCH_FAILED,
        )
        assert not await release_claim(
            session, event_id=event_id, claim_token=stale.claim_token
        )
        assert (
            await check_publish_ownership(
                session, replacement, minimum_remaining=SETTLEMENT_BUDGET
            )
            is OwnershipStatus.OWNED
        )
    row = await read_event(event_id)
    assert row["claim_token"] == replacement.claim_token
    assert row["attempt_count"] == 2


async def test_multiple_events_publish_sequentially_without_database_locks(
    seed_events: SeedEvents,
) -> None:
    ids = await seed_events(3)
    publisher = RecordingPublisher()
    result = await run_batch(publisher)
    assert result.claimed == result.published == 3
    assert result.retry_scheduled == result.ownership_lost == 0
    assert result.released == result.failed == 0
    assert publisher.calls == ids
    assert publisher.maximum_active == 1
    for event_id in ids:
        row = await read_event(event_id)
        assert row["published_at"] is not None
        assert row["claim_token"] is row["claimed_until"] is None


async def test_real_unroutable_retry_then_same_event_succeeds(
    seed_events: SeedEvents, broker: Broker, publisher: RabbitMQPublisher
) -> None:
    (event_id,) = await seed_events(1)
    before = await read_event(event_id)
    queue = await publisher._channel.get_queue(QUEUE_NAME)
    await queue.unbind(EXCHANGE_NAME, routing_key=ROUTING_KEY, timeout=5)
    async with sessions() as session:
        lower = await session.scalar(select(func.clock_timestamp()))
    failed = await run_batch(publisher)
    async with sessions() as session:
        upper = await session.scalar(select(func.clock_timestamp()))
    assert failed.claimed == failed.retry_scheduled == 1
    assert failed.published == failed.failed == 0
    assert failed.events[0].error_code.value == "unroutable"
    row = await read_event(event_id)
    assert row["published_at"] is None
    assert row["claim_token"] is row["claimed_until"] is None
    assert row["attempt_count"] == 1
    assert row["last_error"] == "unroutable"
    assert lower + timedelta(seconds=1.5) <= row["next_attempt_at"]
    assert row["next_attempt_at"] <= upper + timedelta(seconds=1.5)
    assert immutable(row) == immutable(before)
    assert await probe(broker) is None
    await declare_topology(publisher._channel, timeout=5)
    await make_due(event_id)
    succeeded = await run_batch(publisher)
    assert succeeded.claimed == succeeded.published == 1
    assert (await probe(broker)).message_id == str(event_id)
    row = await read_event(event_id)
    assert row["published_at"] is not None
    assert row["attempt_count"] == 2
    assert row["last_error"] is None
    assert immutable(row) == immutable(before)


async def test_real_nack_schedules_safe_retry(
    seed_events: SeedEvents, broker: Broker, publisher: RabbitMQPublisher
) -> None:
    ids = await seed_events(2)
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    async with connection:
        channel = await connection.channel()
        await channel.queue_delete(QUEUE_NAME, if_unused=False, if_empty=True)
        queue = await channel.declare_queue(
            QUEUE_NAME,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={
                "x-queue-type": "classic",
                "x-max-length": 1,
                "x-overflow": "reject-publish",
            },
        )
        await queue.bind(EXCHANGE_NAME, routing_key=ROUTING_KEY)
        result = await run_batch(publisher)
    assert result.claimed == 2
    assert result.published == result.retry_scheduled == 1
    assert result.failed == result.released == 0
    assert not result.publisher_retired
    rows = [await read_event(event_id) for event_id in ids]
    assert sum(row["published_at"] is not None for row in rows) == 1
    retry = next(row for row in rows if row["published_at"] is None)
    assert retry["last_error"] == "dispatch_failed"
    assert retry["claim_token"] is retry["claimed_until"] is None
    assert "amqp" not in retry["last_error"]


async def test_real_broker_loss_retries_current_and_releases_remaining(
    seed_events: SeedEvents, broker: Broker, publisher: RabbitMQPublisher
) -> None:
    ids = await seed_events(3)
    before = {event_id: await read_event(event_id) for event_id in ids}
    (await broker.api.delete(f"/api/vhosts/{broker.vhost}")).raise_for_status()
    result = await run_batch(publisher, publish_timeout=0.2)
    assert result.claimed == 3
    assert result.retry_scheduled == 1
    assert result.released == 2
    assert result.published == result.failed == result.ownership_lost == 0
    assert result.publisher_retired
    retry_id = result.events[0].event_id
    assert result.events[0].error_code.value in {
        "publish_timeout",
        "dispatch_failed",
    }
    assert all(item.outcome is DispatchOutcome.RELEASED for item in result.events[1:])
    for event_id in ids:
        row = await read_event(event_id)
        assert row["published_at"] is None
        assert row["attempt_count"] == 1
        assert immutable(row) == immutable(before[event_id])
        if event_id == retry_id:
            assert row["last_error"] in {"publish_timeout", "dispatch_failed"}
        else:
            assert row["claim_token"] is row["claimed_until"] is None


async def test_invalid_stored_event_gets_visible_delayed_retry(
    seed_events: SeedEvents, publisher: RabbitMQPublisher
) -> None:
    (event_id,) = await seed_events(1)
    async with sessions.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(event_type="unsupported.event")
        )
    async with sessions() as session:
        lower = await session.scalar(select(func.clock_timestamp()))
    result = await run_batch(publisher)
    async with sessions() as session:
        upper = await session.scalar(select(func.clock_timestamp()))
    row = await read_event(event_id)
    assert result.claimed == result.retry_scheduled == 1
    assert result.events[0].error_code.value == "invalid_event"
    assert row["last_error"] == "invalid_event"
    assert row["published_at"] is None
    assert row["claim_token"] is row["claimed_until"] is None
    assert lower + timedelta(hours=24) <= row["next_attempt_at"]
    assert row["next_attempt_at"] <= upper + timedelta(hours=24)


@pytest.mark.parametrize("settlement", ["success", "retry"])
async def test_database_settlement_failure_is_reported_without_looping(
    seed_events: SeedEvents,
    monkeypatch: pytest.MonkeyPatch,
    settlement: str,
) -> None:
    (event_id,) = await seed_events(1)
    error = None
    target = "settle_success"
    if settlement == "retry":
        error = PublishFailedError("sanitized failure", ambiguous=False)
        target = "settle_retry"
    publisher = RecordingPublisher(error)

    async def fail(*args: Any, **kwargs: Any) -> bool:
        raise SQLAlchemyError("simulated settlement failure")

    monkeypatch.setattr(orchestrator, target, fail)
    result = await run_batch(publisher)
    assert publisher.calls == [event_id]
    assert result.claimed == result.failed == 1
    assert result.published == result.retry_scheduled == 0
    assert result.events[0].outcome is DispatchOutcome.DATABASE_FAILED
    assert result.events[0].publication_confirmed is (settlement == "success")
    row = await read_event(event_id)
    assert row["published_at"] is None
    assert row["claim_token"] is not None
    assert row["claimed_until"] is not None
    assert row["attempt_count"] == 1


async def test_success_settlement_ownership_loss_is_not_reported_published(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)

    class SupersedingPublisher(RecordingPublisher):
        async def publish(
            self,
            claim: ClaimSnapshot,
            *,
            timeout: float,  # noqa: ASYNC109 - matches production publisher API
        ) -> None:
            await super().publish(claim, timeout=timeout)
            async with sessions.begin() as session:
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.id == claim.id)
                    .values(
                        claim_token=uuid4(),
                        claimed_until=func.clock_timestamp() + LEASE,
                    )
                )

    publisher = SupersedingPublisher()
    result = await run_batch(publisher)
    assert result.claimed == result.ownership_lost == 1
    assert result.published == result.failed == 0
    assert result.events[0].publication_confirmed
    assert (await read_event(event_id))["published_at"] is None


async def test_invalid_configuration_claims_nothing(seed_events: SeedEvents) -> None:
    (event_id,) = await seed_events(1)
    before = await read_event(event_id)
    with pytest.raises(ValueError, match="lease_duration"):
        await dispatch_batch(
            sessions,
            RecordingPublisher(),
            batch_limit=1,
            lease_duration=timedelta(seconds=1),
            publish_timeout=1,
            settlement_budget=timedelta(seconds=1),
        )
    assert await read_event(event_id) == before
