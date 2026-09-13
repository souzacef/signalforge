import asyncio
import json
import os
import signal
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID, uuid4

import aio_pika
import pytest
from httpx import AsyncClient
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker
from yarl import URL

from signalforge.db.session import engine
from signalforge.outbox import dispatcher
from signalforge.outbox.dispatcher import DispatcherSettings, run_dispatcher
from signalforge.outbox.dispatching import ClaimSnapshot
from signalforge.outbox.models import OutboxEvent
from signalforge.outbox.rabbitmq import (
    QUEUE_NAME,
    RabbitMQPublisher,
)

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)
type SeedEvents = Callable[[int], Awaitable[list[UUID]]]
type AsyncPredicate = Callable[[], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class Broker:
    url: URL
    api: AsyncClient
    vhost: str


async def provision_vhost(broker: Broker) -> None:
    (await broker.api.put(f"/api/vhosts/{broker.vhost}", json={})).raise_for_status()
    (
        await broker.api.put(
            f"/api/permissions/{broker.vhost}/{quote(broker.url.user or '', safe='')}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
    ).raise_for_status()


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
        runtime_broker = Broker(url.with_path(f"/{vhost}"), api, vhost)
        response = await api.get("/api/overview")
        response.raise_for_status()
        assert response.json()["rabbitmq_version"] == "4.3.5"
        await provision_vhost(runtime_broker)
        try:
            yield runtime_broker
        finally:
            response = await api.delete(f"/api/vhosts/{vhost}")
            assert response.status_code in {204, 404}


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
                            "title": f"runtime event {event_id}",
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


def runtime_settings(rabbitmq_url: str, **changes: object) -> DispatcherSettings:
    database_url = os.environ["SIGNALFORGE_DATABASE_URL"]
    return DispatcherSettings.model_validate(
        {
            "database_url": database_url,
            "rabbitmq_url": rabbitmq_url,
            "dispatcher_batch_size": 10,
            "dispatcher_claim_lease_seconds": 2,
            "dispatcher_publish_timeout_seconds": 0.2,
            "dispatcher_settlement_budget_seconds": 0.1,
            "dispatcher_idle_poll_interval_seconds": 0.02,
            "dispatcher_reconnect_base_seconds": 0.05,
            "dispatcher_reconnect_max_seconds": 0.1,
            "dispatcher_shutdown_drain_timeout_seconds": 0.2,
            **changes,
        }
    )


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


async def eventually(predicate: AsyncPredicate, *, timeout: float = 10) -> None:  # noqa: ASYNC109
    async with asyncio.timeout(timeout):
        while not await predicate():  # noqa: ASYNC110 - poll external services
            await asyncio.sleep(0.02)


async def all_published(event_ids: list[UUID]) -> bool:
    async with sessions() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.id.in_(event_ids),
                OutboxEvent.published_at.is_not(None),
            )
        )
    return count == len(event_ids)


async def probe(broker: Broker):
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue(QUEUE_NAME)
        return await queue.get(no_ack=True, fail=False, timeout=5)


async def test_real_runtime_publishes_settles_keeps_running_and_stops_cleanly(
    broker: Broker,
    seed_events: SeedEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_ids = await seed_events(2)
    connected: list[RabbitMQPublisher] = []
    real_connect = dispatcher._connect_publisher

    async def recording_connect(
        value: DispatcherSettings,
    ) -> RabbitMQPublisher:
        publisher = await real_connect(value)
        connected.append(publisher)
        return publisher

    monkeypatch.setattr(dispatcher, "_connect_publisher", recording_connect)
    stop = asyncio.Event()
    runtime = asyncio.create_task(
        run_dispatcher(
            runtime_settings(str(broker.url)),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    await eventually(lambda: all_published(event_ids))
    assert not runtime.done()
    messages = [await probe(broker), await probe(broker)]
    assert {UUID(json.loads(message.body)["event_id"]) for message in messages} == set(
        event_ids
    )

    stop.set()
    await asyncio.wait_for(runtime, timeout=2)
    assert len(connected) == 1
    assert connected[0].is_retired
    for event_id in event_ids:
        row = await read_event(event_id)
        assert row["published_at"] is not None
        assert row["claim_token"] is row["claimed_until"] is None


async def test_runtime_starts_with_broker_down_without_claiming_then_recovers(
    broker: Broker,
    seed_events: SeedEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (event_id,) = await seed_events(1)
    (await broker.api.delete(f"/api/vhosts/{broker.vhost}")).raise_for_status()
    attempted = asyncio.Event()
    attempts = 0
    real_connect = dispatcher._connect_publisher

    async def recording_connect(
        value: DispatcherSettings,
    ) -> RabbitMQPublisher:
        nonlocal attempts
        try:
            return await real_connect(value)
        finally:
            attempts += 1
            attempted.set()

    monkeypatch.setattr(dispatcher, "_connect_publisher", recording_connect)
    stop = asyncio.Event()
    runtime = asyncio.create_task(
        run_dispatcher(
            runtime_settings(str(broker.url)),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    await asyncio.wait_for(attempted.wait(), timeout=3)
    row = await read_event(event_id)
    assert not runtime.done()
    assert row["attempt_count"] == 0
    assert row["claim_token"] is row["claimed_until"] is None

    await provision_vhost(broker)
    await eventually(lambda: all_published([event_id]))
    assert attempts >= 2
    assert not runtime.done()
    assert (await probe(broker)).message_id == str(event_id)
    stop.set()
    await asyncio.wait_for(runtime, timeout=2)


async def test_runtime_replaces_publisher_after_broker_loss_and_recovers(
    broker: Broker,
    seed_events: SeedEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (first_id,) = await seed_events(1)
    connected: list[RabbitMQPublisher] = []
    real_connect = dispatcher._connect_publisher

    async def recording_connect(
        value: DispatcherSettings,
    ) -> RabbitMQPublisher:
        publisher = await real_connect(value)
        connected.append(publisher)
        return publisher

    monkeypatch.setattr(dispatcher, "_connect_publisher", recording_connect)
    stop = asyncio.Event()
    runtime = asyncio.create_task(
        run_dispatcher(
            runtime_settings(str(broker.url)),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    await eventually(lambda: all_published([first_id]))
    assert (await probe(broker)).message_id == str(first_id)

    (await broker.api.delete(f"/api/vhosts/{broker.vhost}")).raise_for_status()
    (second_id,) = await seed_events(1)

    async def failure_settled() -> bool:
        if not connected or not connected[0].is_retired:
            return False
        row = await read_event(second_id)
        return (
            row["claim_token"] is None
            and row["claimed_until"] is None
            and row["last_error"] in {"publish_timeout", "dispatch_failed"}
        )

    await eventually(failure_settled)
    failed_row = await read_event(second_id)
    assert failed_row["published_at"] is None
    assert failed_row["claim_token"] is failed_row["claimed_until"] is None
    assert failed_row["last_error"] in {"publish_timeout", "dispatch_failed"}
    assert not runtime.done()

    await provision_vhost(broker)

    async def replacement_connected() -> bool:
        return len(connected) >= 2

    await eventually(replacement_connected)
    async with sessions.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == second_id)
            .values(next_attempt_at=func.clock_timestamp())
        )
    await eventually(lambda: all_published([second_id]))
    assert (await probe(broker)).message_id == str(second_id)
    assert connected[0] is not connected[1]
    assert not runtime.done()

    stop.set()
    await asyncio.wait_for(runtime, timeout=2)
    assert all(publisher.is_retired for publisher in connected)


class BlockingPublisher:
    def __init__(self, *, complete: bool) -> None:
        self.complete = complete
        self.is_retired = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.close_calls = 0
        self.publish_calls = 0

    async def publish(
        self,
        claim: ClaimSnapshot,
        *,
        timeout: float,  # noqa: ASYNC109 - matches the publisher contract
    ) -> None:
        self.publish_calls += 1
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.is_retired = True
            self.cancelled.set()
            raise
        if not self.complete:
            raise AssertionError("blocked publication unexpectedly released")

    async def close(self) -> None:
        self.close_calls += 1
        self.is_retired = True


async def test_shutdown_drains_active_real_database_batch_through_settlement(
    seed_events: SeedEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (event_id,) = await seed_events(1)
    publisher = BlockingPublisher(complete=True)

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    monkeypatch.setattr(dispatcher, "_connect_publisher", connect)
    stop = asyncio.Event()
    runtime = asyncio.create_task(
        run_dispatcher(
            runtime_settings("amqp://user:password@localhost/test"),
            stop_event=stop,
        )
    )
    await asyncio.wait_for(publisher.started.wait(), timeout=2)
    stop.set()
    await asyncio.sleep(0)
    assert not runtime.done()
    publisher.release.set()
    await asyncio.wait_for(runtime, timeout=2)

    row = await read_event(event_id)
    assert publisher.publish_calls == publisher.close_calls == 1
    assert not publisher.cancelled.is_set()
    assert row["published_at"] is not None
    assert row["claim_token"] is row["claimed_until"] is None


async def test_forced_shutdown_leaves_claim_recoverable_by_lease(
    seed_events: SeedEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (event_id,) = await seed_events(1)
    publisher = BlockingPublisher(complete=False)

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    monkeypatch.setattr(dispatcher, "_connect_publisher", connect)
    stop = asyncio.Event()
    runtime = asyncio.create_task(
        run_dispatcher(
            runtime_settings(
                "amqp://user:password@localhost/test",
                dispatcher_shutdown_drain_timeout_seconds=0.02,
            ),
            stop_event=stop,
        )
    )
    await asyncio.wait_for(publisher.started.wait(), timeout=2)
    stop.set()
    await asyncio.wait_for(runtime, timeout=2)

    row = await read_event(event_id)
    assert publisher.publish_calls == publisher.close_calls == 1
    assert publisher.cancelled.is_set()
    assert row["published_at"] is None
    assert row["attempt_count"] == 1
    assert row["claim_token"] is not None
    assert row["claimed_until"] is not None


async def test_real_module_entrypoint_publishes_and_exits_cleanly_on_sigterm(
    broker: Broker,
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    environment = os.environ.copy()
    environment.pop("SIGNALFORGE_JWT_SECRET", None)
    environment.update(
        {
            "SIGNALFORGE_RABBITMQ_URL": str(broker.url),
            "SIGNALFORGE_DISPATCHER_BATCH_SIZE": "10",
            "SIGNALFORGE_DISPATCHER_CLAIM_LEASE_SECONDS": "2",
            "SIGNALFORGE_DISPATCHER_PUBLISH_TIMEOUT_SECONDS": "0.2",
            "SIGNALFORGE_DISPATCHER_SETTLEMENT_BUDGET_SECONDS": "0.1",
            "SIGNALFORGE_DISPATCHER_IDLE_POLL_INTERVAL_SECONDS": "0.02",
            "SIGNALFORGE_DISPATCHER_RECONNECT_BASE_SECONDS": "0.05",
            "SIGNALFORGE_DISPATCHER_RECONNECT_MAX_SECONDS": "0.1",
            "SIGNALFORGE_DISPATCHER_SHUTDOWN_DRAIN_TIMEOUT_SECONDS": "0.2",
        }
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "signalforge.outbox.dispatcher",
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        await eventually(lambda: all_published([event_id]))
        assert process.returncode is None
        assert (await probe(broker)).message_id == str(event_id)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=3)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

    assert process.returncode == 0
    output = (stdout + stderr).decode()
    assert str(broker.url) not in output
    assert broker.url.password not in output
    row = await read_event(event_id)
    assert row["published_at"] is not None
    assert row["claim_token"] is row["claimed_until"] is None
