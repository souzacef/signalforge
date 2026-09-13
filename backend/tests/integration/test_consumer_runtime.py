from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, Message
from httpx import AsyncClient
from pamqp.commands import Basic
from sqlalchemy import delete, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker
from yarl import URL

from signalforge.consumers import runtime
from signalforge.consumers.models import ProcessedEvent
from signalforge.consumers.runtime import ConsumerBroker, ConsumerSettings, run_consumer
from signalforge.db.session import engine
from signalforge.incidents.events import IncidentCreated, IncidentCreatedPayload
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.outbox.rabbitmq import QUEUE_NAME, ROUTING_KEY, declare_topology
from signalforge.triage.models import IncidentTriage

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)
type AsyncPredicate = Callable[[], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class Broker:
    url: URL
    api: AsyncClient
    vhost: str


@dataclass(slots=True)
class TcpProxy:
    upstream_host: str
    upstream_port: int
    listen_port: int
    server: asyncio.Server | None = None
    writers: set[asyncio.StreamWriter] = field(default_factory=set)

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._forward,
            "127.0.0.1",
            self.listen_port,
        )

    async def _forward(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.upstream_host,
                self.upstream_port,
            )
            self.writers.update((client_writer, upstream_writer))

            async def copy(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
            ) -> None:
                while data := await reader.read(65536):
                    writer.write(data)
                    await writer.drain()

            client_to_server = asyncio.create_task(copy(client_reader, upstream_writer))
            server_to_client = asyncio.create_task(copy(upstream_reader, client_writer))
            _, pending = await asyncio.wait(
                {client_to_server, server_to_client},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        except (ConnectionError, OSError):
            pass
        finally:
            for writer in (client_writer, upstream_writer):
                if writer is not None:
                    self.writers.discard(writer)
                    writer.close()
                    await writer.wait_closed()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in tuple(self.writers):
            writer.close()
        await asyncio.gather(
            *(writer.wait_closed() for writer in tuple(self.writers)),
            return_exceptions=True,
        )


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
        base_url=management_url,
        auth=(url.user, url.password),
        timeout=10,
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


@pytest.fixture(autouse=True)
async def clean_processed_events() -> AsyncIterator[None]:
    async with sessions.begin() as session:
        await session.execute(delete(ProcessedEvent))
        await session.execute(delete(IncidentTriage))
    yield
    async with sessions.begin() as session:
        await session.execute(delete(ProcessedEvent))
        await session.execute(delete(IncidentTriage))


@pytest.fixture
async def incident_event() -> AsyncIterator[IncidentCreated]:
    event = IncidentCreated(
        event_id=uuid4(),
        occurred_at=datetime(2026, 9, 13, 12, tzinfo=UTC),
        aggregate_id=uuid4(),
        payload=IncidentCreatedPayload(
            source="integration",
            title="Consumer runtime probe",
            description="Process durably",
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


def runtime_settings(database_url: str, rabbitmq_url: str) -> ConsumerSettings:
    return ConsumerSettings.model_validate(
        {
            "database_url": database_url,
            "rabbitmq_url": rabbitmq_url,
            "consumer_prefetch_count": 1,
            "consumer_reconnect_base_seconds": 0.05,
            "consumer_reconnect_max_seconds": 0.1,
            "consumer_shutdown_drain_timeout_seconds": 0.2,
        }
    )


async def eventually(predicate: AsyncPredicate, *, timeout: float = 10) -> None:  # noqa: ASYNC109
    async with asyncio.timeout(timeout):
        while not await predicate():  # noqa: ASYNC110 - poll external services
            await asyncio.sleep(0.02)


async def publish(broker: Broker, body: bytes, *, message_id: UUID) -> None:
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    try:
        channel = await connection.channel()
        exchange = await declare_topology(channel, timeout=5)
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
    finally:
        await connection.close()


async def receipt_count(event_id: UUID) -> int:
    async with sessions() as session:
        return (
            await session.scalar(
                select(func.count())
                .select_from(ProcessedEvent)
                .where(ProcessedEvent.event_id == event_id)
            )
        ) or 0


async def queue_state(broker: Broker) -> dict[str, Any] | None:
    response = await broker.api.get(
        f"/api/queues/{quote(broker.vhost, safe='')}/{quote(QUEUE_NAME, safe='')}"
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


async def event_processed(event_id: UUID) -> bool:
    return await receipt_count(event_id) == 1


async def test_runtime_processes_duplicate_and_poison_then_keeps_consuming(
    broker: Broker,
    incident_event: IncidentCreated,
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_consumer(
            runtime_settings(
                os.environ["SIGNALFORGE_DATABASE_URL"],
                str(broker.url),
            ),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    try:
        await publish(broker, b'{"password":"must-not-be-logged"', message_id=uuid4())
        body = incident_event.model_dump_json().encode()
        await publish(broker, body, message_id=incident_event.event_id)
        await publish(broker, body, message_id=incident_event.event_id)
        await eventually(lambda: event_processed(incident_event.event_id))

        async def queue_drained() -> bool:
            state = await queue_state(broker)
            return state is not None and state.get("messages") == 0

        await eventually(queue_drained)
        assert not task.done()
        assert await receipt_count(incident_event.event_id) == 1
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)


async def test_runtime_starts_with_broker_down_then_recovers_same_process(
    broker: Broker,
    incident_event: IncidentCreated,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (await broker.api.delete(f"/api/vhosts/{broker.vhost}")).raise_for_status()
    attempted = asyncio.Event()
    attempts = 0
    real_connect = runtime._connect_broker

    async def recording_connect(value: ConsumerSettings) -> ConsumerBroker:
        nonlocal attempts
        try:
            return await real_connect(value)
        finally:
            attempts += 1
            attempted.set()

    monkeypatch.setattr(runtime, "_connect_broker", recording_connect)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_consumer(
            runtime_settings(
                os.environ["SIGNALFORGE_DATABASE_URL"],
                str(broker.url),
            ),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    try:
        await asyncio.wait_for(attempted.wait(), timeout=3)
        assert not task.done()
        await provision_vhost(broker)
        await publish(
            broker,
            incident_event.model_dump_json().encode(),
            message_id=incident_event.event_id,
        )
        await eventually(lambda: event_processed(incident_event.event_id))
        assert attempts >= 2
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)


async def test_runtime_discards_broken_broker_state_and_recovers(
    broker: Broker,
    incident_event: IncidentCreated,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connected: list[ConsumerBroker] = []
    real_connect = runtime._connect_broker

    async def recording_connect(value: ConsumerSettings) -> ConsumerBroker:
        resource = await real_connect(value)
        connected.append(resource)
        return resource

    monkeypatch.setattr(runtime, "_connect_broker", recording_connect)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_consumer(
            runtime_settings(
                os.environ["SIGNALFORGE_DATABASE_URL"],
                str(broker.url),
            ),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    try:

        async def first_consumer_ready() -> bool:
            state = await queue_state(broker)
            return state is not None and state.get("consumers") == 1

        await eventually(first_consumer_ready)
        (await broker.api.delete(f"/api/vhosts/{broker.vhost}")).raise_for_status()

        async def first_connection_closed() -> bool:
            return bool(connected and connected[0].connection.is_closed)

        await eventually(first_connection_closed)
        await provision_vhost(broker)
        await publish(
            broker,
            incident_event.model_dump_json().encode(),
            message_id=incident_event.event_id,
        )
        await eventually(lambda: event_processed(incident_event.event_id))
        assert len(connected) >= 2
        assert connected[0] is not connected[-1]
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)


async def test_postgres_outage_requeues_without_receipt_then_recovers(
    broker: Broker,
    incident_event: IncidentCreated,
) -> None:
    source_url = make_url(os.environ["SIGNALFORGE_DATABASE_URL"])
    assert source_url.host is not None and source_url.port is not None
    reservation = await asyncio.start_server(
        lambda reader, writer: None,
        "127.0.0.1",
        0,
    )
    listen_port = reservation.sockets[0].getsockname()[1]
    reservation.close()
    await reservation.wait_closed()
    proxy = TcpProxy(source_url.host, source_url.port, listen_port)
    proxy_url = source_url.set(host="127.0.0.1", port=listen_port).render_as_string(
        hide_password=False
    )
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_consumer(
            runtime_settings(proxy_url, str(broker.url)),
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    try:
        await publish(
            broker,
            incident_event.model_dump_json().encode(),
            message_id=incident_event.event_id,
        )

        async def delivery_requeued() -> bool:
            state = await queue_state(broker)
            return state is not None and state.get("messages_ready") == 1

        await eventually(delivery_requeued)
        assert await receipt_count(incident_event.event_id) == 0
        assert not task.done()

        await proxy.start()
        await eventually(lambda: event_processed(incident_event.event_id))
        assert await receipt_count(incident_event.event_id) == 1
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        await proxy.close()


async def test_module_process_exits_cleanly_on_sigterm_without_orphan_consumer(
    broker: Broker,
) -> None:
    environment = {
        **os.environ,
        "PYTHONPATH": "src",
        "SIGNALFORGE_DATABASE_URL": os.environ["SIGNALFORGE_DATABASE_URL"],
        "SIGNALFORGE_RABBITMQ_URL": str(broker.url),
        "SIGNALFORGE_CONSUMER_RECONNECT_BASE_SECONDS": "0.05",
        "SIGNALFORGE_CONSUMER_RECONNECT_MAX_SECONDS": "0.1",
        "SIGNALFORGE_CONSUMER_SHUTDOWN_DRAIN_TIMEOUT_SECONDS": "0.2",
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "signalforge.consumers.runtime",
        cwd=os.fspath(os.path.join(os.path.dirname(__file__), "../..")),
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:

        async def process_connected() -> bool:
            state = await queue_state(broker)
            return state is not None and state.get("consumers") == 1

        await eventually(process_connected)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
        output = (stdout + stderr).decode(errors="replace")
        assert process.returncode == 0
        assert "consumer shutdown requested" in output
        assert str(broker.url) not in output

        async def no_consumer_remains() -> bool:
            state = await queue_state(broker)
            return state is not None and state.get("consumers") == 0

        await eventually(no_consumer_remains)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
