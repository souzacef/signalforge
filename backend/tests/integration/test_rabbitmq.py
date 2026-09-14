import asyncio
import json
import os
import traceback
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import aio_pika
import pytest
from httpx import AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker
from yarl import URL

from signalforge.db.session import engine
from signalforge.outbox.dispatching import ClaimSnapshot, claim_pending, settle_success
from signalforge.outbox.models import OutboxEvent
from signalforge.outbox.rabbitmq import (
    ENRICHMENT_QUEUE_NAME,
    ENRICHMENT_ROUTING_KEY,
    EXCHANGE_NAME,
    QUEUE_NAME,
    REMEDIATION_EXECUTION_QUEUE_NAME,
    REMEDIATION_EXECUTION_ROUTING_KEY,
    ROUTING_KEY,
    PublishFailedError,
    RabbitMQPublisher,
    StoredEventError,
    UnroutablePublishError,
    declare_topology,
)

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)


@dataclass
class Broker:
    url: URL
    api: AsyncClient
    vhost: str


@pytest.fixture
async def broker() -> AsyncIterator[Broker]:
    raw_url = os.environ.get("SIGNALFORGE_TEST_RABBITMQ_URL")
    if raw_url is None:
        pytest.skip("Set SIGNALFORGE_TEST_RABBITMQ_URL to run real RabbitMQ tests")
    # Opt in to isolated test-vhost creation; never operate on the supplied vhost.
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
async def claim() -> AsyncIterator[ClaimSnapshot]:
    event_id = uuid4()
    try:
        async with sessions.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
            )
            now = await session.scalar(select(func.clock_timestamp()))
            session.add(
                OutboxEvent(
                    id=event_id,
                    event_type="incident.created",
                    event_version=1,
                    aggregate_id=uuid4(),
                    occurred_at=now,
                    payload={
                        "source": "manual",
                        "title": "Alerta São Paulo",
                        "description": None,
                        "severity": "high",
                        "incident_occurred_at": "2026-09-11T15:30:00-03:00",
                    },
                )
            )
        async with sessions.begin() as session:
            claims = await claim_pending(
                session, batch_limit=1, lease_duration=timedelta(minutes=5)
            )
        assert len(claims) == 1 and claims[0].id == event_id
        yield claims[0]
    finally:
        async with sessions.begin() as session:
            await session.execute(delete(OutboxEvent).where(OutboxEvent.id == event_id))


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


async def probe(broker: Broker):
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    try:
        async with connection:
            channel = await connection.channel()
            queue = await channel.get_queue(QUEUE_NAME)
            return await queue.get(no_ack=True, fail=False, timeout=5)
    finally:
        await connection.close()


async def test_confirmed_persistent_publication_matches_durable_snapshot(
    broker: Broker,
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
) -> None:
    before = await read_event(claim.id)
    assert await publisher.publish(claim, timeout=5) is None
    message = await probe(broker)
    assert message is not None
    body = json.loads(message.body)
    assert set(body) == {
        "event_id",
        "event_type",
        "event_version",
        "aggregate_id",
        "occurred_at",
        "payload",
    }
    assert UUID(body["event_id"]) == claim.id
    assert body["event_type"] == before["event_type"] == "incident.created"
    assert body["event_version"] == before["event_version"] == 1
    assert UUID(body["aggregate_id"]) == before["aggregate_id"]
    assert datetime.fromisoformat(body["occurred_at"]) == before["occurred_at"]
    # Pydantic normalizes datetime offsets without changing their instant.
    payload = body["payload"]
    assert datetime.fromisoformat(
        payload.pop("incident_occurred_at")
    ) == datetime.fromisoformat(before["payload"]["incident_occurred_at"])
    assert payload == {
        k: v for k, v in before["payload"].items() if k != "incident_occurred_at"
    }
    assert message.message_id == str(claim.id)
    assert message.type == "incident.created"
    assert message.content_type == "application/json"
    assert message.content_encoding == "utf-8"
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert message.headers == {}
    assert await read_event(claim.id) == before
    # Composition only in this test, never inside the publisher.
    async with sessions.begin() as session:
        assert await settle_success(
            session, event_id=claim.id, claim_token=claim.claim_token
        )
    assert (await read_event(claim.id))["published_at"] is not None


async def test_remediation_request_routes_to_durable_execution_queue(
    broker: Broker,
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
) -> None:
    remediation_claim = replace(
        claim,
        event_type=REMEDIATION_EXECUTION_ROUTING_KEY,
        payload=MappingProxyType(
            {
                "proposal_id": str(uuid4()),
                "action_kind": "restart_service",
                "target": "checkout-api",
            }
        ),
    )
    await publisher.publish(remediation_claim, timeout=5)

    queue = await publisher._channel.get_queue(REMEDIATION_EXECUTION_QUEUE_NAME)
    message = await queue.get(no_ack=True, fail=True, timeout=5)
    assert message.message_id == str(remediation_claim.id)
    assert message.type == REMEDIATION_EXECUTION_ROUTING_KEY
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert json.loads(message.body)["payload"] == {
        "proposal_id": remediation_claim.payload["proposal_id"],
        "action_kind": "restart_service",
        "target": "checkout-api",
    }


async def test_topology_is_idempotent_and_usable_across_startups(
    broker: Broker, claim: ClaimSnapshot
) -> None:
    for _ in range(2):
        publisher = await RabbitMQPublisher.connect(str(broker.url), timeout=5)
        try:
            await declare_topology(publisher._channel, timeout=5)
            await publisher.publish(claim, timeout=5)
            assert (await probe(broker)).message_id == str(claim.id)
        finally:
            await publisher.close()
    exchange = (
        await broker.api.get(f"/api/exchanges/{broker.vhost}/{EXCHANGE_NAME}")
    ).json()
    queue = (await broker.api.get(f"/api/queues/{broker.vhost}/{QUEUE_NAME}")).json()
    bindings = (
        await broker.api.get(
            f"/api/bindings/{broker.vhost}/e/{EXCHANGE_NAME}/q/{QUEUE_NAME}"
        )
    ).json()
    enrichment_queue = (
        await broker.api.get(f"/api/queues/{broker.vhost}/{ENRICHMENT_QUEUE_NAME}")
    ).json()
    enrichment_bindings = (
        await broker.api.get(
            f"/api/bindings/{broker.vhost}/e/{EXCHANGE_NAME}/q/{ENRICHMENT_QUEUE_NAME}"
        )
    ).json()
    remediation_queue = (
        await broker.api.get(
            f"/api/queues/{broker.vhost}/{REMEDIATION_EXECUTION_QUEUE_NAME}"
        )
    ).json()
    remediation_bindings = (
        await broker.api.get(
            f"/api/bindings/{broker.vhost}/e/{EXCHANGE_NAME}/q/"
            f"{REMEDIATION_EXECUTION_QUEUE_NAME}"
        )
    ).json()
    assert exchange["type"] == "direct" and exchange["durable"] is True
    assert queue["type"] == "classic" and queue["durable"] is True
    assert queue["auto_delete"] is queue["exclusive"] is False
    assert [binding["routing_key"] for binding in bindings] == [ROUTING_KEY]
    assert enrichment_queue["type"] == "classic"
    assert enrichment_queue["durable"] is True
    assert enrichment_queue["auto_delete"] is enrichment_queue["exclusive"] is False
    assert [binding["routing_key"] for binding in enrichment_bindings] == [
        ENRICHMENT_ROUTING_KEY
    ]
    assert remediation_queue["type"] == "classic"
    assert remediation_queue["durable"] is True
    assert remediation_queue["auto_delete"] is remediation_queue["exclusive"] is False
    assert [binding["routing_key"] for binding in remediation_bindings] == [
        REMEDIATION_EXECUTION_ROUTING_KEY
    ]


async def test_incompatible_topology_is_not_repaired(broker: Broker) -> None:
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    async with connection:
        channel = await connection.channel()
        await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True
        )
    with pytest.raises(PublishFailedError) as caught:
        await RabbitMQPublisher.connect(str(broker.url), timeout=2, cleanup_timeout=0.2)
    assert not caught.value.ambiguous
    exchange = (
        await broker.api.get(f"/api/exchanges/{broker.vhost}/{EXCHANGE_NAME}")
    ).json()
    assert exchange["type"] == "fanout"


async def test_mandatory_return_fails_then_same_event_succeeds(
    broker: Broker,
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
) -> None:
    before = await read_event(claim.id)
    queue = await publisher._channel.get_queue(QUEUE_NAME)
    await queue.unbind(EXCHANGE_NAME, routing_key=ROUTING_KEY, timeout=5)
    with pytest.raises(UnroutablePublishError) as caught:
        await publisher.publish(claim, timeout=5)
    assert not caught.value.ambiguous
    assert await probe(broker) is None
    assert await read_event(claim.id) == before
    assert before["published_at"] is None
    await declare_topology(publisher._channel, timeout=5)
    await publisher.publish(claim, timeout=5)
    message = await probe(broker)
    assert message.message_id == str(claim.id)
    assert json.loads(message.body)["event_id"] == str(claim.id)
    assert await read_event(claim.id) == before


async def test_real_broker_nack_is_not_success(
    broker: Broker,
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
) -> None:
    before = await read_event(claim.id)
    # Deliberate test-only overflow topology, confined to this fresh disposable vhost.
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
        await publisher.publish(claim, timeout=5)
        with pytest.raises(PublishFailedError) as caught:
            await publisher.publish(claim, timeout=5)
        assert not caught.value.ambiguous
        assert "negatively acknowledged" in str(caught.value)
    assert await read_event(claim.id) == before


async def test_vhost_loss_cannot_mutate_committed_claim(
    broker: Broker,
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
) -> None:
    before = await read_event(claim.id)
    # Real server-side connection loss, with reconnect prevented by vhost removal.
    (await broker.api.delete(f"/api/vhosts/{broker.vhost}")).raise_for_status()
    async with asyncio.timeout(5):
        with pytest.raises(PublishFailedError) as caught:
            await publisher.publish(claim, timeout=0.2)
    assert caught.value.ambiguous
    assert await read_event(claim.id) == before
    assert before["published_at"] is None
    assert publisher._retired


@pytest.mark.parametrize("case", ["type", "version", "payload", "nested"])
async def test_invalid_event_never_reaches_real_queue(
    broker: Broker,
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
    case: str,
) -> None:
    changes = {
        "type": {"event_type": "unknown.event"},
        "version": {"event_version": 2},
        "payload": {"payload": MappingProxyType({})},
        "nested": {
            "payload": MappingProxyType(
                {**claim.payload, "title": (MappingProxyType({"x": 1}),)}
            )
        },
    }
    before = await read_event(claim.id)
    with pytest.raises(StoredEventError):
        await publisher.publish(replace(claim, **changes[case]), timeout=5)
    assert await probe(broker) is None
    assert await read_event(claim.id) == before


async def test_real_authentication_error_does_not_expose_credentials(
    broker: Broker,
) -> None:
    wrong_password = uuid4().hex
    invalid_url = str(broker.url.with_password(wrong_password))
    with pytest.raises(PublishFailedError) as caught:
        await RabbitMQPublisher.connect(invalid_url, timeout=2, cleanup_timeout=0.2)
    assert not caught.value.ambiguous
    formatted = "".join(traceback.format_exception(caught.value))
    assert wrong_password not in formatted
    assert invalid_url not in formatted
