import asyncio
import json
import traceback
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from aio_pika import RobustChannel
from aio_pika.abc import (
    AbstractExchange,
    AbstractRobustChannel,
    AbstractRobustConnection,
)
from aio_pika.exceptions import (
    AMQPConnectionError,
    ChannelInvalidStateError,
    DeliveryError,
)
from pamqp.commands import Basic

from signalforge.outbox import rabbitmq
from signalforge.outbox.dispatching import ClaimSnapshot
from signalforge.outbox.rabbitmq import (
    PublishFailedError,
    PublishTimeoutError,
    RabbitMQPublisher,
    StoredEventError,
    reconstruct_event,
    thaw_json,
)


@pytest.fixture
def claim() -> ClaimSnapshot:
    now = datetime(2026, 9, 12, tzinfo=UTC)
    return ClaimSnapshot(
        id=uuid4(),
        claim_token=uuid4(),
        attempt_count=2,
        claimed_until=now + timedelta(minutes=5),
        event_type="incident.created",
        event_version=1,
        aggregate_id=uuid4(),
        payload=MappingProxyType(
            {
                "source": "manual",
                "title": "Alerta São Paulo",
                "description": None,
                "severity": "high",
                "incident_occurred_at": "2026-09-11T15:30:00-03:00",
            }
        ),
        occurred_at=now,
        created_at=now,
    )


@pytest.fixture
def publisher() -> RabbitMQPublisher:
    connection = Mock(spec=AbstractRobustConnection)
    connection.close = AsyncMock()
    channel = Mock(spec=AbstractRobustChannel)
    channel.close = AsyncMock()
    channel.ready = AsyncMock()
    exchange = Mock(spec=AbstractExchange)
    exchange.publish = AsyncMock(return_value=Basic.Ack(delivery_tag=1))
    return RabbitMQPublisher(connection, channel, exchange, cleanup_timeout=0.02)


def test_reconstructs_existing_envelope_without_changing_snapshot(
    claim: ClaimSnapshot,
) -> None:
    before = dict(claim.payload)
    event = reconstruct_event(claim)
    body = json.loads(event.model_dump_json().encode("utf-8"))
    assert set(body) == {
        "event_id",
        "event_type",
        "event_version",
        "aggregate_id",
        "occurred_at",
        "payload",
    }
    assert event.event_id == claim.id
    assert event.aggregate_id == claim.aggregate_id
    assert event.occurred_at == claim.occurred_at
    assert body["payload"]["title"] == "Alerta São Paulo"
    assert dict(claim.payload) == before
    assert event.model_dump(mode="json")["payload"]["severity"] == "high"


def test_nested_frozen_payload_is_thawed_without_mutating_claim(
    claim: ClaimSnapshot,
) -> None:
    nested = MappingProxyType(
        {"items": (MappingProxyType({"values": (None, True, 2, 1.5, "x")}),)}
    )
    frozen_claim = replace(claim, payload=nested)
    ordinary = thaw_json(frozen_claim.payload)
    assert json.loads(json.dumps(ordinary)) == {
        "items": [{"values": [None, True, 2, 1.5, "x"]}]
    }
    ordinary["items"][0]["values"].append("copy only")
    assert frozen_claim.payload["items"][0]["values"] == (None, True, 2, 1.5, "x")
    # v1 has no array/object-valued payload fields. Thawing must not weaken it.
    with pytest.raises(StoredEventError, match="invalid_event"):
        reconstruct_event(frozen_claim)


@pytest.mark.parametrize(
    "value", [object(), b"bytes", {1: "key"}, [1], {1, 2}, float("nan"), float("inf")]
)
def test_thaw_rejects_non_frozen_json_values(value: object) -> None:
    with pytest.raises(StoredEventError):
        thaw_json(value)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"event_type": "incident.resolved"}, "unsupported_type"),
        ({"event_version": 2}, "unsupported_version"),
        ({"payload": MappingProxyType({})}, "invalid_event"),
        ({"occurred_at": datetime(2026, 1, 1)}, "invalid_event"),
    ],
)
async def test_invalid_stored_event_is_rejected_before_broker_access(
    claim: ClaimSnapshot,
    publisher: RabbitMQPublisher,
    changes: dict,
    reason: str,
) -> None:
    with pytest.raises(StoredEventError) as caught:
        await publisher.publish(replace(claim, **changes), timeout=1)
    assert caught.value.reason == reason
    publisher._channel.ready.assert_not_awaited()
    publisher._exchange.publish.assert_not_awaited()


@pytest.mark.anyio
async def test_waits_for_positive_confirmation(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher
) -> None:
    entered, confirmed = asyncio.Event(), asyncio.Event()

    async def confirm(*args, **kwargs):
        entered.set()
        await confirmed.wait()
        return Basic.Ack(delivery_tag=1)

    publisher._exchange.publish.side_effect = confirm
    async with asyncio.timeout(1):
        task = asyncio.create_task(publisher.publish(claim, timeout=1))
        await entered.wait()
        assert not task.done()
        confirmed.set()
        assert await task is None
    kwargs = publisher._exchange.publish.call_args.kwargs
    assert kwargs == {
        "routing_key": "incident.created",
        "mandatory": True,
        "timeout": 1,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "frame", [Basic.Nack(delivery_tag=1), Basic.Reject(delivery_tag=1)]
)
async def test_negative_confirmation_is_failure(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher, frame
) -> None:
    publisher._exchange.publish.side_effect = DeliveryError(None, frame)
    with pytest.raises(PublishFailedError) as caught:
        await publisher.publish(claim, timeout=1)
    assert not caught.value.ambiguous


@pytest.mark.anyio
@pytest.mark.parametrize(
    "confirmation",
    [None, True, Basic.Nack(delivery_tag=1), Basic.Reject(delivery_tag=1)],
)
async def test_no_non_ack_result_can_mean_success(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher, confirmation
) -> None:
    publisher._exchange.publish.return_value = confirmation
    with pytest.raises(PublishFailedError):
        await publisher.publish(claim, timeout=1)


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["ready", "publish"])
async def test_timeout_bounds_readiness_and_confirm_and_retires_publisher(
    claim: ClaimSnapshot,
    publisher: RabbitMQPublisher,
    stage: str,
) -> None:
    blocker = asyncio.Event()
    operation = (
        publisher._channel.ready if stage == "ready" else publisher._exchange.publish
    )

    async def hang(*args, **kwargs):
        await blocker.wait()

    operation.side_effect = hang
    async with asyncio.timeout(1):
        with pytest.raises(PublishTimeoutError) as caught:
            await publisher.publish(claim, timeout=0.01)
    assert caught.value.ambiguous
    publisher._channel.close.assert_awaited_once()
    publisher._connection.close.assert_awaited_once()
    with pytest.raises(PublishFailedError, match="retired"):
        await publisher.publish(claim, timeout=1)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_type", [AMQPConnectionError, OSError, ChannelInvalidStateError]
)
async def test_transport_errors_are_sanitized_and_channel_retired(
    claim: ClaimSnapshot,
    publisher: RabbitMQPublisher,
    error_type,
) -> None:
    password = uuid4().hex
    url = f"amqp://user:{password}@broker/signalforge"
    publisher._exchange.publish.side_effect = error_type(url)
    with pytest.raises(PublishFailedError) as caught:
        await publisher.publish(claim, timeout=1)
    assert caught.value.ambiguous
    assert password not in "".join(traceback.format_exception(caught.value))
    assert url not in str(caught.value)
    publisher._channel.close.assert_awaited_once()
    publisher._connection.close.assert_awaited_once()


@pytest.mark.anyio
async def test_cleanup_is_bounded_even_when_close_hangs(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher
) -> None:
    publisher._exchange.publish.side_effect = TimeoutError()

    async def hang():
        await asyncio.Event().wait()

    publisher._channel.close.side_effect = hang
    publisher._connection.close.side_effect = hang
    async with asyncio.timeout(1):
        with pytest.raises(PublishTimeoutError):
            await publisher.publish(claim, timeout=0.01)
    assert publisher._retired


@pytest.mark.anyio
async def test_caller_cancellation_propagates_after_retirement(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher
) -> None:
    entered = asyncio.Event()

    async def hang(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    publisher._exchange.publish.side_effect = hang
    async with asyncio.timeout(1):
        task = asyncio.create_task(publisher.publish(claim, timeout=1))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert publisher._retired


@pytest.mark.anyio
async def test_cancelled_library_future_is_ambiguous(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher
) -> None:
    publisher._exchange.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(PublishFailedError) as caught:
        await publisher.publish(claim, timeout=1)
    assert caught.value.ambiguous
    assert publisher._retired


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_timeout", [0, -1, float("inf"), float("nan")])
async def test_timeout_must_be_finite_and_positive(
    claim: ClaimSnapshot, publisher: RabbitMQPublisher, invalid_timeout: float
) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        await publisher.publish(claim, timeout=invalid_timeout)
    publisher._exchange.publish.assert_not_awaited()


@pytest.mark.anyio
async def test_startup_failure_closes_connection_and_hides_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    password = uuid4().hex
    url = f"amqp://user:{password}@broker/signalforge"
    connection = Mock(spec=AbstractRobustConnection)
    connection.connect = AsyncMock(side_effect=AMQPConnectionError(url))
    connection.close = AsyncMock()
    monkeypatch.setattr(rabbitmq, "RobustConnection", Mock(return_value=connection))
    with pytest.raises(PublishFailedError) as caught:
        await RabbitMQPublisher.connect(url, timeout=1)
    assert not caught.value.ambiguous
    assert password not in "".join(traceback.format_exception(caught.value))
    connection.close.assert_awaited_once()


@pytest.mark.anyio
async def test_startup_uses_confirms_and_return_handling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Mock(spec=AbstractRobustConnection)
    connection.connect = AsyncMock()
    connection.close = AsyncMock()
    channel = Mock(spec=RobustChannel)
    channel.initialize = AsyncMock()
    channel.close = AsyncMock()
    connection.channel.return_value = channel
    exchange = Mock(spec=AbstractExchange)
    declare = AsyncMock(return_value=exchange)
    monkeypatch.setattr(rabbitmq, "RobustConnection", Mock(return_value=connection))
    monkeypatch.setattr(rabbitmq, "declare_topology", declare)
    publisher = await RabbitMQPublisher.connect(
        "amqp://localhost/signalforge", timeout=1
    )
    connection.channel.assert_called_once_with(
        publisher_confirms=True, on_return_raises=True
    )
    channel.initialize.assert_awaited_once()
    declare.assert_awaited_once_with(channel, timeout=1)
    await publisher.close()


@pytest.mark.anyio
async def test_startup_timeout_closes_its_reconnecting_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Mock(spec=AbstractRobustConnection)

    async def hang(**kwargs):
        await asyncio.Event().wait()

    connection.connect = AsyncMock(side_effect=hang)
    connection.close = AsyncMock()
    monkeypatch.setattr(rabbitmq, "RobustConnection", Mock(return_value=connection))
    async with asyncio.timeout(1):
        with pytest.raises(PublishFailedError, match="startup timed out") as caught:
            await RabbitMQPublisher.connect(
                "amqp://localhost/signalforge", timeout=0.01
            )
    assert not caught.value.ambiguous
    connection.close.assert_awaited_once()


@pytest.mark.anyio
async def test_unrelated_programming_error_is_not_disguised_as_broker_failure(
    publisher: RabbitMQPublisher,
    claim: ClaimSnapshot,
) -> None:
    publisher._exchange.publish.side_effect = RuntimeError("programming defect")
    with pytest.raises(RuntimeError, match="programming defect"):
        await publisher.publish(claim, timeout=1)
