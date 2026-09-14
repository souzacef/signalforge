from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from aio_pika.abc import (
    AbstractExchange,
    AbstractRobustChannel,
    AbstractRobustConnection,
)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode
from pamqp.commands import Basic

from signalforge.core.config import TracingSettings
from signalforge.observability.tracing import (
    DISPATCHER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)
from signalforge.outbox.dispatching import ClaimSnapshot
from signalforge.outbox.rabbitmq import PublishFailedError, RabbitMQPublisher

pytestmark = pytest.mark.anyio

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
PARENT_ID = "b7ad6b7169203331"


def claim(*, sampled: bool = True) -> ClaimSnapshot:
    now = datetime(2026, 9, 12, tzinfo=UTC)
    flags = "01" if sampled else "00"
    return ClaimSnapshot(
        id=uuid4(),
        claim_token=uuid4(),
        attempt_count=1,
        claimed_until=now + timedelta(minutes=5),
        event_type="incident.created",
        event_version=1,
        aggregate_id=uuid4(),
        payload=MappingProxyType(
            {
                "source": "manual",
                "title": "Database latency",
                "description": None,
                "severity": "high",
                "incident_occurred_at": "2026-09-11T15:30:00-03:00",
            }
        ),
        occurred_at=now,
        created_at=now,
        traceparent=f"00-{TRACE_ID}-{PARENT_ID}-{flags}",
        tracestate="vendor=value",
    )


def publisher() -> RabbitMQPublisher:
    connection = Mock(spec=AbstractRobustConnection)
    connection.close = AsyncMock()
    channel = Mock(spec=AbstractRobustChannel)
    channel.close = AsyncMock()
    channel.ready = AsyncMock()
    exchange = Mock(spec=AbstractExchange)
    exchange.publish = AsyncMock(return_value=Basic.Ack(delivery_tag=1))
    return RabbitMQPublisher(connection, channel, exchange, cleanup_timeout=0.02)


def tracing_runtime(
    *, sample_ratio: float = 1.0
) -> tuple[TracingRuntime, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    runtime = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
            tracing_sample_ratio=sample_ratio,
        ),
        service_name=DISPATCHER_SERVICE_NAME,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    return runtime, exporter


async def test_producer_span_continues_stored_context_and_injects_its_context() -> None:
    runtime, exporter = tracing_runtime()
    broker = publisher()
    stored = claim()
    try:
        tracer = runtime.get_tracer("test.publisher")
        assert tracer is not None
        await broker.publish(stored, timeout=1, tracer=tracer)

        (span,) = exporter.get_finished_spans()
        assert span.kind is SpanKind.PRODUCER
        assert span.name == "publish signalforge.events:incident.created"
        assert span.context is not None
        assert span.context.trace_id == int(TRACE_ID, 16)
        assert span.parent is not None
        assert span.parent.span_id == int(PARENT_ID, 16)
        assert span.attributes is not None
        assert span.attributes == {
            "messaging.system": "rabbitmq",
            "messaging.operation.name": "publish",
            "messaging.operation.type": "send",
            "messaging.destination.name": "signalforge.events:incident.created",
            "messaging.rabbitmq.destination.routing_key": "incident.created",
            "messaging.message.id": str(stored.id),
        }

        message = broker._exchange.publish.call_args.args[0]
        outgoing_parent = message.headers["traceparent"]
        assert isinstance(outgoing_parent, str)
        assert outgoing_parent.split("-")[1] == TRACE_ID
        assert outgoing_parent.split("-")[2] == f"{span.context.span_id:016x}"
        assert outgoing_parent != stored.traceparent
        assert message.headers["tracestate"] == "vendor=value"
        body = json.loads(message.body)
        assert "traceparent" not in body
        assert "tracestate" not in body
        assert "headers" not in body
    finally:
        runtime.shutdown()


async def test_repeated_attempts_are_distinct_siblings_of_durable_parent() -> None:
    runtime, exporter = tracing_runtime()
    broker = publisher()
    stored = claim()
    try:
        tracer = runtime.get_tracer("test.publisher")
        assert tracer is not None
        await broker.publish(stored, timeout=1, tracer=tracer)
        await broker.publish(stored, timeout=1, tracer=tracer)

        first, second = exporter.get_finished_spans()
        assert first.parent is not None and second.parent is not None
        assert first.parent.span_id == second.parent.span_id == int(PARENT_ID, 16)
        assert first.context is not None and second.context is not None
        assert first.context.span_id != second.context.span_id
        messages = [call.args[0] for call in broker._exchange.publish.call_args_list]
        assert messages[0].headers["traceparent"].split("-")[2] == (
            f"{first.context.span_id:016x}"
        )
        assert messages[1].headers["traceparent"].split("-")[2] == (
            f"{second.context.span_id:016x}"
        )
        assert stored.traceparent == f"00-{TRACE_ID}-{PARENT_ID}-01"
    finally:
        runtime.shutdown()


async def test_unsampled_parent_stays_unsampled_but_is_injected() -> None:
    runtime, exporter = tracing_runtime(sample_ratio=1.0)
    broker = publisher()
    try:
        tracer = runtime.get_tracer("test.publisher")
        assert tracer is not None
        await broker.publish(claim(sampled=False), timeout=1, tracer=tracer)

        assert exporter.get_finished_spans() == ()
        message = broker._exchange.publish.call_args.args[0]
        outgoing = message.headers["traceparent"]
        assert outgoing.split("-")[1] == TRACE_ID
        assert outgoing.endswith("-00")
    finally:
        runtime.shutdown()


async def test_invalid_durable_context_starts_root_producer_span() -> None:
    from dataclasses import replace

    runtime, exporter = tracing_runtime()
    broker = publisher()
    try:
        tracer = runtime.get_tracer("test.publisher")
        assert tracer is not None
        await broker.publish(
            replace(claim(), traceparent="malformed"), timeout=1, tracer=tracer
        )

        (span,) = exporter.get_finished_spans()
        assert span.parent is None
    finally:
        runtime.shutdown()


async def test_transport_failure_marks_span_without_sensitive_exception_text() -> None:
    runtime, exporter = tracing_runtime()
    broker = publisher()
    secret = "amqp://user:private-password@broker/private-vhost"
    broker._exchange.publish.side_effect = OSError(secret)
    try:
        tracer = runtime.get_tracer("test.publisher")
        assert tracer is not None
        with pytest.raises(PublishFailedError) as caught:
            await broker.publish(claim(), timeout=1, tracer=tracer)

        assert caught.value.ambiguous
        assert secret not in str(caught.value)
        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is StatusCode.ERROR
        assert span.attributes is not None
        assert span.attributes["error.type"] == "builtins.OSError"
        assert span.events == ()
        assert secret not in repr(span.attributes)
    finally:
        runtime.shutdown()
