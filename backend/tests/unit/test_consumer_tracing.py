from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aio_pika import IncomingMessage
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.consumers import incident_created
from signalforge.consumers.incident_created import (
    PROCESS_SPAN_NAME,
    ProcessingResult,
    handle_message,
)
from signalforge.core.config import TracingSettings
from signalforge.observability.propagation import capture_current_trace_context
from signalforge.observability.tracing import (
    INCIDENT_CONSUMER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)

pytestmark = pytest.mark.anyio

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
PRODUCER_ID = "b7ad6b7169203331"


class FakeMessage:
    def __init__(self, *, headers: object = None) -> None:
        event_id = uuid4()
        self.body = json.dumps(
            {
                "event_id": str(event_id),
                "event_type": "incident.created",
                "event_version": 1,
                "occurred_at": datetime(2026, 9, 13, tzinfo=UTC).isoformat(),
                "aggregate_id": str(uuid4()),
                "payload": {
                    "source": "manual",
                    "title": "Database latency",
                    "description": None,
                    "severity": "high",
                    "incident_occurred_at": datetime(
                        2026, 9, 12, 23, tzinfo=UTC
                    ).isoformat(),
                },
            }
        ).encode()
        self.headers = headers
        self.message_id = str(event_id)
        self.ack_calls = 0
        self.reject_calls: list[bool] = []
        self.nack_calls: list[bool] = []

    async def ack(self) -> None:
        self.ack_calls += 1

    async def reject(self, *, requeue: bool) -> None:
        self.reject_calls.append(requeue)

    async def nack(self, *, requeue: bool) -> None:
        self.nack_calls.append(requeue)


def runtime(
    *, sample_ratio: float = 1.0
) -> tuple[TracingRuntime, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    tracing = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
            tracing_sample_ratio=sample_ratio,
        ),
        service_name=INCIDENT_CONSUMER_SERVICE_NAME,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    return tracing, exporter


def incoming(message: FakeMessage) -> IncomingMessage:
    return cast(IncomingMessage, message)


def factory() -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], object())


async def test_process_span_continues_producer_and_is_active_during_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing, exporter = runtime()
    message = FakeMessage(
        headers={
            "traceparent": (f"00-{TRACE_ID}-{PRODUCER_ID}-01").encode(),
            "tracestate": b"vendor=value",
            "baggage": "private=must-not-propagate",
        }
    )
    captured = None

    async def process(*args: object) -> ProcessingResult:
        nonlocal captured
        captured = capture_current_trace_context()
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(incident_created, "process_event", process)
    try:
        tracer = tracing.get_tracer("test.consumer")
        assert tracer is not None
        result = await handle_message(incoming(message), factory(), tracer=tracer)

        assert result is ProcessingResult.PROCESSED
        assert message.ack_calls == 1
        assert captured is not None
        (span,) = exporter.get_finished_spans()
        assert span.kind is SpanKind.CONSUMER
        assert span.name == PROCESS_SPAN_NAME
        assert span.context is not None
        assert span.context.trace_id == int(TRACE_ID, 16)
        assert span.parent is not None
        assert span.parent.span_id == int(PRODUCER_ID, 16)
        assert span.attributes is not None
        assert span.attributes == {
            "messaging.system": "rabbitmq",
            "messaging.operation.name": "process",
            "messaging.operation.type": "process",
            "messaging.destination.name": (
                "signalforge.events:incident.created:signalforge.incident-events"
            ),
            "messaging.rabbitmq.destination.routing_key": "incident.created",
            "messaging.message.id": message.message_id,
        }
        assert captured.traceparent is not None
        assert captured.traceparent.split("-")[2] == f"{span.context.span_id:016x}"
        assert captured.tracestate == "vendor=value"
        assert "baggage" not in captured.as_dict()
    finally:
        tracing.shutdown()


@pytest.mark.parametrize(
    "headers",
    [
        None,
        {},
        {"traceparent": "malformed"},
        {"traceparent": "x" * 513},
        {"traceparent": b"\xff"},
        {"traceparent": object()},
    ],
)
async def test_invalid_or_missing_headers_do_not_change_processing(
    headers: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing, exporter = runtime()
    message = FakeMessage(headers=headers)
    monkeypatch.setattr(
        incident_created,
        "process_event",
        AsyncMock(return_value=ProcessingResult.PROCESSED),
    )
    try:
        tracer = tracing.get_tracer("test.consumer")
        assert tracer is not None
        result = await handle_message(incoming(message), factory(), tracer=tracer)

        assert result is ProcessingResult.PROCESSED
        assert message.ack_calls == 1
        assert message.reject_calls == []
        assert message.nack_calls == []
        (span,) = exporter.get_finished_spans()
        assert span.parent is None
    finally:
        tracing.shutdown()


async def test_unsampled_producer_parent_keeps_consumer_context_unsampled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing, exporter = runtime(sample_ratio=1.0)
    message = FakeMessage(headers={"traceparent": f"00-{TRACE_ID}-{PRODUCER_ID}-00"})
    captured = None

    async def process(*args: object) -> ProcessingResult:
        nonlocal captured
        captured = capture_current_trace_context()
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(incident_created, "process_event", process)
    try:
        tracer = tracing.get_tracer("test.consumer")
        assert tracer is not None
        result = await handle_message(incoming(message), factory(), tracer=tracer)

        assert result is ProcessingResult.PROCESSED
        assert message.ack_calls == 1
        assert exporter.get_finished_spans() == ()
        assert captured is not None
        assert captured.traceparent is not None
        assert captured.traceparent.split("-")[1] == TRACE_ID
        assert captured.traceparent.endswith("-00")
    finally:
        tracing.shutdown()
