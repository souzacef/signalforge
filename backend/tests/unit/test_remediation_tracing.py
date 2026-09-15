from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from aio_pika import IncomingMessage
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME
from opentelemetry.trace import SpanKind, StatusCode

from signalforge.core.config import RemediationExecutionSettings, TracingSettings
from signalforge.observability.propagation import (
    extract_trace_context,
    inject_trace_context,
)
from signalforge.observability.tracing import (
    DISPATCHER_SERVICE_NAME,
    REMEDIATION_WORKER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)
from signalforge.remediation import consumer
from signalforge.remediation.consumer import (
    PROCESS_DESTINATION,
    PROCESS_SPAN_NAME,
    InvalidEventError,
    ProcessingResult,
    handle_message,
)
from signalforge.remediation.errors import (
    RemediationExecutionRejectedError,
    RemediationExecutionTimeoutError,
    RemediationExecutionTransportError,
    RemediationTargetNotAllowedError,
)
from signalforge.remediation.execution import (
    AllowlistedHttpRemediationExecutor,
    HttpRestartServiceAdapter,
    RemediationCommand,
    RemediationExecutionOutcome,
)
from signalforge.remediation.models import RemediationActionKind

pytestmark = pytest.mark.anyio

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
DURABLE_PARENT_ID = "b7ad6b7169203331"


class FakeMessage:
    def __init__(self, *, headers: object = None, message_id: object = None) -> None:
        self.body = b'{"target":"PRIVATE_TARGET","proposal_id":"PRIVATE_PROPOSAL"}'
        self.headers = headers
        self.message_id = message_id


def incoming(message: FakeMessage) -> IncomingMessage:
    return cast(IncomingMessage, message)


def tracing_runtime(
    service_name: str = REMEDIATION_WORKER_SERVICE_NAME,
) -> tuple[TracingRuntime, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    runtime = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
            tracing_sample_ratio=1.0,
        ),
        service_name=cast(object, service_name),
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    return runtime, exporter


def command(target: str = "checkout-api") -> RemediationCommand:
    return RemediationCommand(
        proposal_id=uuid4(),
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target=target,
    )


def settings() -> RemediationExecutionSettings:
    return RemediationExecutionSettings.model_validate(
        {
            "restart_endpoints": {
                "checkout-api": "http://private-control/internal/restart"
            },
            "request_timeout_seconds": 0.1,
            "attempt_lease_seconds": 1,
        }
    )


def one_span(spans: tuple[ReadableSpan, ...], kind: SpanKind) -> ReadableSpan:
    matching = tuple(span for span in spans if span.kind is kind)
    assert len(matching) == 1
    return matching[0]


async def test_consumer_process_span_continues_rabbitmq_parent_and_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.remediation.consumer")
    assert tracer is not None
    event_id = uuid4()
    monkeypatch.setattr(
        consumer, "_handle_message", AsyncMock(return_value=ProcessingResult.PROCESSED)
    )
    message = FakeMessage(
        headers={
            "traceparent": f"00-{TRACE_ID}-{DURABLE_PARENT_ID}-01",
            "tracestate": "vendor=value",
            "baggage": "PRIVATE_BAGGAGE",
        },
        message_id=str(event_id),
    )

    try:
        result = await handle_message(
            incoming(message),
            cast(object, SimpleNamespace()),
            cast(object, SimpleNamespace()),
            tracer=tracer,
        )

        assert result is ProcessingResult.PROCESSED
        span = one_span(exporter.get_finished_spans(), SpanKind.CONSUMER)
        assert span.name == PROCESS_SPAN_NAME
        assert span.context is not None
        assert span.context.trace_id == int(TRACE_ID, 16)
        assert span.parent is not None
        assert span.parent.span_id == int(DURABLE_PARENT_ID, 16)
        assert span.attributes == {
            "messaging.system": "rabbitmq",
            "messaging.operation.name": "process",
            "messaging.operation.type": "process",
            "messaging.destination.name": PROCESS_DESTINATION,
            "messaging.rabbitmq.destination.routing_key": (
                "remediation.execution.requested"
            ),
            "messaging.message.id": str(event_id),
        }
        assert span.status.status_code is StatusCode.UNSET
        assert span.resource.attributes[SERVICE_NAME] == (
            REMEDIATION_WORKER_SERVICE_NAME
        )
        captured = repr(span.attributes)
        for secret in ("PRIVATE_TARGET", "PRIVATE_PROPOSAL", "PRIVATE_BAGGAGE"):
            assert secret not in captured
    finally:
        runtime.shutdown()


@pytest.mark.parametrize(
    "headers",
    [None, {}, {"traceparent": "malformed"}, {"traceparent": b"\xff"}],
)
async def test_consumer_missing_or_malformed_context_safely_starts_root(
    headers: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.remediation.consumer")
    assert tracer is not None
    monkeypatch.setattr(
        consumer, "_handle_message", AsyncMock(return_value=ProcessingResult.DUPLICATE)
    )
    try:
        result = await handle_message(
            incoming(FakeMessage(headers=headers, message_id="not-a-uuid")),
            cast(object, SimpleNamespace()),
            cast(object, SimpleNamespace()),
            tracer=tracer,
        )
        assert result is ProcessingResult.DUPLICATE
        span = one_span(exporter.get_finished_spans(), SpanKind.CONSUMER)
        assert span.parent is None
        assert "messaging.message.id" not in (span.attributes or {})
    finally:
        runtime.shutdown()


async def test_consumer_escaping_error_marks_process_span_without_exception_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.remediation.consumer")
    assert tracer is not None
    secret = "PRIVATE_INVALID_EVENT_DETAIL"
    error = InvalidEventError("invalid_event")
    error.args = (secret,)
    monkeypatch.setattr(consumer, "_handle_message", AsyncMock(side_effect=error))
    try:
        with pytest.raises(InvalidEventError) as caught:
            await handle_message(
                incoming(FakeMessage()),
                cast(object, SimpleNamespace()),
                cast(object, SimpleNamespace()),
                tracer=tracer,
            )
        assert caught.value is error
        span = one_span(exporter.get_finished_spans(), SpanKind.CONSUMER)
        assert span.status.status_code is StatusCode.ERROR
        assert span.events == ()
        assert span.attributes is not None
        assert span.attributes["error.type"] == (
            "signalforge.remediation.consumer.InvalidEventError"
        )
        assert secret not in repr(span)
    finally:
        runtime.shutdown()


async def test_actuator_success_span_is_one_sanitized_client_span() -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.remediation.http_actuator")
    assert tracer is not None
    remediation_command = command()

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await HttpRestartServiceAdapter(
            settings(), client=client, tracer=tracer
        ).restart(remediation_command)

    try:
        assert result.outcome is RemediationExecutionOutcome.SUCCEEDED
        span = one_span(exporter.get_finished_spans(), SpanKind.CLIENT)
        assert span.name == "remediation restart_service"
        assert span.status.status_code is StatusCode.UNSET
        assert span.attributes == {
            "remediation.action_kind": "restart_service",
            "http.response.status_code": 204,
        }
        captured = repr(span.attributes)
        for secret in (
            "private-control",
            "checkout-api",
            str(remediation_command.proposal_id),
            "Idempotency-Key",
        ):
            assert secret not in captured
    finally:
        runtime.shutdown()


@pytest.mark.parametrize(
    ("case", "expected_error"),
    [
        ("http", RemediationExecutionRejectedError),
        ("timeout", RemediationExecutionTimeoutError),
        ("transport", RemediationExecutionTransportError),
        ("unexpected", ValueError),
    ],
)
async def test_actuator_failures_mark_sanitized_client_span(
    case: str,
    expected_error: type[BaseException],
) -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.remediation.http_actuator")
    assert tracer is not None
    secret = f"PRIVATE_{case.upper()}_DETAIL"

    def fail(request: httpx.Request) -> httpx.Response:
        if case == "http":
            return httpx.Response(503, text=secret)
        if case == "timeout":
            raise httpx.ReadTimeout(secret, request=request)
        if case == "transport":
            raise httpx.ConnectError(secret, request=request)
        raise ValueError(secret)

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
            with pytest.raises(expected_error):
                await HttpRestartServiceAdapter(
                    settings(), client=client, tracer=tracer
                ).restart(command())

        span = one_span(exporter.get_finished_spans(), SpanKind.CLIENT)
        assert span.status.status_code is StatusCode.ERROR
        assert span.events == ()
        assert span.attributes is not None
        assert span.attributes["remediation.action_kind"] == "restart_service"
        if case == "http":
            assert span.attributes["http.response.status_code"] == 503
        else:
            assert "http.response.status_code" not in span.attributes
        assert secret not in repr(span)
        assert "private-control" not in repr(span.attributes)
    finally:
        runtime.shutdown()


async def test_unallowlisted_target_creates_no_http_client_span() -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.remediation.http_actuator")
    assert tracer is not None
    try:
        with pytest.raises(RemediationTargetNotAllowedError):
            await HttpRestartServiceAdapter(settings(), tracer=tracer).restart(
                command("unknown-service")
            )
        assert exporter.get_finished_spans() == ()
    finally:
        runtime.shutdown()


async def test_durable_context_to_headers_to_consumer_to_actuator_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatcher_runtime, dispatcher_exporter = tracing_runtime(DISPATCHER_SERVICE_NAME)
    worker_runtime, worker_exporter = tracing_runtime()
    producer = dispatcher_runtime.get_tracer("signalforge.outbox.rabbitmq")
    consumer_tracer = worker_runtime.get_tracer("signalforge.remediation.consumer")
    actuator_tracer = worker_runtime.get_tracer("signalforge.remediation.http_actuator")
    assert producer is not None and consumer_tracer is not None
    assert actuator_tracer is not None
    durable_context = extract_trace_context(
        traceparent=f"00-{TRACE_ID}-{DURABLE_PARENT_ID}-01",
        tracestate="vendor=value",
    )
    with producer.start_as_current_span(
        "publish signalforge.events:remediation.execution.requested",
        context=durable_context,
        kind=SpanKind.PRODUCER,
    ):
        headers = inject_trace_context().as_dict()

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        executor = AllowlistedHttpRemediationExecutor(
            HttpRestartServiceAdapter(settings(), client=client, tracer=actuator_tracer)
        )

        async def process(*args: object, **kwargs: object) -> ProcessingResult:
            await executor.execute(command())
            return ProcessingResult.PROCESSED

        monkeypatch.setattr(consumer, "_handle_message", process)
        try:
            result = await handle_message(
                incoming(FakeMessage(headers=headers, message_id=str(uuid4()))),
                executor,
                cast(object, SimpleNamespace()),
                tracer=consumer_tracer,
            )
            assert result is ProcessingResult.PROCESSED

            (producer_span,) = dispatcher_exporter.get_finished_spans()
            worker_spans = worker_exporter.get_finished_spans()
            process_span = one_span(worker_spans, SpanKind.CONSUMER)
            client_span = one_span(worker_spans, SpanKind.CLIENT)
            assert producer_span.context is not None
            assert process_span.context is not None
            assert process_span.parent is not None
            assert client_span.parent is not None
            assert process_span.context.trace_id == producer_span.context.trace_id
            assert process_span.parent.span_id == producer_span.context.span_id
            assert client_span.context is not None
            assert client_span.context.trace_id == process_span.context.trace_id
            assert client_span.parent.span_id == process_span.context.span_id
        finally:
            worker_runtime.shutdown()
            dispatcher_runtime.shutdown()


async def test_terminal_http_failure_keeps_successfully_processed_consumer_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    consumer_tracer = runtime.get_tracer("signalforge.remediation.consumer")
    actuator_tracer = runtime.get_tracer("signalforge.remediation.http_actuator")
    assert consumer_tracer is not None and actuator_tracer is not None

    def reject(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject)) as client:
        executor = AllowlistedHttpRemediationExecutor(
            HttpRestartServiceAdapter(settings(), client=client, tracer=actuator_tracer)
        )

        async def classify(*args: object, **kwargs: object) -> ProcessingResult:
            with pytest.raises(RemediationExecutionRejectedError):
                await executor.execute(command())
            return ProcessingResult.PROCESSED

        monkeypatch.setattr(consumer, "_handle_message", classify)
        try:
            result = await handle_message(
                incoming(FakeMessage()),
                executor,
                cast(object, SimpleNamespace()),
                tracer=consumer_tracer,
            )
            assert result is ProcessingResult.PROCESSED
            spans = exporter.get_finished_spans()
            assert (
                one_span(spans, SpanKind.CONSUMER).status.status_code
                is StatusCode.UNSET
            )
            assert (
                one_span(spans, SpanKind.CLIENT).status.status_code is StatusCode.ERROR
            )
        finally:
            runtime.shutdown()
