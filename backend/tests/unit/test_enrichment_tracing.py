from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from aio_pika import IncomingMessage
from google.genai import errors
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode, Tracer
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.core.config import EnrichmentSettings, TracingSettings
from signalforge.enrichment import consumer, gemini
from signalforge.enrichment.consumer import (
    PROCESS_SPAN_NAME,
    EnrichmentProcessingResult,
    InvalidEnrichmentEventError,
    handle_message,
)
from signalforge.enrichment.gemini import GeminiEnrichmentProvider
from signalforge.enrichment.provider import (
    PermanentEnrichmentError,
    ProviderFailureReason,
    TransientEnrichmentError,
)
from signalforge.observability.metrics import ProviderMetrics
from signalforge.observability.tracing import (
    ENRICHMENT_WORKER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)

pytestmark = pytest.mark.anyio

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
PRODUCER_ID = "b7ad6b7169203331"
MODEL = "gemini-test-flash"


class FakeModels:
    def __init__(self, *, parsed: object = None, error: BaseException | None = None):
        self.parsed = parsed
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def generate_content(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(parsed=self.parsed)


class FakeClient:
    def __init__(self, models: FakeModels) -> None:
        self.aio = SimpleNamespace(models=models)


class FakeMessage:
    def __init__(
        self,
        body: bytes,
        *,
        event_id: UUID,
        headers: object = None,
    ) -> None:
        self.body = body
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


def valid_result(*, private: bool = False) -> dict[str, object]:
    suffix = " PRIVATE_PARSED_RESULT" if private else ""
    return {
        "summary": f"Checkout latency requires investigation.{suffix}",
        "category": "performance",
        "suspected_component": f"checkout database{suffix}",
        "investigation_steps": [f"Review query latency.{suffix}"],
    }


def message_body(event_id: UUID, *, private: bool = False) -> bytes:
    marker = "PRIVATE_INCIDENT_CONTENT" if private else "Checkout latency"
    return json.dumps(
        {
            "event_id": str(event_id),
            "event_type": "triage.enrichment.requested",
            "event_version": 1,
            "occurred_at": datetime(2026, 9, 13, 12, tzinfo=UTC).isoformat(),
            "aggregate_id": str(uuid4()),
            "payload": {
                "trigger_event_id": str(uuid4()),
                "source": f"source-{marker}",
                "title": f"title-{marker}",
                "description": f"description-{marker}",
                "original_severity": "high",
                "priority": "P2",
                "requires_human_review": False,
                "incident_occurred_at": datetime(
                    2026, 9, 13, 11, tzinfo=UTC
                ).isoformat(),
            },
        }
    ).encode()


def incoming(message: FakeMessage) -> IncomingMessage:
    return cast(IncomingMessage, message)


def session_factory() -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], object())


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
        service_name=ENRICHMENT_WORKER_SERVICE_NAME,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    return runtime, exporter


def build_provider(
    monkeypatch: pytest.MonkeyPatch,
    models: FakeModels,
    tracer: Tracer,
    metrics: ProviderMetrics,
    *,
    api_key: str = "PRIVATE_GEMINI_API_KEY",
) -> GeminiEnrichmentProvider:
    monkeypatch.setattr(
        gemini.genai,
        "Client",
        lambda **kwargs: FakeClient(models),
    )
    return GeminiEnrichmentProvider(
        EnrichmentSettings(gemini_api_key=api_key, gemini_model=MODEL),
        metrics=metrics,
        tracer=tracer,
    )


def one_span(spans: tuple[ReadableSpan, ...], kind: SpanKind) -> ReadableSpan:
    matching = tuple(span for span in spans if span.kind is kind)
    assert len(matching) == 1
    return matching[0]


def provider_call_count(metrics: ProviderMetrics, result: str) -> float | None:
    return metrics.registry.get_sample_value(
        "signalforge_enrichment_provider_calls_total",
        {"provider": "gemini", "model": MODEL, "result": result},
    )


def provider_duration_count(metrics: ProviderMetrics, result: str) -> float | None:
    return metrics.registry.get_sample_value(
        "signalforge_enrichment_provider_duration_seconds_count",
        {"provider": "gemini", "model": MODEL, "result": result},
    )


async def test_final_hop_lineage_and_no_content_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    consumer_tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    gemini_tracer = runtime.get_tracer("signalforge.enrichment.gemini")
    assert consumer_tracer is not None and gemini_tracer is not None
    metrics = ProviderMetrics()
    models = FakeModels(parsed=valid_result(private=True))
    provider = build_provider(monkeypatch, models, gemini_tracer, metrics)
    monkeypatch.setattr(gemini, "SYSTEM_INSTRUCTION", "PRIVATE_SYSTEM_INSTRUCTION")
    monkeypatch.setattr(consumer, "_already_processed", AsyncMock(return_value=False))
    persist = AsyncMock(return_value=EnrichmentProcessingResult.PROCESSED)
    monkeypatch.setattr(consumer, "_persist_result", persist)
    event_id = uuid4()
    message = FakeMessage(
        message_body(event_id, private=True),
        event_id=event_id,
        headers={
            "traceparent": f"00-{TRACE_ID}-{PRODUCER_ID}-01",
            "tracestate": "vendor=value",
            "baggage": "PRIVATE_BAGGAGE",
        },
    )

    try:
        result = await handle_message(
            incoming(message), provider, session_factory(), tracer=consumer_tracer
        )

        assert result is EnrichmentProcessingResult.PROCESSED
        assert message.ack_calls == 1
        assert message.nack_calls == message.reject_calls == []
        assert len(models.calls) == 1
        persist.assert_awaited_once()
        spans = exporter.get_finished_spans()
        process_span = one_span(spans, SpanKind.CONSUMER)
        gemini_span = one_span(spans, SpanKind.CLIENT)
        assert process_span.name == PROCESS_SPAN_NAME
        assert process_span.context is not None
        assert process_span.context.trace_id == int(TRACE_ID, 16)
        assert process_span.parent is not None
        assert process_span.parent.span_id == int(PRODUCER_ID, 16)
        assert process_span.attributes == {
            "messaging.system": "rabbitmq",
            "messaging.operation.name": "process",
            "messaging.operation.type": "process",
            "messaging.destination.name": (
                "signalforge.events:triage.enrichment.requested:"
                "signalforge.triage-enrichment"
            ),
            "messaging.rabbitmq.destination.routing_key": (
                "triage.enrichment.requested"
            ),
            "messaging.message.id": str(event_id),
        }
        assert gemini_span.name == f"generate_content {MODEL}"
        assert gemini_span.context is not None
        assert gemini_span.context.trace_id == process_span.context.trace_id
        assert gemini_span.parent is not None
        assert gemini_span.parent.span_id == process_span.context.span_id
        assert gemini_span.attributes == {
            "gen_ai.operation.name": "generate_content",
            "gen_ai.provider.name": "gcp.gemini",
            "gen_ai.request.model": MODEL,
            "gen_ai.output.type": "json",
        }
        assert process_span.status.status_code is StatusCode.UNSET
        assert gemini_span.status.status_code is StatusCode.UNSET
        assert provider.provider_name == "gemini"
        assert provider_call_count(metrics, "success") == 1
        assert provider_duration_count(metrics, "success") == 1

        captured = repr(
            [
                (span.name, span.attributes, span.events, span.status.description)
                for span in spans
            ]
        )
        for secret in (
            "PRIVATE_INCIDENT_CONTENT",
            "PRIVATE_SYSTEM_INSTRUCTION",
            "PRIVATE_PARSED_RESULT",
            "PRIVATE_GEMINI_API_KEY",
            "PRIVATE_BAGGAGE",
        ):
            assert secret not in captured
    finally:
        runtime.shutdown()


@pytest.mark.parametrize(
    ("case", "expected_type", "reason", "metric", "disposition"),
    [
        (
            "transient",
            TransientEnrichmentError,
            ProviderFailureReason.UNAVAILABLE,
            "transient_failure",
            "nack",
        ),
        (
            "permanent",
            PermanentEnrichmentError,
            ProviderFailureReason.REQUEST_REJECTED,
            "permanent_failure",
            "reject",
        ),
        (
            "invalid_response",
            TransientEnrichmentError,
            ProviderFailureReason.INVALID_RESPONSE,
            "invalid_response",
            "nack",
        ),
    ],
)
async def test_provider_failures_keep_status_metrics_and_disposition(
    case: str,
    expected_type: type[Exception],
    reason: ProviderFailureReason,
    metric: str,
    disposition: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = f"PRIVATE_{case.upper()}_PROVIDER_DETAIL"
    if case == "transient":
        models = FakeModels(error=httpx.ConnectError(secret))
    elif case == "permanent":
        models = FakeModels(error=errors.APIError(400, {"message": secret}))
    else:
        models = FakeModels(parsed={"summary": secret})
    runtime, exporter = tracing_runtime()
    consumer_tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    gemini_tracer = runtime.get_tracer("signalforge.enrichment.gemini")
    assert consumer_tracer is not None and gemini_tracer is not None
    metrics = ProviderMetrics()
    provider = build_provider(monkeypatch, models, gemini_tracer, metrics)
    monkeypatch.setattr(consumer, "_already_processed", AsyncMock(return_value=False))
    event_id = uuid4()
    message = FakeMessage(message_body(event_id), event_id=event_id)

    try:
        with pytest.raises(expected_type) as caught:
            await handle_message(
                incoming(message), provider, session_factory(), tracer=consumer_tracer
            )

        assert caught.value.reason is reason  # type: ignore[attr-defined]
        assert message.nack_calls == ([True] if disposition == "nack" else [])
        assert message.reject_calls == ([False] if disposition == "reject" else [])
        assert message.ack_calls == 0
        assert provider_call_count(metrics, metric) == 1
        assert provider_duration_count(metrics, metric) == 1
        spans = exporter.get_finished_spans()
        process_span = one_span(spans, SpanKind.CONSUMER)
        gemini_span = one_span(spans, SpanKind.CLIENT)
        for span in (process_span, gemini_span):
            assert span.status.status_code is StatusCode.ERROR
            assert span.status.description is None
            assert span.events == ()
            assert span.attributes is not None
            assert span.attributes["error.type"] == (
                f"signalforge.enrichment.provider.{expected_type.__name__}"
            )
        assert secret not in repr(spans)
    finally:
        runtime.shutdown()


async def test_database_failure_after_successful_gemini_keeps_client_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    consumer_tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    gemini_tracer = runtime.get_tracer("signalforge.enrichment.gemini")
    assert consumer_tracer is not None and gemini_tracer is not None
    metrics = ProviderMetrics()
    provider = build_provider(
        monkeypatch, FakeModels(parsed=valid_result()), gemini_tracer, metrics
    )
    monkeypatch.setattr(consumer, "_already_processed", AsyncMock(return_value=False))
    monkeypatch.setattr(
        consumer,
        "_persist_result",
        AsyncMock(
            side_effect=OperationalError(
                "database unavailable", {}, OSError("PRIVATE_DATABASE_DETAIL")
            )
        ),
    )
    event_id = uuid4()
    message = FakeMessage(message_body(event_id), event_id=event_id)

    try:
        with pytest.raises(OperationalError):
            await handle_message(
                incoming(message), provider, session_factory(), tracer=consumer_tracer
            )

        assert message.nack_calls == [True]
        assert message.ack_calls == 0
        spans = exporter.get_finished_spans()
        assert one_span(spans, SpanKind.CLIENT).status.status_code is StatusCode.UNSET
        assert one_span(spans, SpanKind.CONSUMER).status.status_code is StatusCode.ERROR
        assert provider_call_count(metrics, "success") == 1
        assert provider_duration_count(metrics, "success") == 1
    finally:
        runtime.shutdown()


async def test_duplicate_delivery_has_process_span_without_gemini_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    consumer_tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    gemini_tracer = runtime.get_tracer("signalforge.enrichment.gemini")
    assert consumer_tracer is not None and gemini_tracer is not None
    models = FakeModels(parsed=valid_result())
    metrics = ProviderMetrics()
    provider = build_provider(monkeypatch, models, gemini_tracer, metrics)
    monkeypatch.setattr(consumer, "_already_processed", AsyncMock(return_value=True))
    event_id = uuid4()
    message = FakeMessage(message_body(event_id), event_id=event_id)

    try:
        result = await handle_message(
            incoming(message), provider, session_factory(), tracer=consumer_tracer
        )

        assert result is EnrichmentProcessingResult.DUPLICATE
        assert message.ack_calls == 1
        assert models.calls == []
        spans = exporter.get_finished_spans()
        one_span(spans, SpanKind.CONSUMER)
        assert tuple(span for span in spans if span.kind is SpanKind.CLIENT) == ()
        assert provider_call_count(metrics, "success") is None
    finally:
        runtime.shutdown()


async def test_redelivery_creates_another_process_span_but_skips_second_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    consumer_tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    gemini_tracer = runtime.get_tracer("signalforge.enrichment.gemini")
    assert consumer_tracer is not None and gemini_tracer is not None
    models = FakeModels(parsed=valid_result())
    provider = build_provider(monkeypatch, models, gemini_tracer, ProviderMetrics())
    monkeypatch.setattr(
        consumer, "_already_processed", AsyncMock(side_effect=[False, True])
    )
    monkeypatch.setattr(
        consumer,
        "_persist_result",
        AsyncMock(return_value=EnrichmentProcessingResult.PROCESSED),
    )
    event_id = uuid4()
    messages = [
        FakeMessage(message_body(event_id), event_id=event_id) for _ in range(2)
    ]

    try:
        first = await handle_message(
            incoming(messages[0]), provider, session_factory(), tracer=consumer_tracer
        )
        second = await handle_message(
            incoming(messages[1]), provider, session_factory(), tracer=consumer_tracer
        )

        assert first is EnrichmentProcessingResult.PROCESSED
        assert second is EnrichmentProcessingResult.DUPLICATE
        assert [message.ack_calls for message in messages] == [1, 1]
        assert len(models.calls) == 1
        spans = exporter.get_finished_spans()
        process_spans = tuple(span for span in spans if span.kind is SpanKind.CONSUMER)
        assert len(process_spans) == 2
        assert process_spans[0].context is not None
        assert process_spans[1].context is not None
        assert process_spans[0].context.span_id != process_spans[1].context.span_id
        assert len(tuple(span for span in spans if span.kind is SpanKind.CLIENT)) == 1
    finally:
        runtime.shutdown()


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
async def test_missing_or_invalid_context_is_observational(
    headers: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    assert tracer is not None
    monkeypatch.setattr(consumer, "_already_processed", AsyncMock(return_value=True))
    event_id = uuid4()
    message = FakeMessage(message_body(event_id), event_id=event_id, headers=headers)
    provider = cast(GeminiEnrichmentProvider, SimpleNamespace())

    try:
        result = await handle_message(
            incoming(message), provider, session_factory(), tracer=tracer
        )

        assert result is EnrichmentProcessingResult.DUPLICATE
        assert message.ack_calls == 1
        assert message.reject_calls == message.nack_calls == []
        span = one_span(exporter.get_finished_spans(), SpanKind.CONSUMER)
        assert span.parent is None
    finally:
        runtime.shutdown()


async def test_invalid_event_still_rejects_under_process_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime()
    tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    assert tracer is not None
    event_id = uuid4()
    message = FakeMessage(b"{", event_id=event_id)

    try:
        with pytest.raises(InvalidEnrichmentEventError):
            await handle_message(
                incoming(message),
                cast(GeminiEnrichmentProvider, SimpleNamespace()),
                session_factory(),
                tracer=tracer,
            )

        assert message.reject_calls == [False]
        assert message.nack_calls == []
        span = one_span(exporter.get_finished_spans(), SpanKind.CONSUMER)
        assert span.status.status_code is StatusCode.ERROR
        assert (
            tuple(
                item
                for item in exporter.get_finished_spans()
                if item.kind is SpanKind.CLIENT
            )
            == ()
        )
    finally:
        runtime.shutdown()


async def test_unsampled_parent_keeps_process_and_gemini_unsampled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime(sample_ratio=1.0)
    consumer_tracer = runtime.get_tracer("signalforge.enrichment.consumer")
    gemini_tracer = runtime.get_tracer("signalforge.enrichment.gemini")
    assert consumer_tracer is not None and gemini_tracer is not None
    models = FakeModels(parsed=valid_result())
    provider = build_provider(monkeypatch, models, gemini_tracer, ProviderMetrics())
    monkeypatch.setattr(consumer, "_already_processed", AsyncMock(return_value=False))
    monkeypatch.setattr(
        consumer,
        "_persist_result",
        AsyncMock(return_value=EnrichmentProcessingResult.PROCESSED),
    )
    event_id = uuid4()
    message = FakeMessage(
        message_body(event_id),
        event_id=event_id,
        headers={"traceparent": f"00-{TRACE_ID}-{PRODUCER_ID}-00"},
    )

    try:
        result = await handle_message(
            incoming(message), provider, session_factory(), tracer=consumer_tracer
        )

        assert result is EnrichmentProcessingResult.PROCESSED
        assert message.ack_calls == 1
        assert len(models.calls) == 1
        assert exporter.get_finished_spans() == ()
    finally:
        runtime.shutdown()
