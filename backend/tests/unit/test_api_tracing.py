from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from signalforge.core.config import TracingSettings
from signalforge.main import create_app
from signalforge.observability.tracing import (
    API_SERVICE_NAME,
    DISPATCHER_SERVICE_NAME,
    INCIDENT_CONSUMER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def tracing_runtime() -> Iterator[tuple[TracingRuntime, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    runtime = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
        ),
        service_name=API_SERVICE_NAME,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    yield runtime, exporter
    runtime.shutdown()


async def request(app: FastAPI, method: str, path: str, **kwargs: object) -> Response:
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, **kwargs)


def server_spans(exporter: InMemorySpanExporter) -> tuple[ReadableSpan, ...]:
    return tuple(
        span for span in exporter.get_finished_spans() if span.kind is SpanKind.SERVER
    )


async def test_isolated_providers_and_minimal_resource(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    first, first_exporter = tracing_runtime
    second_exporter = InMemorySpanExporter()
    second = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
        ),
        service_name=API_SERVICE_NAME,
        exporter=second_exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    try:
        assert first.provider is not second.provider
        assert first.provider is not None
        assert dict(first.provider.resource.attributes) == {
            "service.name": "signalforge-api"
        }
        await request(create_app(tracing=first), "GET", "/api/v1/incidents")
        assert len(server_spans(first_exporter)) == 1
        assert server_spans(second_exporter) == ()
        await request(create_app(tracing=second), "GET", "/api/v1/incidents")
        assert len(server_spans(first_exporter)) == 1
        assert len(server_spans(second_exporter)) == 1
    finally:
        second.shutdown()


async def test_successful_request_exports_server_span(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    app = create_app(tracing=runtime)

    @app.get("/test-success")
    async def success() -> dict[str, str]:
        return {"status": "ok"}

    response = await request(app, "GET", "/test-success")
    assert response.status_code == 200
    (span,) = server_spans(exporter)
    assert span.name == "GET /test-success"
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    assert span.attributes.get("http.status_code") == 200


async def test_one_server_span_with_no_asgi_noise(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    response = await request(create_app(tracing=runtime), "GET", "/api/v1/incidents")
    spans = exporter.get_finished_spans()
    assert response.status_code == 401
    assert len(spans) == 1
    (span,) = spans
    assert span.kind is SpanKind.SERVER
    assert span.name == "GET /api/v1/incidents"
    assert span.attributes is not None
    assert span.attributes["http.route"] == "/api/v1/incidents"
    assert span.end_time is not None
    assert span.status.status_code is StatusCode.UNSET
    assert span.parent is None


async def test_incident_ids_use_route_template(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    app = create_app(tracing=runtime)
    ids = [str(uuid4()), str(uuid4())]
    for incident_id in ids:
        response = await request(app, "GET", f"/api/v1/incidents/{incident_id}")
        assert response.status_code == 401
    spans = server_spans(exporter)
    assert len(spans) == 2
    assert {span.name for span in spans} == {"GET /api/v1/incidents/{incident_id}"}
    assert all(
        span.attributes is not None
        and span.attributes["http.route"] == "/api/v1/incidents/{incident_id}"
        for span in spans
    )
    assert all(incident_id not in span.name for incident_id in ids for span in spans)


async def test_valid_w3c_traceparent_continues_remote_parent(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    trace_id = "0af7651916cd43dd8448eb211c80319c"
    parent_id = "b7ad6b7169203331"
    response = await request(
        create_app(tracing=runtime),
        "GET",
        "/api/v1/incidents",
        headers={"traceparent": f"00-{trace_id}-{parent_id}-01"},
    )
    assert response.status_code == 401
    (span,) = server_spans(exporter)
    assert span.context is not None
    assert span.context.trace_id == int(trace_id, 16)
    assert span.parent is not None
    assert span.parent.span_id == int(parent_id, 16)
    assert span.parent.is_remote is True


async def test_invalid_traceparent_is_ignored(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    response = await request(
        create_app(tracing=runtime),
        "GET",
        "/api/v1/incidents",
        headers={"traceparent": "invalid-sentinel"},
    )
    assert response.status_code == 401
    (span,) = server_spans(exporter)
    assert span.parent is None
    assert span.context is not None and span.context.trace_id != 0


async def test_operational_routes_are_excluded(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    app = create_app(tracing=runtime)
    for path in ("/metrics", "/health/live"):
        assert (await request(app, "GET", path)).status_code == 200
    assert (await request(app, "GET", "/health/ready")).status_code in {200, 503}
    assert server_spans(exporter) == ()


async def test_authorization_and_body_are_not_captured(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    response = await request(
        create_app(tracing=runtime),
        "POST",
        "/api/v1/incidents",
        headers={"Authorization": "Bearer fake-jwt-secret"},
        json={"source": "private-body-sentinel", "title": "private-body-sentinel"},
    )
    assert response.status_code == 401
    (span,) = server_spans(exporter)
    captured = repr(span.attributes) + repr(span.events)
    assert "fake-jwt-secret" not in captured
    assert "private-body-sentinel" not in captured
    assert "authorization" not in captured.lower()


async def test_404_and_500_keep_response_and_status_semantics(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
) -> None:
    runtime, exporter = tracing_runtime
    app = create_app(tracing=runtime)

    @app.get("/test-unexpected-failure")
    async def fail() -> None:
        raise RuntimeError("private-exception-value")

    missing = await request(app, "GET", "/missing")
    failure = await request(app, "GET", "/test-unexpected-failure")
    assert missing.status_code == 404
    assert failure.status_code == 500
    assert failure.text == "Internal Server Error"
    assert UUID(failure.headers["X-Request-ID"])
    missing_span, failure_span = server_spans(exporter)
    assert missing_span.status.status_code is StatusCode.UNSET
    assert failure_span.end_time is not None
    assert failure_span.status.status_code is StatusCode.ERROR
    assert failure_span.attributes is not None
    assert failure_span.attributes.get("http.status_code") == 500


async def test_disabled_mode_has_no_exporter_or_instrumentation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import signalforge.observability.tracing as tracing_module

    constructor = Mock(side_effect=AssertionError("exporter created"))
    monkeypatch.setattr(tracing_module, "OTLPSpanExporter", constructor)
    runtime = create_tracing_runtime(
        TracingSettings(tracing_enabled=False), service_name=API_SERVICE_NAME
    )
    app = create_app(tracing=runtime)
    response = await request(app, "GET", "/api/v1/incidents")
    assert response.status_code == 401
    assert response.headers["X-Request-ID"]
    assert runtime.enabled is False
    assert not getattr(app, "_is_instrumented_by_opentelemetry", False)
    constructor.assert_not_called()


async def test_lifespan_shuts_provider_and_exporter_once(
    tracing_runtime: tuple[TracingRuntime, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, exporter = tracing_runtime
    assert runtime.provider is not None
    provider_shutdown = Mock(wraps=runtime.provider.shutdown)
    exporter_shutdown = Mock(wraps=exporter.shutdown)
    monkeypatch.setattr(runtime.provider, "shutdown", provider_shutdown)
    monkeypatch.setattr(exporter, "shutdown", exporter_shutdown)
    app = create_app(tracing=runtime)
    async with app.router.lifespan_context(app):
        assert (await request(app, "GET", "/api/v1/incidents")).status_code == 401
    runtime.shutdown()
    provider_shutdown.assert_called_once_with()
    exporter_shutdown.assert_called_once_with()
    assert len(server_spans(exporter)) == 1


async def test_parent_based_ratio_sampler_respects_upstream_decision() -> None:
    exporter = InMemorySpanExporter()
    runtime = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
            tracing_sample_ratio=0.0,
        ),
        service_name=API_SERVICE_NAME,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    try:
        app = create_app(tracing=runtime)
        assert (await request(app, "GET", "/api/v1/incidents")).status_code == 401
        assert server_spans(exporter) == ()
        sampled_parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        response = await request(
            app,
            "GET",
            "/api/v1/incidents",
            headers={"traceparent": sampled_parent},
        )
        assert response.status_code == 401
        assert len(server_spans(exporter)) == 1
    finally:
        runtime.shutdown()


async def test_batch_processor_worker_stops_on_shutdown() -> None:
    import threading

    exporter = InMemorySpanExporter()
    runtime = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
        ),
        service_name=API_SERVICE_NAME,
        exporter=exporter,
    )
    worker_names = {thread.name for thread in threading.enumerate()}
    assert "OtelBatchSpanRecordProcessor" in worker_names

    runtime.shutdown()

    assert "OtelBatchSpanRecordProcessor" not in {
        thread.name for thread in threading.enumerate()
    }


@pytest.mark.parametrize(
    "service_name",
    [
        API_SERVICE_NAME,
        DISPATCHER_SERVICE_NAME,
        INCIDENT_CONSUMER_SERVICE_NAME,
    ],
)
async def test_runtime_uses_explicit_code_owned_service_identity(
    service_name: str,
) -> None:
    exporter = InMemorySpanExporter()
    runtime = create_tracing_runtime(
        TracingSettings(
            tracing_enabled=True,
            otlp_traces_endpoint="http://localhost:4318/v1/traces",
        ),
        service_name=service_name,
        exporter=exporter,
        span_processor_factory=SimpleSpanProcessor,
    )
    try:
        assert runtime.provider is not None
        assert runtime.provider.resource.attributes["service.name"] == service_name
    finally:
        runtime.shutdown()


async def test_runtime_rejects_unknown_service_identity() -> None:
    with pytest.raises(ValueError, match="code-owned"):
        create_tracing_runtime(
            TracingSettings(tracing_enabled=False),
            service_name="user-controlled",  # type: ignore[arg-type]
        )
