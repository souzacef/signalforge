"""Explicit OpenTelemetry tracing runtimes for SignalForge processes."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Final, Literal

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME
from opentelemetry.trace import Tracer

from signalforge.core.config import TracingSettings

API_SERVICE_NAME: Final = "signalforge-api"
DISPATCHER_SERVICE_NAME: Final = "signalforge-dispatcher"
INCIDENT_CONSUMER_SERVICE_NAME: Final = "signalforge-incident-consumer"
type ServiceName = Literal[
    "signalforge-api",
    "signalforge-dispatcher",
    "signalforge-incident-consumer",
]
_SERVICE_NAMES: Final = frozenset(
    {
        API_SERVICE_NAME,
        DISPATCHER_SERVICE_NAME,
        INCIDENT_CONSUMER_SERVICE_NAME,
    }
)
SpanProcessorFactory = Callable[[SpanExporter], SpanProcessor]


class TracingRuntime:
    """Own one isolated tracer provider and its shutdown lifecycle."""

    def __init__(
        self,
        *,
        provider: TracerProvider | None = None,
        processor: SpanProcessor | None = None,
        exporter: SpanExporter | None = None,
    ) -> None:
        self.provider = provider
        self.processor = processor
        self.exporter = exporter
        self._shutdown = False
        self._shutdown_lock = Lock()

    @property
    def enabled(self) -> bool:
        return self.provider is not None

    def shutdown(self) -> None:
        """Flush and stop the provider exactly once through the SDK shutdown path."""
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True
        if self.provider is not None:
            self.provider.shutdown()

    def get_tracer(self, instrumentation_name: str) -> Tracer | None:
        """Return a tracer from this isolated provider, or None when disabled."""
        if self.provider is None:
            return None
        return self.provider.get_tracer(instrumentation_name)


def create_tracing_runtime(
    settings: TracingSettings,
    *,
    service_name: ServiceName,
    exporter: SpanExporter | None = None,
    span_processor_factory: SpanProcessorFactory = BatchSpanProcessor,
) -> TracingRuntime:
    """Build an isolated runtime, optionally with an injected test exporter."""
    if service_name not in _SERVICE_NAMES:
        raise ValueError("service_name must be a code-owned SignalForge identity")
    if not settings.tracing_enabled:
        return TracingRuntime()

    endpoint = settings.otlp_traces_endpoint
    if endpoint is None:  # Defensive guard for non-Pydantic construction.
        raise ValueError("OTLP traces endpoint is required when tracing is enabled")

    span_exporter = exporter or OTLPSpanExporter(endpoint=str(endpoint))
    processor = span_processor_factory(span_exporter)
    provider = TracerProvider(
        sampler=ParentBased(TraceIdRatioBased(settings.tracing_sample_ratio)),
        resource=Resource({SERVICE_NAME: service_name}),
        shutdown_on_exit=False,
    )
    provider.add_span_processor(processor)
    return TracingRuntime(
        provider=provider,
        processor=processor,
        exporter=span_exporter,
    )
