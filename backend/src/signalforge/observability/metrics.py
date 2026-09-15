"""Application-owned, process-local Prometheus metrics."""

from enum import StrEnum
from typing import Final

from prometheus_client import CollectorRegistry, Counter, Histogram

HTTP_DURATION_BUCKETS: Final = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
REMEDIATION_EXECUTOR_DURATION_BUCKETS: Final = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    15.0,
    20.0,
    30.0,
)
UNMATCHED_ROUTE: Final = "__unmatched__"


class HttpMetrics:
    """Own an isolated registry and the bounded HTTP metric label sets."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "signalforge_http_requests_total",
            "Total SignalForge HTTP requests.",
            ("method", "route", "status_code"),
            registry=self.registry,
        )
        self.duration = Histogram(
            "signalforge_http_request_duration_seconds",
            "SignalForge HTTP request duration in seconds.",
            ("method", "route"),
            buckets=HTTP_DURATION_BUCKETS,
            registry=self.registry,
        )

    def observe(
        self, *, method: str, route: str, status_code: int, duration: float
    ) -> None:
        """Record one completed request using only bounded labels."""
        normalized_method = method.upper()
        self.requests.labels(
            method=normalized_method,
            route=route,
            status_code=str(status_code),
        ).inc()
        self.duration.labels(method=normalized_method, route=route).observe(duration)


class OutboxDispatchMetricResult(StrEnum):
    PUBLISHED = "published"
    RETRY_SCHEDULED = "retry_scheduled"
    INVALID = "invalid"
    OWNERSHIP_LOST = "ownership_lost"
    AMBIGUOUS = "ambiguous"
    DATABASE_FAILED = "database_failed"
    RELEASED = "released"


class MessageMetricResult(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    REQUEUED = "requeued"


class RemediationMessageMetricResult(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    REQUEUED = "requeued"
    IN_PROGRESS = "in_progress"


class RemediationExecutorMetricResult(StrEnum):
    SUCCESS = "success"
    TARGET_NOT_ALLOWED = "target_not_allowed"
    UNSUPPORTED_ACTION = "unsupported_action"
    HTTP_REJECTED = "http_rejected"
    HTTP_SERVER_ERROR = "http_server_error"
    TIMEOUT = "timeout"
    TRANSPORT = "transport"
    INTERRUPTED = "interrupted"
    UNEXPECTED = "unexpected"


class ProviderMetricResult(StrEnum):
    SUCCESS = "success"
    TRANSIENT_FAILURE = "transient_failure"
    PERMANENT_FAILURE = "permanent_failure"
    INVALID_RESPONSE = "invalid_response"


_ALLOWED_EVENT_TYPES: Final = frozenset(
    {
        "incident.created",
        "triage.enrichment.requested",
        "remediation.execution.requested",
    }
)


def _safe_increment(counter: Counter, **labels: str) -> None:
    """Keep metrics strictly observational if the client ever rejects an update."""
    try:
        counter.labels(**labels).inc()
    except Exception:
        return


def _safe_observe(histogram: Histogram, duration: float, **labels: str) -> None:
    """Keep observation failures from changing provider outcomes."""
    try:
        histogram.labels(**labels).observe(duration)
    except Exception:
        return


class OutboxMetrics:
    """Process-local outbox dispatch outcomes with bounded labels."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.dispatch = Counter(
            "signalforge_outbox_dispatch_total",
            "Final outbox dispatch outcomes.",
            ("event_type", "result"),
            registry=self.registry,
        )

    def record(self, *, event_type: str, result: OutboxDispatchMetricResult) -> None:
        normalized_type = (
            event_type if event_type in _ALLOWED_EVENT_TYPES else "unknown"
        )
        _safe_increment(self.dispatch, event_type=normalized_type, result=result.value)


class IncidentConsumerMetrics:
    """Process-local Incident consumer delivery outcomes."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.messages = Counter(
            "signalforge_incident_consumer_messages_total",
            "Final Incident consumer message outcomes.",
            ("result",),
            registry=self.registry,
        )

    def record(self, result: MessageMetricResult) -> None:
        _safe_increment(self.messages, result=result.value)


class EnrichmentMetrics:
    """Process-local enrichment consumer delivery outcomes."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.messages = Counter(
            "signalforge_enrichment_messages_total",
            "Final enrichment consumer message outcomes.",
            ("result",),
            registry=self.registry,
        )

    def record(self, result: MessageMetricResult) -> None:
        _safe_increment(self.messages, result=result.value)


class RemediationWorkerMetrics:
    """Process-local remediation consumer delivery outcomes."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.messages = Counter(
            "signalforge_remediation_messages_total",
            "Final remediation consumer message outcomes.",
            ("result",),
            registry=self.registry,
        )

    def record(self, result: RemediationMessageMetricResult) -> None:
        _safe_increment(self.messages, result=result.value)


class RemediationExecutorMetrics:
    """Production remediation executor call outcomes and durations."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.calls = Counter(
            "signalforge_remediation_executor_calls_total",
            "Remediation executor call outcomes.",
            ("result",),
            registry=self.registry,
        )
        self.duration = Histogram(
            "signalforge_remediation_executor_duration_seconds",
            "Remediation executor call duration in seconds.",
            ("result",),
            buckets=REMEDIATION_EXECUTOR_DURATION_BUCKETS,
            registry=self.registry,
        )

    def record(
        self, *, result: RemediationExecutorMetricResult, duration: float
    ) -> None:
        labels = {"result": result.value}
        _safe_increment(self.calls, **labels)
        _safe_observe(self.duration, duration, **labels)


class ProviderMetrics:
    """Gemini call outcomes in the enrichment worker's registry."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        labels = ("provider", "model", "result")
        self.calls = Counter(
            "signalforge_enrichment_provider_calls_total",
            "Enrichment provider call outcomes.",
            labels,
            registry=self.registry,
        )
        self.duration = Histogram(
            "signalforge_enrichment_provider_duration_seconds",
            "Enrichment provider call duration in seconds.",
            labels,
            registry=self.registry,
        )

    def record(
        self,
        *,
        provider: str,
        model: str,
        result: ProviderMetricResult,
        duration: float,
    ) -> None:
        labels = {"provider": provider, "model": model, "result": result.value}
        _safe_increment(self.calls, **labels)
        _safe_observe(self.duration, duration, **labels)
