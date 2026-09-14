"""Application-owned Prometheus HTTP metrics."""

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
