from __future__ import annotations

import socket
from urllib.request import urlopen

import pytest
from prometheus_client import CollectorRegistry, Counter

from signalforge.observability.exposition import MetricsHttpServer


def test_explicit_registry_is_exposed_and_server_releases_ephemeral_port() -> None:
    registry = CollectorRegistry()
    counter = Counter(
        "signalforge_test_exposition_total",
        "Test-only exposition counter.",
        registry=registry,
    )
    counter.inc(3)
    other_registry = CollectorRegistry()
    Counter(
        "signalforge_other_registry_total",
        "Metric that must remain isolated.",
        registry=other_registry,
    ).inc()

    metrics_server = MetricsHttpServer.start(registry, host="127.0.0.1", port=0)
    host, port = metrics_server.server.server_address[:2]
    try:
        with urlopen(f"http://{host}:{port}/metrics", timeout=2) as response:
            body = response.read().decode()
            assert response.status == 200
            assert response.headers["Content-Type"] == (
                "text/plain; version=0.0.4; charset=utf-8"
            )

        assert "signalforge_test_exposition_total 3.0" in body
        assert "signalforge_other_registry_total" not in body
        assert "python_gc_" not in body
        assert "python_info" not in body
        assert "process_" not in body
    finally:
        metrics_server.stop()

    assert metrics_server.server.socket.fileno() == -1
    assert not metrics_server.thread.is_alive()
    with pytest.raises(OSError):
        socket.create_connection((host, port), timeout=0.2)
