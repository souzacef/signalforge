from __future__ import annotations

import logging
import socket
from collections.abc import Callable
from typing import Any

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from signalforge.consumers import runtime as consumer_runtime
from signalforge.enrichment import runtime as enrichment_runtime
from signalforge.outbox import dispatcher

pytestmark = pytest.mark.anyio

DATABASE_URL = "postgresql+asyncpg://user:password@localhost/signalforge_test"
RABBITMQ_URL = "amqp://user:password@localhost/signalforge_test_runtime"


class FakeMetricsServer:
    def __init__(self, *, stop_error: bool = False) -> None:
        self.stop_calls = 0
        self.stop_error = stop_error

    def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_error:
            raise OSError("private cleanup detail")


def disable_signals(monkeypatch: pytest.MonkeyPatch, module: Any) -> None:
    monkeypatch.setattr(module, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(module, "remove_signal_handlers", lambda *args: None)


async def test_dispatcher_exposes_instrumentation_registry_and_stops_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    runtime_settings = dispatcher.DispatcherSettings(
        _env_file=None,
        database_url=DATABASE_URL,
        rabbitmq_url=RABBITMQ_URL,
    )
    server = FakeMetricsServer()
    exposed: list[CollectorRegistry] = []
    used: list[CollectorRegistry] = []

    monkeypatch.setattr(dispatcher, "DispatcherSettings", lambda: runtime_settings)
    monkeypatch.setattr(
        dispatcher.MetricsHttpServer,
        "start",
        lambda registry, **kwargs: exposed.append(registry) or server,
    )

    async def run(*args: object, **kwargs: Any) -> None:
        used.append(kwargs["metrics"].registry)

    monkeypatch.setattr(dispatcher, "run_dispatcher", run)
    disable_signals(monkeypatch, dispatcher)

    with caplog.at_level(logging.INFO, logger=dispatcher.__name__):
        await dispatcher.async_main()

    assert exposed == used
    assert b"signalforge_outbox_dispatch_total" in generate_latest(exposed[0])
    assert server.stop_calls == 1
    started = next(
        record for record in caplog.records if record.event == "metrics_server_started"
    )
    assert started.metrics_host == "127.0.0.1"
    assert started.metrics_port == 9101
    assert any(record.event == "metrics_server_stopped" for record in caplog.records)


async def test_consumer_exposes_instrumentation_registry_and_stops_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = consumer_runtime.ConsumerSettings(
        _env_file=None,
        database_url=DATABASE_URL,
        rabbitmq_url=RABBITMQ_URL,
    )
    server = FakeMetricsServer()
    exposed: list[CollectorRegistry] = []
    used: list[CollectorRegistry] = []

    monkeypatch.setattr(consumer_runtime, "ConsumerSettings", lambda: runtime_settings)
    monkeypatch.setattr(
        consumer_runtime.MetricsHttpServer,
        "start",
        lambda registry, **kwargs: exposed.append(registry) or server,
    )

    async def run(*args: object, **kwargs: Any) -> None:
        used.append(kwargs["metrics"].registry)

    monkeypatch.setattr(consumer_runtime, "run_consumer", run)
    disable_signals(monkeypatch, consumer_runtime)

    await consumer_runtime.async_main()

    assert exposed == used
    assert b"signalforge_incident_consumer_messages_total" in generate_latest(
        exposed[0]
    )
    assert server.stop_calls == 1


async def test_enrichment_exposes_shared_worker_and_provider_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = enrichment_runtime.EnrichmentWorkerSettings(
        _env_file=None,
        database_url=DATABASE_URL,
        rabbitmq_url=RABBITMQ_URL,
    )
    server = FakeMetricsServer()
    exposed: list[CollectorRegistry] = []
    worker_registries: list[CollectorRegistry] = []
    provider_registries: list[CollectorRegistry] = []

    monkeypatch.setattr(
        enrichment_runtime, "EnrichmentWorkerSettings", lambda: runtime_settings
    )
    monkeypatch.setattr(enrichment_runtime, "EnrichmentSettings", lambda: object())
    monkeypatch.setattr(
        enrichment_runtime.MetricsHttpServer,
        "start",
        lambda registry, **kwargs: exposed.append(registry) or server,
    )

    def provider(settings: object, *, metrics: Any) -> object:
        provider_registries.append(metrics.registry)
        return object()

    async def run(*args: object, **kwargs: Any) -> None:
        worker_registries.append(kwargs["metrics"].registry)

    monkeypatch.setattr(enrichment_runtime, "GeminiEnrichmentProvider", provider)
    monkeypatch.setattr(enrichment_runtime, "run_worker", run)
    disable_signals(monkeypatch, enrichment_runtime)

    await enrichment_runtime.async_main()

    assert exposed == worker_registries == provider_registries
    body = generate_latest(exposed[0])
    assert b"signalforge_enrichment_messages_total" in body
    assert b"signalforge_enrichment_provider_calls_total" in body
    assert b"signalforge_enrichment_provider_duration_seconds" in body
    assert server.stop_calls == 1


async def test_bind_failure_is_sanitized_and_dispatcher_still_runs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    occupied = socket.socket()
    occupied.bind(("127.0.0.1", 0))
    occupied.listen()
    port = occupied.getsockname()[1]
    runtime_settings = dispatcher.DispatcherSettings(
        _env_file=None,
        database_url=DATABASE_URL,
        rabbitmq_url=RABBITMQ_URL,
        metrics_port=port,
    )
    business_calls = 0

    async def run(*args: object, **kwargs: object) -> None:
        nonlocal business_calls
        business_calls += 1

    monkeypatch.setattr(dispatcher, "DispatcherSettings", lambda: runtime_settings)
    monkeypatch.setattr(dispatcher, "run_dispatcher", run)
    disable_signals(monkeypatch, dispatcher)
    try:
        with caplog.at_level(logging.WARNING, logger=dispatcher.__name__):
            await dispatcher.async_main()
    finally:
        occupied.close()

    failures = [
        record
        for record in caplog.records
        if record.event == "metrics_server_start_failed"
    ]
    assert business_calls == 1
    assert len(failures) == 1
    assert failures[0].getMessage() == "metrics server could not start"
    assert str(port) not in failures[0].getMessage()


async def test_runtime_error_is_not_masked_and_metrics_server_stops_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    runtime_settings = dispatcher.DispatcherSettings(
        _env_file=None,
        database_url=DATABASE_URL,
        rabbitmq_url=RABBITMQ_URL,
    )
    server = FakeMetricsServer(stop_error=True)

    async def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("business failure")

    monkeypatch.setattr(dispatcher, "DispatcherSettings", lambda: runtime_settings)
    monkeypatch.setattr(
        dispatcher.MetricsHttpServer, "start", lambda *args, **kwargs: server
    )
    monkeypatch.setattr(dispatcher, "run_dispatcher", fail)
    disable_signals(monkeypatch, dispatcher)

    with caplog.at_level(logging.WARNING, logger=dispatcher.__name__):
        with pytest.raises(RuntimeError, match="business failure"):
            await dispatcher.async_main()

    assert server.stop_calls == 1
    cleanup = next(
        record
        for record in caplog.records
        if record.event == "metrics_server_stop_failed"
    )
    assert cleanup.getMessage() == "metrics server cleanup failed"
    assert "private cleanup detail" not in cleanup.getMessage()


@pytest.mark.parametrize(
    ("factory", "default_port"),
    [
        (
            lambda **values: dispatcher.DispatcherSettings(
                _env_file=None,
                database_url=DATABASE_URL,
                rabbitmq_url=RABBITMQ_URL,
                **values,
            ),
            9101,
        ),
        (
            lambda **values: consumer_runtime.ConsumerSettings(
                _env_file=None,
                database_url=DATABASE_URL,
                rabbitmq_url=RABBITMQ_URL,
                **values,
            ),
            9102,
        ),
        (
            lambda **values: enrichment_runtime.EnrichmentWorkerSettings(
                _env_file=None,
                database_url=DATABASE_URL,
                rabbitmq_url=RABBITMQ_URL,
                **values,
            ),
            9103,
        ),
    ],
)
def test_metrics_bind_settings_validate(
    factory: Callable[..., object], default_port: int
) -> None:
    settings = factory()
    assert settings.metrics_host == "127.0.0.1"
    assert settings.metrics_port == default_port

    for values in (
        {"metrics_host": "   "},
        {"metrics_port": 0},
        {"metrics_port": 65536},
    ):
        with pytest.raises(ValueError):
            factory(**values)
