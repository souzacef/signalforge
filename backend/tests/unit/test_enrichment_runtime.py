from __future__ import annotations

import asyncio
import logging
import signal
from collections import deque
from typing import Any, cast

import pytest
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractIncomingMessage, AbstractQueueIterator
from aio_pika.exceptions import ChannelInvalidStateError
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from signalforge.db.errors import DatabaseTransportError
from signalforge.enrichment import runtime
from signalforge.enrichment.consumer import (
    EnrichmentProcessingResult,
    InvalidEnrichmentEventError,
)
from signalforge.enrichment.domain import EnrichmentInput, EnrichmentResult
from signalforge.enrichment.provider import (
    EnrichmentProvider,
    PermanentEnrichmentError,
    ProviderFailureReason,
    TransientEnrichmentError,
)
from signalforge.enrichment.runtime import (
    EnrichmentBroker,
    EnrichmentConnectionError,
    EnrichmentWorkerSettings,
    run_worker,
)
from signalforge.observability.metrics import EnrichmentMetrics

pytestmark = pytest.mark.anyio

BASE_SETTINGS: dict[str, object] = {
    "database_url": "postgresql+asyncpg://user:password@localhost/signalforge_test",
    "rabbitmq_url": "amqp://user:password@localhost/signalforge_test_runtime",
    "enrichment_worker_prefetch_count": 1,
    "enrichment_worker_reconnect_base_seconds": 1,
    "enrichment_worker_reconnect_max_seconds": 4,
    "enrichment_worker_processing_retry_base_seconds": 1,
    "enrichment_worker_processing_retry_max_seconds": 4,
    "enrichment_worker_shutdown_drain_timeout_seconds": 0.05,
}


def settings(**changes: object) -> EnrichmentWorkerSettings:
    return EnrichmentWorkerSettings.model_validate({**BASE_SETTINGS, **changes})


class FakeEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1


class FakeMessage:
    def __init__(self, body: bytes = b"{}") -> None:
        self.body = body
        self.ack_calls = 0
        self.reject_calls: list[bool] = []
        self.nack_calls: list[bool] = []

    async def ack(self) -> None:
        self.ack_calls += 1

    async def reject(self, *, requeue: bool) -> None:
        self.reject_calls.append(requeue)

    async def nack(self, *, requeue: bool) -> None:
        self.nack_calls.append(requeue)


class FakeIterator:
    def __init__(self, items: list[object] | None = None) -> None:
        self.items = deque(items or [])
        self.waiting = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> FakeIterator:
        self.enter_calls += 1
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        self.exit_calls += 1

    def __aiter__(self) -> FakeIterator:
        return self

    async def __anext__(self) -> AbstractIncomingMessage:
        if self.items:
            item = self.items.popleft()
            if isinstance(item, BaseException):
                raise item
            return cast(AbstractIncomingMessage, item)
        self.waiting.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        raise AssertionError("unreachable")


class FakeQueue:
    def __init__(self, iterator: FakeIterator) -> None:
        self.fake_iterator = iterator

    def iterator(self) -> AbstractQueueIterator:
        return cast(AbstractQueueIterator, self.fake_iterator)


class FakeBroker:
    def __init__(self, iterator: FakeIterator | None = None) -> None:
        self.queue = FakeQueue(iterator or FakeIterator())
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class FakeProvider:
    provider_name = "fake"
    model_name = "fake-model"

    async def enrich(self, input: EnrichmentInput) -> EnrichmentResult:
        raise AssertionError("runtime tests replace handle_message")


def install_resources(monkeypatch: pytest.MonkeyPatch) -> tuple[FakeEngine, object]:
    engine = FakeEngine()
    session_factory = object()
    monkeypatch.setattr(runtime, "_create_database_engine", lambda value: engine)
    monkeypatch.setattr(
        runtime,
        "async_sessionmaker",
        lambda *args, **kwargs: session_factory,
    )
    return engine, session_factory


def test_worker_settings_have_safe_defaults_without_gemini_or_api_settings() -> None:
    configured = EnrichmentWorkerSettings.model_validate(
        {
            "database_url": BASE_SETTINGS["database_url"],
            "rabbitmq_url": BASE_SETTINGS["rabbitmq_url"],
        }
    )

    assert not hasattr(configured, "gemini_api_key")
    assert not hasattr(configured, "jwt_secret")
    assert configured.enrichment_worker_prefetch_count == 1
    assert configured.enrichment_worker_reconnect_base_seconds == 1
    assert configured.enrichment_worker_reconnect_max_seconds == 30
    assert configured.enrichment_worker_processing_retry_base_seconds == 1
    assert configured.enrichment_worker_processing_retry_max_seconds == 30
    assert configured.enrichment_worker_shutdown_drain_timeout_seconds == 30


@pytest.mark.parametrize("missing", ["database_url", "rabbitmq_url"])
def test_worker_settings_require_database_and_rabbitmq_urls(
    missing: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SIGNALFORGE_DATABASE_URL", raising=False)
    monkeypatch.delenv("SIGNALFORGE_RABBITMQ_URL", raising=False)
    values = BASE_SETTINGS.copy()
    values.pop(missing)

    with pytest.raises(ValidationError):
        EnrichmentWorkerSettings(_env_file=None, **values)  # type: ignore[arg-type]


def test_worker_settings_hide_connection_credentials_in_errors() -> None:
    database_password = "database-password-must-stay-private"
    rabbitmq_password = "rabbit-password-must-stay-private"

    with pytest.raises(ValidationError) as caught:
        settings(
            database_url=(
                "postgresql+asyncpg://user:"
                f"{database_password}@localhost/signalforge_test"
            ),
            rabbitmq_url=(
                f"amqp://user:{rabbitmq_password}@localhost/signalforge_test_runtime"
            ),
            enrichment_worker_reconnect_base_seconds=5,
            enrichment_worker_reconnect_max_seconds=4,
        )

    rendered = str(caught.value)
    assert database_password not in rendered
    assert rabbitmq_password not in rendered


@pytest.mark.parametrize(
    "field",
    [
        "enrichment_worker_prefetch_count",
        "enrichment_worker_reconnect_base_seconds",
        "enrichment_worker_reconnect_max_seconds",
        "enrichment_worker_processing_retry_base_seconds",
        "enrichment_worker_processing_retry_max_seconds",
        "enrichment_worker_shutdown_drain_timeout_seconds",
    ],
)
def test_worker_settings_reject_nonpositive_values(field: str) -> None:
    with pytest.raises(ValidationError):
        settings(**{field: 0})


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {"database_url": "postgresql://user:password@localhost/database"},
            r"postgresql\+asyncpg",
        ),
        ({"rabbitmq_url": "http://localhost"}, "rabbitmq_url"),
        (
            {
                "enrichment_worker_reconnect_base_seconds": 5,
                "enrichment_worker_reconnect_max_seconds": 4,
            },
            "reconnect base",
        ),
        (
            {
                "enrichment_worker_processing_retry_base_seconds": 5,
                "enrichment_worker_processing_retry_max_seconds": 4,
            },
            "processing retry base",
        ),
    ],
)
def test_worker_settings_reject_invalid_urls_and_backoff_bounds(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        settings(**changes)


async def test_connection_declares_topology_and_selects_enrichment_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class Queue:
        pass

    class Channel:
        async def set_qos(self, *, prefetch_count: int) -> None:
            calls.append(("qos", prefetch_count))

        async def get_queue(self, name: str, *, ensure: bool) -> Queue:
            calls.append(("queue", (name, ensure)))
            return Queue()

        async def close(self) -> None:
            calls.append(("channel_close", True))

    class Connection:
        def __init__(self, url: object) -> None:
            calls.append(("url", str(url)))

        async def connect(self, *, timeout: float) -> None:  # noqa: ASYNC109
            calls.append(("connect", timeout))

        async def channel(self) -> Channel:
            calls.append(("channel", True))
            return Channel()

        async def close(self) -> None:
            calls.append(("connection_close", True))

    async def declare(channel: object, *, timeout: float) -> None:  # noqa: ASYNC109
        calls.append(("topology", timeout))

    monkeypatch.setattr(runtime, "Connection", Connection)
    monkeypatch.setattr(runtime, "declare_topology", declare)
    broker = await EnrichmentBroker.connect(settings())

    assert broker.queue.__class__ is Queue
    assert ("qos", 1) in calls
    assert ("queue", ("signalforge.triage-enrichment", True)) in calls
    assert any(name == "topology" for name, _ in calls)


async def test_broker_down_retries_with_bounded_backoff_and_safe_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine, _ = install_resources(monkeypatch)
    attempts = 0
    delays: list[float] = []
    stop = asyncio.Event()
    secret = "amqp://user:secret-must-not-be-logged@host/vhost"

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        nonlocal attempts
        attempts += 1
        raise EnrichmentConnectionError(secret)

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        if len(delays) == 4:
            event.set()
            return True
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    with caplog.at_level(logging.WARNING):
        await run_worker(
            settings(),
            FakeProvider(),
            stop_event=stop,
            jitter_source=lambda: 0,
        )

    assert attempts == 4
    assert delays == [0.5, 1.0, 2.0, 2.0]
    assert secret not in caplog.text
    assert engine.dispose_calls == 1


async def test_provider_is_reused_across_messages_and_broker_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    messages = [FakeMessage(b"first"), FakeMessage(b"second")]
    first = FakeBroker(FakeIterator([messages[0], ChannelInvalidStateError()]))
    second = FakeBroker(FakeIterator([messages[1]]))
    brokers = iter((first, second))
    provider = FakeProvider()
    providers_seen: list[EnrichmentProvider] = []
    stop = asyncio.Event()

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        return cast(EnrichmentBroker, next(brokers))

    async def handle(
        message: IncomingMessage,
        actual_provider: EnrichmentProvider,
        factory: object,
    ) -> EnrichmentProcessingResult:
        providers_seen.append(actual_provider)
        if message.body == b"second":
            stop.set()
        return EnrichmentProcessingResult.PROCESSED

    async def wait(event: asyncio.Event, delay: float) -> bool:
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_worker(settings(), provider, stop_event=stop, jitter_source=lambda: 0)

    assert providers_seen == [provider, provider]
    assert first.close_calls == second.close_calls == 1
    assert engine.dispose_calls == 1


async def test_entrypoint_constructs_provider_once_outside_worker_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = settings()
    enrichment_settings = object()
    provider = FakeProvider()
    constructed: list[object] = []
    worker_calls: list[tuple[object, object]] = []
    worker_metrics: list[EnrichmentMetrics] = []
    created_registry: list[object] = []
    order: list[str] = []
    gemini_tracer = object()
    consumer_tracer = object()

    class FakeTracingRuntime:
        shutdown_calls = 0

        def get_tracer(self, name: str) -> object:
            return {
                "signalforge.enrichment.gemini": gemini_tracer,
                "signalforge.enrichment.consumer": consumer_tracer,
            }[name]

        def shutdown(self) -> None:
            self.shutdown_calls += 1
            order.append("tracing_shutdown")

    class FakeMetricsServer:
        def stop(self) -> None:
            order.append("metrics_stop")

    tracing = FakeTracingRuntime()
    monkeypatch.setattr(runtime, "EnrichmentWorkerSettings", lambda: runtime_settings)
    monkeypatch.setattr(runtime, "EnrichmentSettings", lambda: enrichment_settings)
    monkeypatch.setattr(runtime, "_create_worker_tracing_runtime", lambda: tracing)
    monkeypatch.setattr(
        runtime.MetricsHttpServer, "start", lambda *args, **kwargs: FakeMetricsServer()
    )

    def create_provider(
        value: object, *, metrics: object, tracer: object
    ) -> FakeProvider:
        constructed.append(value)
        created_registry.append(metrics.registry)
        assert tracer is gemini_tracer
        return provider

    async def run(
        worker_settings: object,
        actual_provider: object,
        **kwargs: object,
    ) -> None:
        worker_calls.append((worker_settings, actual_provider))
        worker_metrics.append(cast(EnrichmentMetrics, kwargs["metrics"]))
        assert kwargs["tracer"] is consumer_tracer
        order.append("worker_done")

    monkeypatch.setattr(runtime, "GeminiEnrichmentProvider", create_provider)
    monkeypatch.setattr(runtime, "run_worker", run)
    monkeypatch.setattr(runtime, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(runtime, "remove_signal_handlers", lambda *args: None)

    await runtime.async_main()

    assert constructed == [enrichment_settings]
    assert worker_calls == [(runtime_settings, provider)]
    assert len(worker_metrics) == 1
    assert created_registry == [worker_metrics[0].registry]
    assert tracing.shutdown_calls == 1
    assert order == ["worker_done", "metrics_stop", "tracing_shutdown"]


async def test_terminal_failures_continue_and_reset_processing_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(str(index).encode()) for index in range(6)]
    broker = cast(
        EnrichmentBroker,
        FakeBroker(FakeIterator(cast(list[object], messages))),
    )
    stop = asyncio.Event()
    metrics = EnrichmentMetrics()
    calls = 0
    delays: list[float] = []

    async def handle(
        message: IncomingMessage,
        provider: EnrichmentProvider,
        factory: object,
    ) -> EnrichmentProcessingResult:
        nonlocal calls
        calls += 1
        if message.body in {b"0", b"2", b"4"}:
            cast(FakeMessage, message).nack_calls.append(True)
            raise TransientEnrichmentError(ProviderFailureReason.UNAVAILABLE)
        if message.body == b"1":
            cast(FakeMessage, message).reject_calls.append(False)
            raise InvalidEnrichmentEventError("invalid_json")
        if message.body == b"3":
            cast(FakeMessage, message).reject_calls.append(False)
            raise PermanentEnrichmentError(ProviderFailureReason.REQUEST_REJECTED)
        stop.set()
        return EnrichmentProcessingResult.PROCESSED

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    result = await runtime._consume_connected(
        broker,
        FakeProvider(),
        cast(Any, object()),
        settings(),
        stop,
        lambda: 0,
        metrics,
    )

    assert result is runtime._ConsumeOutcome.STOPPED
    assert calls == 6
    assert delays == [0.5, 0.5, 0.5]
    assert messages[1].reject_calls == [False]
    assert messages[3].reject_calls == [False]
    assert all(messages[index].nack_calls == [True] for index in (0, 2, 4))
    for metric_result, count in (("requeued", 3), ("rejected", 2), ("processed", 1)):
        assert (
            metrics.registry.get_sample_value(
                "signalforge_enrichment_messages_total", {"result": metric_result}
            )
            == count
        )
    assert (
        sum(
            sample.value
            for sample in metrics.messages.collect()[0].samples
            if sample.name.endswith("_total")
        )
        == 6
    )


async def test_transient_processing_backoff_is_interruptible_by_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    broker = cast(EnrichmentBroker, FakeBroker(FakeIterator([message])))
    stop = asyncio.Event()
    handled = asyncio.Event()

    async def handle(*args: object) -> EnrichmentProcessingResult:
        message.nack_calls.append(True)
        handled.set()
        raise TransientEnrichmentError(ProviderFailureReason.RATE_LIMITED)

    monkeypatch.setattr(runtime, "handle_message", handle)
    task = asyncio.create_task(
        runtime._consume_connected(
            broker,
            FakeProvider(),
            cast(Any, object()),
            settings(
                enrichment_worker_processing_retry_base_seconds=30,
                enrichment_worker_processing_retry_max_seconds=30,
            ),
            stop,
            lambda: 0,
        )
    )
    await handled.wait()
    stop.set()

    assert await asyncio.wait_for(task, timeout=1) is runtime._ConsumeOutcome.STOPPED
    assert message.nack_calls == [True]


async def test_success_and_duplicate_reset_processing_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(str(index).encode()) for index in range(6)]
    broker = cast(
        EnrichmentBroker,
        FakeBroker(FakeIterator(cast(list[object], messages))),
    )
    stop = asyncio.Event()
    metrics = EnrichmentMetrics()
    calls = 0
    delays: list[float] = []

    async def handle(
        message: IncomingMessage,
        provider: EnrichmentProvider,
        factory: object,
    ) -> EnrichmentProcessingResult:
        nonlocal calls
        calls += 1
        if message.body in {b"0", b"2", b"4"}:
            raise TransientEnrichmentError(ProviderFailureReason.UNAVAILABLE)
        if message.body == b"5":
            stop.set()
        return (
            EnrichmentProcessingResult.DUPLICATE
            if message.body == b"3"
            else EnrichmentProcessingResult.PROCESSED
        )

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    result = await runtime._consume_connected(
        broker,
        FakeProvider(),
        cast(Any, object()),
        settings(),
        stop,
        lambda: 0,
        metrics,
    )

    assert result is runtime._ConsumeOutcome.STOPPED
    assert calls == 6
    assert delays == [0.5, 0.5, 0.5]
    for metric_result, count in (("requeued", 3), ("processed", 2), ("duplicate", 1)):
        assert (
            metrics.registry.get_sample_value(
                "signalforge_enrichment_messages_total", {"result": metric_result}
            )
            == count
        )


async def test_database_failure_backs_off_without_recreating_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(b"first"), FakeMessage(b"second")]
    broker = cast(
        EnrichmentBroker,
        FakeBroker(FakeIterator(cast(list[object], messages))),
    )
    stop = asyncio.Event()
    delays: list[float] = []
    calls = 0

    async def handle(*args: object) -> EnrichmentProcessingResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            messages[0].nack_calls.append(True)
            raise OperationalError("unavailable", {}, OSError("private"))
        stop.set()
        return EnrichmentProcessingResult.PROCESSED

    async def database_available(factory: object) -> bool:
        return True

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_database_available", database_available)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    result = await runtime._consume_connected(
        broker,
        FakeProvider(),
        cast(Any, object()),
        settings(),
        stop,
        lambda: 0,
    )

    assert result is runtime._ConsumeOutcome.STOPPED
    assert calls == 2
    assert delays == [0.5]
    assert messages[0].nack_calls == [True]


async def test_database_transport_failure_retries_on_same_broker_and_records_requeue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(b"first"), FakeMessage(b"second")]
    broker = cast(
        EnrichmentBroker,
        FakeBroker(FakeIterator(cast(list[object], messages))),
    )
    stop = asyncio.Event()
    metrics = EnrichmentMetrics()
    calls = 0
    delays: list[float] = []

    async def handle(*args: object) -> EnrichmentProcessingResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            messages[0].nack_calls.append(True)
            raise DatabaseTransportError
        stop.set()
        return EnrichmentProcessingResult.PROCESSED

    availability = iter((False, True))

    async def database_available(factory: object) -> bool:
        return next(availability)

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_database_available", database_available)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    outcome = await runtime._consume_connected(
        broker,
        FakeProvider(),
        cast(Any, object()),
        settings(),
        stop,
        lambda: 0,
        metrics,
    )

    assert outcome is runtime._ConsumeOutcome.STOPPED
    assert calls == 2
    assert messages[0].nack_calls == [True]
    assert delays == [0.5, 1.0]
    assert (
        metrics.registry.get_sample_value(
            "signalforge_enrichment_messages_total", {"result": "requeued"}
        )
        == 1
    )


async def test_raw_oserror_recycles_broker_generation_without_second_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    first_message = FakeMessage(b"same-event")
    redelivery = FakeMessage(b"same-event")
    first = FakeBroker(FakeIterator([first_message]))
    replacement = FakeBroker(FakeIterator([redelivery]))
    brokers = iter((first, replacement))
    stop = asyncio.Event()
    provider = FakeProvider()
    provider_ids: list[int] = []
    calls = 0

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        return cast(EnrichmentBroker, next(brokers))

    async def handle(
        message: IncomingMessage,
        actual_provider: EnrichmentProvider,
        factory: object,
    ) -> EnrichmentProcessingResult:
        nonlocal calls
        calls += 1
        provider_ids.append(id(actual_provider))
        if calls == 1:
            first_message.ack_calls += 1
            raise OSError("ambiguous ACK transport loss")
        stop.set()
        return EnrichmentProcessingResult.DUPLICATE

    async def wait(event: asyncio.Event, delay: float) -> bool:
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_worker(settings(), provider, stop_event=stop, jitter_source=lambda: 0)

    assert calls == 2
    assert provider_ids == [id(provider), id(provider)]
    assert first_message.ack_calls == 1
    assert first_message.nack_calls == []
    assert first_message.reject_calls == []
    assert first.close_calls == replacement.close_calls == 1
    assert engine.dispose_calls == 1


async def test_idle_shutdown_closes_broker_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    iterator = FakeIterator()
    broker = FakeBroker(iterator)

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        return cast(EnrichmentBroker, broker)

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    stop = asyncio.Event()
    task = asyncio.create_task(run_worker(settings(), FakeProvider(), stop_event=stop))
    await iterator.waiting.wait()
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert iterator.cancelled.is_set()
    assert iterator.exit_calls == 1
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_shutdown_drains_active_handler_without_accepting_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(b"active"), FakeMessage(b"unstarted")]
    broker = cast(
        EnrichmentBroker,
        FakeBroker(FakeIterator(cast(list[object], messages))),
    )
    started = asyncio.Event()
    release = asyncio.Event()
    seen: list[bytes] = []
    stop = asyncio.Event()

    async def handle(
        message: IncomingMessage,
        provider: EnrichmentProvider,
        factory: object,
    ) -> EnrichmentProcessingResult:
        seen.append(message.body)
        started.set()
        await release.wait()
        return EnrichmentProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    task = asyncio.create_task(
        runtime._consume_connected(
            broker,
            FakeProvider(),
            cast(Any, object()),
            settings(),
            stop,
            lambda: 0,
        )
    )
    await started.wait()
    stop.set()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()

    assert await task is runtime._ConsumeOutcome.STOPPED
    assert seen == [b"active"]


async def test_shutdown_timeout_cancels_handler_then_cleans_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    iterator = FakeIterator([FakeMessage()])
    broker = FakeBroker(iterator)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        return cast(EnrichmentBroker, broker)

    async def handle(*args: object) -> EnrichmentProcessingResult:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_worker(
            settings(enrichment_worker_shutdown_drain_timeout_seconds=0.01),
            FakeProvider(),
            stop_event=stop,
        )
    )
    await started.wait()
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert cancelled.is_set()
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_external_cancellation_propagates_after_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    iterator = FakeIterator()
    broker = FakeBroker(iterator)

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        return cast(EnrichmentBroker, broker)

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    task = asyncio.create_task(run_worker(settings(), FakeProvider()))
    await iterator.waiting.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert iterator.cancelled.is_set()
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


def test_signal_handlers_cover_sigterm_and_sigint() -> None:
    calls: list[tuple[signal.Signals, object, tuple[object, ...]]] = []

    class Loop:
        def add_signal_handler(
            self,
            process_signal: signal.Signals,
            callback: object,
            *args: object,
        ) -> None:
            calls.append((process_signal, callback, args))

    stop = asyncio.Event()
    installed = runtime.install_signal_handlers(cast(Any, Loop()), stop)

    assert installed == (signal.SIGTERM, signal.SIGINT)
    assert [item[0] for item in calls] == [signal.SIGTERM, signal.SIGINT]
    for process_signal, callback, args in calls:
        cast(Any, callback)(*args)
        assert args == (stop, process_signal.name)
    assert stop.is_set()


async def test_programming_error_propagates_after_resource_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    broker = FakeBroker()

    async def connect(value: EnrichmentWorkerSettings) -> EnrichmentBroker:
        return cast(EnrichmentBroker, broker)

    async def consume(*args: object) -> runtime._ConsumeOutcome:
        raise RuntimeError("programming defect")

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "_consume_connected", consume)

    with pytest.raises(RuntimeError, match="programming defect"):
        await run_worker(settings(), FakeProvider())

    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_tracing_cleanup_failure_does_not_hide_worker_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "PRIVATE_TRACING_SHUTDOWN_DETAIL"

    class FailingTracingRuntime:
        shutdown_calls = 0

        def get_tracer(self, name: str) -> None:
            return None

        def shutdown(self) -> None:
            self.shutdown_calls += 1
            raise RuntimeError(secret)

    tracing = FailingTracingRuntime()
    monkeypatch.setattr(runtime, "EnrichmentWorkerSettings", lambda: settings())
    monkeypatch.setattr(runtime, "EnrichmentSettings", lambda: object())
    monkeypatch.setattr(runtime, "_create_worker_tracing_runtime", lambda: tracing)
    monkeypatch.setattr(
        runtime, "GeminiEnrichmentProvider", lambda *args, **kwargs: FakeProvider()
    )
    monkeypatch.setattr(runtime, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(runtime, "remove_signal_handlers", lambda *args: None)
    monkeypatch.setattr(
        runtime.MetricsHttpServer,
        "start",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("metrics off")),
    )

    async def fail(*args: object, **kwargs: object) -> None:
        raise ValueError("original worker failure")

    monkeypatch.setattr(runtime, "run_worker", fail)

    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(ValueError, match="original worker failure"),
    ):
        await runtime.async_main()

    assert tracing.shutdown_calls == 1
    assert secret not in caplog.text
    assert any(
        getattr(record, "exception_type", None) == "RuntimeError"
        for record in caplog.records
    )
