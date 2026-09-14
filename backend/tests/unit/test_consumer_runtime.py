from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, cast

import pytest
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractIncomingMessage, AbstractQueueIterator
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from signalforge.consumers import runtime
from signalforge.consumers.incident_created import (
    InvalidEventError,
    ProcessingResult,
)
from signalforge.consumers.runtime import (
    ConsumerBroker,
    ConsumerConnectionError,
    ConsumerSettings,
    run_consumer,
)
from signalforge.observability.metrics import IncidentConsumerMetrics

pytestmark = pytest.mark.anyio

BASE_SETTINGS: dict[str, object] = {
    "database_url": "postgresql+asyncpg://user:password@localhost/signalforge_test",
    "rabbitmq_url": "amqp://user:password@localhost/signalforge_test_runtime",
    "consumer_prefetch_count": 1,
    "consumer_reconnect_base_seconds": 1,
    "consumer_reconnect_max_seconds": 4,
    "consumer_shutdown_drain_timeout_seconds": 0.05,
}


def settings(**changes: object) -> ConsumerSettings:
    return ConsumerSettings.model_validate({**BASE_SETTINGS, **changes})


class FakeEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1


class FakeMessage:
    def __init__(self, body: bytes = b"{}") -> None:
        self.body = body
        self.nack_calls: list[bool] = []

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


def incoming(message: FakeMessage) -> IncomingMessage:
    return cast(IncomingMessage, message)


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


def test_consumer_settings_have_safe_defaults_without_api_jwt() -> None:
    configured = ConsumerSettings.model_validate(
        {
            "database_url": BASE_SETTINGS["database_url"],
            "rabbitmq_url": BASE_SETTINGS["rabbitmq_url"],
        }
    )
    assert not hasattr(configured, "jwt_secret")
    assert configured.consumer_prefetch_count == 1
    assert configured.consumer_reconnect_base_seconds == 1
    assert configured.consumer_reconnect_max_seconds == 30
    assert configured.consumer_shutdown_drain_timeout_seconds == 30


@pytest.mark.parametrize("missing", ["database_url", "rabbitmq_url"])
def test_consumer_settings_require_database_and_rabbitmq_urls(
    missing: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SIGNALFORGE_DATABASE_URL", raising=False)
    monkeypatch.delenv("SIGNALFORGE_RABBITMQ_URL", raising=False)
    values = BASE_SETTINGS.copy()
    values.pop(missing)
    with pytest.raises(ValidationError):
        ConsumerSettings(_env_file=None, **values)  # type: ignore[arg-type]


def test_consumer_settings_hide_credentials_in_validation_errors() -> None:
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
            consumer_reconnect_base_seconds=5,
            consumer_reconnect_max_seconds=4,
        )
    rendered = str(caught.value)
    assert database_password not in rendered
    assert rabbitmq_password not in rendered


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("consumer_prefetch_count", 0),
        ("consumer_reconnect_base_seconds", 0),
        ("consumer_reconnect_max_seconds", 0),
        ("consumer_shutdown_drain_timeout_seconds", 0),
    ],
)
def test_consumer_settings_reject_nonpositive_values(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        settings(**{field: value})


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
                "consumer_reconnect_base_seconds": 5,
                "consumer_reconnect_max_seconds": 4,
            },
            "reconnect base",
        ),
    ],
)
def test_consumer_settings_reject_invalid_urls_and_backoff(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        settings(**changes)


async def test_connection_declares_existing_topology_and_sets_prefetch(
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
    broker = await ConsumerBroker.connect(settings())

    assert broker.queue.__class__ is Queue
    assert ("qos", 1) in calls
    assert ("queue", ("signalforge.incident-events", True)) in calls
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

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        nonlocal attempts
        attempts += 1
        raise ConsumerConnectionError(secret)

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        if len(delays) == 3:
            event.set()
            return True
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    with caplog.at_level(logging.WARNING):
        await run_consumer(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert attempts == 3
    assert delays == [0.5, 1.0, 2.0]
    assert secret not in caplog.text
    assert engine.dispose_calls == 1


async def test_connection_success_resets_reconnect_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    broker = FakeBroker()
    attempts = 0
    delays: list[float] = []
    stop = asyncio.Event()

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        nonlocal attempts
        attempts += 1
        if attempts in {1, 3}:
            raise ConsumerConnectionError("unavailable")
        return cast(ConsumerBroker, broker)

    async def consume(*args: object) -> runtime._ConsumeOutcome:
        return runtime._ConsumeOutcome.BROKER_UNAVAILABLE

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        if len(delays) == 3:
            event.set()
            return True
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "_consume_connected", consume)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_consumer(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert attempts == 3
    assert delays == [0.5, 0.5, 1.0]
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_processed_duplicate_and_invalid_deliveries_continue_sequentially(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(str(index).encode()) for index in range(4)]
    iterator = FakeIterator(cast(list[object], messages))
    broker = cast(ConsumerBroker, FakeBroker(iterator))
    stop = asyncio.Event()
    metrics = IncidentConsumerMetrics()
    active = 0
    maximum_active = 0
    outcomes: list[bytes] = []

    async def handle(message: IncomingMessage, factory: object) -> ProcessingResult:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        outcomes.append(message.body)
        await asyncio.sleep(0)
        active -= 1
        if message.body == b"1":
            raise InvalidEventError("invalid_json")
        if message.body == b"3":
            stop.set()
        return (
            ProcessingResult.DUPLICATE
            if message.body == b"2"
            else ProcessingResult.PROCESSED
        )

    monkeypatch.setattr(runtime, "handle_message", handle)
    result = await runtime._consume_connected(
        broker,
        cast(Any, object()),
        settings(),
        stop,
        lambda: 0,
        metrics,
    )

    assert result is runtime._ConsumeOutcome.STOPPED
    assert outcomes == [b"0", b"1", b"2", b"3"]
    assert maximum_active == 1
    for metric_result, count in (("processed", 2), ("duplicate", 1), ("rejected", 1)):
        assert (
            metrics.registry.get_sample_value(
                "signalforge_incident_consumer_messages_total",
                {"result": metric_result},
            )
            == count
        )
    assert (
        sum(
            sample.value
            for sample in metrics.messages.collect()[0].samples
            if sample.name.endswith("_total")
        )
        == 4
    )


async def test_database_failure_requeues_then_backs_off_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(b"first"), FakeMessage(b"second")]
    broker = cast(
        ConsumerBroker, FakeBroker(FakeIterator(cast(list[object], messages)))
    )
    stop = asyncio.Event()
    metrics = IncidentConsumerMetrics()
    calls = 0
    delays: list[float] = []

    async def handle(message: IncomingMessage, factory: object) -> ProcessingResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("unavailable", {}, OSError("private"))
        stop.set()
        return ProcessingResult.PROCESSED

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    result = await runtime._consume_connected(
        broker,
        cast(Any, object()),
        settings(),
        stop,
        lambda: 0,
        metrics,
    )

    assert result is runtime._ConsumeOutcome.STOPPED
    assert calls == 2
    assert delays == [0.5]
    assert (
        metrics.registry.get_sample_value(
            "signalforge_incident_consumer_messages_total", {"result": "requeued"}
        )
        == 1
    )
    assert (
        metrics.registry.get_sample_value(
            "signalforge_incident_consumer_messages_total", {"result": "processed"}
        )
        == 1
    )


async def test_oserror_retires_broken_broker_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    first = FakeBroker()
    second = FakeBroker()
    brokers = iter((first, second))
    seen: list[FakeBroker] = []
    stop = asyncio.Event()

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        return cast(ConsumerBroker, next(brokers))

    async def consume(
        broker: ConsumerBroker,
        *args: object,
    ) -> runtime._ConsumeOutcome:
        seen.append(cast(FakeBroker, broker))
        if broker is cast(ConsumerBroker, first):
            raise OSError("ambiguous acknowledgement transport loss")
        stop.set()
        return runtime._ConsumeOutcome.STOPPED

    async def wait(event: asyncio.Event, delay: float) -> bool:
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "_consume_connected", consume)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_consumer(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert seen == [first, second]
    assert first.close_calls == second.close_calls == 1
    assert engine.dispose_calls == 1


async def test_programming_error_propagates_after_resource_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    broker = FakeBroker()

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        return cast(ConsumerBroker, broker)

    async def consume(*args: object) -> runtime._ConsumeOutcome:
        raise RuntimeError("programming defect")

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "_consume_connected", consume)
    with pytest.raises(RuntimeError, match="programming defect"):
        await run_consumer(settings())

    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_idle_shutdown_cancels_pending_delivery_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    iterator = FakeIterator()
    broker = FakeBroker(iterator)

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        return cast(ConsumerBroker, broker)

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    stop = asyncio.Event()
    task = asyncio.create_task(run_consumer(settings(), stop_event=stop))
    await asyncio.wait_for(iterator.waiting.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert iterator.cancelled.is_set()
    assert iterator.exit_calls == 1
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_stop_before_handler_requeues_without_starting_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    called = False

    async def handle(*args: object) -> ProcessingResult:
        nonlocal called
        called = True
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    stop = asyncio.Event()
    stop.set()
    result, stopping = await runtime._run_handler_or_drain(
        cast(AbstractIncomingMessage, message),
        cast(Any, object()),
        settings(),
        stop,
    )

    assert result is None and stopping
    assert not called
    assert message.nack_calls == [True]


async def test_shutdown_drains_active_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    cancelled = False

    async def handle(*args: object) -> ProcessingResult:
        nonlocal cancelled
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled = True
            raise
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    stop = asyncio.Event()
    task = asyncio.create_task(
        runtime._run_handler_or_drain(
            cast(AbstractIncomingMessage, FakeMessage()),
            cast(Any, object()),
            settings(),
            stop,
        )
    )
    await started.wait()
    stop.set()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()

    assert await task == (ProcessingResult.PROCESSED, True)
    assert not cancelled


async def test_shutdown_forces_active_handler_cancellation_after_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handle(*args: object) -> ProcessingResult:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    monkeypatch.setattr(runtime, "handle_message", handle)
    stop = asyncio.Event()
    task = asyncio.create_task(
        runtime._run_handler_or_drain(
            cast(AbstractIncomingMessage, FakeMessage()),
            cast(Any, object()),
            settings(consumer_shutdown_drain_timeout_seconds=0.01),
            stop,
        )
    )
    await started.wait()
    stop.set()

    assert await asyncio.wait_for(task, timeout=1) == (None, True)
    assert cancelled.is_set()


async def test_external_cancellation_propagates_without_orphan_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    iterator = FakeIterator()
    broker = FakeBroker(iterator)

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        return cast(ConsumerBroker, broker)

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    task = asyncio.create_task(run_consumer(settings()))
    await iterator.waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert iterator.cancelled.is_set()
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_shutdown_cancels_active_connection_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    connecting = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        connecting.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    stop = asyncio.Event()
    task = asyncio.create_task(run_consumer(settings(), stop_event=stop))
    await connecting.wait()
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert cancelled.is_set()
    assert engine.dispose_calls == 1


async def test_ack_loss_reconnects_and_duplicate_redelivery_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    first_message = FakeMessage(b"same-event")
    redelivery = FakeMessage(b"same-event")
    first = FakeBroker(FakeIterator([first_message]))
    replacement = FakeBroker(FakeIterator([redelivery]))
    brokers = iter((first, replacement))
    stop = asyncio.Event()
    calls = 0

    async def connect(value: ConsumerSettings) -> ConsumerBroker:
        return cast(ConsumerBroker, next(brokers))

    async def handle(message: IncomingMessage, factory: object) -> ProcessingResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("ambiguous ACK transport loss")
        stop.set()
        return ProcessingResult.DUPLICATE

    async def wait(event: asyncio.Event, delay: float) -> bool:
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_consumer(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert calls == 2
    assert first.close_calls == replacement.close_calls == 1
    assert engine.dispose_calls == 1


async def test_invalid_event_log_does_not_expose_body_or_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "raw-payload-and-error-must-stay-private"
    messages = [FakeMessage(secret.encode()), FakeMessage(b"valid")]
    broker = cast(
        ConsumerBroker, FakeBroker(FakeIterator(cast(list[object], messages)))
    )
    stop = asyncio.Event()

    async def handle(message: IncomingMessage, factory: object) -> ProcessingResult:
        if message.body != b"valid":
            error = InvalidEventError("invalid_json")
            error.add_note(secret)
            raise error
        stop.set()
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    with caplog.at_level(logging.WARNING):
        await runtime._consume_connected(
            broker,
            cast(Any, object()),
            settings(),
            stop,
            lambda: 0,
        )

    assert secret not in caplog.text
