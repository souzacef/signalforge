from __future__ import annotations

import asyncio
import logging
import signal
from collections import deque
from typing import Any, cast
from uuid import uuid4

import pytest
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractIncomingMessage, AbstractQueueIterator
from aio_pika.exceptions import ChannelInvalidStateError
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from signalforge.core.config import RemediationExecutionSettings, Settings
from signalforge.db.errors import DatabaseTransportError
from signalforge.observability.metrics import (
    RemediationMessageMetricResult,
    RemediationWorkerMetrics,
)
from signalforge.observability.tracing import (
    REMEDIATION_WORKER_SERVICE_NAME,
    TracingRuntime,
)
from signalforge.remediation import runtime
from signalforge.remediation.consumer import InvalidEventError, ProcessingResult
from signalforge.remediation.execution import (
    RemediationCommand,
    RemediationExecutionOutcome,
    RemediationExecutionResult,
    RemediationExecutor,
)
from signalforge.remediation.models import RemediationActionKind
from signalforge.remediation.runtime import (
    RemediationBroker,
    RemediationConnectionError,
    RemediationWorkerConfigurationError,
    RemediationWorkerSettings,
    run_worker,
)

pytestmark = pytest.mark.anyio

BASE_SETTINGS: dict[str, object] = {
    "database_url": "postgresql+asyncpg://user:password@localhost/signalforge_test",
    "rabbitmq_url": "amqp://user:password@localhost/signalforge_test_runtime",
    "remediation_worker_reconnect_base_seconds": 1,
    "remediation_worker_reconnect_max_seconds": 4,
    "remediation_worker_processing_retry_base_seconds": 1,
    "remediation_worker_processing_retry_max_seconds": 4,
    "remediation_worker_shutdown_drain_timeout_seconds": 0.05,
}


def settings(**changes: object) -> RemediationWorkerSettings:
    return RemediationWorkerSettings.model_validate({**BASE_SETTINGS, **changes})


def execution_settings(**changes: object) -> RemediationExecutionSettings:
    return RemediationExecutionSettings.model_validate(
        {
            "restart_endpoints": {
                "checkout-api": "http://control-plane.internal/restart/checkout"
            },
            "request_timeout_seconds": 0.01,
            "attempt_lease_seconds": 0.02,
            **changes,
        }
    )


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
        self.exit_calls = 0

    async def __aenter__(self) -> FakeIterator:
        return self

    async def __aexit__(self, *args: object) -> None:
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


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, command: RemediationCommand) -> RemediationExecutionResult:
        self.calls += 1
        return RemediationExecutionResult(
            proposal_id=command.proposal_id,
            action_kind=command.action_kind,
            target=command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )


class BlockingExecutor(RecordingExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def execute(self, command: RemediationCommand) -> RemediationExecutionResult:
        self.calls += 1
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return RemediationExecutionResult(
            proposal_id=command.proposal_id,
            action_kind=command.action_kind,
            target=command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )


def install_resources(monkeypatch: pytest.MonkeyPatch) -> tuple[FakeEngine, object]:
    engine = FakeEngine()
    session_factory = object()
    monkeypatch.setattr(runtime, "_create_database_engine", lambda value: engine)
    monkeypatch.setattr(
        runtime, "async_sessionmaker", lambda *args, **kwargs: session_factory
    )
    return engine, session_factory


def command() -> RemediationCommand:
    return RemediationCommand(
        proposal_id=uuid4(),
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target="checkout-api",
    )


def test_worker_settings_are_runtime_only_with_safe_defaults() -> None:
    configured = RemediationWorkerSettings.model_validate(
        {
            "database_url": BASE_SETTINGS["database_url"],
            "rabbitmq_url": BASE_SETTINGS["rabbitmq_url"],
        }
    )
    assert not hasattr(configured, "jwt_secret")
    assert not hasattr(configured, "restart_endpoints")
    assert not hasattr(configured, "request_timeout_seconds")
    assert not hasattr(configured, "attempt_lease_seconds")
    assert configured.metrics_host == "127.0.0.1"
    assert configured.metrics_port == 9104
    assert configured.remediation_worker_reconnect_base_seconds == 1
    assert configured.remediation_worker_reconnect_max_seconds == 30
    assert configured.remediation_worker_processing_retry_base_seconds == 1
    assert configured.remediation_worker_processing_retry_max_seconds == 30
    assert configured.remediation_worker_shutdown_drain_timeout_seconds == 40


@pytest.mark.parametrize("missing", ["database_url", "rabbitmq_url"])
def test_worker_settings_require_database_and_rabbitmq_urls(
    missing: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SIGNALFORGE_DATABASE_URL", raising=False)
    monkeypatch.delenv("SIGNALFORGE_RABBITMQ_URL", raising=False)
    values = BASE_SETTINGS.copy()
    values.pop(missing)
    with pytest.raises(ValidationError):
        RemediationWorkerSettings(_env_file=None, **values)  # type: ignore[arg-type]


def test_worker_settings_hide_connection_credentials_in_errors() -> None:
    database_password = "database-password-must-stay-private"
    rabbitmq_password = "rabbit-password-must-stay-private"
    with pytest.raises(ValidationError) as caught:
        settings(
            database_url=f"postgresql+asyncpg://user:{database_password}@localhost/db",
            rabbitmq_url=f"amqp://user:{rabbitmq_password}@localhost/vhost",
            remediation_worker_reconnect_base_seconds=5,
            remediation_worker_reconnect_max_seconds=4,
        )
    assert database_password not in str(caught.value)
    assert rabbitmq_password not in str(caught.value)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {"database_url": "postgresql://user:password@localhost/db"},
            r"postgresql\+asyncpg",
        ),
        ({"rabbitmq_url": "amqp:///signalforge"}, "host"),
        (
            {
                "remediation_worker_reconnect_base_seconds": 5,
                "remediation_worker_reconnect_max_seconds": 4,
            },
            "reconnect base",
        ),
        (
            {
                "remediation_worker_processing_retry_base_seconds": 5,
                "remediation_worker_processing_retry_max_seconds": 4,
            },
            "processing retry base",
        ),
    ],
)
def test_worker_settings_reject_invalid_urls_and_backoff_bounds(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        settings(**changes)


@pytest.mark.parametrize(
    "field",
    [
        "remediation_worker_reconnect_base_seconds",
        "remediation_worker_reconnect_max_seconds",
        "remediation_worker_processing_retry_base_seconds",
        "remediation_worker_processing_retry_max_seconds",
        "remediation_worker_shutdown_drain_timeout_seconds",
    ],
)
@pytest.mark.parametrize("value", [0, -1, float("inf"), float("-inf"), float("nan")])
def test_worker_settings_reject_nonpositive_or_nonfinite_timing(
    field: str, value: float
) -> None:
    with pytest.raises(ValidationError):
        settings(**{field: value})


async def test_worker_startup_rejects_empty_allowlist_before_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "_create_database_engine",
        lambda value: pytest.fail("database engine must not be created"),
    )
    monkeypatch.setattr(
        runtime.httpx,
        "AsyncClient",
        lambda **kwargs: pytest.fail("HTTP client must not be created"),
    )
    with pytest.raises(
        RemediationWorkerConfigurationError,
        match="non-empty restart endpoint allowlist",
    ):
        await run_worker(settings(), RemediationExecutionSettings())


def test_worker_startup_requires_drain_longer_than_http_timeout() -> None:
    with pytest.raises(RemediationWorkerConfigurationError, match="drain timeout"):
        runtime._validate_worker_startup(
            settings(remediation_worker_shutdown_drain_timeout_seconds=0.05),
            execution_settings(request_timeout_seconds=0.05, attempt_lease_seconds=0.1),
        )


def test_api_settings_do_not_require_remediation_allowlist() -> None:
    configured = Settings.model_validate(
        {
            "database_url": BASE_SETTINGS["database_url"],
            "jwt_secret": "test-only-secret-with-at-least-32-characters",
        }
    )
    assert configured.app_name == "SignalForge"


async def test_production_executor_owns_one_nonredirecting_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    created_clients: list[FakeClient] = []
    adapters: list[tuple[object, object, object]] = []
    wrappers: list[tuple[object, object]] = []
    actuator_tracer = object()
    executor_metrics = object()

    class FakeClient:
        def __init__(self, *, follow_redirects: bool) -> None:
            self.follow_redirects = follow_redirects
            self.close_calls = 0
            created_clients.append(self)

        async def aclose(self) -> None:
            self.close_calls += 1

    class FakeAdapter:
        def __init__(
            self, configured: object, *, client: object, tracer: object
        ) -> None:
            adapters.append((configured, client, tracer))

    class FakeWrappedExecutor:
        def __init__(self, adapter: object, *, metrics: object) -> None:
            wrappers.append((adapter, metrics))

        async def execute(self, actual_command: RemediationCommand) -> None:
            raise AssertionError("no work should start after stop")

    monkeypatch.setattr(runtime.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(runtime, "HttpRestartServiceAdapter", FakeAdapter)
    monkeypatch.setattr(
        runtime, "AllowlistedHttpRemediationExecutor", FakeWrappedExecutor
    )
    stop = asyncio.Event()
    stop.set()
    configured_execution = execution_settings()

    await run_worker(
        settings(),
        configured_execution,
        stop_event=stop,
        executor_metrics=cast(Any, executor_metrics),
        actuator_tracer=cast(Any, actuator_tracer),
    )

    assert len(created_clients) == 1
    assert created_clients[0].follow_redirects is False
    assert created_clients[0].close_calls == 1
    assert adapters == [(configured_execution, created_clients[0], actuator_tracer)]
    assert len(wrappers) == 1
    assert isinstance(wrappers[0][0], FakeAdapter)
    assert wrappers[0][1] is executor_metrics
    assert engine.dispose_calls == 1


async def test_injected_executor_creates_no_http_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    monkeypatch.setattr(
        runtime.httpx,
        "AsyncClient",
        lambda **kwargs: pytest.fail("injected executor must suppress client creation"),
    )
    executor = RecordingExecutor()
    stop = asyncio.Event()
    stop.set()
    await run_worker(
        settings(), execution_settings(), executor=executor, stop_event=stop
    )
    assert executor.calls == 0
    assert engine.dispose_calls == 1


async def test_connection_declares_topology_prefetch_one_and_exact_queue(
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
    broker = await RemediationBroker.connect(settings())

    assert broker.queue.__class__ is Queue
    assert ("qos", 1) in calls
    assert ("queue", ("signalforge.remediation-execution", True)) in calls
    assert sum(name == "topology" for name, _ in calls) == 1


async def test_connection_failure_closes_partial_resources_and_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    secret = "rabbit-password-must-stay-private"

    class Channel:
        async def set_qos(self, *, prefetch_count: int) -> None:
            calls.append("qos")

        async def close(self) -> None:
            calls.append("channel_close")

    class Connection:
        def __init__(self, url: object) -> None:
            assert secret in str(url)

        async def connect(self, *, timeout: float) -> None:  # noqa: ASYNC109
            calls.append("connect")

        async def channel(self) -> Channel:
            calls.append("channel")
            return Channel()

        async def close(self) -> None:
            calls.append("connection_close")

    async def fail(*args: object, **kwargs: object) -> None:
        raise ChannelInvalidStateError

    monkeypatch.setattr(runtime, "Connection", Connection)
    monkeypatch.setattr(runtime, "declare_topology", fail)
    configured = settings(
        rabbitmq_url=f"amqp://user:{secret}@localhost/signalforge_test_runtime"
    )
    with pytest.raises(RemediationConnectionError) as caught:
        await RemediationBroker.connect(configured)
    assert secret not in str(caught.value)
    assert calls[-2:] == ["channel_close", "connection_close"]


async def test_first_connect_failure_recovers_with_bounded_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    broker = FakeBroker(FakeIterator([FakeMessage(b"delivery")]))
    attempts = 0
    delays: list[float] = []
    stop = asyncio.Event()
    executor = RecordingExecutor()

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RemediationConnectionError("sanitized")
        return cast(RemediationBroker, broker)

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        stop.set()
        return ProcessingResult.PROCESSED

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_worker(
        settings(),
        execution_settings(),
        executor=executor,
        stop_event=stop,
        jitter_source=lambda: 0,
    )
    assert attempts == 2
    assert delays == [0.5]
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1
    assert executor.calls == 0


async def test_reconnect_backoff_is_sanitized_and_stop_interruptible(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine, _ = install_resources(monkeypatch)
    attempted = asyncio.Event()
    attempts = 0
    secret = "amqp://user:secret-must-not-be-logged@host/vhost"

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        nonlocal attempts
        attempts += 1
        attempted.set()
        raise RemediationConnectionError(secret)

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    stop = asyncio.Event()
    with caplog.at_level(logging.WARNING):
        task = asyncio.create_task(
            run_worker(
                settings(
                    remediation_worker_reconnect_base_seconds=30,
                    remediation_worker_reconnect_max_seconds=30,
                ),
                execution_settings(),
                executor=RecordingExecutor(),
                stop_event=stop,
                jitter_source=lambda: 1,
            )
        )
        await attempted.wait()
        await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, timeout=1)
    assert attempts == 1
    assert secret not in caplog.text
    assert engine.dispose_calls == 1


async def test_success_delegates_once_without_processing_delay_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, factory = install_resources(monkeypatch)
    message = FakeMessage(b"delivery")
    broker = FakeBroker(FakeIterator([message]))
    executor = RecordingExecutor()
    stop = asyncio.Event()
    handled: list[tuple[object, object, object, float]] = []
    delays: list[float] = []

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        return cast(RemediationBroker, broker)

    async def handle(
        actual_message: IncomingMessage,
        actual_executor: RemediationExecutor,
        actual_factory: object,
        *,
        lease_seconds: float,
    ) -> ProcessingResult:
        handled.append((actual_message, actual_executor, actual_factory, lease_seconds))
        stop.set()
        return ProcessingResult.PROCESSED

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    configured_execution = execution_settings()
    await run_worker(
        settings(),
        configured_execution,
        executor=executor,
        stop_event=stop,
        jitter_source=lambda: 0,
    )
    assert handled == [
        (message, executor, factory, configured_execution.attempt_lease_seconds)
    ]
    assert executor.calls == 0
    assert delays == []
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_in_progress_backs_off_without_runtime_actuator_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    broker = cast(RemediationBroker, FakeBroker(FakeIterator([message])))
    executor = RecordingExecutor()
    stop = asyncio.Event()
    handled = asyncio.Event()

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        message.nack_calls.append(True)
        handled.set()
        return ProcessingResult.IN_PROGRESS

    monkeypatch.setattr(runtime, "handle_message", handle)
    task = asyncio.create_task(
        runtime._consume_connected(
            broker,
            executor,
            cast(Any, object()),
            settings(
                remediation_worker_processing_retry_base_seconds=30,
                remediation_worker_processing_retry_max_seconds=30,
            ),
            execution_settings(),
            stop,
            lambda: 0,
        )
    )
    await handled.wait()
    stop.set()
    assert await asyncio.wait_for(task, timeout=1) is runtime._ConsumeOutcome.STOPPED
    assert message.nack_calls == [True]
    assert executor.calls == 0


async def test_invalid_event_resets_processing_backoff_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    messages = [FakeMessage(str(index).encode()) for index in range(4)]
    broker = cast(
        RemediationBroker, FakeBroker(FakeIterator(cast(list[object], messages)))
    )
    executor = RecordingExecutor()
    stop = asyncio.Event()
    delays: list[float] = []

    async def handle(
        message: IncomingMessage, *args: object, **kwargs: object
    ) -> ProcessingResult:
        if message.body in {b"0", b"2"}:
            cast(FakeMessage, message).nack_calls.append(True)
            return ProcessingResult.IN_PROGRESS
        if message.body == b"1":
            cast(FakeMessage, message).reject_calls.append(False)
            raise InvalidEventError("invalid_json")
        stop.set()
        return ProcessingResult.DUPLICATE

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    with caplog.at_level(logging.WARNING):
        outcome = await runtime._consume_connected(
            broker,
            executor,
            cast(Any, object()),
            settings(),
            execution_settings(),
            stop,
            lambda: 0,
        )
    assert outcome is runtime._ConsumeOutcome.STOPPED
    assert delays == [0.5, 0.5]
    assert messages[1].reject_calls == [False]
    assert any(
        getattr(record, "reason", None) == "invalid_json" for record in caplog.records
    )
    assert executor.calls == 0


@pytest.mark.parametrize(
    "database_error",
    [
        OperationalError("unavailable", {}, OSError("private")),
        DatabaseTransportError(),
    ],
)
async def test_database_failure_requeues_probes_and_recovers_without_actuator_retry(
    monkeypatch: pytest.MonkeyPatch, database_error: Exception
) -> None:
    messages = [FakeMessage(b"first"), FakeMessage(b"second")]
    broker = cast(
        RemediationBroker, FakeBroker(FakeIterator(cast(list[object], messages)))
    )
    executor = RecordingExecutor()
    stop = asyncio.Event()
    calls = 0
    delays: list[float] = []
    probes = 0

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            messages[0].nack_calls.append(True)
            raise database_error
        stop.set()
        return ProcessingResult.DUPLICATE

    async def database_available(factory: object) -> bool:
        nonlocal probes
        probes += 1
        return probes >= 2

    async def wait(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return False

    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_database_available", database_available)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    outcome = await runtime._consume_connected(
        broker,
        executor,
        cast(Any, object()),
        settings(),
        execution_settings(),
        stop,
        lambda: 0,
    )
    assert outcome is runtime._ConsumeOutcome.STOPPED
    assert calls == 2
    assert probes == 2
    assert delays == [0.5, 1.0]
    assert messages[0].nack_calls == [True]
    assert messages[0].ack_calls == 0
    assert executor.calls == 0


async def test_raw_broker_failure_recycles_without_second_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    first_message = FakeMessage(b"same-event")
    redelivery = FakeMessage(b"same-event")
    first = FakeBroker(FakeIterator([first_message]))
    replacement = FakeBroker(FakeIterator([redelivery]))
    brokers = iter((first, replacement))
    executor = RecordingExecutor()
    stop = asyncio.Event()
    calls = 0

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        return cast(RemediationBroker, next(brokers))

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_message.ack_calls += 1
            raise OSError("ambiguous ACK transport loss")
        stop.set()
        return ProcessingResult.DUPLICATE

    async def wait(event: asyncio.Event, delay: float) -> bool:
        return False

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    monkeypatch.setattr(runtime, "_wait_for_stop", wait)
    await run_worker(
        settings(),
        execution_settings(),
        executor=executor,
        stop_event=stop,
        jitter_source=lambda: 0,
    )
    assert calls == 2
    assert first_message.ack_calls == 1
    assert first_message.nack_calls == []
    assert first_message.reject_calls == []
    assert first.close_calls == replacement.close_calls == 1
    assert engine.dispose_calls == 1
    assert executor.calls == 0


async def test_shutdown_before_work_starts_no_handler_or_actuator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    executor = RecordingExecutor()
    stop = asyncio.Event()
    stop.set()

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        raise AssertionError("broker must not connect after shutdown")

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        raise AssertionError("handler must not start after shutdown")

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    monkeypatch.setattr(runtime, "handle_message", handle)
    await run_worker(
        settings(), execution_settings(), executor=executor, stop_event=stop
    )
    assert executor.calls == 0
    assert engine.dispose_calls == 1


async def test_fetch_stop_race_requeues_delivered_unstarted_message() -> None:
    message = FakeMessage(b"unstarted")
    release = asyncio.Event()

    class RacingIterator:
        async def __anext__(self) -> AbstractIncomingMessage:
            await release.wait()
            return cast(AbstractIncomingMessage, message)

    stop = asyncio.Event()
    task = asyncio.create_task(
        runtime._next_or_stop(cast(AbstractQueueIterator, RacingIterator()), stop)
    )
    await asyncio.sleep(0)
    stop.set()
    release.set()
    assert await task is None
    assert message.nack_calls == [True]
    assert message.ack_calls == 0


async def test_active_shutdown_drains_one_handler_to_normal_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(b"active"), FakeMessage(b"must-not-start")]
    broker = cast(
        RemediationBroker, FakeBroker(FakeIterator(cast(list[object], messages)))
    )
    executor = BlockingExecutor()
    stop = asyncio.Event()

    async def handle(
        message: IncomingMessage,
        actual_executor: RemediationExecutor,
        *args: object,
        **kwargs: object,
    ) -> ProcessingResult:
        await actual_executor.execute(command())
        await message.ack()
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    task = asyncio.create_task(
        runtime._consume_connected(
            broker,
            executor,
            cast(Any, object()),
            settings(),
            execution_settings(),
            stop,
            lambda: 0,
        )
    )
    await executor.entered.wait()
    stop.set()
    await asyncio.sleep(0)
    assert not task.done()
    executor.release.set()
    assert await task is runtime._ConsumeOutcome.STOPPED
    assert executor.calls == 1
    assert messages[0].ack_calls == 1
    assert messages[1].ack_calls == 0
    assert messages[1].nack_calls == []


async def test_forced_drain_timeout_cancels_without_settlement_or_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage(b"active")
    broker = cast(RemediationBroker, FakeBroker(FakeIterator([message])))
    executor = BlockingExecutor()
    stop = asyncio.Event()

    async def handle(
        actual_message: IncomingMessage,
        actual_executor: RemediationExecutor,
        *args: object,
        **kwargs: object,
    ) -> ProcessingResult:
        await actual_executor.execute(command())
        await actual_message.ack()
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    task = asyncio.create_task(
        runtime._consume_connected(
            broker,
            executor,
            cast(Any, object()),
            settings(remediation_worker_shutdown_drain_timeout_seconds=0.01),
            execution_settings(
                request_timeout_seconds=0.001, attempt_lease_seconds=0.02
            ),
            stop,
            lambda: 0,
        )
    )
    await executor.entered.wait()
    stop.set()
    assert await asyncio.wait_for(task, timeout=1) is runtime._ConsumeOutcome.STOPPED
    assert executor.cancelled.is_set()
    assert executor.calls == 1
    assert message.ack_calls == 0
    assert message.nack_calls == []
    assert message.reject_calls == []


async def test_idle_shutdown_closes_broker_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    iterator = FakeIterator()
    broker = FakeBroker(iterator)

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        return cast(RemediationBroker, broker)

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_worker(
            settings(),
            execution_settings(),
            executor=RecordingExecutor(),
            stop_event=stop,
        )
    )
    await iterator.waiting.wait()
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert iterator.cancelled.is_set()
    assert iterator.exit_calls == 1
    assert broker.close_calls == 1
    assert engine.dispose_calls == 1


async def test_stop_cancels_active_connection_attempt_and_cleans_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(value: RemediationWorkerSettings) -> RemediationBroker:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    monkeypatch.setattr(runtime, "_connect_broker", connect)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_worker(
            settings(),
            execution_settings(),
            executor=RecordingExecutor(),
            stop_event=stop,
        )
    )
    await started.wait()
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert cancelled.is_set()
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
    for process_signal, callback, args in calls:
        cast(Any, callback)(*args)
        assert args == (stop, process_signal.name)
    assert stop.is_set()


def test_database_engine_enables_pre_ping(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, bool]] = []
    expected = object()

    def create(url: str, *, pool_pre_ping: bool) -> object:
        calls.append((url, pool_pre_ping))
        return expected

    monkeypatch.setattr(runtime, "create_async_engine", create)
    assert runtime._create_database_engine(settings()) is expected
    assert calls == [(str(settings().database_url), True)]


async def test_database_backoff_is_interruptible_by_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    broker = cast(RemediationBroker, FakeBroker(FakeIterator([message])))
    executor = RecordingExecutor()
    stop = asyncio.Event()
    failed = asyncio.Event()

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        message.nack_calls.append(True)
        failed.set()
        raise OperationalError("unavailable", {}, OSError())

    monkeypatch.setattr(runtime, "handle_message", handle)
    task = asyncio.create_task(
        runtime._consume_connected(
            broker,
            executor,
            cast(Any, object()),
            settings(
                remediation_worker_processing_retry_base_seconds=30,
                remediation_worker_processing_retry_max_seconds=30,
            ),
            execution_settings(),
            stop,
            lambda: 0,
        )
    )
    await failed.wait()
    stop.set()
    assert await asyncio.wait_for(task, timeout=1) is runtime._ConsumeOutcome.STOPPED
    assert message.nack_calls == [True]
    assert executor.calls == 0


def test_worker_metrics_port_is_configurable_and_validated() -> None:
    assert settings(metrics_port=19104).metrics_port == 19104
    for invalid in (0, 65536):
        with pytest.raises(ValidationError):
            settings(metrics_port=invalid)


def test_tracing_startup_uses_remediation_identity_and_degrades_safely(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    captured: list[tuple[object, object]] = []
    expected = TracingRuntime()

    def create(configured: object, *, service_name: object) -> TracingRuntime:
        captured.append((configured, service_name))
        return expected

    monkeypatch.setattr(runtime, "get_tracing_settings", lambda: object())
    monkeypatch.setattr(runtime, "create_tracing_runtime", create)
    assert runtime._create_worker_tracing_runtime() is expected
    assert captured[0][1] == REMEDIATION_WORKER_SERVICE_NAME

    secret = "PRIVATE_TRACING_STARTUP_DETAIL"

    def fail(*args: object, **kwargs: object) -> TracingRuntime:
        raise RuntimeError(secret)

    monkeypatch.setattr(runtime, "create_tracing_runtime", fail)
    with caplog.at_level(logging.ERROR):
        disabled = runtime._create_worker_tracing_runtime()
    assert disabled.enabled is False
    assert secret not in caplog.text
    assert any(
        getattr(record, "exception_type", None) == "RuntimeError"
        for record in caplog.records
    )


async def test_entrypoint_owns_shared_metrics_and_tracing_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured = settings()
    configured_execution = execution_settings()
    consumer_tracer = object()
    actuator_tracer = object()
    order: list[str] = []
    starts: list[tuple[object, str, int]] = []
    worker_kwargs: list[dict[str, object]] = []

    class FakeTracingRuntime:
        shutdown_calls = 0

        def get_tracer(self, name: str) -> object:
            return {
                "signalforge.remediation.consumer": consumer_tracer,
                "signalforge.remediation.http_actuator": actuator_tracer,
            }[name]

        def shutdown(self) -> None:
            self.shutdown_calls += 1
            order.append("tracing_shutdown")

    class FakeMetricsServer:
        def stop(self) -> None:
            order.append("metrics_stop")

    tracing = FakeTracingRuntime()
    monkeypatch.setattr(runtime, "RemediationWorkerSettings", lambda: configured)
    monkeypatch.setattr(
        runtime, "RemediationExecutionSettings", lambda: configured_execution
    )
    monkeypatch.setattr(runtime, "_create_worker_tracing_runtime", lambda: tracing)

    def start(registry: object, *, host: str, port: int) -> FakeMetricsServer:
        starts.append((registry, host, port))
        return FakeMetricsServer()

    async def run(*args: object, **kwargs: object) -> None:
        worker_kwargs.append(dict(kwargs))
        order.append("worker_done")

    monkeypatch.setattr(runtime.MetricsHttpServer, "start", start)
    monkeypatch.setattr(runtime, "run_worker", run)
    monkeypatch.setattr(runtime, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(runtime, "remove_signal_handlers", lambda *args: None)

    await runtime.async_main()

    assert len(starts) == 1
    assert starts[0][1:] == ("127.0.0.1", 9104)
    assert len(worker_kwargs) == 1
    kwargs = worker_kwargs[0]
    worker_metrics = cast(RemediationWorkerMetrics, kwargs["metrics"])
    executor_metrics = kwargs["executor_metrics"]
    assert starts[0][0] is worker_metrics.registry
    assert cast(Any, executor_metrics).registry is worker_metrics.registry
    assert kwargs["tracer"] is consumer_tracer
    assert kwargs["actuator_tracer"] is actuator_tracer
    assert tracing.shutdown_calls == 1
    assert order == ["worker_done", "metrics_stop", "tracing_shutdown"]


async def test_metrics_startup_failure_does_not_stop_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing = TracingRuntime()
    ran = 0
    monkeypatch.setattr(runtime, "RemediationWorkerSettings", lambda: settings())
    monkeypatch.setattr(
        runtime, "RemediationExecutionSettings", lambda: execution_settings()
    )
    monkeypatch.setattr(runtime, "_create_worker_tracing_runtime", lambda: tracing)
    monkeypatch.setattr(
        runtime.MetricsHttpServer,
        "start",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("unavailable")),
    )
    monkeypatch.setattr(runtime, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(runtime, "remove_signal_handlers", lambda *args: None)

    async def run(*args: object, **kwargs: object) -> None:
        nonlocal ran
        ran += 1

    monkeypatch.setattr(runtime, "run_worker", run)
    await runtime.async_main()
    assert ran == 1


async def test_metrics_cleanup_failure_does_not_hide_worker_failure_and_tracing_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeTracingRuntime:
        shutdown_calls = 0

        def get_tracer(self, name: str) -> None:
            return None

        def shutdown(self) -> None:
            self.shutdown_calls += 1

    class FailingMetricsServer:
        def stop(self) -> None:
            raise RuntimeError("metrics cleanup unavailable")

    tracing = FakeTracingRuntime()
    monkeypatch.setattr(runtime, "RemediationWorkerSettings", lambda: settings())
    monkeypatch.setattr(
        runtime, "RemediationExecutionSettings", lambda: execution_settings()
    )
    monkeypatch.setattr(runtime, "_create_worker_tracing_runtime", lambda: tracing)
    monkeypatch.setattr(
        runtime.MetricsHttpServer,
        "start",
        lambda *args, **kwargs: FailingMetricsServer(),
    )
    monkeypatch.setattr(runtime, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(runtime, "remove_signal_handlers", lambda *args: None)

    async def fail(*args: object, **kwargs: object) -> None:
        raise ValueError("original worker failure")

    monkeypatch.setattr(runtime, "run_worker", fail)
    with pytest.raises(ValueError, match="original worker failure"):
        await runtime.async_main()
    assert tracing.shutdown_calls == 1


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("processed", RemediationMessageMetricResult.PROCESSED),
        ("duplicate", RemediationMessageMetricResult.DUPLICATE),
        ("in_progress", RemediationMessageMetricResult.IN_PROGRESS),
        ("invalid", RemediationMessageMetricResult.REJECTED),
        ("database", RemediationMessageMetricResult.REQUEUED),
    ],
)
async def test_runtime_records_exactly_one_message_outcome(
    case: str,
    expected: RemediationMessageMetricResult,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    broker = cast(RemediationBroker, FakeBroker(FakeIterator([message])))
    stop = asyncio.Event()
    metrics = RemediationWorkerMetrics()
    tracer = object()

    async def handle(*args: object, **kwargs: object) -> ProcessingResult:
        assert kwargs["tracer"] is tracer
        stop.set()
        if case == "invalid":
            raise InvalidEventError("invalid_event")
        if case == "database":
            raise DatabaseTransportError()
        return {
            "processed": ProcessingResult.PROCESSED,
            "duplicate": ProcessingResult.DUPLICATE,
            "in_progress": ProcessingResult.IN_PROGRESS,
        }[case]

    monkeypatch.setattr(runtime, "handle_message", handle)
    result = await runtime._consume_connected(
        broker,
        RecordingExecutor(),
        cast(Any, object()),
        settings(),
        execution_settings(),
        stop,
        lambda: 0,
        metrics,
        cast(Any, tracer),
    )
    assert result is runtime._ConsumeOutcome.STOPPED
    assert (
        metrics.registry.get_sample_value(
            "signalforge_remediation_messages_total", {"result": expected.value}
        )
        == 1
    )
    total = sum(
        sample.value
        for sample in metrics.messages.collect()[0].samples
        if sample.name == "signalforge_remediation_messages_total"
    )
    assert total == 1


async def test_runtime_metric_failure_does_not_change_worker_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    broker = cast(RemediationBroker, FakeBroker(FakeIterator([message])))
    stop = asyncio.Event()
    stop.set()

    class FailingMetrics:
        def record(self, result: object) -> None:
            raise RuntimeError("metrics unavailable")

    async def run_handler(
        *args: object, **kwargs: object
    ) -> tuple[ProcessingResult, bool]:
        return ProcessingResult.PROCESSED, True

    monkeypatch.setattr(runtime, "_run_handler_or_drain", run_handler)
    stop.clear()
    result = await runtime._consume_connected(
        broker,
        RecordingExecutor(),
        cast(Any, object()),
        settings(),
        execution_settings(),
        stop,
        lambda: 0,
        cast(Any, FailingMetrics()),
    )
    assert result is runtime._ConsumeOutcome.STOPPED


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
    monkeypatch.setattr(runtime, "RemediationWorkerSettings", lambda: settings())
    monkeypatch.setattr(
        runtime, "RemediationExecutionSettings", lambda: execution_settings()
    )
    monkeypatch.setattr(runtime, "_create_worker_tracing_runtime", lambda: tracing)
    monkeypatch.setattr(
        runtime.MetricsHttpServer,
        "start",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("metrics off")),
    )
    monkeypatch.setattr(runtime, "install_signal_handlers", lambda *args: ())
    monkeypatch.setattr(runtime, "remove_signal_handlers", lambda *args: None)

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
