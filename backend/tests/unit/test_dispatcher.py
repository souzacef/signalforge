import asyncio
import logging
import signal
from collections.abc import Callable
from typing import Any, cast

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from signalforge.outbox import dispatcher
from signalforge.outbox.dispatcher import (
    DispatcherSettings,
    install_signal_handlers,
    remove_signal_handlers,
    run_dispatcher,
)
from signalforge.outbox.orchestrator import DispatchBatchResult
from signalforge.outbox.rabbitmq import PublishFailedError, RabbitMQPublisher

pytestmark = pytest.mark.anyio

BASE_SETTINGS: dict[str, object] = {
    "database_url": "postgresql+asyncpg://user:password@localhost/signalforge_test",
    "rabbitmq_url": "amqp://user:password@localhost/signalforge_test_runtime",
    "dispatcher_batch_size": 10,
    "dispatcher_claim_lease_seconds": 3,
    "dispatcher_publish_timeout_seconds": 1,
    "dispatcher_settlement_budget_seconds": 1,
    "dispatcher_idle_poll_interval_seconds": 10,
    "dispatcher_reconnect_base_seconds": 1,
    "dispatcher_reconnect_max_seconds": 4,
    "dispatcher_shutdown_drain_timeout_seconds": 0.1,
}


def settings(**changes: object) -> DispatcherSettings:
    return DispatcherSettings.model_validate({**BASE_SETTINGS, **changes})


def result(
    *, claimed: int = 0, retired: bool = False, failed: int = 0
) -> DispatchBatchResult:
    return DispatchBatchResult(
        claimed=claimed,
        published=claimed - failed,
        retry_scheduled=0,
        ownership_lost=0,
        released=0,
        failed=failed,
        publisher_retired=retired,
        events=(),
    )


class FakeEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1


class FakePublisher:
    def __init__(self, name: str = "publisher") -> None:
        self.name = name
        self.is_retired = False
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        self.is_retired = True


def install_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[FakeEngine, object]:
    engine = FakeEngine()
    session_factory = object()
    monkeypatch.setattr(dispatcher, "_create_database_engine", lambda value: engine)
    monkeypatch.setattr(
        dispatcher,
        "async_sessionmaker",
        lambda *args, **kwargs: session_factory,
    )
    return engine, session_factory


def install_connector(
    monkeypatch: pytest.MonkeyPatch,
    connect: Callable[[DispatcherSettings], Any],
) -> None:
    monkeypatch.setattr(dispatcher, "_connect_publisher", connect)


def test_dispatcher_settings_do_not_require_api_jwt_configuration() -> None:
    configured = settings()
    assert not hasattr(configured, "jwt_secret")
    assert configured.claim_lease.total_seconds() == 3
    assert configured.settlement_budget.total_seconds() == 1


@pytest.mark.parametrize("missing", ["database_url", "rabbitmq_url"])
def test_dispatcher_settings_require_database_and_rabbitmq_urls(
    missing: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SIGNALFORGE_DATABASE_URL", raising=False)
    monkeypatch.delenv("SIGNALFORGE_RABBITMQ_URL", raising=False)
    values = BASE_SETTINGS.copy()
    values.pop(missing)
    with pytest.raises(ValidationError):
        DispatcherSettings(_env_file=None, **values)  # type: ignore[arg-type]


def test_dispatcher_settings_hide_credentials_in_validation_errors() -> None:
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
            dispatcher_claim_lease_seconds=1,
        )
    rendered = str(caught.value)
    assert database_password not in rendered
    assert rabbitmq_password not in rendered


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dispatcher_batch_size", 0),
        ("dispatcher_claim_lease_seconds", 0),
        ("dispatcher_publish_timeout_seconds", 0),
        ("dispatcher_settlement_budget_seconds", 0),
        ("dispatcher_idle_poll_interval_seconds", 0),
        ("dispatcher_reconnect_base_seconds", 0),
        ("dispatcher_reconnect_max_seconds", 0),
        ("dispatcher_shutdown_drain_timeout_seconds", 0),
    ],
)
def test_dispatcher_settings_reject_nonpositive_tuning_values(
    field: str, value: object
) -> None:
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
                "dispatcher_claim_lease_seconds": 1,
                "dispatcher_publish_timeout_seconds": 1,
                "dispatcher_settlement_budget_seconds": 1,
            },
            "claim lease",
        ),
        (
            {
                "dispatcher_reconnect_base_seconds": 5,
                "dispatcher_reconnect_max_seconds": 4,
            },
            "reconnect base",
        ),
    ],
)
def test_dispatcher_settings_reject_invalid_urls_and_budgets(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        settings(**changes)


async def test_empty_batch_waits_without_busy_loop_and_stop_wakes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, session_factory = install_resources(monkeypatch)
    publisher = FakePublisher()
    batch_completed = asyncio.Event()
    calls = 0

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        nonlocal calls
        assert args[0] is session_factory
        calls += 1
        batch_completed.set()
        return result()

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    stop = asyncio.Event()
    runtime = asyncio.create_task(run_dispatcher(settings(), stop_event=stop))
    await asyncio.wait_for(batch_completed.wait(), timeout=1)
    await asyncio.sleep(0)
    assert calls == 1
    assert not runtime.done()

    stop.set()
    await asyncio.wait_for(runtime, timeout=1)
    assert calls == 1
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_productive_batch_runs_next_iteration_without_idle_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()
    stop = asyncio.Event()
    calls = 0

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        nonlocal calls
        calls += 1
        if calls == 2:
            stop.set()
            return result()
        return result(claimed=1)

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    await asyncio.wait_for(run_dispatcher(settings(), stop_event=stop), timeout=1)
    assert calls == 2
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_broker_down_retries_with_bounded_backoff_without_dispatching(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine, _ = install_resources(monkeypatch)
    connection_attempts = 0
    dispatch_calls = 0
    delays: list[float] = []
    secret = "rabbit-password-must-not-be-logged"
    stop = asyncio.Event()

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        nonlocal connection_attempts
        connection_attempts += 1
        raise PublishFailedError(secret, ambiguous=False)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        nonlocal dispatch_calls
        dispatch_calls += 1
        return result()

    async def wait_for_stop(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        if len(delays) == 2:
            event.set()
            return True
        return False

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    monkeypatch.setattr(dispatcher, "_wait_for_stop", wait_for_stop)
    with caplog.at_level(logging.WARNING):
        await run_dispatcher(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert connection_attempts == 2
    assert dispatch_calls == 0
    assert delays == [0.5, 1.0]
    assert secret not in caplog.text
    assert engine.dispose_calls == 1


async def test_shutdown_cancels_an_active_connection_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    connecting = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        connecting.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    install_connector(monkeypatch, connect)
    stop = asyncio.Event()
    runtime = asyncio.create_task(run_dispatcher(settings(), stop_event=stop))
    await asyncio.wait_for(connecting.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(runtime, timeout=1)
    assert cancelled.is_set()
    assert engine.dispose_calls == 1


async def test_retired_publisher_is_closed_and_replaced_before_next_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    old = FakePublisher("old")
    replacement = FakePublisher("replacement")
    publishers = iter((old, replacement))
    seen: list[FakePublisher] = []
    delays: list[float] = []
    stop = asyncio.Event()

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, next(publishers))

    async def dispatch_once(
        factory: object,
        publisher: RabbitMQPublisher,
        value: DispatcherSettings,
        jitter_source: Callable[[], float],
    ) -> DispatchBatchResult:
        seen.append(cast(FakePublisher, publisher))
        if publisher is cast(RabbitMQPublisher, old):
            return result(claimed=1, retired=True)
        stop.set()
        return result()

    async def wait_for_stop(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        return event.is_set()

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    monkeypatch.setattr(dispatcher, "_wait_for_stop", wait_for_stop)
    await run_dispatcher(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert seen == [old, replacement]
    assert old.close_calls == replacement.close_calls == 1
    assert delays == [0.5]
    assert engine.dispose_calls == 1


async def test_transient_database_failure_backs_off_and_remains_interruptible(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()
    stop = asyncio.Event()
    delays: list[float] = []
    secret = "postgresql://user:database-password@host/database"

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        raise OperationalError("statement unavailable", {}, OSError(secret))

    async def wait_for_stop(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        event.set()
        return True

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    monkeypatch.setattr(dispatcher, "_wait_for_stop", wait_for_stop)
    with caplog.at_level(logging.WARNING):
        await run_dispatcher(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert delays == [0.5]
    assert secret not in caplog.text
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_reported_database_settlement_failure_uses_bounded_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()
    stop = asyncio.Event()
    delays: list[float] = []
    calls = 0

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        nonlocal calls
        calls += 1
        return result(claimed=1, failed=1)

    async def wait_for_stop(event: asyncio.Event, delay: float) -> bool:
        delays.append(delay)
        event.set()
        return True

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    monkeypatch.setattr(dispatcher, "_wait_for_stop", wait_for_stop)
    await run_dispatcher(settings(), stop_event=stop, jitter_source=lambda: 0)

    assert calls == 1
    assert delays == [0.5]
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_programming_error_propagates_after_resource_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        raise RuntimeError("programming defect")

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    with pytest.raises(RuntimeError, match="programming defect"):
        await run_dispatcher(settings())
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_shutdown_drains_active_batch_without_starting_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return result(claimed=1)

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    stop = asyncio.Event()
    runtime = asyncio.create_task(run_dispatcher(settings(), stop_event=stop))
    await asyncio.wait_for(started.wait(), timeout=1)
    stop.set()
    await asyncio.sleep(0)
    assert not runtime.done()
    release.set()
    await asyncio.wait_for(runtime, timeout=1)
    assert calls == 1
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_shutdown_forces_and_awaits_cancellation_after_drain_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()
    started = asyncio.Event()
    cancelled = asyncio.Event()
    calls = 0

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        nonlocal calls
        calls += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    stop = asyncio.Event()
    runtime = asyncio.create_task(
        run_dispatcher(
            settings(dispatcher_shutdown_drain_timeout_seconds=0.01),
            stop_event=stop,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(runtime, timeout=1)
    assert calls == 1
    assert cancelled.is_set()
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


async def test_external_runtime_cancellation_awaits_active_batch_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _ = install_resources(monkeypatch)
    publisher = FakePublisher()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(value: DispatcherSettings) -> RabbitMQPublisher:
        return cast(RabbitMQPublisher, publisher)

    async def dispatch_once(*args: object) -> DispatchBatchResult:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    install_connector(monkeypatch, connect)
    monkeypatch.setattr(dispatcher, "_dispatch_once", dispatch_once)
    runtime = asyncio.create_task(run_dispatcher(settings()))
    await asyncio.wait_for(started.wait(), timeout=1)
    runtime.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runtime
    assert cancelled.is_set()
    assert publisher.close_calls == 1
    assert engine.dispose_calls == 1


class FakeLoop:
    def __init__(self, *, supported: bool = True) -> None:
        self.supported = supported
        self.handlers: dict[
            signal.Signals, tuple[Callable[..., None], tuple[object, ...]]
        ] = {}
        self.removed: list[signal.Signals] = []

    def add_signal_handler(
        self,
        process_signal: signal.Signals,
        callback: Callable[..., None],
        *args: object,
    ) -> None:
        if not self.supported:
            raise NotImplementedError
        self.handlers[process_signal] = (callback, args)

    def remove_signal_handler(self, process_signal: signal.Signals) -> bool:
        self.removed.append(process_signal)
        return True


def test_signal_callbacks_only_request_shutdown_and_can_be_removed() -> None:
    loop = FakeLoop()
    stop = asyncio.Event()
    installed = install_signal_handlers(cast(Any, loop), stop)
    assert installed == (signal.SIGTERM, signal.SIGINT)
    assert not stop.is_set()

    callback, args = loop.handlers[signal.SIGTERM]
    callback(*args)
    assert stop.is_set()

    remove_signal_handlers(cast(Any, loop), installed)
    assert loop.removed == [signal.SIGTERM, signal.SIGINT]


def test_unsupported_signal_handlers_degrade_without_crashing() -> None:
    loop = FakeLoop(supported=False)
    installed = install_signal_handlers(cast(Any, loop), asyncio.Event())
    assert installed == ()
