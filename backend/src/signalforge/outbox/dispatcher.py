"""Standalone long-running runtime for one-shot outbox dispatch batches."""

import asyncio
import logging
import signal
from collections.abc import Callable
from contextlib import suppress
from datetime import timedelta
from random import random
from typing import Annotated, Self

from opentelemetry.trace import Tracer
from pydantic import AmqpDsn, Field, model_validator
from pydantic_settings import SettingsConfigDict
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from signalforge.core.config import (
    DatabaseSettings,
    MetricsHost,
    MetricsPort,
    get_tracing_settings,
)
from signalforge.observability.exposition import MetricsHttpServer
from signalforge.observability.logging import configure_logging
from signalforge.observability.metrics import (
    OutboxDispatchMetricResult,
    OutboxMetrics,
)
from signalforge.observability.tracing import (
    DISPATCHER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)
from signalforge.outbox.dispatching import retry_delay
from signalforge.outbox.orchestrator import (
    DispatchBatchResult,
    DispatchOutcome,
    dispatch_batch,
)
from signalforge.outbox.rabbitmq import PublishFailedError, RabbitMQPublisher

logger = logging.getLogger(__name__)

PositiveSeconds = Annotated[float, Field(gt=0, allow_inf_nan=False)]
PositiveInteger = Annotated[int, Field(gt=0)]


class DispatcherSettings(DatabaseSettings):
    """Configuration required only by the standalone dispatcher process."""

    model_config = SettingsConfigDict(hide_input_in_errors=True)

    rabbitmq_url: AmqpDsn
    metrics_host: MetricsHost = "127.0.0.1"
    metrics_port: MetricsPort = 9101
    dispatcher_batch_size: PositiveInteger = 100
    dispatcher_claim_lease_seconds: PositiveSeconds = 30
    dispatcher_publish_timeout_seconds: PositiveSeconds = 10
    dispatcher_settlement_budget_seconds: PositiveSeconds = 5
    dispatcher_idle_poll_interval_seconds: PositiveSeconds = 1
    dispatcher_reconnect_base_seconds: PositiveSeconds = 1
    dispatcher_reconnect_max_seconds: PositiveSeconds = 30
    dispatcher_shutdown_drain_timeout_seconds: PositiveSeconds = 30

    @model_validator(mode="after")
    def validate_runtime_configuration(self) -> Self:
        if self.database_url.scheme != "postgresql+asyncpg":
            raise ValueError("database_url must use the postgresql+asyncpg driver")
        if self.rabbitmq_url.host is None:
            raise ValueError("rabbitmq_url must include a host")
        minimum_lease = (
            self.dispatcher_publish_timeout_seconds
            + self.dispatcher_settlement_budget_seconds
        )
        if self.dispatcher_claim_lease_seconds < minimum_lease:
            raise ValueError(
                "dispatcher claim lease must cover publication and settlement budgets"
            )
        if (
            self.dispatcher_reconnect_base_seconds
            > self.dispatcher_reconnect_max_seconds
        ):
            raise ValueError(
                "dispatcher reconnect base must not exceed reconnect maximum"
            )
        return self

    @property
    def claim_lease(self) -> timedelta:
        return timedelta(seconds=self.dispatcher_claim_lease_seconds)

    @property
    def settlement_budget(self) -> timedelta:
        return timedelta(seconds=self.dispatcher_settlement_budget_seconds)

    @property
    def reconnect_base(self) -> timedelta:
        return timedelta(seconds=self.dispatcher_reconnect_base_seconds)

    @property
    def reconnect_max(self) -> timedelta:
        return timedelta(seconds=self.dispatcher_reconnect_max_seconds)


def _create_database_engine(settings: DispatcherSettings) -> AsyncEngine:
    return create_async_engine(str(settings.database_url), pool_pre_ping=True)


async def _connect_publisher(settings: DispatcherSettings) -> RabbitMQPublisher:
    return await RabbitMQPublisher.connect(
        str(settings.rabbitmq_url),
        timeout=settings.dispatcher_publish_timeout_seconds,
    )


async def _dispatch_once(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: RabbitMQPublisher,
    settings: DispatcherSettings,
    jitter_source: Callable[[], float],
    tracer: Tracer | None = None,
) -> DispatchBatchResult:
    return await dispatch_batch(
        session_factory,
        publisher,
        batch_limit=settings.dispatcher_batch_size,
        lease_duration=settings.claim_lease,
        publish_timeout=settings.dispatcher_publish_timeout_seconds,
        settlement_budget=settings.settlement_budget,
        jitter_source=jitter_source,
        tracer=tracer,
    )


def _backoff_seconds(
    attempt_count: int,
    settings: DispatcherSettings,
    jitter_source: Callable[[], float],
) -> float:
    return retry_delay(
        attempt_count,
        jitter=jitter_source(),
        base_delay=settings.reconnect_base,
        max_delay=settings.reconnect_max,
    ).total_seconds()


async def _wait_for_stop(stop_event: asyncio.Event, delay: float) -> bool:
    """Wait for a stop request or timeout; return whether stop was requested."""
    if stop_event.is_set():
        return True
    try:
        async with asyncio.timeout(delay):
            await stop_event.wait()
    except TimeoutError:
        return False
    return True


async def _cancel_and_await(task: asyncio.Task[object]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def _cancel_and_collect(*tasks: asyncio.Task[object]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _connect_or_stop(
    settings: DispatcherSettings, stop_event: asyncio.Event
) -> RabbitMQPublisher | None:
    """Connect unless shutdown wins the race, cancelling startup if it does."""
    connect_task = asyncio.create_task(_connect_publisher(settings))
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        done, _ = await asyncio.wait(
            {connect_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if connect_task in done:
            await _cancel_and_await(stop_task)
            return await connect_task
        await _cancel_and_await(connect_task)
        return None
    except asyncio.CancelledError:
        await _cancel_and_collect(connect_task, stop_task)
        raise


async def _run_batch_or_drain(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: RabbitMQPublisher,
    settings: DispatcherSettings,
    stop_event: asyncio.Event,
    jitter_source: Callable[[], float],
    tracer: Tracer | None = None,
) -> tuple[DispatchBatchResult | None, bool]:
    """Run one batch, draining it when shutdown wins, then force cancellation."""
    if stop_event.is_set():
        return None, True
    batch_task = asyncio.create_task(
        _dispatch_once(session_factory, publisher, settings, jitter_source)
        if tracer is None
        else _dispatch_once(session_factory, publisher, settings, jitter_source, tracer)
    )
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        done, _ = await asyncio.wait(
            {batch_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if batch_task in done:
            await _cancel_and_await(stop_task)
            return await batch_task, stop_event.is_set()

        logger.info(
            "dispatcher draining active batch",
            extra={"event": "dispatcher_stopping"},
        )
        try:
            result = await asyncio.wait_for(
                asyncio.shield(batch_task),
                timeout=settings.dispatcher_shutdown_drain_timeout_seconds,
            )
        except TimeoutError:
            logger.warning(
                "dispatcher forcing active batch cancellation",
                extra={"event": "dispatcher_batch_cancelled"},
            )
            await _cancel_and_await(batch_task)
            return None, True
        return result, True
    except asyncio.CancelledError:
        await _cancel_and_collect(batch_task, stop_task)
        raise


def request_shutdown(stop_event: asyncio.Event, signal_name: str) -> None:
    """Signal-safe callback: record the request and wake interruptible waits."""
    logger.info(
        "dispatcher shutdown requested",
        extra={"event": "dispatcher_stopping", "signal": signal_name},
    )
    stop_event.set()


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event
) -> tuple[signal.Signals, ...]:
    """Install Unix handlers when supported, without doing async signal work."""
    installed = []
    for process_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                process_signal,
                request_shutdown,
                stop_event,
                process_signal.name,
            )
        except (NotImplementedError, RuntimeError):
            logger.warning(
                "dispatcher signal handler unavailable",
                extra={
                    "event": "dispatcher_signal_handler_unavailable",
                    "signal": process_signal.name,
                },
            )
        else:
            installed.append(process_signal)
    return tuple(installed)


def remove_signal_handlers(
    loop: asyncio.AbstractEventLoop, installed: tuple[signal.Signals, ...]
) -> None:
    for process_signal in installed:
        loop.remove_signal_handler(process_signal)


def _dispatch_metric_result(
    outcome: DispatchOutcome, *, error_code: object, publication_confirmed: bool
) -> OutboxDispatchMetricResult:
    if outcome is DispatchOutcome.PUBLISHED:
        return OutboxDispatchMetricResult.PUBLISHED
    if outcome is DispatchOutcome.RETRY_SCHEDULED:
        return (
            OutboxDispatchMetricResult.INVALID
            if error_code == "invalid_event"
            else OutboxDispatchMetricResult.RETRY_SCHEDULED
        )
    if outcome is DispatchOutcome.OWNERSHIP_LOST:
        return OutboxDispatchMetricResult.OWNERSHIP_LOST
    if outcome is DispatchOutcome.RELEASED:
        return OutboxDispatchMetricResult.RELEASED
    return (
        OutboxDispatchMetricResult.AMBIGUOUS
        if publication_confirmed
        else OutboxDispatchMetricResult.DATABASE_FAILED
    )


def _log_batch(result: DispatchBatchResult, metrics: OutboxMetrics) -> None:
    logger.info(
        "dispatcher batch completed",
        extra={
            "event": "outbox_batch_claimed",
            "claimed": result.claimed,
            "published": result.published,
            "retry_scheduled": result.retry_scheduled,
            "ownership_lost": result.ownership_lost,
            "released": result.released,
            "failed": result.failed,
            "publisher_retired": result.publisher_retired,
        },
    )
    for event_result in result.events:
        metrics.record(
            event_type=event_result.event_type,
            result=_dispatch_metric_result(
                event_result.outcome,
                error_code=event_result.error_code,
                publication_confirmed=event_result.publication_confirmed,
            ),
        )
        event_name = {
            DispatchOutcome.PUBLISHED: "outbox_event_published",
            DispatchOutcome.RETRY_SCHEDULED: (
                "outbox_event_invalid"
                if event_result.error_code == "invalid_event"
                else "outbox_event_retry_scheduled"
            ),
            DispatchOutcome.OWNERSHIP_LOST: "outbox_event_ownership_lost",
            DispatchOutcome.RELEASED: "outbox_event_released",
            DispatchOutcome.DATABASE_FAILED: (
                "outbox_publication_ambiguous"
                if event_result.publication_confirmed
                else "outbox_event_database_failed"
            ),
        }[event_result.outcome]
        log = (
            logger.info
            if event_result.outcome
            in {
                DispatchOutcome.PUBLISHED,
                DispatchOutcome.RELEASED,
            }
            else logger.warning
        )
        log(
            "outbox event dispatch completed",
            extra={
                "event": event_name,
                "event_id": event_result.event_id,
                "result": event_result.outcome,
                "error_code": event_result.error_code,
            },
        )


async def run_dispatcher(
    settings: DispatcherSettings,
    *,
    stop_event: asyncio.Event | None = None,
    jitter_source: Callable[[], float] = random,
    metrics: OutboxMetrics | None = None,
    tracing: TracingRuntime | None = None,
) -> None:
    """Run batches until stopped, owning and cleaning up DB/broker resources."""
    if tracing is None:
        try:
            tracing_runtime = create_tracing_runtime(
                get_tracing_settings(), service_name=DISPATCHER_SERVICE_NAME
            )
        except Exception as error:
            logger.error(
                "tracing startup failed",
                extra={
                    "event": "tracing_startup_failed",
                    "exception_type": type(error).__name__,
                },
            )
            tracing_runtime = TracingRuntime()
    else:
        tracing_runtime = tracing
    tracer = tracing_runtime.get_tracer(__name__)
    stop = stop_event or asyncio.Event()
    pipeline_metrics = metrics or OutboxMetrics()
    engine = _create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    publisher: RabbitMQPublisher | None = None
    reconnect_failures = 0
    database_failures = 0
    logger.info("dispatcher starting", extra={"event": "dispatcher_started"})
    try:
        while not stop.is_set():
            if publisher is None:
                try:
                    publisher = await _connect_or_stop(settings, stop)
                except PublishFailedError:
                    reconnect_failures += 1
                    delay = _backoff_seconds(
                        reconnect_failures, settings, jitter_source
                    )
                    logger.warning(
                        "RabbitMQ connection unavailable; reconnect scheduled",
                        extra={
                            "event": "broker_connect_failed",
                            "attempt": reconnect_failures,
                            "delay_seconds": delay,
                        },
                    )
                    if await _wait_for_stop(stop, delay):
                        break
                    continue
                if publisher is None:
                    break
                reconnect_failures = 0
                logger.info(
                    "dispatcher publisher connected",
                    extra={"event": "broker_connected"},
                )

            if stop.is_set():
                break

            try:
                result, stopping = await _run_batch_or_drain(
                    session_factory,
                    publisher,
                    settings,
                    stop,
                    jitter_source,
                    tracer,
                )
            # asyncpg can expose connection-resolution failures before SQLAlchemy
            # wraps them, so raw transport OSErrors are database outages here too.
            except (SQLAlchemyError, OSError):
                database_failures += 1
                delay = _backoff_seconds(database_failures, settings, jitter_source)
                logger.warning(
                    "dispatcher database operation unavailable; retry scheduled",
                    extra={
                        "event": "dispatcher_database_retry_scheduled",
                        "attempt": database_failures,
                        "delay_seconds": delay,
                    },
                )
                if await _wait_for_stop(stop, delay):
                    break
                continue

            if result is None:
                break
            _log_batch(result, pipeline_metrics)
            if stopping:
                break

            publisher_retired = result.publisher_retired or publisher.is_retired
            if publisher_retired:
                logger.warning(
                    "dispatcher publisher retired",
                    extra={"event": "dispatcher_publisher_retired"},
                )
                await publisher.close()
                publisher = None
                reconnect_failures = 0

            if result.failed:
                database_failures += 1
                delay = _backoff_seconds(database_failures, settings, jitter_source)
                logger.warning(
                    "dispatcher database settlement unavailable; retry scheduled",
                    extra={
                        "event": "dispatcher_database_retry_scheduled",
                        "attempt": database_failures,
                        "delay_seconds": delay,
                    },
                )
                if await _wait_for_stop(stop, delay):
                    break
                continue

            database_failures = 0
            if publisher_retired:
                reconnect_failures = 1
                delay = _backoff_seconds(reconnect_failures, settings, jitter_source)
                logger.warning(
                    "RabbitMQ reconnect scheduled",
                    extra={
                        "event": "broker_connect_failed",
                        "attempt": reconnect_failures,
                        "delay_seconds": delay,
                    },
                )
                if await _wait_for_stop(stop, delay):
                    break
                continue

            if result.claimed == 0:
                if await _wait_for_stop(
                    stop, settings.dispatcher_idle_poll_interval_seconds
                ):
                    break
            else:
                await asyncio.sleep(0)
    finally:
        try:
            if publisher is not None:
                await publisher.close()
        finally:
            try:
                await engine.dispose()
            except SQLAlchemyError:
                logger.error(
                    "dispatcher database resource cleanup failed",
                    extra={"event": "dispatcher_cleanup_failed"},
                )
            try:
                tracing_runtime.shutdown()
            except Exception as error:
                logger.error(
                    "tracing shutdown failed",
                    extra={
                        "event": "tracing_shutdown_failed",
                        "exception_type": type(error).__name__,
                    },
                )
            logger.info(
                "dispatcher stopped",
                extra={"event": "dispatcher_shutdown_complete"},
            )


async def async_main() -> None:
    settings = DispatcherSettings()  # type: ignore[call-arg]
    pipeline_metrics = OutboxMetrics()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = install_signal_handlers(loop, stop_event)
    metrics_server: MetricsHttpServer | None = None
    try:
        try:
            metrics_server = MetricsHttpServer.start(
                pipeline_metrics.registry,
                host=settings.metrics_host,
                port=settings.metrics_port,
            )
        except Exception:
            logger.warning(
                "metrics server could not start",
                extra={"event": "metrics_server_start_failed"},
            )
        else:
            logger.info(
                "metrics server started",
                extra={
                    "event": "metrics_server_started",
                    "metrics_host": settings.metrics_host,
                    "metrics_port": settings.metrics_port,
                },
            )
        await run_dispatcher(settings, stop_event=stop_event, metrics=pipeline_metrics)
    finally:
        try:
            if metrics_server is not None:
                try:
                    await asyncio.to_thread(metrics_server.stop)
                except Exception:
                    logger.warning(
                        "metrics server cleanup failed",
                        extra={"event": "metrics_server_stop_failed"},
                    )
                else:
                    logger.info(
                        "metrics server stopped",
                        extra={"event": "metrics_server_stopped"},
                    )
        finally:
            remove_signal_handlers(loop, installed)


def main() -> None:
    configure_logging("signalforge-dispatcher")
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
