"""Standalone long-running runtime for safe remediation execution."""

import asyncio
import logging
import signal
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from random import random
from typing import Annotated, Self, cast

import httpx
from aio_pika import Connection, IncomingMessage
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractQueueIterator,
)
from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
from opentelemetry.trace import Tracer
from pamqp.exceptions import PAMQPException
from pydantic import AmqpDsn, Field, model_validator
from pydantic_settings import SettingsConfigDict
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from yarl import URL

from signalforge.core.config import (
    DatabaseSettings,
    MetricsHost,
    MetricsPort,
    RemediationExecutionSettings,
    get_tracing_settings,
)
from signalforge.db.errors import DatabaseTransportError
from signalforge.observability.exposition import MetricsHttpServer
from signalforge.observability.logging import (
    bind_log_context,
    configure_logging,
    delivery_log_context,
)
from signalforge.observability.metrics import (
    RemediationExecutorMetrics,
    RemediationMessageMetricResult,
    RemediationWorkerMetrics,
)
from signalforge.observability.tracing import (
    REMEDIATION_WORKER_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)
from signalforge.outbox.dispatching import retry_delay
from signalforge.outbox.rabbitmq import (
    REMEDIATION_EXECUTION_QUEUE_NAME,
    declare_topology,
)
from signalforge.remediation.consumer import (
    CONSUMER_NAME,
    InvalidEventError,
    ProcessingResult,
    handle_message,
)
from signalforge.remediation.execution import (
    AllowlistedHttpRemediationExecutor,
    HttpRestartServiceAdapter,
    RemediationExecutor,
)

logger = logging.getLogger(__name__)

PositiveSeconds = Annotated[float, Field(gt=0, allow_inf_nan=False)]
_BROKER_OPERATION_TIMEOUT_SECONDS = 10.0
_DATABASE_PROBE_TIMEOUT_SECONDS = 10.0
_BROKER_ERRORS = (AMQPError, ChannelInvalidStateError, OSError, PAMQPException)


class RemediationWorkerSettings(DatabaseSettings):
    """Configuration loaded only by the standalone remediation worker."""

    model_config = SettingsConfigDict(hide_input_in_errors=True)

    rabbitmq_url: AmqpDsn
    metrics_host: MetricsHost = "127.0.0.1"
    metrics_port: MetricsPort = 9104
    remediation_worker_reconnect_base_seconds: PositiveSeconds = 1
    remediation_worker_reconnect_max_seconds: PositiveSeconds = 30
    remediation_worker_processing_retry_base_seconds: PositiveSeconds = 1
    remediation_worker_processing_retry_max_seconds: PositiveSeconds = 30
    remediation_worker_shutdown_drain_timeout_seconds: PositiveSeconds = 40

    @model_validator(mode="after")
    def validate_runtime_configuration(self) -> Self:
        if self.database_url.scheme != "postgresql+asyncpg":
            raise ValueError("database_url must use the postgresql+asyncpg driver")
        if self.rabbitmq_url.host is None:
            raise ValueError("rabbitmq_url must include a host")
        if (
            self.remediation_worker_reconnect_base_seconds
            > self.remediation_worker_reconnect_max_seconds
        ):
            raise ValueError(
                "remediation worker reconnect base must not exceed reconnect maximum"
            )
        if (
            self.remediation_worker_processing_retry_base_seconds
            > self.remediation_worker_processing_retry_max_seconds
        ):
            raise ValueError(
                "remediation worker processing retry base must not exceed retry maximum"
            )
        return self

    @property
    def reconnect_base(self) -> timedelta:
        return timedelta(seconds=self.remediation_worker_reconnect_base_seconds)

    @property
    def reconnect_max(self) -> timedelta:
        return timedelta(seconds=self.remediation_worker_reconnect_max_seconds)

    @property
    def processing_retry_base(self) -> timedelta:
        return timedelta(seconds=self.remediation_worker_processing_retry_base_seconds)

    @property
    def processing_retry_max(self) -> timedelta:
        return timedelta(seconds=self.remediation_worker_processing_retry_max_seconds)


class RemediationWorkerConfigurationError(ValueError):
    """A sanitized unsafe standalone-worker configuration."""


class RemediationConnectionError(Exception):
    """A sanitized RabbitMQ connection or topology failure."""


class _ConsumeOutcome(StrEnum):
    STOPPED = "stopped"
    BROKER_UNAVAILABLE = "broker_unavailable"


@dataclass(slots=True)
class RemediationBroker:
    """Resources for one explicit RabbitMQ connection generation."""

    connection: AbstractConnection
    channel: AbstractChannel
    queue: AbstractQueue

    @classmethod
    async def connect(cls, settings: RemediationWorkerSettings) -> Self:
        try:
            connection = Connection(URL(str(settings.rabbitmq_url)))
        except ValueError:
            raise RemediationConnectionError(
                "Invalid RabbitMQ connection configuration"
            ) from None

        channel: AbstractChannel | None = None
        try:
            async with asyncio.timeout(_BROKER_OPERATION_TIMEOUT_SECONDS):
                await connection.connect(timeout=_BROKER_OPERATION_TIMEOUT_SECONDS)
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                await declare_topology(
                    channel,
                    timeout=_BROKER_OPERATION_TIMEOUT_SECONDS,
                )
                queue = await channel.get_queue(
                    REMEDIATION_EXECUTION_QUEUE_NAME,
                    ensure=True,
                )
            return cls(connection, channel, queue)
        except BaseException as error:
            if channel is not None:
                await _bounded_close(channel)
            await _bounded_close(connection)
            if isinstance(error, (TimeoutError, *_BROKER_ERRORS)):
                raise RemediationConnectionError(
                    "RabbitMQ connection or topology declaration failed"
                ) from None
            raise

    async def close(self) -> None:
        await _bounded_close(self.channel)
        await _bounded_close(self.connection)


def _validate_worker_startup(
    settings: RemediationWorkerSettings,
    execution_settings: RemediationExecutionSettings,
) -> None:
    if not execution_settings.restart_endpoints:
        raise RemediationWorkerConfigurationError(
            "remediation worker requires a non-empty restart endpoint allowlist"
        )
    if (
        settings.remediation_worker_shutdown_drain_timeout_seconds
        <= execution_settings.request_timeout_seconds
    ):
        raise RemediationWorkerConfigurationError(
            "remediation worker shutdown drain timeout must exceed request timeout"
        )


def _create_database_engine(settings: RemediationWorkerSettings) -> AsyncEngine:
    return create_async_engine(str(settings.database_url), pool_pre_ping=True)


async def _bounded_close(resource: AbstractConnection | AbstractChannel) -> None:
    with suppress(TimeoutError, *_BROKER_ERRORS):
        async with asyncio.timeout(_BROKER_OPERATION_TIMEOUT_SECONDS):
            await resource.close()


async def _connect_broker(
    settings: RemediationWorkerSettings,
) -> RemediationBroker:
    return await RemediationBroker.connect(settings)


def _create_worker_tracing_runtime() -> TracingRuntime:
    try:
        return create_tracing_runtime(
            get_tracing_settings(), service_name=REMEDIATION_WORKER_SERVICE_NAME
        )
    except Exception as error:
        logger.error(
            "tracing startup failed",
            extra={
                "event": "tracing_startup_failed",
                "exception_type": type(error).__name__,
            },
        )
        return TracingRuntime()


def _record_message_metric(
    metrics: RemediationWorkerMetrics,
    result: RemediationMessageMetricResult,
) -> None:
    try:
        metrics.record(result)
    except Exception:
        pass


def _backoff_seconds(
    attempt_count: int,
    *,
    base_delay: timedelta,
    max_delay: timedelta,
    jitter_source: Callable[[], float],
) -> float:
    return retry_delay(
        attempt_count,
        jitter=jitter_source(),
        base_delay=base_delay,
        max_delay=max_delay,
    ).total_seconds()


async def _wait_for_stop(stop_event: asyncio.Event, delay: float) -> bool:
    if stop_event.is_set():
        return True
    try:
        async with asyncio.timeout(delay):
            await stop_event.wait()
    except TimeoutError:
        return False
    return True


async def _database_available(
    session_factory: async_sessionmaker[AsyncSession],
) -> bool:
    """Probe the shared pool with a bounded, short-lived session."""
    try:
        async with asyncio.timeout(_DATABASE_PROBE_TIMEOUT_SECONDS):
            async with session_factory() as session:
                await session.connection()
    except (TimeoutError, SQLAlchemyError, DatabaseTransportError, OSError):
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
    settings: RemediationWorkerSettings,
    stop_event: asyncio.Event,
) -> RemediationBroker | None:
    connect_task = asyncio.create_task(_connect_broker(settings))
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


async def _requeue_unstarted(message: AbstractIncomingMessage) -> None:
    with suppress(TimeoutError, *_BROKER_ERRORS):
        async with asyncio.timeout(_BROKER_OPERATION_TIMEOUT_SECONDS):
            await message.nack(requeue=True)


async def _next_or_stop(
    iterator: AbstractQueueIterator,
    stop_event: asyncio.Event,
) -> AbstractIncomingMessage | None:
    next_task = asyncio.create_task(anext(iterator))
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        done, _ = await asyncio.wait(
            {next_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if stop_task in done:
            if next_task.done() and not next_task.cancelled():
                try:
                    message = next_task.result()
                except StopAsyncIteration:
                    return None
                await _requeue_unstarted(message)
            else:
                await _cancel_and_await(next_task)
            return None
        await _cancel_and_await(stop_task)
        return await next_task
    except asyncio.CancelledError:
        await _cancel_and_collect(next_task, stop_task)
        raise


async def _run_handler_with_context(
    message: AbstractIncomingMessage,
    executor: RemediationExecutor,
    session_factory: async_sessionmaker[AsyncSession],
    settings: RemediationWorkerSettings,
    execution_settings: RemediationExecutionSettings,
    stop_event: asyncio.Event,
    tracer: Tracer | None = None,
) -> tuple[ProcessingResult | None, bool]:
    if stop_event.is_set():
        await _requeue_unstarted(message)
        return None, True

    if tracer is None:
        handler = handle_message(
            cast(IncomingMessage, message),
            executor,
            session_factory,
            lease_seconds=execution_settings.attempt_lease_seconds,
        )
    else:
        handler = handle_message(
            cast(IncomingMessage, message),
            executor,
            session_factory,
            lease_seconds=execution_settings.attempt_lease_seconds,
            tracer=tracer,
        )
    handler_task = asyncio.create_task(handler)
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        done, _ = await asyncio.wait(
            {handler_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if handler_task in done:
            await _cancel_and_await(stop_task)
            return await handler_task, stop_event.is_set()

        logger.info(
            "remediation worker draining active delivery",
            extra={"event": "remediation_delivery_draining"},
        )
        try:
            result = await asyncio.wait_for(
                asyncio.shield(handler_task),
                timeout=(settings.remediation_worker_shutdown_drain_timeout_seconds),
            )
        except TimeoutError:
            logger.warning(
                "remediation worker forcing active delivery cancellation",
                extra={"event": "remediation_delivery_cancelled"},
            )
            await _cancel_and_await(handler_task)
            return None, True
        return result, True
    except asyncio.CancelledError:
        await _cancel_and_collect(handler_task, stop_task)
        raise


async def _run_handler_or_drain(
    message: AbstractIncomingMessage,
    executor: RemediationExecutor,
    session_factory: async_sessionmaker[AsyncSession],
    settings: RemediationWorkerSettings,
    execution_settings: RemediationExecutionSettings,
    stop_event: asyncio.Event,
    tracer: Tracer | None = None,
) -> tuple[ProcessingResult | None, bool]:
    context = delivery_log_context(message, consumer_name=CONSUMER_NAME)
    with bind_log_context(**context):
        if tracer is None:
            return await _run_handler_with_context(
                message,
                executor,
                session_factory,
                settings,
                execution_settings,
                stop_event,
            )
        return await _run_handler_with_context(
            message,
            executor,
            session_factory,
            settings,
            execution_settings,
            stop_event,
            tracer,
        )


async def _consume_connected(
    broker: RemediationBroker,
    executor: RemediationExecutor,
    session_factory: async_sessionmaker[AsyncSession],
    settings: RemediationWorkerSettings,
    execution_settings: RemediationExecutionSettings,
    stop_event: asyncio.Event,
    jitter_source: Callable[[], float],
    metrics: RemediationWorkerMetrics | None = None,
    tracer: Tracer | None = None,
) -> _ConsumeOutcome:
    pipeline_metrics = metrics or RemediationWorkerMetrics()
    processing_failures = 0
    while not stop_event.is_set():
        database_retry_delay: float | None = None
        message_context: dict[str, object] = {"consumer_name": CONSUMER_NAME}
        async with broker.queue.iterator() as iterator:
            while not stop_event.is_set():
                try:
                    message = await _next_or_stop(iterator, stop_event)
                except StopAsyncIteration:
                    return _ConsumeOutcome.BROKER_UNAVAILABLE
                if message is None:
                    return _ConsumeOutcome.STOPPED

                message_context = delivery_log_context(
                    message, consumer_name=CONSUMER_NAME
                )
                try:
                    if tracer is None:
                        result, stopping = await _run_handler_or_drain(
                            message,
                            executor,
                            session_factory,
                            settings,
                            execution_settings,
                            stop_event,
                        )
                    else:
                        result, stopping = await _run_handler_or_drain(
                            message,
                            executor,
                            session_factory,
                            settings,
                            execution_settings,
                            stop_event,
                            tracer,
                        )
                except InvalidEventError as error:
                    processing_failures = 0
                    _record_message_metric(
                        pipeline_metrics, RemediationMessageMetricResult.REJECTED
                    )
                    logger.warning(
                        "remediation worker rejected invalid event",
                        extra={
                            **message_context,
                            "event": "message_rejected",
                            "reason": error.reason,
                        },
                    )
                    if stop_event.is_set():
                        return _ConsumeOutcome.STOPPED
                    continue
                except (SQLAlchemyError, DatabaseTransportError) as error:
                    processing_failures += 1
                    _record_message_metric(
                        pipeline_metrics, RemediationMessageMetricResult.REQUEUED
                    )
                    database_retry_delay = _backoff_seconds(
                        processing_failures,
                        base_delay=settings.processing_retry_base,
                        max_delay=settings.processing_retry_max,
                        jitter_source=jitter_source,
                    )
                    logger.warning(
                        "remediation database unavailable; delivery requeued",
                        extra={
                            **message_context,
                            "event": "message_requeued",
                            "attempt": processing_failures,
                            "delay_seconds": database_retry_delay,
                            "exception_type": type(error).__name__,
                        },
                    )
                    break
                except _BROKER_ERRORS:
                    return _ConsumeOutcome.BROKER_UNAVAILABLE

                if result is None:
                    return _ConsumeOutcome.STOPPED
                if result is ProcessingResult.IN_PROGRESS:
                    _record_message_metric(
                        pipeline_metrics, RemediationMessageMetricResult.IN_PROGRESS
                    )
                    if stopping:
                        return _ConsumeOutcome.STOPPED
                    processing_failures += 1
                    delay = _backoff_seconds(
                        processing_failures,
                        base_delay=settings.processing_retry_base,
                        max_delay=settings.processing_retry_max,
                        jitter_source=jitter_source,
                    )
                    logger.info(
                        "remediation attempt remains in progress; consumption delayed",
                        extra={
                            **message_context,
                            "event": "remediation_in_progress",
                            "processing_result": result.value,
                            "attempt": processing_failures,
                            "delay_seconds": delay,
                        },
                    )
                    if await _wait_for_stop(stop_event, delay):
                        return _ConsumeOutcome.STOPPED
                    continue

                _record_message_metric(
                    pipeline_metrics,
                    RemediationMessageMetricResult.PROCESSED
                    if result is ProcessingResult.PROCESSED
                    else RemediationMessageMetricResult.DUPLICATE,
                )
                if stopping:
                    return _ConsumeOutcome.STOPPED
                processing_failures = 0
                logger.info(
                    "remediation delivery handled",
                    extra={
                        **message_context,
                        "event": "message_processed",
                        "processing_result": result.value,
                    },
                )

        if database_retry_delay is None:
            return _ConsumeOutcome.STOPPED
        if await _wait_for_stop(stop_event, database_retry_delay):
            return _ConsumeOutcome.STOPPED
        while not await _database_available(session_factory):
            processing_failures += 1
            database_retry_delay = _backoff_seconds(
                processing_failures,
                base_delay=settings.processing_retry_base,
                max_delay=settings.processing_retry_max,
                jitter_source=jitter_source,
            )
            logger.warning(
                "remediation database unavailable; consumption delayed",
                extra={
                    **message_context,
                    "event": "database_unavailable",
                    "attempt": processing_failures,
                    "delay_seconds": database_retry_delay,
                },
            )
            if await _wait_for_stop(stop_event, database_retry_delay):
                return _ConsumeOutcome.STOPPED

    return _ConsumeOutcome.STOPPED


def request_shutdown(stop_event: asyncio.Event, signal_name: str) -> None:
    logger.info(
        "remediation worker shutdown requested",
        extra={"event": "remediation_worker_stopping", "signal": signal_name},
    )
    stop_event.set()


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    stop_event: asyncio.Event,
) -> tuple[signal.Signals, ...]:
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
                "remediation worker signal handler unavailable",
                extra={
                    "event": "remediation_signal_handler_unavailable",
                    "signal": process_signal.name,
                },
            )
        else:
            installed.append(process_signal)
    return tuple(installed)


def remove_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    installed: tuple[signal.Signals, ...],
) -> None:
    for process_signal in installed:
        loop.remove_signal_handler(process_signal)


async def run_worker(
    settings: RemediationWorkerSettings,
    execution_settings: RemediationExecutionSettings,
    *,
    executor: RemediationExecutor | None = None,
    stop_event: asyncio.Event | None = None,
    jitter_source: Callable[[], float] = random,
    metrics: RemediationWorkerMetrics | None = None,
    executor_metrics: RemediationExecutorMetrics | None = None,
    tracer: Tracer | None = None,
    actuator_tracer: Tracer | None = None,
) -> None:
    """Consume sequentially until stopped, owning shared runtime resources."""
    _validate_worker_startup(settings, execution_settings)
    stop = stop_event or asyncio.Event()
    pipeline_metrics = metrics or RemediationWorkerMetrics()
    client: httpx.AsyncClient | None = None
    if executor is None:
        client = httpx.AsyncClient(follow_redirects=False)
        production_executor_metrics = executor_metrics or RemediationExecutorMetrics(
            pipeline_metrics.registry
        )
        executor = AllowlistedHttpRemediationExecutor(
            HttpRestartServiceAdapter(
                execution_settings,
                client=client,
                tracer=actuator_tracer,
            ),
            metrics=production_executor_metrics,
        )

    engine: AsyncEngine | None = None
    broker: RemediationBroker | None = None
    reconnect_failures = 0
    logger.info(
        "remediation worker starting", extra={"event": "remediation_worker_started"}
    )
    try:
        engine = _create_database_engine(settings)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        while not stop.is_set():
            if broker is None:
                try:
                    broker = await _connect_or_stop(settings, stop)
                except RemediationConnectionError:
                    reconnect_failures += 1
                    delay = _backoff_seconds(
                        reconnect_failures,
                        base_delay=settings.reconnect_base,
                        max_delay=settings.reconnect_max,
                        jitter_source=jitter_source,
                    )
                    logger.warning(
                        "RabbitMQ unavailable; remediation reconnect scheduled",
                        extra={
                            "event": "broker_connect_failed",
                            "attempt": reconnect_failures,
                            "delay_seconds": delay,
                        },
                    )
                    if await _wait_for_stop(stop, delay):
                        break
                    continue
                if broker is None:
                    break
                reconnect_failures = 0
                logger.info(
                    "remediation worker broker connected",
                    extra={"event": "broker_connected"},
                )

            try:
                outcome = await _consume_connected(
                    broker,
                    executor,
                    session_factory,
                    settings,
                    execution_settings,
                    stop,
                    jitter_source,
                    pipeline_metrics,
                    tracer,
                )
            except _BROKER_ERRORS:
                outcome = _ConsumeOutcome.BROKER_UNAVAILABLE

            if outcome is _ConsumeOutcome.STOPPED:
                break

            logger.warning(
                "remediation worker broker connection lost",
                extra={"event": "broker_connection_lost"},
            )
            await broker.close()
            broker = None
            reconnect_failures = 1
            delay = _backoff_seconds(
                reconnect_failures,
                base_delay=settings.reconnect_base,
                max_delay=settings.reconnect_max,
                jitter_source=jitter_source,
            )
            logger.warning(
                "RabbitMQ remediation reconnect scheduled",
                extra={
                    "event": "broker_connect_failed",
                    "attempt": reconnect_failures,
                    "delay_seconds": delay,
                },
            )
            if await _wait_for_stop(stop, delay):
                break
    finally:
        try:
            if broker is not None:
                await broker.close()
        finally:
            try:
                if engine is not None:
                    await engine.dispose()
            except (SQLAlchemyError, OSError):
                logger.error(
                    "remediation worker database resource cleanup failed",
                    extra={"event": "remediation_cleanup_failed"},
                )
            finally:
                if client is not None:
                    await client.aclose()
            logger.info(
                "remediation worker stopped",
                extra={"event": "remediation_worker_shutdown_complete"},
            )


async def async_main() -> None:
    settings = RemediationWorkerSettings()  # type: ignore[call-arg]
    execution_settings = RemediationExecutionSettings()
    pipeline_metrics = RemediationWorkerMetrics()
    executor_metrics = RemediationExecutorMetrics(pipeline_metrics.registry)
    tracing_runtime = _create_worker_tracing_runtime()
    try:
        consumer_tracer = tracing_runtime.get_tracer("signalforge.remediation.consumer")
        actuator_tracer = tracing_runtime.get_tracer(
            "signalforge.remediation.http_actuator"
        )
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
            await run_worker(
                settings,
                execution_settings,
                stop_event=stop_event,
                metrics=pipeline_metrics,
                executor_metrics=executor_metrics,
                tracer=consumer_tracer,
                actuator_tracer=actuator_tracer,
            )
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
    finally:
        try:
            await asyncio.to_thread(tracing_runtime.shutdown)
        except Exception as error:
            logger.error(
                "tracing shutdown failed",
                extra={
                    "event": "tracing_shutdown_failed",
                    "exception_type": type(error).__name__,
                },
            )


def main() -> None:
    configure_logging("signalforge-remediation-worker")
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
