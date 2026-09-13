"""Standalone long-running runtime for advisory enrichment consumption."""

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

from aio_pika import Connection, IncomingMessage
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractQueueIterator,
)
from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
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

from signalforge.core.config import DatabaseSettings, EnrichmentSettings
from signalforge.enrichment.consumer import (
    EnrichmentProcessingResult,
    InvalidEnrichmentEventError,
    handle_message,
)
from signalforge.enrichment.gemini import GeminiEnrichmentProvider
from signalforge.enrichment.provider import (
    EnrichmentProvider,
    PermanentEnrichmentError,
    TransientEnrichmentError,
)
from signalforge.outbox.dispatching import retry_delay
from signalforge.outbox.rabbitmq import ENRICHMENT_QUEUE_NAME, declare_topology

logger = logging.getLogger(__name__)

PositiveSeconds = Annotated[float, Field(gt=0, allow_inf_nan=False)]
PositiveInteger = Annotated[int, Field(gt=0)]
_BROKER_OPERATION_TIMEOUT_SECONDS = 10.0
_BROKER_ERRORS = (AMQPError, ChannelInvalidStateError, OSError, PAMQPException)


class EnrichmentWorkerSettings(DatabaseSettings):
    """Runtime configuration loaded only by the enrichment worker process."""

    model_config = SettingsConfigDict(hide_input_in_errors=True)

    rabbitmq_url: AmqpDsn
    enrichment_worker_prefetch_count: PositiveInteger = 1
    enrichment_worker_reconnect_base_seconds: PositiveSeconds = 1
    enrichment_worker_reconnect_max_seconds: PositiveSeconds = 30
    enrichment_worker_processing_retry_base_seconds: PositiveSeconds = 1
    enrichment_worker_processing_retry_max_seconds: PositiveSeconds = 30
    enrichment_worker_shutdown_drain_timeout_seconds: PositiveSeconds = 30

    @model_validator(mode="after")
    def validate_runtime_configuration(self) -> Self:
        if self.database_url.scheme != "postgresql+asyncpg":
            raise ValueError("database_url must use the postgresql+asyncpg driver")
        if self.rabbitmq_url.host is None:
            raise ValueError("rabbitmq_url must include a host")
        if (
            self.enrichment_worker_reconnect_base_seconds
            > self.enrichment_worker_reconnect_max_seconds
        ):
            raise ValueError(
                "enrichment worker reconnect base must not exceed reconnect maximum"
            )
        if (
            self.enrichment_worker_processing_retry_base_seconds
            > self.enrichment_worker_processing_retry_max_seconds
        ):
            raise ValueError(
                "enrichment worker processing retry base must not exceed retry maximum"
            )
        return self

    @property
    def reconnect_base(self) -> timedelta:
        return timedelta(seconds=self.enrichment_worker_reconnect_base_seconds)

    @property
    def reconnect_max(self) -> timedelta:
        return timedelta(seconds=self.enrichment_worker_reconnect_max_seconds)

    @property
    def processing_retry_base(self) -> timedelta:
        return timedelta(seconds=self.enrichment_worker_processing_retry_base_seconds)

    @property
    def processing_retry_max(self) -> timedelta:
        return timedelta(seconds=self.enrichment_worker_processing_retry_max_seconds)


class EnrichmentConnectionError(Exception):
    """A sanitized RabbitMQ connection or topology failure."""


class _ConsumeOutcome(StrEnum):
    STOPPED = "stopped"
    BROKER_UNAVAILABLE = "broker_unavailable"


@dataclass(slots=True)
class EnrichmentBroker:
    """Resources for one RabbitMQ connection generation."""

    connection: AbstractConnection
    channel: AbstractChannel
    queue: AbstractQueue

    @classmethod
    async def connect(cls, settings: EnrichmentWorkerSettings) -> Self:
        try:
            connection = Connection(URL(str(settings.rabbitmq_url)))
        except ValueError:
            raise EnrichmentConnectionError(
                "Invalid RabbitMQ connection configuration"
            ) from None

        channel: AbstractChannel | None = None
        try:
            async with asyncio.timeout(_BROKER_OPERATION_TIMEOUT_SECONDS):
                await connection.connect(timeout=_BROKER_OPERATION_TIMEOUT_SECONDS)
                channel = await connection.channel()
                await channel.set_qos(
                    prefetch_count=settings.enrichment_worker_prefetch_count
                )
                await declare_topology(
                    channel,
                    timeout=_BROKER_OPERATION_TIMEOUT_SECONDS,
                )
                queue = await channel.get_queue(
                    ENRICHMENT_QUEUE_NAME,
                    ensure=True,
                )
            return cls(connection, channel, queue)
        except BaseException as error:
            if channel is not None:
                await _bounded_close(channel)
            await _bounded_close(connection)
            if isinstance(error, (TimeoutError, *_BROKER_ERRORS)):
                raise EnrichmentConnectionError(
                    "RabbitMQ connection or topology declaration failed"
                ) from None
            raise

    async def close(self) -> None:
        await _bounded_close(self.channel)
        await _bounded_close(self.connection)


def _create_database_engine(settings: EnrichmentWorkerSettings) -> AsyncEngine:
    return create_async_engine(str(settings.database_url), pool_pre_ping=True)


async def _bounded_close(resource: AbstractConnection | AbstractChannel) -> None:
    with suppress(TimeoutError, *_BROKER_ERRORS):
        async with asyncio.timeout(_BROKER_OPERATION_TIMEOUT_SECONDS):
            await resource.close()


async def _connect_broker(settings: EnrichmentWorkerSettings) -> EnrichmentBroker:
    return await EnrichmentBroker.connect(settings)


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


async def _cancel_and_await(task: asyncio.Task[object]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def _cancel_and_collect(*tasks: asyncio.Task[object]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _connect_or_stop(
    settings: EnrichmentWorkerSettings,
    stop_event: asyncio.Event,
) -> EnrichmentBroker | None:
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


async def _run_handler_or_drain(
    message: AbstractIncomingMessage,
    provider: EnrichmentProvider,
    session_factory: async_sessionmaker[AsyncSession],
    settings: EnrichmentWorkerSettings,
    stop_event: asyncio.Event,
) -> tuple[EnrichmentProcessingResult | None, bool]:
    if stop_event.is_set():
        await _requeue_unstarted(message)
        return None, True

    handler_task = asyncio.create_task(
        handle_message(
            cast(IncomingMessage, message),
            provider,
            session_factory,
        )
    )
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        done, _ = await asyncio.wait(
            {handler_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if handler_task in done:
            await _cancel_and_await(stop_task)
            return await handler_task, stop_event.is_set()

        logger.info("enrichment worker draining active delivery")
        try:
            result = await asyncio.wait_for(
                asyncio.shield(handler_task),
                timeout=settings.enrichment_worker_shutdown_drain_timeout_seconds,
            )
        except TimeoutError:
            logger.warning("enrichment worker forcing active delivery cancellation")
            await _cancel_and_await(handler_task)
            return None, True
        return result, True
    except asyncio.CancelledError:
        await _cancel_and_collect(handler_task, stop_task)
        raise


async def _consume_connected(
    broker: EnrichmentBroker,
    provider: EnrichmentProvider,
    session_factory: async_sessionmaker[AsyncSession],
    settings: EnrichmentWorkerSettings,
    stop_event: asyncio.Event,
    jitter_source: Callable[[], float],
) -> _ConsumeOutcome:
    processing_failures = 0
    async with broker.queue.iterator() as iterator:
        while not stop_event.is_set():
            try:
                message = await _next_or_stop(iterator, stop_event)
            except StopAsyncIteration:
                return _ConsumeOutcome.BROKER_UNAVAILABLE
            if message is None:
                return _ConsumeOutcome.STOPPED

            try:
                result, stopping = await _run_handler_or_drain(
                    message,
                    provider,
                    session_factory,
                    settings,
                    stop_event,
                )
            except InvalidEnrichmentEventError as error:
                processing_failures = 0
                logger.warning(
                    "enrichment worker rejected invalid event",
                    extra={"reason": error.reason},
                )
                if stop_event.is_set():
                    return _ConsumeOutcome.STOPPED
                continue
            except PermanentEnrichmentError as error:
                processing_failures = 0
                logger.warning(
                    "enrichment worker handled terminal provider failure",
                    extra={"reason": error.reason},
                )
                if stop_event.is_set():
                    return _ConsumeOutcome.STOPPED
                continue
            except TransientEnrichmentError as error:
                processing_failures += 1
                delay = _backoff_seconds(
                    processing_failures,
                    base_delay=settings.processing_retry_base,
                    max_delay=settings.processing_retry_max,
                    jitter_source=jitter_source,
                )
                logger.warning(
                    "enrichment provider unavailable; retry scheduled",
                    extra={
                        "reason": error.reason,
                        "attempt": processing_failures,
                        "delay_seconds": delay,
                    },
                )
                if await _wait_for_stop(stop_event, delay):
                    return _ConsumeOutcome.STOPPED
                continue
            except SQLAlchemyError:
                processing_failures += 1
                delay = _backoff_seconds(
                    processing_failures,
                    base_delay=settings.processing_retry_base,
                    max_delay=settings.processing_retry_max,
                    jitter_source=jitter_source,
                )
                logger.warning(
                    "enrichment database unavailable; retry scheduled",
                    extra={
                        "attempt": processing_failures,
                        "delay_seconds": delay,
                    },
                )
                if await _wait_for_stop(stop_event, delay):
                    return _ConsumeOutcome.STOPPED
                continue
            except _BROKER_ERRORS:
                return _ConsumeOutcome.BROKER_UNAVAILABLE

            if result is None or stopping:
                return _ConsumeOutcome.STOPPED
            processing_failures = 0
            if result is EnrichmentProcessingResult.PROCESSED:
                logger.info("enrichment message processed")
            else:
                logger.info("duplicate enrichment message ignored")

    return _ConsumeOutcome.BROKER_UNAVAILABLE


def request_shutdown(stop_event: asyncio.Event, signal_name: str) -> None:
    logger.info("enrichment worker shutdown requested", extra={"signal": signal_name})
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
                "enrichment worker signal handler unavailable",
                extra={"signal": process_signal.name},
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
    settings: EnrichmentWorkerSettings,
    provider: EnrichmentProvider,
    *,
    stop_event: asyncio.Event | None = None,
    jitter_source: Callable[[], float] = random,
) -> None:
    """Consume sequentially until stopped, owning all DB/broker resources."""
    stop = stop_event or asyncio.Event()
    engine = _create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    broker: EnrichmentBroker | None = None
    reconnect_failures = 0
    logger.info("enrichment worker starting")
    try:
        while not stop.is_set():
            if broker is None:
                try:
                    broker = await _connect_or_stop(settings, stop)
                except EnrichmentConnectionError:
                    reconnect_failures += 1
                    delay = _backoff_seconds(
                        reconnect_failures,
                        base_delay=settings.reconnect_base,
                        max_delay=settings.reconnect_max,
                        jitter_source=jitter_source,
                    )
                    logger.warning(
                        "RabbitMQ connection unavailable; enrichment reconnect scheduled",
                        extra={
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
                logger.info("enrichment worker broker connected")

            try:
                outcome = await _consume_connected(
                    broker,
                    provider,
                    session_factory,
                    settings,
                    stop,
                    jitter_source,
                )
            except _BROKER_ERRORS:
                outcome = _ConsumeOutcome.BROKER_UNAVAILABLE

            if outcome is _ConsumeOutcome.STOPPED:
                break

            logger.warning("enrichment worker broker connection lost")
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
                "RabbitMQ enrichment reconnect scheduled",
                extra={"attempt": reconnect_failures, "delay_seconds": delay},
            )
            if await _wait_for_stop(stop, delay):
                break
    finally:
        try:
            if broker is not None:
                await broker.close()
        finally:
            try:
                await engine.dispose()
            except (SQLAlchemyError, OSError):
                logger.error("enrichment worker database resource cleanup failed")
            logger.info("enrichment worker stopped")


async def async_main() -> None:
    settings = EnrichmentWorkerSettings()  # type: ignore[call-arg]
    enrichment_settings = EnrichmentSettings()  # type: ignore[call-arg]
    provider = GeminiEnrichmentProvider(enrichment_settings)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = install_signal_handlers(loop, stop_event)
    try:
        await run_worker(settings, provider, stop_event=stop_event)
    finally:
        remove_signal_handlers(loop, installed)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
