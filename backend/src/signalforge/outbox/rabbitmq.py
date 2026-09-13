"""Confirmed RabbitMQ publication only; no database access or retry orchestration."""

import asyncio
import math
from collections.abc import Mapping
from contextlib import suppress
from typing import Literal, Self, cast

from aio_pika import (
    DeliveryMode,
    ExchangeType,
    Message,
    RobustChannel,
    RobustConnection,
)
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractRobustConnection,
)
from aio_pika.exceptions import (
    AMQPError,
    ChannelInvalidStateError,
    DeliveryError,
    PublishError,
)
from pamqp.commands import Basic
from pamqp.exceptions import PAMQPException
from pydantic import JsonValue, ValidationError
from yarl import URL

from signalforge.incidents.events import IncidentCreated, IncidentCreatedPayload
from signalforge.outbox.dispatching import ClaimSnapshot, FrozenJSON
from signalforge.triage.events import (
    TriageEnrichmentRequested,
    TriageEnrichmentRequestedPayload,
)

EXCHANGE_NAME = "signalforge.events"
QUEUE_NAME = "signalforge.incident-events"
ROUTING_KEY = "incident.created"
ENRICHMENT_QUEUE_NAME = "signalforge.triage-enrichment"
ENRICHMENT_ROUTING_KEY = "triage.enrichment.requested"
_ROUTING_KEYS = {
    "incident.created": ROUTING_KEY,
    "triage.enrichment.requested": ENRICHMENT_ROUTING_KEY,
}
_TRANSPORT_ERRORS = (AMQPError, ChannelInvalidStateError, OSError, PAMQPException)

type PublishableEvent = IncidentCreated | TriageEnrichmentRequested


class StoredEventError(ValueError):
    """Persisted data cannot be published under the supported event contract."""

    def __init__(
        self,
        reason: Literal["unsupported_type", "unsupported_version", "invalid_event"],
    ) -> None:
        self.reason = reason
        super().__init__(f"Stored event rejected: {reason}")


class PublishFailedError(Exception):
    """No safe confirmation. ambiguous=True means delivery may have happened."""

    def __init__(self, message: str, *, ambiguous: bool) -> None:
        self.ambiguous = ambiguous
        super().__init__(message)


class UnroutablePublishError(PublishFailedError):
    def __init__(self) -> None:
        super().__init__(
            "Mandatory publication was returned unroutable", ambiguous=False
        )


class PublishTimeoutError(PublishFailedError):
    def __init__(self) -> None:
        super().__init__("Publication timed out; outcome is unknown", ambiguous=True)


def thaw_json(value: FrozenJSON) -> JsonValue:
    """Copy only JSON-shaped values; never deepcopy arbitrary stored objects."""
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise StoredEventError("invalid_event")
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise StoredEventError("invalid_event")


def reconstruct_event(claim: ClaimSnapshot) -> PublishableEvent:
    if claim.event_type not in _ROUTING_KEYS:
        raise StoredEventError("unsupported_type")
    if claim.event_version != 1:
        raise StoredEventError("unsupported_version")
    envelope = {
        "event_id": claim.id,
        "event_type": claim.event_type,
        "event_version": claim.event_version,
        "occurred_at": claim.occurred_at,
        "aggregate_id": claim.aggregate_id,
    }
    try:
        payload = thaw_json(claim.payload)
        if claim.event_type == ROUTING_KEY:
            return IncidentCreated.model_validate(
                {
                    **envelope,
                    "payload": IncidentCreatedPayload.model_validate(payload),
                }
            )
        return TriageEnrichmentRequested.model_validate(
            {
                **envelope,
                "payload": TriageEnrichmentRequestedPayload.model_validate(payload),
            }
        )
    except ValidationError:
        # Validation errors may include sensitive input values.
        raise StoredEventError("invalid_event") from None


def _positive_timeout(timeout: float) -> None:
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")


async def declare_topology(
    channel: AbstractChannel,
    *,
    timeout: float,  # noqa: ASYNC109 - bounded internally
) -> AbstractExchange:
    """Idempotent declarations; broker precondition failures are never repaired.

    The caller provisions the vhost (normally signalforge) through infrastructure.
    AMQP declarations operate only inside the channel's already-selected vhost.
    """
    _positive_timeout(timeout)
    async with asyncio.timeout(timeout):
        exchange = await channel.declare_exchange(
            EXCHANGE_NAME,
            ExchangeType.DIRECT,
            durable=True,
            auto_delete=False,
            timeout=timeout,
        )
        incident_queue = await channel.declare_queue(
            QUEUE_NAME,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={"x-queue-type": "classic"},
            timeout=timeout,
        )
        await incident_queue.bind(exchange, routing_key=ROUTING_KEY, timeout=timeout)
        enrichment_queue = await channel.declare_queue(
            ENRICHMENT_QUEUE_NAME,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={"x-queue-type": "classic"},
            timeout=timeout,
        )
        await enrichment_queue.bind(
            exchange,
            routing_key=ENRICHMENT_ROUTING_KEY,
            timeout=timeout,
        )
        return exchange


async def _bounded_close(
    resource: AbstractConnection | AbstractChannel, budget: float
) -> None:
    # Cleanup is best effort, but a retired publisher can never be reused.
    with suppress(TimeoutError, *_TRANSPORT_ERRORS):
        async with asyncio.timeout(budget):
            await resource.close()


class RabbitMQPublisher:
    """One reusable robust connection/channel; sequential, confirmed publications.

    Use connect() and always close() in a finally block. Following an ambiguous
    failure this instance is retired: a future caller must open a new publisher.
    No event is retried here. Robust recovery is useful between publications.
    """

    def __init__(
        self,
        connection: AbstractRobustConnection,
        channel: RobustChannel,
        exchange: AbstractExchange,
        *,
        cleanup_timeout: float = 5,
    ) -> None:
        _positive_timeout(cleanup_timeout)
        self._connection = connection
        self._channel = channel
        self._exchange = exchange
        self._cleanup_timeout = cleanup_timeout
        self._retired = False
        # aiormq correlates returns by message_id: do not overlap same-ID retries.
        self._publish_lock = asyncio.Lock()

    @property
    def is_retired(self) -> bool:
        """Whether this instance is permanently unavailable for publication."""
        return self._retired

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        timeout: float,  # noqa: ASYNC109 - bounded internally
        cleanup_timeout: float = 5,
    ) -> Self:
        _positive_timeout(timeout)
        _positive_timeout(cleanup_timeout)
        try:
            # Same client as connect_robust, retained BEFORE connecting so startup
            # timeout/cancellation can close the background reconnect task too.
            connection = RobustConnection(URL(url))
        except ValueError:
            raise PublishFailedError(
                "Invalid RabbitMQ connection configuration", ambiguous=False
            ) from None
        channel: RobustChannel | None = None
        try:
            async with asyncio.timeout(timeout):
                await connection.connect(timeout=timeout)
                # aio-pika's abstract annotations omit RobustChannel.ready().
                channel = cast(
                    RobustChannel,
                    connection.channel(publisher_confirms=True, on_return_raises=True),
                )
                await channel.initialize()
                exchange = await declare_topology(channel, timeout=timeout)
            return cls(connection, channel, exchange, cleanup_timeout=cleanup_timeout)
        except BaseException as error:
            # Clean up cancellation/programming errors too, without disguising them.
            if channel is not None:
                await _bounded_close(channel, cleanup_timeout)
            await _bounded_close(connection, cleanup_timeout)
            if isinstance(error, TimeoutError):
                raise PublishFailedError(
                    "RabbitMQ startup timed out; nothing published", ambiguous=False
                ) from None
            if isinstance(error, _TRANSPORT_ERRORS):
                raise PublishFailedError(
                    "RabbitMQ startup or topology declaration failed; nothing published",
                    ambiguous=False,
                ) from None
            raise

    async def close(self) -> None:
        """Retire permanently; each resource gets at most cleanup_timeout seconds."""
        self._retired = True
        await _bounded_close(self._channel, self._cleanup_timeout)
        await _bounded_close(self._connection, self._cleanup_timeout)

    async def publish(self, claim: ClaimSnapshot, *, timeout: float) -> None:  # noqa: ASYNC109
        """Return only for positive confirmation, otherwise raise a sanitized error.

        timeout bounds lock acquisition, channel readiness, send, and confirmation.
        Failure cleanup may additionally take up to twice cleanup_timeout.
        No ownership/lease check or database settlement is performed here.
        """
        _positive_timeout(timeout)
        event = reconstruct_event(claim)
        routing_key = _ROUTING_KEYS[event.event_type]
        message = Message(
            event.model_dump_json().encode("utf-8"),
            message_id=str(event.event_id),
            type=event.event_type,
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=DeliveryMode.PERSISTENT,
        )
        try:
            async with asyncio.timeout(timeout):
                async with self._publish_lock:
                    if self._retired:
                        raise PublishFailedError(
                            "Publisher is retired; open a new instance", ambiguous=False
                        )
                    await self._channel.ready()
                    confirmation = await self._exchange.publish(
                        message,
                        routing_key=routing_key,
                        mandatory=True,
                        timeout=timeout,
                    )
                    if isinstance(confirmation, (Basic.Nack, Basic.Reject)):
                        raise PublishFailedError(
                            "Broker negatively acknowledged publication",
                            ambiguous=False,
                        )
                    if not isinstance(confirmation, Basic.Ack):
                        raise PublishFailedError(
                            "No positive publisher confirmation; outcome is unknown",
                            ambiguous=True,
                        )
        except PublishError:
            # aiormq raises this on mandatory return BEFORE processing any ACK.
            raise UnroutablePublishError() from None
        except DeliveryError:
            raise PublishFailedError(
                "Broker negatively acknowledged publication", ambiguous=False
            ) from None
        except TimeoutError:
            await self.close()
            raise PublishTimeoutError() from None
        except _TRANSPORT_ERRORS:
            await self.close()
            raise PublishFailedError(
                "RabbitMQ transport failed; publication outcome is unknown",
                ambiguous=True,
            ) from None
        except asyncio.CancelledError:
            await self.close()
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            # A cancelled library confirmation future is not caller cancellation.
            raise PublishFailedError(
                "Publication interrupted; outcome is unknown", ambiguous=True
            ) from None
        except PublishFailedError as error:
            if error.ambiguous:
                await self.close()
            raise
