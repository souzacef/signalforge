"""Process one incident.created v1 delivery; no consumer loop or broker setup."""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Literal
from uuid import UUID, uuid4

from aio_pika import IncomingMessage
from opentelemetry.trace import SpanKind, Tracer
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.consumers.models import ProcessedEvent
from signalforge.db.errors import DatabaseTransportError
from signalforge.incidents.events import IncidentCreated
from signalforge.observability.logging import bind_log_context
from signalforge.observability.messaging import mark_span_error, messaging_attributes
from signalforge.observability.propagation import (
    capture_current_trace_context,
    extract_trace_context,
)
from signalforge.outbox.models import OutboxEvent
from signalforge.triage.events import (
    TriageEnrichmentRequested,
    TriageEnrichmentRequestedPayload,
)
from signalforge.triage.models import IncidentTriage
from signalforge.triage.rules import determine_triage

CONSUMER_NAME = "incident-created-consumer"
PROCESS_DESTINATION: Final = (
    "signalforge.events:incident.created:signalforge.incident-events"
)
PROCESS_SPAN_NAME: Final = f"process {PROCESS_DESTINATION}"

logger = logging.getLogger(__name__)


class ProcessingResult(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


class InvalidEventError(ValueError):
    """A terminal, sanitized event validation failure."""

    def __init__(
        self,
        reason: Literal[
            "invalid_encoding",
            "invalid_json",
            "unsupported_type",
            "unsupported_version",
            "invalid_event",
        ],
    ) -> None:
        self.reason = reason
        super().__init__(f"Incoming event rejected: {reason}")


def decode_event(body: bytes) -> IncidentCreated:
    """Validate UTF-8 JSON against the existing published event contract."""
    try:
        data = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError:
        raise InvalidEventError("invalid_encoding") from None
    except json.JSONDecodeError:
        raise InvalidEventError("invalid_json") from None

    if not isinstance(data, dict):
        raise InvalidEventError("invalid_event")
    if "event_type" in data and data["event_type"] != "incident.created":
        raise InvalidEventError("unsupported_type")
    if "event_version" in data and data["event_version"] != 1:
        raise InvalidEventError("unsupported_version")
    try:
        return IncidentCreated.model_validate(data)
    except ValidationError:
        raise InvalidEventError("invalid_event") from None


async def process_event(
    event: IncidentCreated,
    session_factory: async_sessionmaker[AsyncSession],
) -> ProcessingResult:
    """Atomically commit receipt, triage, and its enrichment-request intent."""
    async with session_factory.begin() as session:
        inserted_id = await session.scalar(
            insert(ProcessedEvent)
            .values(
                consumer_name=CONSUMER_NAME,
                event_id=event.event_id,
                event_type=event.event_type,
                event_version=event.event_version,
                aggregate_id=event.aggregate_id,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    ProcessedEvent.consumer_name,
                    ProcessedEvent.event_id,
                ]
            )
            .returning(ProcessedEvent.event_id)
        )
        if inserted_id is None:
            return ProcessingResult.DUPLICATE

        decision = determine_triage(event.payload.severity)
        triage = IncidentTriage(
            incident_id=event.aggregate_id,
            event_id=event.event_id,
            source=event.payload.source,
            original_severity=event.payload.severity,
            priority=decision.priority,
            requires_human_review=decision.requires_human_review,
        )
        session.add(triage)
        # Ensure the outbox failure seam occurs after both earlier inserts.
        await session.flush()

        enrichment_event = TriageEnrichmentRequested(
            event_id=uuid4(),
            occurred_at=datetime.now(UTC),
            aggregate_id=event.aggregate_id,
            payload=TriageEnrichmentRequestedPayload(
                trigger_event_id=event.event_id,
                source=event.payload.source,
                title=event.payload.title,
                description=event.payload.description,
                original_severity=event.payload.severity,
                priority=decision.priority,
                requires_human_review=decision.requires_human_review,
                incident_occurred_at=event.payload.incident_occurred_at,
            ),
        )
        trace_context = capture_current_trace_context()
        session.add(
            OutboxEvent(
                id=enrichment_event.event_id,
                event_type=enrichment_event.event_type,
                event_version=enrichment_event.event_version,
                aggregate_id=enrichment_event.aggregate_id,
                payload=enrichment_event.payload.model_dump(mode="json"),
                occurred_at=enrichment_event.occurred_at,
                traceparent=trace_context.traceparent,
                tracestate=trace_context.tracestate,
            )
        )
    return ProcessingResult.PROCESSED


async def _handle_message(
    message: IncomingMessage,
    session_factory: async_sessionmaker[AsyncSession],
) -> ProcessingResult:
    try:
        event = decode_event(message.body)
    except InvalidEventError:
        await message.reject(requeue=False)
        raise

    with bind_log_context(
        event_id=event.event_id,
        event_type=event.event_type,
        event_version=event.event_version,
        incident_id=event.aggregate_id,
        consumer_name=CONSUMER_NAME,
    ):
        try:
            result = await process_event(event, session_factory)
        except SQLAlchemyError:
            await message.nack(requeue=True)
            raise
        except OSError:
            # asyncpg may expose raw transport failures before SQLAlchemy wraps them.
            # Translate only after NACK succeeds so broker errors remain broker errors.
            await message.nack(requeue=True)
            raise DatabaseTransportError() from None

        await message.ack()
        logger.info(
            "consumer message handled",
            extra={
                "event": (
                    "message_processed"
                    if result is ProcessingResult.PROCESSED
                    else "message_duplicate"
                )
            },
        )
        return result


def _safe_message_id(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 36:
        return None
    try:
        normalized = str(UUID(value))
    except ValueError:
        return None
    return normalized if normalized == value.lower() else None


async def handle_message(
    message: IncomingMessage,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    tracer: Tracer | None = None,
) -> ProcessingResult:
    """Process and settle one delivery, optionally under one consumer span."""
    if tracer is None:
        return await _handle_message(message, session_factory)

    raw_headers = getattr(message, "headers", None)
    headers = raw_headers if isinstance(raw_headers, Mapping) else None
    parent_context = extract_trace_context(headers)
    attributes = messaging_attributes(
        operation_name="process",
        operation_type="process",
        destination_name=PROCESS_DESTINATION,
        routing_key="incident.created",
        message_id=_safe_message_id(getattr(message, "message_id", None)),
    )
    with tracer.start_as_current_span(
        PROCESS_SPAN_NAME,
        context=parent_context,
        kind=SpanKind.CONSUMER,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            return await _handle_message(message, session_factory)
        except BaseException as error:
            mark_span_error(span, error)
            raise
