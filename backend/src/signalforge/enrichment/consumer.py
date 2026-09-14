"""Process one triage.enrichment.requested v1 delivery; no broker runtime."""

import json
import logging
from enum import StrEnum
from typing import Literal
from uuid import UUID

from aio_pika import IncomingMessage
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.consumers.models import ProcessedEvent
from signalforge.db.errors import DatabaseTransportError
from signalforge.enrichment.domain import EnrichmentInput, EnrichmentResult
from signalforge.enrichment.models import TriageEnrichment
from signalforge.enrichment.provider import (
    EnrichmentProvider,
    PermanentEnrichmentError,
    ProviderFailureReason,
    TransientEnrichmentError,
)
from signalforge.observability.logging import bind_log_context
from signalforge.triage.events import TriageEnrichmentRequested

CONSUMER_NAME = "triage-enrichment-consumer"

logger = logging.getLogger(__name__)


class EnrichmentProcessingResult(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


class InvalidEnrichmentEventError(ValueError):
    """A terminal, sanitized enrichment-event validation failure."""

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
        super().__init__(f"Incoming enrichment event rejected: {reason}")


def decode_event(body: bytes) -> TriageEnrichmentRequested:
    try:
        data = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError:
        raise InvalidEnrichmentEventError("invalid_encoding") from None
    except json.JSONDecodeError:
        raise InvalidEnrichmentEventError("invalid_json") from None

    if not isinstance(data, dict):
        raise InvalidEnrichmentEventError("invalid_event")
    if "event_type" in data and data["event_type"] != "triage.enrichment.requested":
        raise InvalidEnrichmentEventError("unsupported_type")
    if "event_version" in data and data["event_version"] != 1:
        raise InvalidEnrichmentEventError("unsupported_version")
    try:
        return TriageEnrichmentRequested.model_validate(data)
    except ValidationError:
        raise InvalidEnrichmentEventError("invalid_event") from None


async def _already_processed(
    event_id: UUID,
    session_factory: async_sessionmaker[AsyncSession],
) -> bool:
    async with session_factory() as session:
        processed = await session.scalar(
            select(ProcessedEvent.event_id).where(
                ProcessedEvent.consumer_name == CONSUMER_NAME,
                ProcessedEvent.event_id == event_id,
            )
        )
    return processed is not None


async def _persist_result(
    event: TriageEnrichmentRequested,
    result: EnrichmentResult,
    provider: EnrichmentProvider,
    session_factory: async_sessionmaker[AsyncSession],
) -> EnrichmentProcessingResult:
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
            return EnrichmentProcessingResult.DUPLICATE

        session.add(
            TriageEnrichment(
                request_event_id=event.event_id,
                incident_id=event.aggregate_id,
                trigger_event_id=event.payload.trigger_event_id,
                provider=provider.provider_name,
                model=provider.model_name,
                summary=result.summary,
                category=result.category,
                suspected_component=result.suspected_component,
                investigation_steps=list(result.investigation_steps),
            )
        )
    return EnrichmentProcessingResult.PROCESSED


async def process_event(
    event: TriageEnrichmentRequested,
    provider: EnrichmentProvider,
    session_factory: async_sessionmaker[AsyncSession],
) -> EnrichmentProcessingResult:
    """Call the provider outside DB scope, then atomically persist receipt and result."""
    try:
        already_processed = await _already_processed(event.event_id, session_factory)
    except OSError:
        raise DatabaseTransportError() from None
    if already_processed:
        return EnrichmentProcessingResult.DUPLICATE

    enrichment_input = EnrichmentInput(
        incident_id=event.aggregate_id,
        source=event.payload.source,
        title=event.payload.title,
        description=event.payload.description,
        original_severity=event.payload.original_severity,
        priority=event.payload.priority,
        requires_human_review=event.payload.requires_human_review,
        incident_occurred_at=event.payload.incident_occurred_at,
    )
    try:
        result = EnrichmentResult.model_validate(
            await provider.enrich(enrichment_input)
        )
    except ValidationError:
        raise TransientEnrichmentError(ProviderFailureReason.INVALID_RESPONSE) from None
    try:
        return await _persist_result(event, result, provider, session_factory)
    except OSError:
        raise DatabaseTransportError() from None


async def handle_message(
    message: IncomingMessage,
    provider: EnrichmentProvider,
    session_factory: async_sessionmaker[AsyncSession],
) -> EnrichmentProcessingResult:
    """Apply one-message ACK/reject/requeue semantics around durable processing."""
    try:
        event = decode_event(message.body)
    except InvalidEnrichmentEventError:
        await message.reject(requeue=False)
        raise

    with bind_log_context(
        event_id=event.event_id,
        trigger_event_id=event.payload.trigger_event_id,
        event_type=event.event_type,
        event_version=event.event_version,
        incident_id=event.aggregate_id,
        consumer_name=CONSUMER_NAME,
    ):
        try:
            result = await process_event(event, provider, session_factory)
        except TransientEnrichmentError:
            await message.nack(requeue=True)
            raise
        except PermanentEnrichmentError:
            await message.reject(requeue=False)
            raise
        except (SQLAlchemyError, DatabaseTransportError):
            await message.nack(requeue=True)
            raise

        await message.ack()
        logger.info(
            "enrichment message handled",
            extra={
                "event": (
                    "enrichment_processed"
                    if result is EnrichmentProcessingResult.PROCESSED
                    else "enrichment_duplicate"
                )
            },
        )
        return result
