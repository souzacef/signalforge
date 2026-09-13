"""Process one incident.created v1 delivery; no consumer loop or broker setup."""

import json
from enum import StrEnum
from typing import Literal

from aio_pika import IncomingMessage
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.consumers.models import ProcessedEvent
from signalforge.incidents.events import IncidentCreated

CONSUMER_NAME = "incident-created-consumer"


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
    """Commit the receipt before reporting success; concurrent inserts are atomic."""
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
    return (
        ProcessingResult.PROCESSED
        if inserted_id is not None
        else ProcessingResult.DUPLICATE
    )


async def handle_message(
    message: IncomingMessage,
    session_factory: async_sessionmaker[AsyncSession],
) -> ProcessingResult:
    """ACK only after commit; reject poison, requeue transient DB failures."""
    try:
        event = decode_event(message.body)
    except InvalidEventError:
        await message.reject(requeue=False)
        raise

    try:
        result = await process_event(event, session_factory)
    except (SQLAlchemyError, OSError):
        # asyncpg may expose raw transport OSErrors before SQLAlchemy wraps them.
        await message.nack(requeue=True)
        raise

    await message.ack()
    return result
