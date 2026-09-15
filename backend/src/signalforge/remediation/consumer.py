"""Safely process one remediation.execution.requested v1 delivery."""

import asyncio
import json
import logging
import math
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from aio_pika import IncomingMessage
from pydantic import ValidationError
from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.consumers.models import ProcessedEvent
from signalforge.core.config import get_remediation_execution_settings
from signalforge.db.errors import DatabaseTransportError
from signalforge.remediation.errors import (
    RemediationExecutionRejectedError,
    RemediationExecutionTimeoutError,
    RemediationExecutionTransportError,
    RemediationTargetNotAllowedError,
    UnsupportedRemediationActionError,
)
from signalforge.remediation.events import RemediationExecutionRequested
from signalforge.remediation.execution import (
    RemediationCommand,
    RemediationExecutionOutcome,
    RemediationExecutor,
)
from signalforge.remediation.models import (
    RemediationExecution,
    RemediationExecutionAttempt,
    RemediationExecutionAttemptStatus,
    RemediationExecutionStatus,
    RemediationFailureKind,
)

CONSUMER_NAME = "remediation-execution-consumer"
logger = logging.getLogger(__name__)


class ProcessingResult(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    IN_PROGRESS = "in_progress"


class InvalidEventError(ValueError):
    """A terminal event validation or durable-snapshot failure."""

    def __init__(
        self,
        reason: Literal[
            "invalid_encoding",
            "invalid_json",
            "unsupported_type",
            "unsupported_version",
            "invalid_event",
            "missing_execution",
            "snapshot_mismatch",
            "attempt_mismatch",
        ],
    ) -> None:
        self.reason = reason
        super().__init__(f"Incoming remediation event rejected: {reason}")


@dataclass(frozen=True, slots=True)
class PreparedAttempt:
    attempt_id: UUID
    command: RemediationCommand


@dataclass(frozen=True, slots=True)
class PreparationResult:
    processing_result: ProcessingResult
    prepared: PreparedAttempt | None = None


def decode_event(body: bytes) -> RemediationExecutionRequested:
    try:
        data = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError:
        raise InvalidEventError("invalid_encoding") from None
    except json.JSONDecodeError:
        raise InvalidEventError("invalid_json") from None

    if not isinstance(data, dict):
        raise InvalidEventError("invalid_event")
    if "event_type" in data and data["event_type"] != "remediation.execution.requested":
        raise InvalidEventError("unsupported_type")
    if "event_version" in data and data["event_version"] != 1:
        raise InvalidEventError("unsupported_version")
    try:
        return RemediationExecutionRequested.model_validate(data)
    except ValidationError:
        raise InvalidEventError("invalid_event") from None


async def _record_receipt(
    session: AsyncSession,
    event: RemediationExecutionRequested,
) -> None:
    await session.execute(
        insert(ProcessedEvent)
        .values(
            consumer_name=CONSUMER_NAME,
            event_id=event.event_id,
            event_type=event.event_type,
            event_version=event.event_version,
            aggregate_id=event.aggregate_id,
        )
        .on_conflict_do_nothing(
            index_elements=[ProcessedEvent.consumer_name, ProcessedEvent.event_id]
        )
    )


def _validate_snapshot(
    event: RemediationExecutionRequested,
    execution: RemediationExecution,
) -> None:
    if (
        event.payload.proposal_id != execution.proposal_id
        or event.payload.action_kind is not execution.action_kind
        or event.payload.target != execution.target
    ):
        raise InvalidEventError("snapshot_mismatch")


async def _get_attempt(
    session: AsyncSession,
    execution_id: UUID,
) -> RemediationExecutionAttempt | None:
    attempt: RemediationExecutionAttempt | None = await session.scalar(
        select(RemediationExecutionAttempt).where(
            RemediationExecutionAttempt.execution_id == execution_id,
            RemediationExecutionAttempt.attempt_number == 1,
        )
    )
    return attempt


async def prepare_execution_attempt(
    event: RemediationExecutionRequested,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_seconds: float,
) -> PreparationResult:
    """Commit the attempt barrier, or conservatively resolve a prior barrier."""
    if not math.isfinite(lease_seconds) or lease_seconds <= 0:
        raise ValueError("lease_seconds must be finite and positive")
    async with session_factory.begin() as session:
        execution = await session.scalar(
            select(RemediationExecution)
            .where(RemediationExecution.id == event.aggregate_id)
            .with_for_update()
        )
        if execution is None:
            raise InvalidEventError("missing_execution")
        _validate_snapshot(event, execution)
        database_now = await session.scalar(select(func.clock_timestamp()))
        assert database_now is not None

        if execution.status is RemediationExecutionStatus.REQUESTED:
            existing_attempt = await session.scalar(
                select(RemediationExecutionAttempt.id).where(
                    or_(
                        RemediationExecutionAttempt.execution_id == execution.id,
                        RemediationExecutionAttempt.request_event_id == event.event_id,
                    )
                )
            )
            if existing_attempt is not None:
                raise InvalidEventError("attempt_mismatch")
            attempt = RemediationExecutionAttempt(
                id=uuid4(),
                execution_id=execution.id,
                request_event_id=event.event_id,
                attempt_number=1,
                status=RemediationExecutionAttemptStatus.IN_PROGRESS,
                started_at=database_now,
                lease_expires_at=database_now + timedelta(seconds=lease_seconds),
                updated_at=database_now,
            )
            session.add(attempt)
            execution.status = RemediationExecutionStatus.IN_PROGRESS
            execution.updated_at = database_now
            await session.flush()
            return PreparationResult(
                ProcessingResult.PROCESSED,
                PreparedAttempt(
                    attempt_id=attempt.id,
                    command=RemediationCommand(
                        proposal_id=execution.proposal_id,
                        action_kind=execution.action_kind,
                        target=execution.target,
                    ),
                ),
            )

        current_attempt = await _get_attempt(session, execution.id)
        if (
            current_attempt is None
            or current_attempt.request_event_id != event.event_id
        ):
            raise InvalidEventError("attempt_mismatch")

        attempt = current_attempt
        if execution.status is RemediationExecutionStatus.IN_PROGRESS:
            if attempt.status is not RemediationExecutionAttemptStatus.IN_PROGRESS:
                raise InvalidEventError("attempt_mismatch")
            if (
                attempt.lease_expires_at is not None
                and attempt.lease_expires_at > database_now
            ):
                return PreparationResult(ProcessingResult.IN_PROGRESS)
            attempt.status = RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
            attempt.failure_kind = RemediationFailureKind.INTERRUPTED
            attempt.lease_expires_at = None
            attempt.completed_at = database_now
            attempt.updated_at = database_now
            execution.status = RemediationExecutionStatus.OUTCOME_UNKNOWN
            execution.completed_at = database_now
            execution.updated_at = database_now
            await _record_receipt(session, event)
            await session.flush()
            return PreparationResult(ProcessingResult.PROCESSED)

        if attempt.status.value != execution.status.value:
            raise InvalidEventError("attempt_mismatch")
        await _record_receipt(session, event)
        return PreparationResult(ProcessingResult.DUPLICATE)


async def _persist_terminal_result(
    event: RemediationExecutionRequested,
    attempt_id: UUID,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: RemediationExecutionAttemptStatus,
    failure_kind: RemediationFailureKind | None = None,
    http_status_code: int | None = None,
) -> ProcessingResult:
    async with session_factory.begin() as session:
        execution = await session.scalar(
            select(RemediationExecution)
            .where(RemediationExecution.id == event.aggregate_id)
            .with_for_update()
        )
        attempt = await session.scalar(
            select(RemediationExecutionAttempt)
            .where(RemediationExecutionAttempt.id == attempt_id)
            .with_for_update()
        )
        if execution is None or attempt is None:
            raise InvalidEventError("attempt_mismatch")
        if (
            attempt.execution_id != execution.id
            or attempt.request_event_id != event.event_id
        ):
            raise InvalidEventError("attempt_mismatch")
        if (
            execution.status is not RemediationExecutionStatus.IN_PROGRESS
            or attempt.status is not RemediationExecutionAttemptStatus.IN_PROGRESS
        ):
            await _record_receipt(session, event)
            return ProcessingResult.DUPLICATE

        database_now = await session.scalar(select(func.clock_timestamp()))
        assert database_now is not None
        attempt.status = status
        attempt.failure_kind = failure_kind
        attempt.http_status_code = http_status_code
        attempt.lease_expires_at = None
        attempt.completed_at = database_now
        attempt.updated_at = database_now
        execution.status = RemediationExecutionStatus(status.value)
        execution.completed_at = database_now
        execution.updated_at = database_now
        await _record_receipt(session, event)
        await session.flush()
    return ProcessingResult.PROCESSED


async def _mark_interrupted_best_effort(
    event: RemediationExecutionRequested,
    attempt_id: UUID,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    cleanup = asyncio.create_task(
        _persist_terminal_result(
            event,
            attempt_id,
            session_factory,
            status=RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN,
            failure_kind=RemediationFailureKind.INTERRUPTED,
        )
    )
    try:
        await asyncio.wait_for(asyncio.shield(cleanup), timeout=5.0)
    except Exception:
        logger.warning("interrupted remediation outcome could not be persisted")


async def process_event(
    event: RemediationExecutionRequested,
    executor: RemediationExecutor,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_seconds: float | None = None,
) -> ProcessingResult:
    if lease_seconds is None:
        lease_seconds = get_remediation_execution_settings().attempt_lease_seconds
    preparation = await prepare_execution_attempt(
        event, session_factory, lease_seconds=lease_seconds
    )
    if preparation.prepared is None:
        return preparation.processing_result

    prepared = preparation.prepared
    terminal_status: RemediationExecutionAttemptStatus
    failure_kind: RemediationFailureKind | None
    http_status_code: int | None
    try:
        result = await executor.execute(prepared.command)
        if result.outcome is not RemediationExecutionOutcome.SUCCEEDED:
            raise RuntimeError("executor returned an unsupported outcome")
    except asyncio.CancelledError:
        await _mark_interrupted_best_effort(event, prepared.attempt_id, session_factory)
        raise
    except RemediationTargetNotAllowedError:
        terminal_status = RemediationExecutionAttemptStatus.FAILED
        failure_kind = RemediationFailureKind.TARGET_NOT_ALLOWED
        http_status_code = None
    except UnsupportedRemediationActionError:
        terminal_status = RemediationExecutionAttemptStatus.FAILED
        failure_kind = RemediationFailureKind.UNSUPPORTED_ACTION
        http_status_code = None
    except RemediationExecutionRejectedError as error:
        if 300 <= error.status_code < 500:
            terminal_status = RemediationExecutionAttemptStatus.FAILED
            failure_kind = RemediationFailureKind.HTTP_REJECTED
            http_status_code = error.status_code
        elif 500 <= error.status_code <= 999:
            terminal_status = RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
            failure_kind = RemediationFailureKind.HTTP_SERVER_ERROR
            http_status_code = error.status_code
        else:
            terminal_status = RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
            failure_kind = RemediationFailureKind.UNEXPECTED
            http_status_code = None
    except RemediationExecutionTimeoutError:
        terminal_status = RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
        failure_kind = RemediationFailureKind.TIMEOUT
        http_status_code = None
    except RemediationExecutionTransportError:
        terminal_status = RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
        failure_kind = RemediationFailureKind.TRANSPORT
        http_status_code = None
    except Exception as error:
        logger.warning(
            "unexpected remediation executor failure",
            extra={"exception_type": type(error).__name__},
        )
        terminal_status = RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
        failure_kind = RemediationFailureKind.UNEXPECTED
        http_status_code = None
    else:
        terminal_status = RemediationExecutionAttemptStatus.SUCCEEDED
        failure_kind = None
        http_status_code = None

    return await _persist_terminal_result(
        event,
        prepared.attempt_id,
        session_factory,
        status=terminal_status,
        failure_kind=failure_kind,
        http_status_code=http_status_code,
    )


async def handle_message(
    message: IncomingMessage,
    executor: RemediationExecutor,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_seconds: float | None = None,
) -> ProcessingResult:
    """Process and settle one delivery with remediation safety semantics."""
    try:
        event = decode_event(message.body)
    except InvalidEventError:
        await message.reject(requeue=False)
        raise

    try:
        result = await process_event(
            event, executor, session_factory, lease_seconds=lease_seconds
        )
    except InvalidEventError:
        await message.reject(requeue=False)
        raise
    except SQLAlchemyError:
        await message.nack(requeue=True)
        raise
    except OSError:
        await message.nack(requeue=True)
        raise DatabaseTransportError() from None

    if result is ProcessingResult.IN_PROGRESS:
        await message.nack(requeue=True)
    else:
        await message.ack()
    return result
