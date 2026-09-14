"""One-shot claim, publish, and settlement orchestration."""

import math
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from enum import StrEnum
from random import random
from uuid import UUID

from opentelemetry.trace import Tracer
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.outbox.dispatching import (
    ClaimSnapshot,
    DispatchErrorCode,
    claim_pending,
    release_claim,
    retry_delay,
    settle_retry,
    settle_success,
)
from signalforge.outbox.models import OutboxEvent
from signalforge.outbox.rabbitmq import (
    PublishFailedError,
    PublishTimeoutError,
    RabbitMQPublisher,
    StoredEventError,
    UnroutablePublishError,
)

INVALID_EVENT_RETRY_DELAY = timedelta(hours=24)


class OwnershipStatus(StrEnum):
    OWNED = "owned"
    LOST = "lost"


class DispatchOutcome(StrEnum):
    PUBLISHED = "published"
    RETRY_SCHEDULED = "retry_scheduled"
    OWNERSHIP_LOST = "ownership_lost"
    RELEASED = "released"
    DATABASE_FAILED = "database_failed"


@dataclass(frozen=True, slots=True)
class EventDispatchResult:
    event_id: UUID
    outcome: DispatchOutcome
    event_type: str = "unknown"
    publication_confirmed: bool = False
    error_code: DispatchErrorCode | None = None


@dataclass(frozen=True, slots=True)
class DispatchBatchResult:
    claimed: int
    published: int
    retry_scheduled: int
    ownership_lost: int
    released: int
    failed: int
    publisher_retired: bool
    events: tuple[EventDispatchResult, ...]


async def check_publish_ownership(
    session: AsyncSession,
    claim: ClaimSnapshot,
    *,
    minimum_remaining: timedelta,
) -> OwnershipStatus:
    """Perform an unlocked, database-time ownership and lease-budget check."""
    if minimum_remaining <= timedelta(0):
        raise ValueError("minimum_remaining must be positive")
    owned = await session.scalar(
        select(OutboxEvent.id).where(
            OutboxEvent.id == claim.id,
            OutboxEvent.published_at.is_(None),
            OutboxEvent.claim_token == claim.claim_token,
            OutboxEvent.claimed_until >= func.clock_timestamp() + minimum_remaining,
        )
    )
    return OwnershipStatus.OWNED if owned is not None else OwnershipStatus.LOST


def _error_code(error: StoredEventError | PublishFailedError) -> DispatchErrorCode:
    if isinstance(error, StoredEventError):
        return DispatchErrorCode.INVALID_EVENT
    if isinstance(error, UnroutablePublishError):
        return DispatchErrorCode.UNROUTABLE
    if isinstance(error, PublishTimeoutError):
        return DispatchErrorCode.PUBLISH_TIMEOUT
    return DispatchErrorCode.DISPATCH_FAILED


async def _settle_publication_success(
    session_factory: async_sessionmaker[AsyncSession], claim: ClaimSnapshot
) -> EventDispatchResult:
    try:
        async with session_factory.begin() as session:
            settled = await settle_success(
                session, event_id=claim.id, claim_token=claim.claim_token
            )
    except SQLAlchemyError:
        return EventDispatchResult(
            claim.id,
            DispatchOutcome.DATABASE_FAILED,
            publication_confirmed=True,
        )
    if not settled:
        return EventDispatchResult(
            claim.id,
            DispatchOutcome.OWNERSHIP_LOST,
            publication_confirmed=True,
        )
    return EventDispatchResult(
        claim.id, DispatchOutcome.PUBLISHED, publication_confirmed=True
    )


async def _settle_publication_failure(
    session_factory: async_sessionmaker[AsyncSession],
    claim: ClaimSnapshot,
    error: StoredEventError | PublishFailedError,
    *,
    jitter_source: Callable[[], float],
    retry_base_delay: timedelta,
    retry_max_delay: timedelta,
) -> EventDispatchResult:
    code = _error_code(error)
    delay = (
        INVALID_EVENT_RETRY_DELAY
        if isinstance(error, StoredEventError)
        else retry_delay(
            claim.attempt_count,
            jitter=jitter_source(),
            base_delay=retry_base_delay,
            max_delay=retry_max_delay,
        )
    )
    try:
        async with session_factory.begin() as session:
            settled = await settle_retry(
                session,
                event_id=claim.id,
                claim_token=claim.claim_token,
                delay=delay,
                error_code=code,
            )
    except SQLAlchemyError:
        return EventDispatchResult(
            claim.id, DispatchOutcome.DATABASE_FAILED, error_code=code
        )
    if not settled:
        return EventDispatchResult(
            claim.id, DispatchOutcome.OWNERSHIP_LOST, error_code=code
        )
    return EventDispatchResult(
        claim.id, DispatchOutcome.RETRY_SCHEDULED, error_code=code
    )


async def _release_unstarted(
    session_factory: async_sessionmaker[AsyncSession],
    claims: tuple[ClaimSnapshot, ...],
) -> tuple[EventDispatchResult, ...]:
    results = []
    for claim in claims:
        try:
            async with session_factory.begin() as session:
                released = await release_claim(
                    session, event_id=claim.id, claim_token=claim.claim_token
                )
        except SQLAlchemyError:
            results.append(
                EventDispatchResult(claim.id, DispatchOutcome.DATABASE_FAILED)
            )
            continue
        results.append(
            EventDispatchResult(
                claim.id,
                DispatchOutcome.RELEASED
                if released
                else DispatchOutcome.OWNERSHIP_LOST,
            )
        )
    return tuple(results)


def _summary(
    claims: tuple[ClaimSnapshot, ...],
    results: list[EventDispatchResult],
    *,
    publisher_retired: bool,
) -> DispatchBatchResult:
    event_types = {claim.id: claim.event_type for claim in claims}
    completed_events = tuple(
        replace(item, event_type=event_types.get(item.event_id, "unknown"))
        for item in results
    )
    return DispatchBatchResult(
        claimed=len(claims),
        published=sum(item.outcome is DispatchOutcome.PUBLISHED for item in results),
        retry_scheduled=sum(
            item.outcome is DispatchOutcome.RETRY_SCHEDULED for item in results
        ),
        ownership_lost=sum(
            item.outcome is DispatchOutcome.OWNERSHIP_LOST for item in results
        ),
        released=sum(item.outcome is DispatchOutcome.RELEASED for item in results),
        failed=sum(item.outcome is DispatchOutcome.DATABASE_FAILED for item in results),
        publisher_retired=publisher_retired,
        events=completed_events,
    )


async def dispatch_batch(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: RabbitMQPublisher,
    *,
    batch_limit: int,
    lease_duration: timedelta,
    publish_timeout: float,
    settlement_budget: timedelta,
    jitter_source: Callable[[], float] = random,
    retry_base_delay: timedelta = timedelta(seconds=2),
    retry_max_delay: timedelta = timedelta(seconds=300),
    tracer: Tracer | None = None,
) -> DispatchBatchResult:
    """Run one sequential dispatch batch and return only sanitized outcomes.

    Claim commit errors propagate before publication begins. Each preflight and
    settlement uses a fresh short transaction; no database transaction spans a
    RabbitMQ await. Ambiguous publication retires the publisher, so the batch
    settles that failure and releases every unstarted claim before returning.
    """
    if not math.isfinite(publish_timeout) or publish_timeout <= 0:
        raise ValueError("publish_timeout must be finite and positive")
    if settlement_budget <= timedelta(0):
        raise ValueError("settlement_budget must be positive")
    minimum_remaining = timedelta(seconds=publish_timeout) + settlement_budget
    if lease_duration < minimum_remaining:
        raise ValueError("lease_duration must cover publication and settlement budgets")
    # Validate retry configuration before allocating durable claims.
    retry_delay(
        1,
        jitter=0.5,
        base_delay=retry_base_delay,
        max_delay=retry_max_delay,
    )
    if publisher.is_retired:
        raise RuntimeError("publisher is retired")

    async with session_factory.begin() as session:
        claims = await claim_pending(
            session, batch_limit=batch_limit, lease_duration=lease_duration
        )

    results: list[EventDispatchResult] = []
    publisher_retired = False
    for position, claim in enumerate(claims):
        async with session_factory.begin() as session:
            ownership = await check_publish_ownership(
                session, claim, minimum_remaining=minimum_remaining
            )
        if ownership is OwnershipStatus.LOST:
            results.append(
                EventDispatchResult(claim.id, DispatchOutcome.OWNERSHIP_LOST)
            )
            continue

        try:
            if tracer is None:
                await publisher.publish(claim, timeout=publish_timeout)
            else:
                await publisher.publish(claim, timeout=publish_timeout, tracer=tracer)
        except (StoredEventError, PublishFailedError) as error:
            results.append(
                await _settle_publication_failure(
                    session_factory,
                    claim,
                    error,
                    jitter_source=jitter_source,
                    retry_base_delay=retry_base_delay,
                    retry_max_delay=retry_max_delay,
                )
            )
            if isinstance(error, PublishFailedError) and (
                error.ambiguous or publisher.is_retired
            ):
                publisher_retired = True
                results.extend(
                    await _release_unstarted(session_factory, claims[position + 1 :])
                )
                break
        else:
            results.append(await _settle_publication_success(session_factory, claim))

    return _summary(claims, results, publisher_retired=publisher_retired)
