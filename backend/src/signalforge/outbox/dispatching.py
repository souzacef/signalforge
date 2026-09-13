"""PostgreSQL delivery-state primitives, with no publisher or hidden commits.

Use a short, dedicated caller-owned transaction for each operation. Commit claims
before using their snapshots outside the transaction. A returned snapshot/result
is provisional until commit; discard it on rollback. No network work belongs in
these transactions. Claim tokens fence database settlement, not external delivery.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import DateTime, and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Update

from signalforge.outbox.models import OutboxEvent

type FrozenJSON = (
    Mapping[str, FrozenJSON] | tuple[FrozenJSON, ...] | str | int | float | bool | None
)


def _freeze_json(value: JsonValue) -> FrozenJSON:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class ClaimSnapshot:
    """Detached event data; nested payload objects/arrays are read-only maps/tuples."""

    id: UUID
    claim_token: UUID
    attempt_count: int
    claimed_until: datetime
    event_type: str
    event_version: int
    aggregate_id: UUID
    payload: Mapping[str, FrozenJSON]
    occurred_at: datetime
    created_at: datetime


class DispatchErrorCode(StrEnum):
    """Allowlisted operational codes, never exception messages or connection URLs."""

    DISPATCH_FAILED = "dispatch_failed"
    DISPATCH_TIMEOUT = "dispatch_timeout"
    INVALID_EVENT = "invalid_event"
    PUBLISH_TIMEOUT = "publish_timeout"
    UNROUTABLE = "unroutable"


async def claim_pending(
    session: AsyncSession, *, batch_limit: int, lease_duration: timedelta
) -> tuple[ClaimSnapshot, ...]:
    """Lock eligible rows and allocate one fresh token per row; flush, never commit.

    attempt_count counts committed dispatch claims, not transmissions. Rollback
    discards an increment; a crash after commit still consumes that attempt.
    Ordering is deterministic within a selection, not global across claimers.
    """
    if batch_limit < 1:
        raise ValueError("batch_limit must be positive")
    if lease_duration <= timedelta(0):
        raise ValueError("lease_duration must be positive")

    now = (
        await session.execute(
            select(func.clock_timestamp(type_=DateTime(timezone=True)))
        )
    ).scalar_one()
    rows = (
        await session.scalars(
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.next_attempt_at <= now,
                or_(
                    and_(
                        OutboxEvent.claim_token.is_(None),
                        OutboxEvent.claimed_until.is_(None),
                    ),
                    and_(
                        OutboxEvent.claim_token.is_not(None),
                        OutboxEvent.claimed_until <= now,
                    ),
                ),
            )
            .order_by(
                OutboxEvent.next_attempt_at, OutboxEvent.created_at, OutboxEvent.id
            )
            .limit(batch_limit)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).all()

    claims = []
    for row in rows:
        row.claim_token = uuid4()
        row.claimed_until = now + lease_duration
        row.attempt_count += 1
        claims.append(
            ClaimSnapshot(
                id=row.id,
                claim_token=row.claim_token,
                attempt_count=row.attempt_count,
                claimed_until=row.claimed_until,
                event_type=row.event_type,
                event_version=row.event_version,
                aggregate_id=row.aggregate_id,
                payload=MappingProxyType(
                    {key: _freeze_json(value) for key, value in row.payload.items()}
                ),
                occurred_at=row.occurred_at,
                created_at=row.created_at,
            )
        )
    await session.flush()
    return tuple(claims)


def _owned_update(event_id: UUID, claim_token: UUID) -> Update:
    # clock_timestamp, unlike now(), is not frozen at transaction start.
    return (
        update(OutboxEvent)
        .where(
            OutboxEvent.id == event_id,
            OutboxEvent.claim_token == claim_token,
            OutboxEvent.published_at.is_(None),
            OutboxEvent.claimed_until > func.clock_timestamp(),
        )
        .returning(OutboxEvent.id)
        .execution_options(synchronize_session=False)
    )


async def settle_success(
    session: AsyncSession, *, event_id: UUID, claim_token: UUID
) -> bool:
    """Mark one actively owned intent published; False means ownership was lost."""
    result = await session.scalar(
        _owned_update(event_id, claim_token).values(
            published_at=func.clock_timestamp(),
            claim_token=None,
            claimed_until=None,
            last_error=None,
        )
    )
    return result is not None


async def settle_retry(
    session: AsyncSession,
    *,
    event_id: UUID,
    claim_token: UUID,
    delay: timedelta,
    error_code: DispatchErrorCode,
) -> bool:
    """Schedule a retry for an active owner, retaining the allocated attempt count."""
    if delay <= timedelta(0):
        raise ValueError("delay must be positive")
    if not isinstance(error_code, DispatchErrorCode):
        raise TypeError("error_code must be a DispatchErrorCode")
    result = await session.scalar(
        _owned_update(event_id, claim_token).values(
            next_attempt_at=func.clock_timestamp() + delay,
            last_error=error_code.value,
            claim_token=None,
            claimed_until=None,
        )
    )
    return result is not None


async def release_claim(
    session: AsyncSession, *, event_id: UUID, claim_token: UUID
) -> bool:
    """Release unused, actively owned work without changing its due time or count.

    Already-due work becomes eligible immediately, useful for graceful shutdown.
    Existing diagnostics are preserved. Stale/expired owners receive False.
    """
    result = await session.scalar(
        _owned_update(event_id, claim_token).values(
            claim_token=None, claimed_until=None
        )
    )
    return result is not None


def retry_delay(
    attempt_count: int,
    *,
    jitter: float,
    base_delay: timedelta = timedelta(seconds=2),
    max_delay: timedelta = timedelta(seconds=300),
) -> timedelta:
    """Pure equal-jitter backoff; caller supplies a random fraction in [0, 1].

    Double only until capped, regardless of how large attempt_count becomes.
    A one-microsecond floor respects timedelta resolution for tiny configurations.
    """
    if attempt_count < 1:
        raise ValueError("attempt_count must be positive")
    if base_delay <= timedelta(0) or max_delay <= timedelta(0):
        raise ValueError("base_delay and max_delay must be positive")
    if not 0 <= jitter <= 1:
        raise ValueError("jitter must be in [0, 1]")
    ceiling = min(base_delay, max_delay)
    for _ in range(attempt_count - 1):
        if ceiling >= max_delay - ceiling:
            ceiling = max_delay
            break
        ceiling *= 2
    return max(timedelta(microseconds=1), ceiling * (0.5 + jitter / 2))
