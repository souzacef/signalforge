import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import FrozenInstanceError
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, inspect, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.db.session import engine
from signalforge.outbox.dispatching import (
    ClaimSnapshot,
    DispatchErrorCode,
    claim_pending,
    release_claim,
    settle_retry,
    settle_success,
)
from signalforge.outbox.models import OutboxEvent

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

LEASE = timedelta(minutes=5)
sessions = async_sessionmaker(engine, expire_on_commit=False)
type SeedEvents = Callable[[int], Awaitable[list[UUID]]]
IMMUTABLE_FIELDS = (
    "id",
    "event_type",
    "event_version",
    "aggregate_id",
    "payload",
    "occurred_at",
    "created_at",
    "traceparent",
    "tracestate",
)


@pytest.fixture
async def seed_events() -> AsyncIterator[SeedEvents]:
    """Real committed rows, independent connections, and cleanup of only our IDs.

    Deliberately does not use the shared-connection/savepoint fixture. Refuse to
    claim unrelated pre-existing work even in the explicitly named test database.
    """
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
    seeded_ids: list[UUID] = []

    async def seed(count: int) -> list[UUID]:
        ids = sorted(uuid4() for _ in range(count))
        seeded_ids.extend(ids)
        async with sessions.begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            for event_id in ids:
                session.add(
                    OutboxEvent(
                        id=event_id,
                        event_type="incident.created",
                        event_version=1,
                        aggregate_id=uuid4(),
                        payload={
                            "title": "original",
                            "nested": {"items": [1, {"ok": True}]},
                        },
                        occurred_at=now - timedelta(days=1),
                        traceparent="00-0af7651916cd43dd8448eb211c80319c-"
                        "b7ad6b7169203331-01",
                        tracestate="vendor=value",
                        created_at=now - timedelta(minutes=2),
                        next_attempt_at=now - timedelta(minutes=1),
                    )
                )
        return ids

    try:
        yield seed
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.id.in_(seeded_ids))
            )


async def read_event(event_id: UUID) -> dict[str, Any]:
    async with sessions() as session:
        return dict(
            (
                await session.execute(
                    select(OutboxEvent.__table__).where(OutboxEvent.id == event_id)
                )
            )
            .mappings()
            .one()
        )


def immutable(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row[field] for field in IMMUTABLE_FIELDS}


async def claim_one() -> ClaimSnapshot:
    async with sessions.begin() as session:
        claims = await claim_pending(session, batch_limit=1, lease_duration=LEASE)
    assert len(claims) == 1
    return claims[0]


async def assert_no_claims() -> None:
    async with sessions.begin() as session:
        assert await claim_pending(session, batch_limit=10, lease_duration=LEASE) == ()


async def change_state(event_id: UUID, **values: Any) -> None:
    async with sessions.begin() as session:
        await session.execute(
            update(OutboxEvent).where(OutboxEvent.id == event_id).values(**values)
        )


async def settle(
    session: AsyncSession, operation: str, event_id: UUID, token: UUID
) -> bool:
    if operation == "success":
        return await settle_success(session, event_id=event_id, claim_token=token)
    if operation == "retry":
        return await settle_retry(
            session,
            event_id=event_id,
            claim_token=token,
            delay=timedelta(seconds=30),
            error_code=DispatchErrorCode.DISPATCH_FAILED,
        )
    assert operation == "release"
    return await release_claim(session, event_id=event_id, claim_token=token)


async def test_concurrent_claimers_skip_locked_on_independent_connections(
    seed_events: SeedEvents,
) -> None:
    ids = await seed_events(4)
    before = {event_id: await read_event(event_id) for event_id in ids}
    barrier = asyncio.Barrier(2)

    async def worker() -> tuple[int, tuple[ClaimSnapshot, ...]]:
        async with sessions.begin() as session:
            pid = await session.scalar(select(func.pg_backend_pid()))
            await barrier.wait()
            claims = await claim_pending(session, batch_limit=2, lease_duration=LEASE)
            # Neither transaction can commit until both have claimed with locks held.
            await barrier.wait()
        return pid, claims

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker())
        second = group.create_task(worker())
    pid_a, claims_a = first.result()
    pid_b, claims_b = second.result()
    assert pid_a != pid_b
    assert len(claims_a) == len(claims_b) == 2
    assert {claim.id for claim in claims_a}.isdisjoint(claim.id for claim in claims_b)
    assert {claim.id for claim in (*claims_a, *claims_b)} == set(ids)
    assert len({claim.claim_token for claim in (*claims_a, *claims_b)}) == 4
    for claim in (*claims_a, *claims_b):
        row = await read_event(claim.id)
        assert row["claim_token"] == claim.claim_token != claim.id
        assert row["attempt_count"] == claim.attempt_count == 1
        assert row["claimed_until"] == claim.claimed_until
        assert row["next_attempt_at"] == before[claim.id]["next_attempt_at"]
        assert claim.traceparent == row["traceparent"]
        assert claim.tracestate == row["tracestate"]
        assert immutable(row) == immutable(before[claim.id])
    await assert_no_claims()


async def test_live_committed_lease_is_not_claimable_on_another_connection(
    seed_events: SeedEvents,
) -> None:
    await seed_events(1)
    async with engine.connect() as first, engine.connect() as second:
        async with AsyncSession(first) as owner, AsyncSession(second) as competitor:
            async with owner.begin():
                owner_pid = await owner.scalar(select(func.pg_backend_pid()))
                claim = (
                    await claim_pending(owner, batch_limit=1, lease_duration=LEASE)
                )[0]
            async with competitor.begin():
                assert (
                    await competitor.scalar(select(func.pg_backend_pid())) != owner_pid
                )
                assert (
                    await claim_pending(competitor, batch_limit=1, lease_duration=LEASE)
                    == ()
                )
    assert (await read_event(claim.id))["claim_token"] == claim.claim_token


@pytest.mark.parametrize("owner_state", ["wrong_token", "expired", "reclaimed"])
@pytest.mark.parametrize("operation", ["success", "retry", "release"])
async def test_stale_or_expired_owner_cannot_settle(
    seed_events: SeedEvents,
    owner_state: str,
    operation: str,
) -> None:
    await seed_events(1)
    original = await claim_one()
    token = original.claim_token
    if owner_state == "wrong_token":
        token = uuid4()
    else:
        await change_state(
            original.id, claimed_until=func.clock_timestamp() - timedelta(seconds=1)
        )
        if owner_state == "reclaimed":
            replacement = await claim_one()
            assert replacement.id == original.id
            assert replacement.claim_token != original.claim_token
            assert replacement.attempt_count == original.attempt_count + 1
            assert replacement.claimed_until > original.claimed_until
    before = await read_event(original.id)
    async with sessions.begin() as session:
        assert not await settle(session, operation, original.id, token)
    assert await read_event(original.id) == before


async def test_future_retry_becomes_claimable_only_when_due(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    await change_state(
        event_id, next_attempt_at=func.clock_timestamp() + timedelta(hours=1)
    )
    await assert_no_claims()
    await change_state(event_id, next_attempt_at=func.clock_timestamp())
    assert (await claim_one()).id == event_id


@pytest.mark.parametrize("operation", ["success", "retry", "release"])
async def test_settlement_does_not_use_stale_transaction_start_time(
    seed_events: SeedEvents,
    operation: str,
) -> None:
    await seed_events(1)
    claim = await claim_one()
    async with sessions.begin() as session:
        transaction_started = await session.scalar(select(func.now()))
        # Expire from another connection AFTER this settlement transaction began.
        # now() would incorrectly see the lease as valid; no wall-clock sleep needed.
        await change_state(claim.id, claimed_until=func.clock_timestamp())
        before = await read_event(claim.id)
        assert transaction_started < before["claimed_until"]
        assert not await settle(session, operation, claim.id, claim.claim_token)
    assert await read_event(claim.id) == before


async def test_published_row_is_never_claimed(seed_events: SeedEvents) -> None:
    (event_id,) = await seed_events(1)
    await change_state(event_id, published_at=func.clock_timestamp())
    before = await read_event(event_id)
    await assert_no_claims()
    assert await read_event(event_id) == before


async def test_success_settlement_clears_claim_and_error(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    await change_state(event_id, last_error=DispatchErrorCode.DISPATCH_FAILED.value)
    before = await read_event(event_id)
    claim = await claim_one()
    async with sessions.begin() as session:
        lower = await session.scalar(select(func.clock_timestamp()))
        assert await settle_success(
            session, event_id=event_id, claim_token=claim.claim_token
        )
        upper = await session.scalar(select(func.clock_timestamp()))
    row = await read_event(event_id)
    assert lower <= row["published_at"] <= upper
    assert row["claim_token"] is row["claimed_until"] is row["last_error"] is None
    assert row["attempt_count"] == claim.attempt_count
    assert row["next_attempt_at"] == before["next_attempt_at"]
    assert immutable(row) == immutable(before)
    await assert_no_claims()
    async with sessions.begin() as session:
        assert not await settle_success(
            session, event_id=event_id, claim_token=claim.claim_token
        )


async def test_retry_settlement_uses_database_time_and_safe_code(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    before = await read_event(event_id)
    claim = await claim_one()
    delay = timedelta(seconds=30)
    async with sessions.begin() as session:
        lower = await session.scalar(select(func.clock_timestamp()))
        assert await settle_retry(
            session,
            event_id=event_id,
            claim_token=claim.claim_token,
            delay=delay,
            error_code=DispatchErrorCode.DISPATCH_TIMEOUT,
        )
        upper = await session.scalar(select(func.clock_timestamp()))
    row = await read_event(event_id)
    assert lower + delay <= row["next_attempt_at"] <= upper + delay
    assert row["published_at"] is row["claim_token"] is row["claimed_until"] is None
    assert row["last_error"] == "dispatch_timeout"
    assert row["attempt_count"] == claim.attempt_count
    assert immutable(row) == immutable(before)
    await assert_no_claims()
    await change_state(event_id, next_attempt_at=func.clock_timestamp())
    replacement = await claim_one()
    assert replacement.claim_token != claim.claim_token
    assert replacement.attempt_count == 2


async def test_release_preserves_count_due_time_and_event_data(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    await change_state(event_id, last_error=DispatchErrorCode.DISPATCH_FAILED.value)
    claim = await claim_one()
    before = await read_event(event_id)
    async with sessions.begin() as session:
        assert await release_claim(
            session, event_id=event_id, claim_token=claim.claim_token
        )
    row = await read_event(event_id)
    assert row == {**before, "claim_token": None, "claimed_until": None}
    replacement = await claim_one()
    assert replacement.id == event_id
    assert replacement.claim_token != claim.claim_token
    assert replacement.attempt_count == 2


async def test_rolled_back_claim_does_not_allocate_a_durable_attempt(
    seed_events: SeedEvents,
) -> None:
    (event_id,) = await seed_events(1)
    before = await read_event(event_id)
    async with sessions() as session:
        (claim,) = await claim_pending(session, batch_limit=1, lease_duration=LEASE)
        assert claim.attempt_count == 1
        # Another transaction cannot see the uncommitted delivery metadata.
        assert await read_event(event_id) == before
        await session.rollback()
    assert await read_event(event_id) == before
    replacement = await claim_one()
    assert replacement.attempt_count == 1
    assert replacement.claim_token != claim.claim_token


async def test_snapshot_is_detached_and_deeply_immutable(
    seed_events: SeedEvents,
) -> None:
    await seed_events(1)
    claim = await claim_one()
    assert inspect(claim, raiseerr=False) is None
    with pytest.raises(FrozenInstanceError):
        claim.attempt_count = 99
    with pytest.raises(TypeError):
        claim.payload["title"] = "changed"
    with pytest.raises(TypeError):
        claim.payload["nested"]["items"][0] = 99
    with pytest.raises(TypeError):
        claim.payload["nested"]["items"][1]["ok"] = False
    async with sessions.begin() as session:
        assert await release_claim(
            session, event_id=claim.id, claim_token=claim.claim_token
        )
    assert (await claim_one()).attempt_count == 2
    assert claim.attempt_count == 1
    assert claim.payload["title"] == "original"


async def test_claim_order_is_due_time_then_creation_then_id(
    seed_events: SeedEvents,
) -> None:
    ids = await seed_events(4)
    # All start tied. Move the last UUID first by due time, then the next by creation.
    await change_state(
        ids[3], next_attempt_at=func.clock_timestamp() - timedelta(days=2)
    )
    await change_state(ids[2], created_at=func.clock_timestamp() - timedelta(days=1))
    async with sessions.begin() as session:
        claims = await claim_pending(session, batch_limit=3, lease_duration=LEASE)
    assert [claim.id for claim in claims] == [ids[3], ids[2], ids[0]]
    assert (await claim_one()).id == ids[1]


@pytest.mark.parametrize(
    ("values", "constraint"),
    [
        ({"attempt_count": -1}, "ck_outbox_events_attempt_count_nonnegative"),
        ({"claim_token": uuid4()}, "ck_outbox_events_claim_pair"),
        ({"claimed_until": func.now()}, "ck_outbox_events_claim_pair"),
        (
            {
                "claim_token": uuid4(),
                "claimed_until": func.now(),
                "published_at": func.now(),
            },
            "ck_outbox_events_published_unclaimed",
        ),
    ],
)
async def test_database_enforces_delivery_constraints(
    seed_events: SeedEvents,
    values: dict[str, Any],
    constraint: str,
) -> None:
    (event_id,) = await seed_events(1)
    before = await read_event(event_id)
    with pytest.raises(IntegrityError, match=constraint):
        await change_state(event_id, **values)
    assert await read_event(event_id) == before


@pytest.mark.parametrize("operation", ["success", "retry", "release"])
async def test_settlement_is_provisional_until_caller_commit(
    seed_events: SeedEvents,
    operation: str,
) -> None:
    (event_id,) = await seed_events(1)
    claim = await claim_one()
    before = await read_event(event_id)
    async with sessions() as session:
        assert await settle(session, operation, event_id, claim.claim_token)
        assert await read_event(event_id) == before
        await session.rollback()
    assert await read_event(event_id) == before


@pytest.mark.parametrize("batch_limit", [0, -1])
async def test_claim_rejects_invalid_batch_limit(batch_limit: int) -> None:
    async with sessions() as session:
        with pytest.raises(ValueError, match="batch_limit"):
            await claim_pending(session, batch_limit=batch_limit, lease_duration=LEASE)
        assert not session.in_transaction()


@pytest.mark.parametrize("duration", [timedelta(0), timedelta(seconds=-1)])
async def test_claim_and_retry_reject_nonpositive_duration(duration: timedelta) -> None:
    async with sessions() as session:
        with pytest.raises(ValueError, match="lease_duration"):
            await claim_pending(session, batch_limit=1, lease_duration=duration)
        with pytest.raises(ValueError, match="delay"):
            await settle_retry(
                session,
                event_id=uuid4(),
                claim_token=uuid4(),
                delay=duration,
                error_code=DispatchErrorCode.DISPATCH_FAILED,
            )
        assert not session.in_transaction()


async def test_retry_rejects_arbitrary_exception_text(seed_events: SeedEvents) -> None:
    (event_id,) = await seed_events(1)
    claim = await claim_one()
    before = await read_event(event_id)
    async with sessions() as session:
        with pytest.raises(TypeError, match="DispatchErrorCode"):
            await settle_retry(
                session,
                event_id=event_id,
                claim_token=claim.claim_token,
                delay=timedelta(seconds=2),
                error_code="connection failed: postgresql://user:secret@host/db",
            )
        assert not session.in_transaction()
    assert await read_event(event_id) == before
