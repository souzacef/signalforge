import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from signalforge.db.session import engine
from signalforge.incidents.models import Incident, IncidentSeverity, IncidentStatus
from signalforge.observability.propagation import TraceContextCarrier
from signalforge.outbox.models import OutboxEvent
from signalforge.remediation import application
from signalforge.remediation.application import request_remediation_execution_with_event
from signalforge.remediation.errors import (
    RemediationExecutionAlreadyRequestedError,
    RemediationProposalNotApprovedForExecutionError,
    RemediationProposalNotFoundError,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationExecution,
    RemediationExecutionStatus,
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.users.models import User, UserRole

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)
EVENT_TYPE = "remediation.execution.requested"


type Seeded = tuple[Incident, User, User, RemediationProposal]


@pytest.fixture
async def seeded() -> AsyncIterator[Seeded]:
    incident = Incident(
        source="test",
        title="Checkout errors",
        severity=IncidentSeverity.HIGH,
        occurred_at=datetime.now(UTC),
    )
    proposer = User(
        email=f"execution-proposer-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.OPERATOR,
    )
    requester = User(
        email=f"execution-requester-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.ADMIN,
    )
    async with sessions() as session:
        session.add_all([incident, proposer, requester])
        await session.flush()
        proposal = RemediationProposal(
            incident_id=incident.id,
            action_kind=RemediationActionKind.RESTART_SERVICE,
            target="checkout-api",
            reason="Restart is the approved runbook response",
            status=RemediationProposalStatus.APPROVED,
            proposed_by_user_id=proposer.id,
            approved_by_user_id=requester.id,
            approved_at=datetime.now(UTC),
        )
        session.add(proposal)
        await session.commit()
    try:
        yield incident, proposer, requester, proposal
    finally:
        async with sessions.begin() as session:
            execution_ids = select(RemediationExecution.id).where(
                RemediationExecution.proposal_id == proposal.id
            )
            await session.execute(
                delete(OutboxEvent).where(
                    OutboxEvent.event_type == EVENT_TYPE,
                    OutboxEvent.aggregate_id.in_(execution_ids),
                )
            )
            await session.execute(
                delete(RemediationExecution).where(
                    RemediationExecution.proposal_id == proposal.id
                )
            )
            await session.execute(
                delete(RemediationProposal).where(RemediationProposal.id == proposal.id)
            )
            await session.execute(delete(Incident).where(Incident.id == incident.id))
            await session.execute(
                delete(User).where(User.id.in_([proposer.id, requester.id]))
            )


@pytest.fixture
def trace_context(monkeypatch: pytest.MonkeyPatch) -> Iterator[TraceContextCarrier]:
    context = TraceContextCarrier(
        traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        tracestate="vendor=value",
    )
    monkeypatch.setattr(application, "capture_current_trace_context", lambda: context)
    yield context


async def test_approved_proposal_persists_safe_execution_and_outbox_snapshot(
    seeded: Seeded,
    trace_context: TraceContextCarrier,
) -> None:
    incident, _, requester, proposal = seeded
    async with sessions() as session:
        response = await request_remediation_execution_with_event(
            session, proposal.id, requester.id
        )

    async with sessions() as session:
        execution = await session.scalar(
            select(RemediationExecution).where(
                RemediationExecution.proposal_id == proposal.id
            )
        )
        outbox = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.event_type == EVENT_TYPE,
                OutboxEvent.aggregate_id == response.id,
            )
        )
        stored_proposal = await session.get(RemediationProposal, proposal.id)
        stored_incident = await session.get(Incident, incident.id)

    assert execution is not None
    assert response.id == execution.id
    assert execution.proposal_id == proposal.id
    assert execution.action_kind is RemediationActionKind.RESTART_SERVICE
    assert execution.target == "checkout-api"
    assert execution.status is RemediationExecutionStatus.REQUESTED
    assert execution.requested_by_user_id == requester.id
    assert execution.requested_at.tzinfo is not None
    assert execution.completed_at is None
    assert execution.updated_at.tzinfo is not None
    assert set(execution.__table__.columns.keys()) == {
        "id",
        "proposal_id",
        "action_kind",
        "target",
        "status",
        "requested_by_user_id",
        "requested_at",
        "completed_at",
        "updated_at",
    }
    assert outbox is not None
    assert outbox.id != execution.id
    assert outbox.event_version == 1
    assert outbox.aggregate_id == execution.id
    assert outbox.occurred_at == execution.requested_at
    assert outbox.payload == {
        "proposal_id": str(proposal.id),
        "action_kind": "restart_service",
        "target": "checkout-api",
    }
    assert outbox.traceparent == trace_context.traceparent
    assert outbox.tracestate == trace_context.tracestate
    serialized = str(outbox.payload)
    assert proposal.reason not in serialized
    assert "https://actuator.internal/restart" not in serialized
    assert stored_proposal is not None
    assert stored_proposal.status is RemediationProposalStatus.APPROVED
    assert stored_incident is not None
    assert stored_incident.status is IncidentStatus.OPEN


@pytest.mark.parametrize(
    "proposal_status",
    [RemediationProposalStatus.PENDING_APPROVAL, RemediationProposalStatus.REJECTED],
)
async def test_non_approved_proposal_is_ineligible_without_persistence(
    seeded: Seeded,
    proposal_status: RemediationProposalStatus,
) -> None:
    _, _, requester, proposal = seeded
    async with sessions.begin() as session:
        stored = await session.get(RemediationProposal, proposal.id)
        assert stored is not None
        stored.status = proposal_status
        stored.approved_by_user_id = None
        stored.approved_at = None
        if proposal_status is RemediationProposalStatus.REJECTED:
            stored.rejected_by_user_id = requester.id
            stored.rejected_at = datetime.now(UTC)
            stored.rejection_reason = "No longer needed"

    async with sessions() as session:
        with pytest.raises(RemediationProposalNotApprovedForExecutionError):
            await request_remediation_execution_with_event(
                session, proposal.id, requester.id
            )
        await session.rollback()
    assert await count_execution_pairs(proposal.id) == (0, 0)


async def test_missing_proposal_is_rejected_without_persistence(seeded: Seeded) -> None:
    _, _, requester, _ = seeded
    async with sessions() as session:
        with pytest.raises(RemediationProposalNotFoundError):
            await request_remediation_execution_with_event(
                session, uuid4(), requester.id
            )
        await session.rollback()


async def count_execution_pairs(proposal_id: UUID) -> tuple[int, int]:
    async with sessions() as session:
        execution_ids = select(RemediationExecution.id).where(
            RemediationExecution.proposal_id == proposal_id
        )
        execution_count = await session.scalar(
            select(func.count())
            .select_from(RemediationExecution)
            .where(RemediationExecution.proposal_id == proposal_id)
        )
        event_count = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.event_type == EVENT_TYPE,
                OutboxEvent.aggregate_id.in_(execution_ids),
            )
        )
    return execution_count or 0, event_count or 0


async def test_duplicate_request_is_stable_and_creates_one_pair(seeded: Seeded) -> None:
    _, _, requester, proposal = seeded
    async with sessions() as session:
        await request_remediation_execution_with_event(
            session, proposal.id, requester.id
        )
    async with sessions() as session:
        with pytest.raises(RemediationExecutionAlreadyRequestedError):
            await request_remediation_execution_with_event(
                session, proposal.id, requester.id
            )
        assert not session.in_transaction()
    assert await count_execution_pairs(proposal.id) == (1, 1)


async def test_concurrent_requests_on_independent_connections_create_one_pair(
    seeded: Seeded,
) -> None:
    _, _, requester, proposal = seeded
    barrier = asyncio.Barrier(2)

    async def worker() -> tuple[int, str]:
        async with sessions() as session:
            pid = await session.scalar(select(func.pg_backend_pid()))
            await session.rollback()
            assert pid is not None
            await barrier.wait()
            try:
                await request_remediation_execution_with_event(
                    session, proposal.id, requester.id
                )
            except RemediationExecutionAlreadyRequestedError:
                assert not session.in_transaction()
                return pid, "duplicate"
            return pid, "requested"

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker())
        second = group.create_task(worker())
    outcomes = [first.result(), second.result()]
    assert outcomes[0][0] != outcomes[1][0]
    assert {result for _, result in outcomes} == {"requested", "duplicate"}
    assert await count_execution_pairs(proposal.id) == (1, 1)


async def test_outbox_integrity_failure_rolls_back_execution(
    seeded: Seeded,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, requester, proposal = seeded
    colliding_event_id = uuid4()
    execution_id = uuid4()
    async with sessions.begin() as session:
        session.add(
            OutboxEvent(
                id=colliding_event_id,
                event_type="test.existing",
                event_version=1,
                aggregate_id=uuid4(),
                payload={},
                occurred_at=datetime.now(UTC),
            )
        )
    generated = iter((execution_id, colliding_event_id))
    monkeypatch.setattr(application, "uuid4", lambda: next(generated))

    async with sessions() as session:
        with pytest.raises(IntegrityError):
            await request_remediation_execution_with_event(
                session, proposal.id, requester.id
            )
        assert not session.in_transaction()
    assert await count_execution_pairs(proposal.id) == (0, 0)
    async with sessions.begin() as session:
        await session.execute(
            delete(OutboxEvent).where(OutboxEvent.id == colliding_event_id)
        )


@pytest.mark.parametrize(
    ("column", "value", "constraint"),
    [
        (
            "action_kind",
            "shell_command",
            "remediation_execution_action_kind",
        ),
        (
            "target",
            "api; reboot",
            "ck_remediation_executions_target_logical_service",
        ),
        ("status", "running", "remediation_execution_status"),
    ],
)
async def test_database_rejects_invalid_execution_snapshot_values(
    seeded: Seeded,
    column: str,
    value: str,
    constraint: str,
) -> None:
    _, _, requester, proposal = seeded
    async with sessions() as session:
        response = await request_remediation_execution_with_event(
            session, proposal.id, requester.id
        )
    async with sessions() as session:
        with pytest.raises(IntegrityError, match=constraint):
            await session.execute(
                text(
                    f"UPDATE remediation_executions SET {column} = :value "
                    "WHERE id = :execution_id"
                ),
                {"value": value, "execution_id": response.id},
            )
            await session.commit()
        await session.rollback()


async def test_database_constraints_and_restrict_foreign_keys(seeded: Seeded) -> None:
    _, _, requester, proposal = seeded
    async with sessions() as session:
        await request_remediation_execution_with_event(
            session, proposal.id, requester.id
        )
    expected = {
        "pk_remediation_executions",
        "uq_remediation_executions_proposal_id",
        "remediation_execution_action_kind",
        "remediation_execution_status",
        "ck_remediation_executions_target_logical_service",
        "fk_remediation_executions_proposal_id_remediation_proposals",
        "fk_remediation_executions_requested_by_user_id_users",
    }
    async with sessions() as session:
        constraints = set(
            (
                await session.scalars(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE conrelid = 'remediation_executions'::regclass"
                    )
                )
            ).all()
        )
    assert expected <= constraints

    for table, row_id in (
        ("remediation_proposals", proposal.id),
        ("users", requester.id),
    ):
        async with sessions() as session:
            with pytest.raises(IntegrityError):
                await session.execute(
                    text(f"DELETE FROM {table} WHERE id = :row_id"),
                    {"row_id": row_id},
                )
                await session.commit()
            await session.rollback()
