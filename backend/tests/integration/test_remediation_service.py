import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from signalforge.db.session import engine
from signalforge.incidents.models import Incident, IncidentSeverity, IncidentStatus
from signalforge.remediation.errors import (
    DuplicatePendingRemediationProposalError,
    IncidentNotEligibleForRemediationError,
    IncidentNotFoundForRemediationError,
    InvalidRemediationTransitionError,
    RemediationProposalNotFoundError,
    RemediationSelfApprovalError,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.remediation.schemas import (
    RemediationProposalCreate,
    RemediationProposalReject,
)
from signalforge.remediation.service import (
    approve_remediation_proposal,
    create_remediation_proposal,
    get_remediation_proposal,
    reject_remediation_proposal,
)
from signalforge.users.models import User, UserRole

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)

type Seeded = tuple[Incident, User, User, User]


@pytest.fixture
async def seeded() -> AsyncIterator[Seeded]:
    incident = Incident(
        source="test",
        title="Checkout errors",
        severity=IncidentSeverity.HIGH,
        occurred_at=datetime.now(UTC),
    )
    users = [
        User(
            email=f"remediation-{uuid4()}@example.com",
            password_hash="test-only-hash",
            role=UserRole.OPERATOR,
        )
        for _ in range(3)
    ]
    async with sessions() as session:
        session.add_all([incident, *users])
        await session.commit()
    try:
        yield incident, users[0], users[1], users[2]
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(RemediationProposal).where(
                    RemediationProposal.incident_id == incident.id
                )
            )
            await session.execute(delete(Incident).where(Incident.id == incident.id))
            await session.execute(
                delete(User).where(User.id.in_([user.id for user in users]))
            )


def data(incident_id: UUID, target: str = "checkout-api") -> RemediationProposalCreate:
    return RemediationProposalCreate(
        incident_id=incident_id,
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target=target,
        reason="  Error rate is high  ",
    )


async def create(seeded: Seeded, target: str = "checkout-api") -> RemediationProposal:
    incident, proposer, _, _ = seeded
    async with sessions() as session:
        return await create_remediation_proposal(
            session, data(incident.id, target), proposer.id
        )


async def stored(proposal_id: UUID) -> RemediationProposal:
    async with sessions() as session:
        return await get_remediation_proposal(session, proposal_id)


async def count_proposals(incident_id: UUID) -> int:
    async with sessions() as session:
        result = await session.scalar(
            select(func.count())
            .select_from(RemediationProposal)
            .where(RemediationProposal.incident_id == incident_id)
        )
        assert result is not None
        return result


async def transition(
    proposal_id: UUID,
    actor_id: UUID,
    operation: str,
) -> RemediationProposal:
    async with sessions() as session:
        if operation == "approve":
            return await approve_remediation_proposal(session, proposal_id, actor_id)
        return await reject_remediation_proposal(
            session,
            proposal_id,
            actor_id,
            RemediationProposalReject(rejection_reason="  No longer needed  "),
        )


async def test_create_persists_pending_attribution_without_mutating_incident(
    seeded: Seeded,
) -> None:
    incident, proposer, _, _ = seeded
    proposal = await create(seeded)
    persisted = await stored(proposal.id)

    assert proposal.id == persisted.id
    assert persisted.incident_id == incident.id
    assert persisted.action_kind is RemediationActionKind.RESTART_SERVICE
    assert persisted.target == "checkout-api"
    assert persisted.reason == "Error rate is high"
    assert persisted.status is RemediationProposalStatus.PENDING_APPROVAL
    assert persisted.proposed_by_user_id == proposer.id
    assert persisted.approved_by_user_id is None
    assert persisted.approved_at is None
    assert persisted.rejected_by_user_id is None
    assert persisted.rejected_at is None
    assert persisted.rejection_reason is None
    assert persisted.created_at.tzinfo is not None
    assert persisted.updated_at.tzinfo is not None
    async with sessions() as session:
        unchanged = await session.get(Incident, incident.id)
    assert unchanged is not None
    assert unchanged.status is IncidentStatus.OPEN


async def test_missing_and_resolved_incidents_cannot_receive_proposals(
    seeded: Seeded,
) -> None:
    incident, proposer, _, _ = seeded
    async with sessions() as session:
        with pytest.raises(IncidentNotFoundForRemediationError):
            await create_remediation_proposal(session, data(uuid4()), proposer.id)
        await session.rollback()

    async with sessions.begin() as session:
        await session.execute(
            update(Incident)
            .where(Incident.id == incident.id)
            .values(status=IncidentStatus.RESOLVED)
        )
    async with sessions() as session:
        with pytest.raises(IncidentNotEligibleForRemediationError):
            await create_remediation_proposal(session, data(incident.id), proposer.id)
        await session.rollback()
    assert await count_proposals(incident.id) == 0


async def test_acknowledged_incident_can_receive_proposal(seeded: Seeded) -> None:
    incident, _, _, _ = seeded
    async with sessions.begin() as session:
        await session.execute(
            update(Incident)
            .where(Incident.id == incident.id)
            .values(status=IncidentStatus.ACKNOWLEDGED)
        )
    assert (await create(seeded)).status is RemediationProposalStatus.PENDING_APPROVAL


async def test_duplicate_pending_is_a_domain_error_but_history_is_allowed(
    seeded: Seeded,
) -> None:
    incident, _, reviewer, _ = seeded
    first = await create(seeded)
    with pytest.raises(DuplicatePendingRemediationProposalError):
        await create(seeded)
    await transition(first.id, reviewer.id, "approve")
    second = await create(seeded)
    await transition(second.id, reviewer.id, "reject")
    third = await create(seeded)
    other_target = await create(seeded, "checkout-worker")
    assert len({first.id, second.id, third.id, other_target.id}) == 4
    assert await count_proposals(incident.id) == 4


async def test_approval_records_review_and_rejects_self_or_repeat(
    seeded: Seeded,
) -> None:
    _, proposer, reviewer, _ = seeded
    proposal = await create(seeded)
    with pytest.raises(RemediationSelfApprovalError):
        await transition(proposal.id, proposer.id, "approve")
    approved = await transition(proposal.id, reviewer.id, "approve")
    assert approved.status is RemediationProposalStatus.APPROVED
    assert approved.approved_by_user_id == reviewer.id
    assert approved.approved_at is not None
    assert approved.approved_at.tzinfo is not None
    assert approved.updated_at == approved.approved_at
    assert (await stored(proposal.id)).approved_by_user_id == reviewer.id
    for operation in ("approve", "reject"):
        with pytest.raises(InvalidRemediationTransitionError):
            await transition(proposal.id, reviewer.id, operation)


async def test_rejection_allows_proposer_and_records_reason(
    seeded: Seeded,
) -> None:
    _, proposer, _, _ = seeded
    proposal = await create(seeded)
    rejected = await transition(proposal.id, proposer.id, "reject")
    assert rejected.status is RemediationProposalStatus.REJECTED
    assert rejected.rejected_by_user_id == proposer.id
    assert rejected.rejected_at is not None
    assert rejected.rejected_at.tzinfo is not None
    assert rejected.rejection_reason == "No longer needed"
    assert rejected.updated_at == rejected.rejected_at
    for operation in ("approve", "reject"):
        with pytest.raises(InvalidRemediationTransitionError):
            await transition(proposal.id, proposer.id, operation)


async def test_rejection_allows_a_different_human_actor(seeded: Seeded) -> None:
    _, _, reviewer, _ = seeded
    proposal = await create(seeded)
    rejected = await transition(proposal.id, reviewer.id, "reject")
    assert rejected.status is RemediationProposalStatus.REJECTED
    assert rejected.rejected_by_user_id == reviewer.id


async def test_missing_proposal_fails_get_and_both_transitions(
    seeded: Seeded,
) -> None:
    _, _, reviewer, _ = seeded
    missing_id = uuid4()
    async with sessions() as session:
        with pytest.raises(RemediationProposalNotFoundError):
            await get_remediation_proposal(session, missing_id)
        await session.rollback()
    for operation in ("approve", "reject"):
        with pytest.raises(RemediationProposalNotFoundError):
            await transition(missing_id, reviewer.id, operation)


async def test_rejection_reason_is_required_before_state_change(seeded: Seeded) -> None:
    proposal = await create(seeded)
    with pytest.raises(ValidationError):
        RemediationProposalReject(rejection_reason=" \t ")
    assert (
        await stored(proposal.id)
    ).status is RemediationProposalStatus.PENDING_APPROVAL


@pytest.mark.parametrize(
    ("column", "value", "constraint"),
    [
        ("action_kind", "shell_command", "remediation_action_kind"),
        ("target", "api; echo x", "ck_remediation_proposals_target_logical_service"),
        ("reason", " ", "ck_remediation_proposals_reason_bounded_trimmed"),
        ("reason", "\t", "ck_remediation_proposals_reason_bounded_trimmed"),
        (
            "status",
            "executing",
            "remediation_proposal_status|ck_remediation_proposals_status_attribution",
        ),
        (
            "approved_by_user_id",
            "proposer",
            "ck_remediation_proposals_no_self_approval",
        ),
        (
            "rejection_reason",
            "orphan",
            "ck_remediation_proposals_status_attribution",
        ),
    ],
)
async def test_database_rejects_invalid_domain_rows(
    seeded: Seeded, column: str, value: str, constraint: str
) -> None:
    _, proposer, _, _ = seeded
    proposal = await create(seeded)
    if value == "proposer":
        value = str(proposer.id)
    sql = text(
        f"UPDATE remediation_proposals SET {column} = :value WHERE id = :proposal_id"
    )
    async with sessions() as session:
        with pytest.raises(IntegrityError, match=constraint):
            await session.execute(sql, {"value": value, "proposal_id": proposal.id})
            await session.commit()
        await session.rollback()
    assert (
        await stored(proposal.id)
    ).status is RemediationProposalStatus.PENDING_APPROVAL


async def test_database_has_named_constraints_and_partial_unique_index() -> None:
    expected_constraints = {
        "pk_remediation_proposals",
        "remediation_action_kind",
        "remediation_proposal_status",
        "ck_remediation_proposals_target_logical_service",
        "ck_remediation_proposals_reason_bounded_trimmed",
        "ck_remediation_proposals_rejection_reason_bounded_trimmed",
        "ck_remediation_proposals_no_self_approval",
        "ck_remediation_proposals_status_attribution",
        "fk_remediation_proposals_incident_id_incidents",
        "fk_remediation_proposals_proposed_by_user_id_users",
        "fk_remediation_proposals_approved_by_user_id_users",
        "fk_remediation_proposals_rejected_by_user_id_users",
    }
    async with sessions() as session:
        constraints = set(
            (
                await session.scalars(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE conrelid = 'remediation_proposals'::regclass"
                    )
                )
            ).all()
        )
        index_definition = await session.scalar(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE schemaname = current_schema() "
                "AND tablename = 'remediation_proposals' "
                "AND indexname = "
                "'uq_remediation_proposals_pending_incident_action_target'"
            )
        )

    assert expected_constraints <= constraints
    assert index_definition is not None
    normalized_index = " ".join(index_definition.lower().split())
    assert "create unique index" in normalized_index
    assert "(incident_id, action_kind, target)" in normalized_index
    assert "where" in normalized_index
    assert "status" in normalized_index
    assert "pending_approval" in normalized_index


async def test_database_rejects_self_approval_even_with_complete_attribution(
    seeded: Seeded,
) -> None:
    _, proposer, _, _ = seeded
    proposal = await create(seeded)
    async with sessions() as session:
        with pytest.raises(
            IntegrityError, match="ck_remediation_proposals_no_self_approval"
        ):
            await session.execute(
                text(
                    "UPDATE remediation_proposals SET status = 'approved', "
                    "approved_by_user_id = :actor_id, approved_at = now() "
                    "WHERE id = :proposal_id"
                ),
                {"actor_id": proposer.id, "proposal_id": proposal.id},
            )
            await session.commit()
        await session.rollback()


async def test_foreign_keys_restrict_incident_and_actor_deletion(
    seeded: Seeded,
) -> None:
    incident, proposer, reviewer, rejector = seeded
    approved_proposal = await create(seeded)
    await transition(approved_proposal.id, reviewer.id, "approve")
    rejected_proposal = await create(seeded, "checkout-worker")
    await transition(rejected_proposal.id, rejector.id, "reject")
    for table, row_id in (
        ("incidents", incident.id),
        ("users", proposer.id),
        ("users", reviewer.id),
        ("users", rejector.id),
    ):
        async with sessions() as session:
            with pytest.raises(IntegrityError):
                await session.execute(
                    text(f"DELETE FROM {table} WHERE id = :row_id"),
                    {"row_id": row_id},
                )
                await session.commit()
            await session.rollback()


async def test_two_approvers_on_independent_connections_have_one_winner(
    seeded: Seeded,
) -> None:
    _, _, first_reviewer, second_reviewer = seeded
    proposal = await create(seeded)
    barrier = asyncio.Barrier(2)

    async def worker(actor_id: UUID) -> tuple[int, str]:
        async with sessions() as session:
            pid = await session.scalar(select(func.pg_backend_pid()))
            await session.rollback()
            assert pid is not None
            await barrier.wait()
            try:
                await approve_remediation_proposal(session, proposal.id, actor_id)
            except InvalidRemediationTransitionError:
                await session.rollback()
                return pid, "conflict"
            return pid, "approved"

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker(first_reviewer.id))
        second = group.create_task(worker(second_reviewer.id))
    results = [first.result(), second.result()]
    assert results[0][0] != results[1][0]
    assert {result for _, result in results} == {"approved", "conflict"}
    persisted = await stored(proposal.id)
    assert persisted.status is RemediationProposalStatus.APPROVED
    assert persisted.approved_by_user_id in {first_reviewer.id, second_reviewer.id}


async def test_approve_reject_race_has_one_durable_terminal_state(
    seeded: Seeded,
) -> None:
    _, _, approver, rejector = seeded
    proposal = await create(seeded)
    barrier = asyncio.Barrier(2)

    async def worker(operation: str, actor_id: UUID) -> tuple[int, str]:
        async with sessions() as session:
            pid = await session.scalar(select(func.pg_backend_pid()))
            await session.rollback()
            assert pid is not None
            await barrier.wait()
            try:
                if operation == "approve":
                    await approve_remediation_proposal(session, proposal.id, actor_id)
                else:
                    await reject_remediation_proposal(
                        session,
                        proposal.id,
                        actor_id,
                        RemediationProposalReject(rejection_reason="No longer needed"),
                    )
            except InvalidRemediationTransitionError:
                await session.rollback()
                return pid, "conflict"
            return pid, operation

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker("approve", approver.id))
        second = group.create_task(worker("reject", rejector.id))
    results = [first.result(), second.result()]
    assert results[0][0] != results[1][0]
    assert [result for _, result in results].count("conflict") == 1
    persisted = await stored(proposal.id)
    if persisted.status is RemediationProposalStatus.APPROVED:
        assert persisted.approved_by_user_id == approver.id
        assert persisted.rejected_by_user_id is None
    else:
        assert persisted.status is RemediationProposalStatus.REJECTED
        assert persisted.rejected_by_user_id == rejector.id
        assert persisted.approved_by_user_id is None


async def test_concurrent_duplicate_creation_is_prevented(
    seeded: Seeded,
) -> None:
    incident, proposer, _, _ = seeded
    barrier = asyncio.Barrier(2)

    async def worker() -> tuple[int, str]:
        async with sessions() as session:
            pid = await session.scalar(select(func.pg_backend_pid()))
            await session.rollback()
            assert pid is not None
            await barrier.wait()
            try:
                await create_remediation_proposal(
                    session, data(incident.id), proposer.id
                )
            except DuplicatePendingRemediationProposalError:
                return pid, "duplicate"
            return pid, "created"

    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        first = group.create_task(worker())
        second = group.create_task(worker())
    results = [first.result(), second.result()]
    assert results[0][0] != results[1][0]
    assert {result for _, result in results} == {"created", "duplicate"}
    assert await count_proposals(incident.id) == 1
