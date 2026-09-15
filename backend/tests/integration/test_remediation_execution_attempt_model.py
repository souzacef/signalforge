from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from signalforge.db.session import engine
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationExecution,
    RemediationExecutionAttempt,
    RemediationExecutionAttemptStatus,
    RemediationExecutionStatus,
    RemediationFailureKind,
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.users.models import User, UserRole

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def execution() -> AsyncIterator[RemediationExecution]:
    incident = Incident(
        source="test",
        title="Attempt constraints",
        severity=IncidentSeverity.HIGH,
        occurred_at=datetime.now(UTC),
    )
    proposer = User(
        email=f"attempt-proposer-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.OPERATOR,
    )
    requester = User(
        email=f"attempt-requester-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.ADMIN,
    )
    async with sessions.begin() as session:
        session.add_all([incident, proposer, requester])
        await session.flush()
        proposal = RemediationProposal(
            incident_id=incident.id,
            action_kind=RemediationActionKind.RESTART_SERVICE,
            target="checkout-api",
            reason="Constraint test",
            status=RemediationProposalStatus.APPROVED,
            proposed_by_user_id=proposer.id,
            approved_by_user_id=requester.id,
            approved_at=datetime.now(UTC),
        )
        session.add(proposal)
        await session.flush()
        stored = RemediationExecution(
            proposal_id=proposal.id,
            action_kind=proposal.action_kind,
            target=proposal.target,
            requested_by_user_id=requester.id,
        )
        session.add(stored)
        await session.flush()
    try:
        yield stored
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(RemediationExecutionAttempt).where(
                    RemediationExecutionAttempt.execution_id == stored.id
                )
            )
            await session.execute(
                delete(RemediationExecution).where(RemediationExecution.id == stored.id)
            )
            await session.execute(
                delete(RemediationProposal).where(RemediationProposal.id == proposal.id)
            )
            await session.execute(delete(Incident).where(Incident.id == incident.id))
            await session.execute(
                delete(User).where(User.id.in_([proposer.id, requester.id]))
            )


def attempt(
    execution_id: object,
    **overrides: object,
) -> RemediationExecutionAttempt:
    now = datetime.now(UTC)
    values: dict[str, object] = {
        "execution_id": execution_id,
        "request_event_id": uuid4(),
        "attempt_number": 1,
        "status": RemediationExecutionAttemptStatus.IN_PROGRESS,
        "started_at": now,
        "lease_expires_at": now + timedelta(seconds=45),
        "completed_at": None,
        "failure_kind": None,
        "http_status_code": None,
    }
    values.update(overrides)
    return RemediationExecutionAttempt(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        (
            {"attempt_number": 0},
            "ck_remediation_execution_attempts_number_positive",
        ),
        (
            {"lease_expires_at": None},
            "ck_remediation_execution_attempts_status_attribution",
        ),
        (
            {
                "status": RemediationExecutionAttemptStatus.SUCCEEDED,
                "completed_at": datetime.now(UTC),
                "lease_expires_at": datetime.now(UTC),
            },
            "ck_remediation_execution_attempts_status_attribution",
        ),
        (
            {
                "status": RemediationExecutionAttemptStatus.SUCCEEDED,
                "completed_at": datetime.now(UTC),
                "lease_expires_at": None,
                "failure_kind": RemediationFailureKind.UNEXPECTED,
            },
            "ck_remediation_execution_attempts_failure_attribution",
        ),
        (
            {
                "status": RemediationExecutionAttemptStatus.FAILED,
                "completed_at": datetime.now(UTC),
                "lease_expires_at": None,
                "failure_kind": None,
            },
            "ck_remediation_execution_attempts_status_attribution",
        ),
        (
            {
                "status": RemediationExecutionAttemptStatus.FAILED,
                "completed_at": datetime.now(UTC),
                "lease_expires_at": None,
                "failure_kind": RemediationFailureKind.HTTP_REJECTED,
                "http_status_code": None,
            },
            "ck_remediation_execution_attempts_http_attribution",
        ),
        (
            {
                "status": RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN,
                "completed_at": datetime.now(UTC),
                "lease_expires_at": None,
                "failure_kind": RemediationFailureKind.TRANSPORT,
                "http_status_code": 500,
            },
            "ck_remediation_execution_attempts_http_attribution",
        ),
    ],
)
async def test_attempt_constraints_reject_inconsistent_rows(
    execution: RemediationExecution,
    overrides: dict[str, object],
    constraint: str,
) -> None:
    async with sessions() as session:
        session.add(attempt(execution.id, **overrides))
        with pytest.raises(IntegrityError, match=constraint):
            await session.commit()
        await session.rollback()


async def test_attempt_unique_constraints(execution: RemediationExecution) -> None:
    event_id = uuid4()
    async with sessions.begin() as session:
        session.add(attempt(execution.id, request_event_id=event_id))

    async with sessions() as session:
        session.add(attempt(execution.id, request_event_id=uuid4(), attempt_number=1))
        with pytest.raises(
            IntegrityError,
            match="uq_remediation_execution_attempts_execution_number",
        ):
            await session.commit()
        await session.rollback()

    async with sessions() as session:
        session.add(attempt(execution.id, request_event_id=event_id, attempt_number=2))
        with pytest.raises(
            IntegrityError,
            match="uq_remediation_execution_attempts_request_event_id",
        ):
            await session.commit()
        await session.rollback()


@pytest.mark.parametrize(
    ("status", "completed_at"),
    [
        (RemediationExecutionStatus.REQUESTED, datetime.now(UTC)),
        (RemediationExecutionStatus.IN_PROGRESS, datetime.now(UTC)),
        (RemediationExecutionStatus.SUCCEEDED, None),
        (RemediationExecutionStatus.FAILED, None),
        (RemediationExecutionStatus.OUTCOME_UNKNOWN, None),
    ],
)
async def test_execution_completion_constraint(
    execution: RemediationExecution,
    status: RemediationExecutionStatus,
    completed_at: datetime | None,
) -> None:
    async with sessions() as session:
        stored = await session.get(RemediationExecution, execution.id)
        assert stored is not None
        stored.status = status
        stored.completed_at = completed_at
        with pytest.raises(
            IntegrityError, match="ck_remediation_executions_status_completion"
        ):
            await session.commit()
        await session.rollback()
