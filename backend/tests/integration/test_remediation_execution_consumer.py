import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest
from aio_pika import IncomingMessage
from sqlalchemy import delete, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker

from signalforge.consumers.models import ProcessedEvent
from signalforge.db.session import engine
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.remediation import consumer
from signalforge.remediation.consumer import ProcessingResult, handle_message
from signalforge.remediation.errors import (
    RemediationExecutionRejectedError,
    RemediationExecutionTimeoutError,
    RemediationExecutionTransportError,
    RemediationTargetNotAllowedError,
    UnsupportedRemediationActionError,
)
from signalforge.remediation.events import (
    RemediationExecutionRequested,
    RemediationExecutionRequestedPayload,
)
from signalforge.remediation.execution import (
    RemediationCommand,
    RemediationExecutionOutcome,
    RemediationExecutionResult,
)
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


@dataclass
class FakeMessage:
    body: bytes
    ack_count: int = 0
    nack_count: int = 0
    reject_count: int = 0
    requeue: bool | None = None

    async def ack(self) -> None:
        self.ack_count += 1

    async def nack(self, *, requeue: bool) -> None:
        self.nack_count += 1
        self.requeue = requeue

    async def reject(self, *, requeue: bool) -> None:
        self.reject_count += 1
        self.requeue = requeue


class BlockingExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def execute(self, command: RemediationCommand) -> RemediationExecutionResult:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return RemediationExecutionResult(
            proposal_id=command.proposal_id,
            action_kind=command.action_kind,
            target=command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )


class RaisingExecutor:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    async def execute(self, command: RemediationCommand) -> RemediationExecutionResult:
        self.calls += 1
        raise self.error


class ImmediateExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, command: RemediationCommand) -> RemediationExecutionResult:
        self.calls += 1
        return RemediationExecutionResult(
            proposal_id=command.proposal_id,
            action_kind=command.action_kind,
            target=command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )


type Seeded = tuple[RemediationExecutionRequested, RemediationExecution]


@pytest.fixture
async def seeded() -> AsyncIterator[Seeded]:
    incident = Incident(
        source="test",
        title="Checkout errors",
        severity=IncidentSeverity.HIGH,
        occurred_at=datetime.now(UTC),
    )
    proposer = User(
        email=f"processor-proposer-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.OPERATOR,
    )
    requester = User(
        email=f"processor-requester-{uuid4()}@example.com",
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
            reason="Restart is the approved runbook response",
            status=RemediationProposalStatus.APPROVED,
            proposed_by_user_id=proposer.id,
            approved_by_user_id=requester.id,
            approved_at=datetime.now(UTC),
        )
        session.add(proposal)
        await session.flush()
        execution = RemediationExecution(
            proposal_id=proposal.id,
            action_kind=proposal.action_kind,
            target=proposal.target,
            requested_by_user_id=requester.id,
        )
        session.add(execution)
        await session.flush()
        event = RemediationExecutionRequested(
            event_id=uuid4(),
            occurred_at=execution.requested_at,
            aggregate_id=execution.id,
            payload=RemediationExecutionRequestedPayload(
                proposal_id=execution.proposal_id,
                action_kind=execution.action_kind,
                target=execution.target,
            ),
        )
    try:
        yield event, execution
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
            )
            await session.execute(
                delete(RemediationExecutionAttempt).where(
                    RemediationExecutionAttempt.execution_id == execution.id
                )
            )
            await session.execute(
                delete(RemediationExecution).where(
                    RemediationExecution.id == execution.id
                )
            )
            await session.execute(
                delete(RemediationProposal).where(RemediationProposal.id == proposal.id)
            )
            await session.execute(delete(Incident).where(Incident.id == incident.id))
            await session.execute(
                delete(User).where(User.id.in_([proposer.id, requester.id]))
            )


def message_for(event: RemediationExecutionRequested) -> FakeMessage:
    return FakeMessage(event.model_dump_json().encode())


async def load_state(
    execution_id: object,
) -> tuple[RemediationExecution, RemediationExecutionAttempt]:
    async with sessions() as session:
        execution = await session.get(RemediationExecution, execution_id)
        attempt = await session.scalar(
            select(RemediationExecutionAttempt).where(
                RemediationExecutionAttempt.execution_id == execution_id
            )
        )
    assert execution is not None
    assert attempt is not None
    return execution, attempt


async def test_attempt_is_committed_and_unlocked_before_executor_io(
    seeded: Seeded,
) -> None:
    event, execution_seed = seeded
    message = message_for(event)
    executor = BlockingExecutor()
    task = asyncio.create_task(
        handle_message(cast(IncomingMessage, message), executor, sessions)
    )
    await executor.entered.wait()

    async with sessions.begin() as session:
        execution = await session.scalar(
            select(RemediationExecution)
            .where(RemediationExecution.id == execution_seed.id)
            .with_for_update(nowait=True)
        )
        attempt = await session.scalar(
            select(RemediationExecutionAttempt)
            .where(RemediationExecutionAttempt.execution_id == execution_seed.id)
            .with_for_update(nowait=True)
        )
        assert execution is not None
        assert execution.status is RemediationExecutionStatus.IN_PROGRESS
        assert execution.completed_at is None
        assert attempt is not None
        assert attempt.status is RemediationExecutionAttemptStatus.IN_PROGRESS
        assert attempt.lease_expires_at is not None
        assert (
            await session.get(ProcessedEvent, (consumer.CONSUMER_NAME, event.event_id))
            is None
        )

    executor.release.set()
    assert await task is ProcessingResult.PROCESSED
    execution, attempt = await load_state(execution_seed.id)
    assert execution.status is RemediationExecutionStatus.SUCCEEDED
    assert execution.completed_at is not None
    assert attempt.status is RemediationExecutionAttemptStatus.SUCCEEDED
    assert attempt.completed_at is not None
    assert attempt.lease_expires_at is None
    assert message.ack_count == 1
    assert executor.calls == 1

    duplicate = message_for(event)
    assert (
        await handle_message(cast(IncomingMessage, duplicate), executor, sessions)
        is ProcessingResult.DUPLICATE
    )
    assert duplicate.ack_count == 1
    assert executor.calls == 1
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(RemediationExecutionAttempt)
                .where(RemediationExecutionAttempt.execution_id == execution_seed.id)
            )
            == 1
        )
        assert (
            await session.get(ProcessedEvent, (consumer.CONSUMER_NAME, event.event_id))
            is not None
        )


async def test_concurrent_duplicate_requeues_without_second_call(
    seeded: Seeded,
) -> None:
    event, execution_seed = seeded
    first_message = message_for(event)
    second_message = message_for(event)
    executor = BlockingExecutor()
    first = asyncio.create_task(
        handle_message(cast(IncomingMessage, first_message), executor, sessions)
    )
    await executor.entered.wait()

    assert (
        await handle_message(cast(IncomingMessage, second_message), executor, sessions)
        is ProcessingResult.IN_PROGRESS
    )
    assert second_message.nack_count == 1
    assert second_message.requeue is True
    current_execution, current_attempt = await load_state(execution_seed.id)
    assert current_execution.status is RemediationExecutionStatus.IN_PROGRESS
    assert current_attempt.status is RemediationExecutionAttemptStatus.IN_PROGRESS

    executor.release.set()
    assert await first is ProcessingResult.PROCESSED
    assert executor.calls == 1
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(RemediationExecutionAttempt)
                .where(RemediationExecutionAttempt.execution_id == execution_seed.id)
            )
            == 1
        )


async def test_post_actuator_database_failure_redelivery_never_calls_again(
    seeded: Seeded,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, execution_seed = seeded
    executor = ImmediateExecutor()
    first_message = message_for(event)
    original_persist = consumer._persist_terminal_result

    async def fail_persistence(*args: object, **kwargs: object) -> ProcessingResult:
        raise SQLAlchemyError("simulated terminal write outage")

    monkeypatch.setattr(consumer, "_persist_terminal_result", fail_persistence)
    with pytest.raises(SQLAlchemyError):
        await handle_message(cast(IncomingMessage, first_message), executor, sessions)
    assert first_message.nack_count == 1
    assert first_message.requeue is True
    assert executor.calls == 1
    execution, attempt = await load_state(execution_seed.id)
    assert execution.status is RemediationExecutionStatus.IN_PROGRESS
    assert attempt.status is RemediationExecutionAttemptStatus.IN_PROGRESS

    monkeypatch.setattr(consumer, "_persist_terminal_result", original_persist)
    async with sessions.begin() as session:
        stored_attempt = await session.get(RemediationExecutionAttempt, attempt.id)
        assert stored_attempt is not None
        stored_attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)

    redelivery = message_for(event)
    assert (
        await handle_message(cast(IncomingMessage, redelivery), executor, sessions)
        is ProcessingResult.PROCESSED
    )
    execution, attempt = await load_state(execution_seed.id)
    assert execution.status is RemediationExecutionStatus.OUTCOME_UNKNOWN
    assert attempt.status is RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
    assert attempt.failure_kind is RemediationFailureKind.INTERRUPTED
    assert executor.calls == 1
    assert redelivery.ack_count == 1


@pytest.mark.parametrize(
    ("error", "expected_status", "failure_kind", "http_status"),
    [
        (
            RemediationTargetNotAllowedError(),
            RemediationExecutionStatus.FAILED,
            RemediationFailureKind.TARGET_NOT_ALLOWED,
            None,
        ),
        (
            UnsupportedRemediationActionError(),
            RemediationExecutionStatus.FAILED,
            RemediationFailureKind.UNSUPPORTED_ACTION,
            None,
        ),
        (
            RemediationExecutionRejectedError(400),
            RemediationExecutionStatus.FAILED,
            RemediationFailureKind.HTTP_REJECTED,
            400,
        ),
        (
            RemediationExecutionRejectedError(500),
            RemediationExecutionStatus.OUTCOME_UNKNOWN,
            RemediationFailureKind.HTTP_SERVER_ERROR,
            500,
        ),
        (
            RemediationExecutionTimeoutError(),
            RemediationExecutionStatus.OUTCOME_UNKNOWN,
            RemediationFailureKind.TIMEOUT,
            None,
        ),
        (
            RemediationExecutionTransportError(),
            RemediationExecutionStatus.OUTCOME_UNKNOWN,
            RemediationFailureKind.TRANSPORT,
            None,
        ),
        (
            RuntimeError("sensitive details"),
            RemediationExecutionStatus.OUTCOME_UNKNOWN,
            RemediationFailureKind.UNEXPECTED,
            None,
        ),
    ],
)
async def test_executor_failure_classification(
    seeded: Seeded,
    error: Exception,
    expected_status: RemediationExecutionStatus,
    failure_kind: RemediationFailureKind,
    http_status: int | None,
) -> None:
    event, execution_seed = seeded
    message = message_for(event)
    executor = RaisingExecutor(error)
    assert (
        await handle_message(cast(IncomingMessage, message), executor, sessions)
        is ProcessingResult.PROCESSED
    )
    execution, attempt = await load_state(execution_seed.id)
    assert execution.status is expected_status
    assert execution.completed_at is not None
    assert attempt.status.value == expected_status.value
    assert attempt.failure_kind is failure_kind
    assert attempt.http_status_code == http_status
    assert attempt.completed_at is not None
    assert attempt.lease_expires_at is None
    assert message.ack_count == 1
    assert executor.calls == 1


@pytest.mark.parametrize("tampering", ["aggregate_id", "proposal_id", "target"])
async def test_tampered_or_missing_snapshot_rejects_without_actuator(
    seeded: Seeded,
    tampering: str,
) -> None:
    event, _ = seeded
    if tampering == "aggregate_id":
        tampered = event.model_copy(update={"aggregate_id": uuid4()})
    elif tampering == "proposal_id":
        tampered = event.model_copy(
            update={
                "payload": event.payload.model_copy(update={"proposal_id": uuid4()})
            }
        )
    else:
        tampered = event.model_copy(
            update={
                "payload": event.payload.model_copy(update={"target": "billing-api"})
            }
        )
    message = message_for(tampered)
    executor = ImmediateExecutor()

    with pytest.raises(consumer.InvalidEventError):
        await handle_message(cast(IncomingMessage, message), executor, sessions)

    assert message.reject_count == 1
    assert message.requeue is False
    assert executor.calls == 0


@pytest.mark.parametrize(
    "body",
    [
        b"{",
        b"[]",
        b'{"event_type":"other"}',
        b'{"event_type":"remediation.execution.requested","event_version":2}',
        (
            b'{"event_id":"00000000-0000-0000-0000-000000000001",'
            b'"event_type":"remediation.execution.requested","event_version":1,'
            b'"occurred_at":"2026-09-14T00:00:00Z",'
            b'"aggregate_id":"00000000-0000-0000-0000-000000000002",'
            b'"payload":{"proposal_id":"00000000-0000-0000-0000-000000000003",'
            b'"action_kind":"shell_command","target":"checkout-api"}}'
        ),
    ],
)
async def test_invalid_envelope_is_rejected_without_actuator(body: bytes) -> None:
    message = FakeMessage(body)
    executor = ImmediateExecutor()

    with pytest.raises(consumer.InvalidEventError):
        await handle_message(cast(IncomingMessage, message), executor, sessions)

    assert message.reject_count == 1
    assert message.requeue is False
    assert executor.calls == 0


async def test_database_failure_before_barrier_requeues_without_actuator(
    seeded: Seeded,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, _ = seeded
    message = message_for(event)
    executor = ImmediateExecutor()

    async def fail_prepare(
        *args: object, **kwargs: object
    ) -> consumer.PreparationResult:
        raise SQLAlchemyError("simulated preparation outage")

    monkeypatch.setattr(consumer, "prepare_execution_attempt", fail_prepare)
    with pytest.raises(SQLAlchemyError):
        await handle_message(cast(IncomingMessage, message), executor, sessions)

    assert message.nack_count == 1
    assert message.requeue is True
    assert executor.calls == 0


async def test_cancellation_after_barrier_marks_outcome_unknown_and_propagates(
    seeded: Seeded,
) -> None:
    event, execution_seed = seeded
    message = message_for(event)
    executor = BlockingExecutor()
    task = asyncio.create_task(
        handle_message(cast(IncomingMessage, message), executor, sessions)
    )
    await executor.entered.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    execution, attempt = await load_state(execution_seed.id)
    assert execution.status is RemediationExecutionStatus.OUTCOME_UNKNOWN
    assert attempt.status is RemediationExecutionAttemptStatus.OUTCOME_UNKNOWN
    assert attempt.failure_kind is RemediationFailureKind.INTERRUPTED
    assert message.ack_count == 0
    assert message.nack_count == 0
    assert executor.calls == 1
