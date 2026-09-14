from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.observability.propagation import capture_current_trace_context
from signalforge.outbox.models import OutboxEvent
from signalforge.remediation.errors import (
    RemediationExecutionAlreadyRequestedError,
    RemediationProposalNotApprovedForExecutionError,
    RemediationProposalNotFoundError,
)
from signalforge.remediation.events import (
    RemediationExecutionRequested,
    RemediationExecutionRequestedPayload,
)
from signalforge.remediation.models import (
    RemediationExecution,
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.remediation.schemas import RemediationExecutionResponse

_DUPLICATE_EXECUTION_CONSTRAINT = "uq_remediation_executions_proposal_id"


def _is_duplicate_execution_violation(error: IntegrityError) -> bool:
    current: BaseException | None = error
    while current is not None:
        if getattr(current, "constraint_name", None) == _DUPLICATE_EXECUTION_CONSTRAINT:
            return True
        current = current.__cause__
    return _DUPLICATE_EXECUTION_CONSTRAINT in str(error)


async def request_remediation_execution_with_event(
    session: AsyncSession,
    proposal_id: UUID,
    requested_by_user_id: UUID,
) -> RemediationExecutionResponse:
    """Atomically persist one approved command snapshot and its outbox event."""
    proposal = await session.scalar(
        select(RemediationProposal)
        .where(RemediationProposal.id == proposal_id)
        .with_for_update(read=True)
    )
    if proposal is None:
        raise RemediationProposalNotFoundError
    if proposal.status is not RemediationProposalStatus.APPROVED:
        raise RemediationProposalNotApprovedForExecutionError

    execution = RemediationExecution(
        id=uuid4(),
        proposal_id=proposal.id,
        action_kind=proposal.action_kind,
        target=proposal.target,
        requested_by_user_id=requested_by_user_id,
    )
    session.add(execution)
    try:
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
        trace_context = capture_current_trace_context()
        session.add(
            OutboxEvent(
                id=event.event_id,
                event_type=event.event_type,
                event_version=event.event_version,
                aggregate_id=event.aggregate_id,
                payload=event.payload.model_dump(mode="json"),
                occurred_at=event.occurred_at,
                traceparent=trace_context.traceparent,
                tracestate=trace_context.tracestate,
            )
        )
        await session.flush()
        response = RemediationExecutionResponse.model_validate(execution)
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        if _is_duplicate_execution_violation(error):
            raise RemediationExecutionAlreadyRequestedError from error
        raise
    return response
