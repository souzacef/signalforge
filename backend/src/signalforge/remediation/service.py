from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from signalforge.incidents.models import Incident, IncidentStatus
from signalforge.remediation.errors import (
    DuplicatePendingRemediationProposalError,
    IncidentNotEligibleForRemediationError,
    IncidentNotFoundForRemediationError,
    InvalidRemediationTransitionError,
    RemediationProposalNotFoundError,
    RemediationSelfApprovalError,
)
from signalforge.remediation.models import (
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.remediation.schemas import (
    RemediationProposalCreate,
    RemediationProposalListQuery,
    RemediationProposalReject,
)

_DUPLICATE_PENDING_CONSTRAINT = (
    "uq_remediation_proposals_pending_incident_action_target"
)


def _is_duplicate_pending_violation(error: IntegrityError) -> bool:
    current: BaseException | None = error
    while current is not None:
        if getattr(current, "constraint_name", None) == _DUPLICATE_PENDING_CONSTRAINT:
            return True
        current = current.__cause__
    return _DUPLICATE_PENDING_CONSTRAINT in str(error)


async def create_remediation_proposal(
    session: AsyncSession,
    proposal_data: RemediationProposalCreate,
    proposed_by_user_id: UUID,
) -> RemediationProposal:
    incident = await session.scalar(
        select(Incident)
        .where(Incident.id == proposal_data.incident_id)
        .with_for_update(read=True)
    )
    if incident is None:
        raise IncidentNotFoundForRemediationError
    if incident.status is IncidentStatus.RESOLVED:
        raise IncidentNotEligibleForRemediationError

    proposal = RemediationProposal(
        **proposal_data.model_dump(),
        proposed_by_user_id=proposed_by_user_id,
    )
    session.add(proposal)
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        if _is_duplicate_pending_violation(error):
            raise DuplicatePendingRemediationProposalError from error
        raise
    await session.refresh(proposal)
    return proposal


async def get_remediation_proposal(
    session: AsyncSession,
    proposal_id: UUID,
) -> RemediationProposal:
    proposal = await session.get(RemediationProposal, proposal_id)
    if proposal is None:
        raise RemediationProposalNotFoundError
    return proposal


async def get_remediation_proposal_proposer_id(
    session: AsyncSession,
    proposal_id: UUID,
) -> UUID:
    proposer_id = await session.scalar(
        select(RemediationProposal.proposed_by_user_id).where(
            RemediationProposal.id == proposal_id
        )
    )
    if proposer_id is None:
        raise RemediationProposalNotFoundError
    return proposer_id


def _proposal_filters(
    query: RemediationProposalListQuery,
) -> tuple[ColumnElement[bool], ...]:
    filters: list[ColumnElement[bool]] = []
    if query.incident_id is not None:
        filters.append(RemediationProposal.incident_id == query.incident_id)
    if query.status is not None:
        filters.append(RemediationProposal.status == query.status)
    if query.action_kind is not None:
        filters.append(RemediationProposal.action_kind == query.action_kind)
    if query.target is not None:
        filters.append(RemediationProposal.target == query.target)
    if query.proposed_by_user_id is not None:
        filters.append(
            RemediationProposal.proposed_by_user_id == query.proposed_by_user_id
        )
    if query.created_from is not None:
        filters.append(RemediationProposal.created_at >= query.created_from)
    if query.created_to is not None:
        filters.append(RemediationProposal.created_at <= query.created_to)
    return tuple(filters)


async def list_remediation_proposals(
    session: AsyncSession,
    query: RemediationProposalListQuery,
) -> tuple[list[RemediationProposal], int]:
    filters = _proposal_filters(query)
    total = await session.scalar(
        select(func.count()).select_from(RemediationProposal).where(*filters)
    )
    proposals = await session.scalars(
        select(RemediationProposal)
        .where(*filters)
        .order_by(RemediationProposal.created_at.desc(), RemediationProposal.id.desc())
        .limit(query.limit)
        .offset(query.offset)
    )
    return list(proposals), total or 0


async def _get_remediation_proposal_for_update(
    session: AsyncSession,
    proposal_id: UUID,
) -> RemediationProposal:
    proposal = await session.scalar(
        select(RemediationProposal)
        .where(RemediationProposal.id == proposal_id)
        .with_for_update()
    )
    if proposal is None:
        raise RemediationProposalNotFoundError
    if proposal.status is not RemediationProposalStatus.PENDING_APPROVAL:
        raise InvalidRemediationTransitionError
    return proposal


async def approve_remediation_proposal(
    session: AsyncSession,
    proposal_id: UUID,
    actor_user_id: UUID,
) -> RemediationProposal:
    """Approve a pending proposal only when actor and proposer differ."""
    proposal = await _get_remediation_proposal_for_update(session, proposal_id)
    if proposal.proposed_by_user_id == actor_user_id:
        raise RemediationSelfApprovalError

    transitioned_at = datetime.now(UTC)
    proposal.status = RemediationProposalStatus.APPROVED
    proposal.approved_by_user_id = actor_user_id
    proposal.approved_at = transitioned_at
    proposal.updated_at = transitioned_at
    await session.commit()
    await session.refresh(proposal)
    return proposal


async def reject_remediation_proposal(
    session: AsyncSession,
    proposal_id: UUID,
    actor_user_id: UUID,
    rejection_data: RemediationProposalReject,
) -> RemediationProposal:
    """Reject a pending proposal by its proposer or another human actor."""
    proposal = await _get_remediation_proposal_for_update(session, proposal_id)

    transitioned_at = datetime.now(UTC)
    proposal.status = RemediationProposalStatus.REJECTED
    proposal.rejected_by_user_id = actor_user_id
    proposal.rejected_at = transitioned_at
    proposal.rejection_reason = rejection_data.rejection_reason
    proposal.updated_at = transitioned_at
    await session.commit()
    await session.refresh(proposal)
    return proposal
