import logging
from typing import Annotated, Never
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.authorization import require_roles
from signalforge.db.session import get_session
from signalforge.remediation import application, service
from signalforge.remediation.errors import (
    DuplicatePendingRemediationProposalError,
    IncidentNotEligibleForRemediationError,
    IncidentNotFoundForRemediationError,
    InvalidRemediationTransitionError,
    RemediationExecutionAlreadyRequestedError,
    RemediationExecutionNotFoundError,
    RemediationProposalNotApprovedForExecutionError,
    RemediationProposalNotFoundError,
    RemediationSelfApprovalError,
)
from signalforge.remediation.models import RemediationProposal
from signalforge.remediation.schemas import (
    RemediationExecutionResponse,
    RemediationProposalCreate,
    RemediationProposalListQuery,
    RemediationProposalListResponse,
    RemediationProposalReject,
    RemediationProposalResponse,
)
from signalforge.users.models import User, UserRole

router = APIRouter(prefix="/api/v1/remediation-proposals", tags=["remediation"])
logger = logging.getLogger(__name__)
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]
ProposalReader = Annotated[
    User,
    Depends(require_roles(UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN)),
]
ProposalCreator = Annotated[
    User,
    Depends(require_roles(UserRole.OPERATOR, UserRole.ADMIN)),
]
ProposalApprover = Annotated[User, Depends(require_roles(UserRole.ADMIN))]
ProposalRejector = Annotated[
    User,
    Depends(require_roles(UserRole.OPERATOR, UserRole.ADMIN)),
]
ProposalQuery = Annotated[RemediationProposalListQuery, Query()]


async def _rollback_and_raise(
    session: AsyncSession,
    error: Exception,
    status_code: int,
    detail: str,
) -> Never:
    await session.rollback()
    raise HTTPException(status_code=status_code, detail=detail) from error


@router.post(
    "",
    response_model=RemediationProposalResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_remediation_proposal(
    proposal_data: RemediationProposalCreate,
    session: DatabaseSession,
    actor: ProposalCreator,
) -> RemediationProposal:
    try:
        proposal = await service.create_remediation_proposal(
            session, proposal_data, actor.id
        )
    except IncidentNotFoundForRemediationError as error:
        await _rollback_and_raise(
            session, error, status.HTTP_404_NOT_FOUND, "Incident not found"
        )
    except IncidentNotEligibleForRemediationError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "Incident is not eligible for remediation",
        )
    except DuplicatePendingRemediationProposalError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "A matching pending remediation proposal already exists",
        )
    logger.info(
        "remediation proposal created",
        extra={
            "event": "remediation_proposal_created",
            "proposal_id": proposal.id,
            "incident_id": proposal.incident_id,
            "action_kind": proposal.action_kind,
            "status": proposal.status,
        },
    )
    return proposal


@router.get("", response_model=RemediationProposalListResponse)
async def list_remediation_proposals(
    query: ProposalQuery,
    session: DatabaseSession,
    _actor: ProposalReader,
) -> RemediationProposalListResponse:
    proposals, total = await service.list_remediation_proposals(session, query)
    return RemediationProposalListResponse(
        items=[
            RemediationProposalResponse.model_validate(proposal)
            for proposal in proposals
        ],
        limit=query.limit,
        offset=query.offset,
        total=total,
    )


@router.get("/{proposal_id}", response_model=RemediationProposalResponse)
async def get_remediation_proposal(
    proposal_id: UUID,
    session: DatabaseSession,
    _actor: ProposalReader,
) -> RemediationProposal:
    try:
        return await service.get_remediation_proposal(session, proposal_id)
    except RemediationProposalNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Remediation proposal not found",
        ) from error


@router.post(
    "/{proposal_id}/approve",
    response_model=RemediationProposalResponse,
)
async def approve_remediation_proposal(
    proposal_id: UUID,
    session: DatabaseSession,
    actor: ProposalApprover,
) -> RemediationProposal:
    try:
        proposal = await service.approve_remediation_proposal(
            session, proposal_id, actor.id
        )
    except RemediationProposalNotFoundError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_404_NOT_FOUND,
            "Remediation proposal not found",
        )
    except InvalidRemediationTransitionError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "Invalid remediation proposal state transition",
        )
    except RemediationSelfApprovalError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "A proposer cannot approve their own remediation proposal",
        )
    logger.info(
        "remediation proposal approved",
        extra={
            "event": "remediation_proposal_approved",
            "proposal_id": proposal.id,
            "incident_id": proposal.incident_id,
            "action_kind": proposal.action_kind,
            "status": proposal.status,
        },
    )
    return proposal


@router.get(
    "/{proposal_id}/execution",
    response_model=RemediationExecutionResponse,
)
async def get_remediation_execution(
    proposal_id: UUID,
    session: DatabaseSession,
    _actor: ProposalReader,
) -> RemediationExecutionResponse:
    try:
        execution = await service.get_remediation_execution(session, proposal_id)
        return RemediationExecutionResponse.model_validate(execution)
    except RemediationExecutionNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Remediation execution not found",
        ) from error


@router.post(
    "/{proposal_id}/execute",
    response_model=RemediationExecutionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def request_remediation_execution(
    proposal_id: UUID,
    session: DatabaseSession,
    actor: ProposalApprover,
) -> RemediationExecutionResponse:
    try:
        execution = await application.request_remediation_execution_with_event(
            session, proposal_id, actor.id
        )
    except RemediationProposalNotFoundError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_404_NOT_FOUND,
            "Remediation proposal not found",
        )
    except RemediationProposalNotApprovedForExecutionError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "Remediation proposal is not approved for execution",
        )
    except RemediationExecutionAlreadyRequestedError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "Execution has already been requested for this remediation proposal",
        )
    logger.info(
        "remediation execution requested",
        extra={
            "event": "remediation_execution_requested",
            "execution_id": execution.id,
            "proposal_id": execution.proposal_id,
            "action_kind": execution.action_kind,
            "target": execution.target,
            "status": execution.status,
        },
    )
    return execution


@router.post(
    "/{proposal_id}/reject",
    response_model=RemediationProposalResponse,
)
async def reject_remediation_proposal(
    proposal_id: UUID,
    rejection_data: RemediationProposalReject,
    session: DatabaseSession,
    actor: ProposalRejector,
) -> RemediationProposal:
    try:
        proposed_by_user_id = await service.get_remediation_proposal_proposer_id(
            session, proposal_id
        )
    except RemediationProposalNotFoundError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_404_NOT_FOUND,
            "Remediation proposal not found",
        )
    if actor.role is UserRole.OPERATOR and proposed_by_user_id != actor.id:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions",
        )

    try:
        proposal = await service.reject_remediation_proposal(
            session, proposal_id, actor.id, rejection_data
        )
    except RemediationProposalNotFoundError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_404_NOT_FOUND,
            "Remediation proposal not found",
        )
    except InvalidRemediationTransitionError as error:
        await _rollback_and_raise(
            session,
            error,
            status.HTTP_409_CONFLICT,
            "Invalid remediation proposal state transition",
        )
    logger.info(
        "remediation proposal rejected",
        extra={
            "event": "remediation_proposal_rejected",
            "proposal_id": proposal.id,
            "incident_id": proposal.incident_id,
            "action_kind": proposal.action_kind,
            "status": proposal.status,
        },
    )
    return proposal
