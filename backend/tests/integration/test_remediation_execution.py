from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.core.config import RemediationExecutionSettings
from signalforge.incidents.models import Incident, IncidentSeverity, IncidentStatus
from signalforge.outbox.models import OutboxEvent
from signalforge.remediation.execution import (
    AllowlistedHttpRemediationExecutor,
    HttpRestartServiceAdapter,
    RemediationExecutionOutcome,
    command_from_approved_proposal,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationExecution,
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.remediation.schemas import RemediationProposalCreate
from signalforge.remediation.service import (
    approve_remediation_proposal,
    create_remediation_proposal,
    get_remediation_proposal,
)
from signalforge.users.models import User, UserRole

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_approved_persisted_proposal_executes_only_via_injected_http_transport(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    incident = Incident(
        source="phase-5c-integration",
        title="Checkout errors",
        severity=IncidentSeverity.HIGH,
        status=IncidentStatus.OPEN,
        occurred_at=datetime.now(UTC),
    )
    proposer = User(
        email=f"phase-5c-proposer-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.OPERATOR,
    )
    approver = User(
        email=f"phase-5c-approver-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.ADMIN,
    )
    async with database_session_factory() as session:
        initial_outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxEvent)
        )
        session.add_all([incident, proposer, approver])
        await session.commit()

    async with database_session_factory() as session:
        persisted = await create_remediation_proposal(
            session,
            RemediationProposalCreate(
                incident_id=incident.id,
                action_kind=RemediationActionKind.RESTART_SERVICE,
                target="checkout-api",
                reason="High error rate",
            ),
            proposer.id,
        )

    async with database_session_factory() as session:
        await approve_remediation_proposal(session, persisted.id, approver.id)

    async with database_session_factory() as session:
        reloaded = await get_remediation_proposal(session, persisted.id)
        command = command_from_approved_proposal(reloaded)

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    execution_settings = RemediationExecutionSettings.model_validate(
        {
            "restart_endpoints": {
                "checkout-api": "http://checkout-control:8080/internal/restart"
            }
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        executor = AllowlistedHttpRemediationExecutor(
            HttpRestartServiceAdapter(execution_settings, client)
        )
        result = await executor.execute(command)

    assert result.outcome is RemediationExecutionOutcome.SUCCEEDED
    assert [str(request.url) for request in requests] == [
        "http://checkout-control:8080/internal/restart"
    ]

    async with database_session_factory() as session:
        proposal_after = await session.get(RemediationProposal, persisted.id)
        incident_after = await session.get(Incident, incident.id)
        final_outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxEvent)
        )
        execution_count = await session.scalar(
            select(func.count()).select_from(RemediationExecution)
        )
        attempts_table = await session.scalar(
            text("SELECT to_regclass('public.execution_attempts')")
        )

    assert proposal_after is not None
    assert proposal_after.status is RemediationProposalStatus.APPROVED
    assert incident_after is not None
    assert incident_after.status is IncidentStatus.OPEN
    assert final_outbox_count == initial_outbox_count
    assert execution_count == 0
    assert attempts_table is None
