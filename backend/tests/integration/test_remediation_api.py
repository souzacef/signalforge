from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.incidents.models import Incident, IncidentSeverity, IncidentStatus
from signalforge.outbox.models import OutboxEvent
from signalforge.remediation.execution import AllowlistedHttpRemediationExecutor
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationExecution,
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
    reject_remediation_proposal,
)
from signalforge.users.models import UserRole
from tests.integration.factories import AuthHeadersFactory

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

PROPOSALS_PATH = "/api/v1/remediation-proposals"
REJECTION = {"rejection_reason": "Runbook conditions no longer apply"}


async def persist_incident(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: IncidentStatus = IncidentStatus.OPEN,
    title: str = "Checkout errors",
) -> Incident:
    async with session_factory() as session:
        incident = Incident(
            source="test",
            title=title,
            severity=IncidentSeverity.HIGH,
            status=status,
            occurred_at=datetime.now(UTC),
        )
        session.add(incident)
        await session.commit()
        await session.refresh(incident)
        return incident


def payload(incident_id: UUID, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "incident_id": str(incident_id),
        "action_kind": "restart_service",
        "target": "checkout-api",
        "reason": "Restart is the approved runbook response",
    }
    values.update(overrides)
    return values


async def create_via_service(
    session_factory: async_sessionmaker[AsyncSession],
    incident_id: UUID,
    proposer_id: UUID,
    *,
    target: str = "checkout-api",
    reason: str = "Restart is the approved runbook response",
) -> RemediationProposal:
    async with session_factory() as session:
        return await create_remediation_proposal(
            session,
            RemediationProposalCreate(
                incident_id=incident_id,
                action_kind=RemediationActionKind.RESTART_SERVICE,
                target=target,
                reason=reason,
            ),
            proposer_id,
        )


async def transition_via_service(
    session_factory: async_sessionmaker[AsyncSession],
    proposal_id: UUID,
    actor_id: UUID,
    operation: str,
) -> RemediationProposal:
    async with session_factory() as session:
        if operation == "approve":
            return await approve_remediation_proposal(session, proposal_id, actor_id)
        return await reject_remediation_proposal(
            session,
            proposal_id,
            actor_id,
            RemediationProposalReject(rejection_reason="No longer needed"),
        )


async def stored_proposal(
    session_factory: async_sessionmaker[AsyncSession],
    proposal_id: UUID,
) -> RemediationProposal:
    async with session_factory() as session:
        proposal = await session.get(RemediationProposal, proposal_id)
        assert proposal is not None
        return proposal


type RequestCall = Callable[[dict[str, str] | None], Awaitable[Response]]


@pytest.fixture
async def incident(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> Incident:
    return await persist_incident(database_session_factory)


@pytest.fixture
def endpoint_calls(client: AsyncClient, incident: Incident) -> list[RequestCall]:
    missing = uuid4()

    async def create(headers: dict[str, str] | None) -> Response:
        return await client.post(
            PROPOSALS_PATH, json=payload(incident.id), headers=headers
        )

    async def list_items(headers: dict[str, str] | None) -> Response:
        return await client.get(PROPOSALS_PATH, headers=headers)

    async def get(headers: dict[str, str] | None) -> Response:
        return await client.get(f"{PROPOSALS_PATH}/{missing}", headers=headers)

    async def approve(headers: dict[str, str] | None) -> Response:
        return await client.post(f"{PROPOSALS_PATH}/{missing}/approve", headers=headers)

    async def reject(headers: dict[str, str] | None) -> Response:
        return await client.post(
            f"{PROPOSALS_PATH}/{missing}/reject", json=REJECTION, headers=headers
        )

    async def execute(headers: dict[str, str] | None) -> Response:
        return await client.post(f"{PROPOSALS_PATH}/{missing}/execute", headers=headers)

    return [create, list_items, get, approve, reject, execute]


async def test_all_endpoints_require_authentication(
    endpoint_calls: list[RequestCall],
) -> None:
    for call in endpoint_calls:
        response = await call(None)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"


async def test_all_endpoints_reject_invalid_tokens(
    endpoint_calls: list[RequestCall],
) -> None:
    headers = {"Authorization": "Bearer not-a-jwt"}
    for call in endpoint_calls:
        response = await call(headers)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"


async def test_viewer_cannot_create(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(UserRole.VIEWER)
    response = await client.post(
        PROPOSALS_PATH, json=payload(incident.id), headers=headers
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "Insufficient permissions"}


@pytest.mark.parametrize("role", [UserRole.OPERATOR, UserRole.ADMIN])
async def test_operator_and_admin_create_with_authenticated_attribution(
    role: UserRole,
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers, actor = await auth_headers_factory(role)
    response = await client.post(
        PROPOSALS_PATH,
        json=payload(incident.id, target=" checkout-api "),
        headers=headers,
    )
    assert response.status_code == 201
    body = response.json()
    assert body["proposed_by_user_id"] == str(actor.id)
    assert body["target"] == "checkout-api"
    assert body["status"] == "pending_approval"
    assert body["approved_by_user_id"] is None
    assert body["rejected_by_user_id"] is None
    persisted = await stored_proposal(database_session_factory, UUID(body["id"]))
    assert persisted.proposed_by_user_id == actor.id


async def test_create_domain_failures_have_stable_http_mapping(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers, _ = await auth_headers_factory(UserRole.OPERATOR)

    missing = await client.post(PROPOSALS_PATH, json=payload(uuid4()), headers=headers)
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Incident not found"}

    async with database_session_factory() as session:
        stored = await session.get(Incident, incident.id)
        assert stored is not None
        stored.status = IncidentStatus.RESOLVED
        await session.commit()
    resolved = await client.post(
        PROPOSALS_PATH, json=payload(incident.id), headers=headers
    )
    assert resolved.status_code == 409
    assert resolved.json() == {"detail": "Incident is not eligible for remediation"}

    eligible = await persist_incident(database_session_factory, title="Eligible")
    first = await client.post(
        PROPOSALS_PATH, json=payload(eligible.id), headers=headers
    )
    duplicate = await client.post(
        PROPOSALS_PATH, json=payload(eligible.id), headers=headers
    )
    assert first.status_code == 201
    assert duplicate.status_code == 409
    assert duplicate.json() == {
        "detail": "A matching pending remediation proposal already exists"
    }


@pytest.mark.parametrize(
    "override",
    [
        {"target": "checkout; restart"},
        {"target": " "},
        {"reason": " "},
        {"action_kind": "shell_command"},
    ],
)
async def test_create_rejects_invalid_action_target_and_reason(
    override: dict[str, object],
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(UserRole.OPERATOR)
    response = await client.post(
        PROPOSALS_PATH, json=payload(incident.id, **override), headers=headers
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "00000000-0000-0000-0000-000000000000"),
        ("status", "approved"),
        ("proposed_by_user_id", "00000000-0000-0000-0000-000000000000"),
        ("approved_by_user_id", "00000000-0000-0000-0000-000000000000"),
    ],
)
async def test_create_cannot_inject_server_managed_fields(
    field: str,
    value: str,
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(UserRole.OPERATOR)
    response = await client.post(
        PROPOSALS_PATH,
        json=payload(incident.id, **{field: value}),
        headers=headers,
    )
    assert response.status_code == 422


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN])
async def test_all_roles_can_get_persisted_response(
    role: UserRole,
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )
    headers, _ = await auth_headers_factory(role)
    response = await client.get(f"{PROPOSALS_PATH}/{proposal.id}", headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "id",
        "incident_id",
        "action_kind",
        "target",
        "reason",
        "status",
        "proposed_by_user_id",
        "approved_by_user_id",
        "approved_at",
        "rejected_by_user_id",
        "rejected_at",
        "rejection_reason",
        "created_at",
        "updated_at",
    }
    assert body["id"] == str(proposal.id)
    assert body["incident_id"] == str(incident.id)
    assert body["proposed_by_user_id"] == str(proposer.id)


async def test_get_missing_proposal_returns_404(
    client: AsyncClient,
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(UserRole.VIEWER)
    response = await client.get(f"{PROPOSALS_PATH}/{uuid4()}", headers=headers)
    assert response.status_code == 404
    assert response.json() == {"detail": "Remediation proposal not found"}


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN])
async def test_all_roles_can_list(
    role: UserRole,
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )
    headers, _ = await auth_headers_factory(role)
    response = await client.get(PROPOSALS_PATH, headers=headers)
    assert response.status_code == 200
    assert response.json()["items"][0]["id"] == str(proposal.id)


async def test_list_paginates_total_and_orders_deterministically(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers, proposer = await auth_headers_factory(UserRole.OPERATOR)
    first = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="service-a"
    )
    second = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="service-b"
    )
    third = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="service-c"
    )
    same_time = datetime(2026, 9, 14, 12, tzinfo=UTC)
    async with database_session_factory() as session:
        for proposal_id in (first.id, second.id, third.id):
            stored = await session.get(RemediationProposal, proposal_id)
            assert stored is not None
            stored.created_at = same_time
        await session.commit()

    response = await client.get(
        PROPOSALS_PATH, params={"limit": 2, "offset": 1}, headers=headers
    )
    assert response.status_code == 200
    body = response.json()
    expected = sorted([first.id, second.id, third.id], reverse=True)[1:]
    assert [UUID(item["id"]) for item in body["items"]] == expected
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert body["total"] == 3


async def test_list_supports_each_filter_and_combination(
    client: AsyncClient,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers, proposer = await auth_headers_factory(UserRole.OPERATOR)
    _, other_proposer = await auth_headers_factory(UserRole.OPERATOR)
    _, reviewer = await auth_headers_factory(UserRole.ADMIN)
    incident_a = await persist_incident(database_session_factory, title="A")
    incident_b = await persist_incident(database_session_factory, title="B")
    approved = await create_via_service(
        database_session_factory,
        incident_a.id,
        proposer.id,
        target="checkout-api",
    )
    rejected = await create_via_service(
        database_session_factory,
        incident_b.id,
        other_proposer.id,
        target="billing-api",
    )
    pending = await create_via_service(
        database_session_factory,
        incident_b.id,
        proposer.id,
        target="checkout-worker",
    )
    await transition_via_service(
        database_session_factory, approved.id, reviewer.id, "approve"
    )
    await transition_via_service(
        database_session_factory, rejected.id, reviewer.id, "reject"
    )
    base_time = datetime(2026, 9, 14, 10, tzinfo=UTC)
    async with database_session_factory() as session:
        for index, proposal_id in enumerate((approved.id, rejected.id, pending.id)):
            stored = await session.get(RemediationProposal, proposal_id)
            assert stored is not None
            stored.created_at = base_time + timedelta(hours=index)
        await session.commit()

    cases: list[tuple[dict[str, str], set[UUID]]] = [
        ({"incident_id": str(incident_a.id)}, {approved.id}),
        ({"status": "approved"}, {approved.id}),
        ({"action_kind": "restart_service"}, {approved.id, rejected.id, pending.id}),
        ({"target": " billing-api "}, {rejected.id}),
        ({"proposed_by_user_id": str(other_proposer.id)}, {rejected.id}),
        ({"created_from": "2026-09-14T11:00:00Z"}, {rejected.id, pending.id}),
        ({"created_to": "2026-09-14T11:00:00Z"}, {approved.id, rejected.id}),
        (
            {
                "incident_id": str(incident_b.id),
                "status": "pending_approval",
                "target": "checkout-worker",
                "proposed_by_user_id": str(proposer.id),
            },
            {pending.id},
        ),
    ]
    for query, expected in cases:
        response = await client.get(PROPOSALS_PATH, params=query, headers=headers)
        assert response.status_code == 200
        body = response.json()
        assert {UUID(item["id"]) for item in body["items"]} == expected
        assert body["total"] == len(expected)


@pytest.mark.parametrize(
    "query",
    [
        {"limit": "0"},
        {"limit": "101"},
        {"offset": "-1"},
        {
            "created_from": "2026-09-15T00:00:00Z",
            "created_to": "2026-09-14T00:00:00Z",
        },
    ],
)
async def test_list_rejects_invalid_pagination_and_time_range(
    query: dict[str, str],
    client: AsyncClient,
    auth_headers_factory: AuthHeadersFactory,
) -> None:
    headers, _ = await auth_headers_factory(UserRole.VIEWER)
    response = await client.get(PROPOSALS_PATH, params=query, headers=headers)
    assert response.status_code == 422


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.OPERATOR])
async def test_only_admin_can_approve(
    role: UserRole,
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )
    headers, _ = await auth_headers_factory(role)
    response = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/approve", headers=headers
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "Insufficient permissions"}
    assert (
        await stored_proposal(database_session_factory, proposal.id)
    ).status is RemediationProposalStatus.PENDING_APPROVAL


async def test_admin_approval_persists_without_execution_or_outbox_effect(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    headers, admin = await auth_headers_factory(UserRole.ADMIN)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )
    async with database_session_factory() as session:
        outbox_before = (
            await session.scalar(select(func.count()).select_from(OutboxEvent)) or 0
        )

    response = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/approve", headers=headers
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "approved"
    assert body["approved_by_user_id"] == str(admin.id)
    assert body["approved_at"] is not None
    stored = await stored_proposal(database_session_factory, proposal.id)
    assert stored.status is RemediationProposalStatus.APPROVED
    assert stored.approved_by_user_id == admin.id
    assert stored.approved_at is not None
    assert stored.rejected_by_user_id is None
    async with database_session_factory() as session:
        outbox_after = (
            await session.scalar(select(func.count()).select_from(OutboxEvent)) or 0
        )
        execution_count = (
            await session.scalar(select(func.count()).select_from(RemediationExecution))
            or 0
        )
        unchanged_incident = await session.get(Incident, incident.id)
    assert outbox_after == outbox_before
    assert execution_count == 0
    assert unchanged_incident is not None
    assert unchanged_incident.status is IncidentStatus.OPEN


async def test_admin_self_approval_conflict_rolls_back_for_later_approval(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    proposer_headers, proposer = await auth_headers_factory(UserRole.ADMIN)
    reviewer_headers, reviewer = await auth_headers_factory(UserRole.ADMIN)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )

    conflict = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/approve", headers=proposer_headers
    )
    assert conflict.status_code == 409
    assert conflict.json() == {
        "detail": "A proposer cannot approve their own remediation proposal"
    }
    success = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/approve", headers=reviewer_headers
    )
    assert success.status_code == 200
    assert success.json()["approved_by_user_id"] == str(reviewer.id)


async def test_approval_invalid_transitions_and_missing_map_to_conflict_or_not_found(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    headers, admin = await auth_headers_factory(UserRole.ADMIN)
    approved = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="approved-api"
    )
    rejected = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="rejected-api"
    )
    await transition_via_service(
        database_session_factory, rejected.id, admin.id, "reject"
    )

    first = await client.post(
        f"{PROPOSALS_PATH}/{approved.id}/approve", headers=headers
    )
    repeat = await client.post(
        f"{PROPOSALS_PATH}/{approved.id}/approve", headers=headers
    )
    rejected_response = await client.post(
        f"{PROPOSALS_PATH}/{rejected.id}/approve", headers=headers
    )
    missing = await client.post(f"{PROPOSALS_PATH}/{uuid4()}/approve", headers=headers)
    assert first.status_code == 200
    for response in (repeat, rejected_response):
        assert response.status_code == 409
        assert response.json() == {
            "detail": "Invalid remediation proposal state transition"
        }
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Remediation proposal not found"}
    recovery = await client.get(PROPOSALS_PATH, headers=headers)
    assert recovery.status_code == 200


async def test_viewer_cannot_reject(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )
    headers, _ = await auth_headers_factory(UserRole.VIEWER)
    response = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/reject",
        json=REJECTION,
        headers=headers,
    )
    assert response.status_code == 403


async def test_operator_can_withdraw_own_but_not_another_users_proposal(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers, operator = await auth_headers_factory(UserRole.OPERATOR)
    _, other = await auth_headers_factory(UserRole.OPERATOR)
    own = await create_via_service(
        database_session_factory, incident.id, operator.id, target="own-api"
    )
    other_proposal = await create_via_service(
        database_session_factory, incident.id, other.id, target="other-api"
    )

    denied = await client.post(
        f"{PROPOSALS_PATH}/{other_proposal.id}/reject",
        json=REJECTION,
        headers=headers,
    )
    assert denied.status_code == 403
    assert denied.json() == {"detail": "Insufficient permissions"}
    assert (
        await stored_proposal(database_session_factory, other_proposal.id)
    ).status is RemediationProposalStatus.PENDING_APPROVAL

    withdrawn = await client.post(
        f"{PROPOSALS_PATH}/{own.id}/reject", json=REJECTION, headers=headers
    )
    assert withdrawn.status_code == 200
    body = withdrawn.json()
    assert body["status"] == "rejected"
    assert body["rejected_by_user_id"] == str(operator.id)
    assert body["rejected_at"] is not None
    assert body["rejection_reason"] == REJECTION["rejection_reason"]


async def test_admin_can_reject_any_pending_proposal_including_own(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, operator = await auth_headers_factory(UserRole.OPERATOR)
    headers, admin = await auth_headers_factory(UserRole.ADMIN)
    operator_proposal = await create_via_service(
        database_session_factory, incident.id, operator.id, target="operator-api"
    )
    own_proposal = await create_via_service(
        database_session_factory, incident.id, admin.id, target="admin-api"
    )
    for proposal in (operator_proposal, own_proposal):
        response = await client.post(
            f"{PROPOSALS_PATH}/{proposal.id}/reject",
            json=REJECTION,
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json()["rejected_by_user_id"] == str(admin.id)


async def test_rejection_invalid_transitions_and_missing_are_stable_and_recover(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    headers, admin = await auth_headers_factory(UserRole.ADMIN)
    rejected = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="rejected-api"
    )
    approved = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="approved-api"
    )

    first = await client.post(
        f"{PROPOSALS_PATH}/{rejected.id}/reject",
        json=REJECTION,
        headers=headers,
    )
    await transition_via_service(
        database_session_factory, approved.id, admin.id, "approve"
    )
    repeat = await client.post(
        f"{PROPOSALS_PATH}/{rejected.id}/reject",
        json=REJECTION,
        headers=headers,
    )
    approved_response = await client.post(
        f"{PROPOSALS_PATH}/{approved.id}/reject",
        json=REJECTION,
        headers=headers,
    )
    missing = await client.post(
        f"{PROPOSALS_PATH}/{uuid4()}/reject",
        json=REJECTION,
        headers=headers,
    )

    assert first.status_code == 200
    for response in (repeat, approved_response):
        assert response.status_code == 409
        assert response.json() == {
            "detail": "Invalid remediation proposal state transition"
        }
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Remediation proposal not found"}
    recovery = await client.get(f"{PROPOSALS_PATH}/{approved.id}", headers=headers)
    assert recovery.status_code == 200
    assert recovery.json()["status"] == "approved"


async def test_reject_body_is_required_and_forbids_extra_fields(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers, operator = await auth_headers_factory(UserRole.OPERATOR)
    proposal = await create_via_service(
        database_session_factory, incident.id, operator.id
    )
    for body in ({}, {"rejection_reason": "No", "status": "rejected"}):
        response = await client.post(
            f"{PROPOSALS_PATH}/{proposal.id}/reject",
            json=body,
            headers=headers,
        )
        assert response.status_code == 422
    assert (
        await stored_proposal(database_session_factory, proposal.id)
    ).status is RemediationProposalStatus.PENDING_APPROVAL


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.OPERATOR])
async def test_only_admin_can_request_execution(
    role: UserRole,
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    _, reviewer = await auth_headers_factory(UserRole.ADMIN)
    proposal = await create_via_service(
        database_session_factory, incident.id, proposer.id
    )
    await transition_via_service(
        database_session_factory, proposal.id, reviewer.id, "approve"
    )
    headers, _ = await auth_headers_factory(role)

    response = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/execute", headers=headers
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Insufficient permissions"}
    async with database_session_factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(RemediationExecution)
            .where(RemediationExecution.proposal_id == proposal.id)
        )
    assert count == 0


async def test_admin_execute_returns_durable_snapshot_and_never_calls_actuator(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    _, reviewer = await auth_headers_factory(UserRole.ADMIN)
    headers, requester = await auth_headers_factory(UserRole.ADMIN)
    proposal = await create_via_service(
        database_session_factory,
        incident.id,
        proposer.id,
        target="checkout-api",
        reason="Sensitive operational rationale that must stay on the proposal",
    )
    await transition_via_service(
        database_session_factory, proposal.id, reviewer.id, "approve"
    )
    actuator = AsyncMock(side_effect=AssertionError("actuator must remain dormant"))
    monkeypatch.setattr(AllowlistedHttpRemediationExecutor, "execute", actuator)

    response = await client.post(
        f"{PROPOSALS_PATH}/{proposal.id}/execute",
        json={
            "target": "attacker-target",
            "action_kind": "shell_command",
            "status": "succeeded",
            "endpoint_url": "https://attacker.invalid",
        },
        headers=headers,
    )

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {
        "id",
        "proposal_id",
        "action_kind",
        "target",
        "status",
        "requested_by_user_id",
        "requested_at",
        "updated_at",
    }
    assert body["proposal_id"] == str(proposal.id)
    assert body["action_kind"] == "restart_service"
    assert body["target"] == "checkout-api"
    assert body["status"] == "requested"
    assert body["requested_by_user_id"] == str(requester.id)
    actuator.assert_not_awaited()

    execution_id = UUID(body["id"])
    async with database_session_factory() as session:
        execution = await session.get(RemediationExecution, execution_id)
        outbox = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.event_type == "remediation.execution.requested",
                OutboxEvent.aggregate_id == execution_id,
            )
        )
        unchanged_incident = await session.get(Incident, incident.id)
    assert execution is not None
    assert execution.target == "checkout-api"
    assert outbox is not None
    assert outbox.payload == {
        "proposal_id": str(proposal.id),
        "action_kind": "restart_service",
        "target": "checkout-api",
    }
    assert unchanged_incident is not None
    assert unchanged_incident.status is IncidentStatus.OPEN


async def test_execute_eligibility_and_duplicate_errors_are_stable(
    client: AsyncClient,
    incident: Incident,
    auth_headers_factory: AuthHeadersFactory,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, proposer = await auth_headers_factory(UserRole.OPERATOR)
    headers, admin = await auth_headers_factory(UserRole.ADMIN)
    pending = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="pending-api"
    )
    rejected = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="rejected-api"
    )
    approved = await create_via_service(
        database_session_factory, incident.id, proposer.id, target="approved-api"
    )
    await transition_via_service(
        database_session_factory, rejected.id, admin.id, "reject"
    )
    await transition_via_service(
        database_session_factory, approved.id, admin.id, "approve"
    )

    missing = await client.post(f"{PROPOSALS_PATH}/{uuid4()}/execute", headers=headers)
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Remediation proposal not found"}
    for proposal in (pending, rejected):
        response = await client.post(
            f"{PROPOSALS_PATH}/{proposal.id}/execute", headers=headers
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": "Remediation proposal is not approved for execution"
        }

    first = await client.post(
        f"{PROPOSALS_PATH}/{approved.id}/execute", headers=headers
    )
    duplicate = await client.post(
        f"{PROPOSALS_PATH}/{approved.id}/execute", headers=headers
    )
    assert first.status_code == 202
    assert duplicate.status_code == 409
    assert duplicate.json() == {
        "detail": "Execution has already been requested for this remediation proposal"
    }
    recovery = await client.get(f"{PROPOSALS_PATH}/{approved.id}", headers=headers)
    assert recovery.status_code == 200
    async with database_session_factory() as session:
        executions = await session.scalar(
            select(func.count())
            .select_from(RemediationExecution)
            .where(RemediationExecution.proposal_id == approved.id)
        )
        events = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.event_type == "remediation.execution.requested",
                OutboxEvent.aggregate_id == UUID(first.json()["id"]),
            )
        )
    assert executions == 1
    assert events == 1
