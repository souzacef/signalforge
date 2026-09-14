from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

import httpx

from signalforge.core.config import RemediationExecutionSettings
from signalforge.remediation.errors import (
    RemediationExecutionRejectedError,
    RemediationExecutionTimeoutError,
    RemediationExecutionTransportError,
    RemediationNotApprovedForExecutionError,
    RemediationTargetNotAllowedError,
    UnsupportedRemediationActionError,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationProposal,
    RemediationProposalStatus,
)


@dataclass(frozen=True, slots=True)
class RemediationCommand:
    proposal_id: UUID
    action_kind: RemediationActionKind
    target: str


class RemediationExecutionOutcome(StrEnum):
    SUCCEEDED = "succeeded"


@dataclass(frozen=True, slots=True)
class RemediationExecutionResult:
    proposal_id: UUID
    action_kind: RemediationActionKind
    target: str
    outcome: RemediationExecutionOutcome


class RemediationExecutor(Protocol):
    async def execute(
        self,
        command: RemediationCommand,
    ) -> RemediationExecutionResult: ...


class RestartServiceAdapter(Protocol):
    async def restart(
        self,
        command: RemediationCommand,
    ) -> RemediationExecutionResult: ...


def command_from_approved_proposal(
    proposal: RemediationProposal,
) -> RemediationCommand:
    if proposal.status is not RemediationProposalStatus.APPROVED:
        raise RemediationNotApprovedForExecutionError(
            f"proposal {proposal.id} is not approved for execution"
        )
    return RemediationCommand(
        proposal_id=proposal.id,
        action_kind=proposal.action_kind,
        target=proposal.target,
    )


class AllowlistedHttpRemediationExecutor:
    def __init__(self, restart_adapter: RestartServiceAdapter) -> None:
        self._restart_adapter = restart_adapter

    async def execute(
        self,
        command: RemediationCommand,
    ) -> RemediationExecutionResult:
        if command.action_kind is RemediationActionKind.RESTART_SERVICE:
            return await self._restart_adapter.restart(command)
        raise UnsupportedRemediationActionError(
            f"unsupported remediation action: {command.action_kind}"
        )


class HttpRestartServiceAdapter:
    """Posts once to the trusted endpoint configured for a logical target."""

    def __init__(
        self,
        settings: RemediationExecutionSettings,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._endpoints = {
            target: str(endpoint)
            for target, endpoint in settings.restart_endpoints.items()
        }
        self._timeout = settings.request_timeout_seconds
        self._client = client

    async def restart(
        self,
        command: RemediationCommand,
    ) -> RemediationExecutionResult:
        endpoint = self._endpoints.get(command.target)
        if endpoint is None:
            raise RemediationTargetNotAllowedError(
                f"remediation target is not allowlisted: {command.target}"
            )

        if self._client is not None:
            response = await self._post(self._client, endpoint, command.proposal_id)
        else:
            async with httpx.AsyncClient(follow_redirects=False) as client:
                response = await self._post(client, endpoint, command.proposal_id)

        if not 200 <= response.status_code < 300:
            raise RemediationExecutionRejectedError(response.status_code)
        return RemediationExecutionResult(
            proposal_id=command.proposal_id,
            action_kind=command.action_kind,
            target=command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )

    async def _post(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        proposal_id: UUID,
    ) -> httpx.Response:
        try:
            return await client.post(
                endpoint,
                json={"proposal_id": str(proposal_id)},
                headers={"Idempotency-Key": str(proposal_id)},
                timeout=self._timeout,
                follow_redirects=False,
            )
        except httpx.TimeoutException as error:
            raise RemediationExecutionTimeoutError(
                "remediation endpoint request timed out"
            ) from error
        except httpx.RequestError as error:
            raise RemediationExecutionTransportError(
                "remediation endpoint transport failed"
            ) from error
