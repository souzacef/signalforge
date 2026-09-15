import asyncio
from dataclasses import dataclass
from enum import StrEnum
from time import monotonic
from typing import Protocol
from uuid import UUID

import httpx
from opentelemetry.semconv.attributes.http_attributes import HTTP_RESPONSE_STATUS_CODE
from opentelemetry.trace import SpanKind, Tracer

from signalforge.core.config import RemediationExecutionSettings
from signalforge.observability.messaging import mark_span_error
from signalforge.observability.metrics import (
    RemediationExecutorMetricResult,
    RemediationExecutorMetrics,
)
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
    def __init__(
        self,
        restart_adapter: RestartServiceAdapter,
        metrics: RemediationExecutorMetrics | None = None,
    ) -> None:
        self._restart_adapter = restart_adapter
        self._metrics = metrics

    async def execute(
        self,
        command: RemediationCommand,
    ) -> RemediationExecutionResult:
        started = monotonic()
        metric_result = RemediationExecutorMetricResult.UNEXPECTED
        try:
            if command.action_kind is RemediationActionKind.RESTART_SERVICE:
                result = await self._restart_adapter.restart(command)
            else:
                raise UnsupportedRemediationActionError(
                    f"unsupported remediation action: {command.action_kind}"
                )
        except RemediationTargetNotAllowedError:
            metric_result = RemediationExecutorMetricResult.TARGET_NOT_ALLOWED
            raise
        except UnsupportedRemediationActionError:
            metric_result = RemediationExecutorMetricResult.UNSUPPORTED_ACTION
            raise
        except RemediationExecutionRejectedError as error:
            metric_result = (
                RemediationExecutorMetricResult.HTTP_REJECTED
                if 300 <= error.status_code < 500
                else RemediationExecutorMetricResult.HTTP_SERVER_ERROR
                if error.status_code >= 500
                else RemediationExecutorMetricResult.UNEXPECTED
            )
            raise
        except RemediationExecutionTimeoutError:
            metric_result = RemediationExecutorMetricResult.TIMEOUT
            raise
        except RemediationExecutionTransportError:
            metric_result = RemediationExecutorMetricResult.TRANSPORT
            raise
        except asyncio.CancelledError:
            metric_result = RemediationExecutorMetricResult.INTERRUPTED
            raise
        except Exception:
            metric_result = RemediationExecutorMetricResult.UNEXPECTED
            raise
        else:
            metric_result = RemediationExecutorMetricResult.SUCCESS
            return result
        finally:
            if self._metrics is not None:
                try:
                    self._metrics.record(
                        result=metric_result,
                        duration=monotonic() - started,
                    )
                except Exception:
                    pass


class HttpRestartServiceAdapter:
    """Posts once to the trusted endpoint configured for a logical target."""

    def __init__(
        self,
        settings: RemediationExecutionSettings,
        client: httpx.AsyncClient | None = None,
        tracer: Tracer | None = None,
    ) -> None:
        self._endpoints = {
            target: str(endpoint)
            for target, endpoint in settings.restart_endpoints.items()
        }
        self._timeout = settings.request_timeout_seconds
        self._client = client
        self._tracer = tracer

    async def restart(
        self,
        command: RemediationCommand,
    ) -> RemediationExecutionResult:
        endpoint = self._endpoints.get(command.target)
        if endpoint is None:
            raise RemediationTargetNotAllowedError(
                f"remediation target is not allowlisted: {command.target}"
            )

        if self._tracer is None:
            return await self._restart_allowlisted(command, endpoint)

        with self._tracer.start_as_current_span(
            "remediation restart_service",
            kind=SpanKind.CLIENT,
            attributes={"remediation.action_kind": "restart_service"},
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                result, status_code = await self._restart_allowlisted_with_status(
                    command, endpoint
                )
            except BaseException as error:
                if isinstance(error, RemediationExecutionRejectedError):
                    span.set_attribute(HTTP_RESPONSE_STATUS_CODE, error.status_code)
                mark_span_error(span, error)
                raise
            span.set_attribute(HTTP_RESPONSE_STATUS_CODE, status_code)
            return result

    async def _restart_allowlisted(
        self, command: RemediationCommand, endpoint: str
    ) -> RemediationExecutionResult:
        result, _ = await self._restart_allowlisted_with_status(command, endpoint)
        return result

    async def _restart_allowlisted_with_status(
        self, command: RemediationCommand, endpoint: str
    ) -> tuple[RemediationExecutionResult, int]:
        if self._client is not None:
            response = await self._post(self._client, endpoint, command.proposal_id)
        else:
            async with httpx.AsyncClient(follow_redirects=False) as client:
                response = await self._post(client, endpoint, command.proposal_id)

        if not 200 <= response.status_code < 300:
            raise RemediationExecutionRejectedError(response.status_code)
        return (
            RemediationExecutionResult(
                proposal_id=command.proposal_id,
                action_kind=command.action_kind,
                target=command.target,
                outcome=RemediationExecutionOutcome.SUCCEEDED,
            ),
            response.status_code,
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
