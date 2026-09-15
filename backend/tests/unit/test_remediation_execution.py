import ast
import asyncio
import json
from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from typing import cast
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from signalforge.core.config import RemediationExecutionSettings
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
from signalforge.remediation.execution import (
    AllowlistedHttpRemediationExecutor,
    HttpRestartServiceAdapter,
    RemediationCommand,
    RemediationExecutionOutcome,
    RemediationExecutionResult,
    command_from_approved_proposal,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationProposal,
    RemediationProposalStatus,
)

pytestmark = pytest.mark.anyio


def proposal(status: RemediationProposalStatus) -> RemediationProposal:
    return RemediationProposal(
        id=uuid4(),
        incident_id=uuid4(),
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target="checkout-api",
        reason="Free-form reason that must never reach the actuator",
        status=status,
        proposed_by_user_id=uuid4(),
    )


def command(target: str = "checkout-api") -> RemediationCommand:
    return RemediationCommand(
        proposal_id=uuid4(),
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target=target,
    )


def settings(
    endpoints: dict[str, str] | None = None,
    *,
    timeout: float = 5.0,
) -> RemediationExecutionSettings:
    return RemediationExecutionSettings.model_validate(
        {
            "restart_endpoints": endpoints or {},
            "request_timeout_seconds": timeout,
        }
    )


def test_approved_proposal_produces_small_immutable_command() -> None:
    approved = proposal(RemediationProposalStatus.APPROVED)

    result = command_from_approved_proposal(approved)

    assert result == RemediationCommand(
        proposal_id=approved.id,
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target="checkout-api",
    )
    assert [field.name for field in fields(result)] == [
        "proposal_id",
        "action_kind",
        "target",
    ]
    with pytest.raises(FrozenInstanceError):
        result.target = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    "status",
    [
        RemediationProposalStatus.PENDING_APPROVAL,
        RemediationProposalStatus.REJECTED,
    ],
)
def test_non_approved_proposal_cannot_produce_command(
    status: RemediationProposalStatus,
) -> None:
    with pytest.raises(RemediationNotApprovedForExecutionError):
        command_from_approved_proposal(proposal(status))


def test_execution_settings_allow_empty_and_multiple_valid_targets() -> None:
    assert settings().restart_endpoints == {}

    configured = settings(
        {
            "checkout-api": "http://checkout-control:8080/internal/restart",
            "billing.worker": "https://billing-control/internal/restart",
        }
    )

    assert set(configured.restart_endpoints) == {"checkout-api", "billing.worker"}


@pytest.mark.parametrize(
    "target",
    [
        "Checkout",
        "checkout api",
        "checkout/api",
        "checkout-api\n",
        "http://attacker.example",
        "-checkout",
        "checkout-",
    ],
)
def test_execution_settings_reject_invalid_logical_targets(target: str) -> None:
    with pytest.raises(ValidationError):
        settings({target: "http://control/internal/restart"})


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://control/internal/restart",
        "http://user:secret@control/internal/restart",
        "http://control/internal/restart?token=secret",
        "http://control/internal/restart#fragment",
    ],
)
def test_execution_settings_reject_unsafe_endpoint_urls(endpoint: str) -> None:
    with pytest.raises(ValidationError):
        settings({"checkout-api": endpoint})


def test_execution_settings_parse_environment_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "SIGNALFORGE_REMEDIATION_RESTART_ENDPOINTS",
        '{"checkout-api":"http://control/internal/restart"}',
    )

    configured = RemediationExecutionSettings(_env_file=None)

    assert str(configured.restart_endpoints["checkout-api"]) == (
        "http://control/internal/restart"
    )


class RecordingRestartAdapter:
    def __init__(self) -> None:
        self.commands: list[RemediationCommand] = []

    async def restart(
        self,
        remediation_command: RemediationCommand,
    ) -> RemediationExecutionResult:
        self.commands.append(remediation_command)
        return RemediationExecutionResult(
            proposal_id=remediation_command.proposal_id,
            action_kind=remediation_command.action_kind,
            target=remediation_command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )


async def test_executor_dispatches_restart_service_explicitly() -> None:
    adapter = RecordingRestartAdapter()
    executor = AllowlistedHttpRemediationExecutor(adapter)
    remediation_command = command()

    result = await executor.execute(remediation_command)

    assert adapter.commands == [remediation_command]
    assert result.outcome is RemediationExecutionOutcome.SUCCEEDED


async def test_executor_fails_closed_for_unsupported_action() -> None:
    adapter = RecordingRestartAdapter()
    executor = AllowlistedHttpRemediationExecutor(adapter)
    unsupported = RemediationCommand(
        proposal_id=uuid4(),
        action_kind=cast(RemediationActionKind, "stop_service"),
        target="checkout-api",
    )

    with pytest.raises(UnsupportedRemediationActionError):
        await executor.execute(unsupported)

    assert adapter.commands == []


async def test_unknown_or_url_like_target_fails_before_network_call() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = HttpRestartServiceAdapter(settings(), client)
        with pytest.raises(RemediationTargetNotAllowedError):
            await adapter.restart(command("http://attacker.example/restart"))

    assert requests == []


async def test_allowlisted_target_posts_once_to_exact_configured_endpoint() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202, json={"ignored": "response"})

    remediation_command = command()
    endpoint = "http://checkout-control:8080/internal/restart"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = HttpRestartServiceAdapter(
            settings(
                {
                    "checkout-api": endpoint,
                    "billing-api": "http://billing-control/internal/restart",
                }
            ),
            client,
        )
        result = await adapter.restart(remediation_command)

    assert result == RemediationExecutionResult(
        proposal_id=remediation_command.proposal_id,
        action_kind=RemediationActionKind.RESTART_SERVICE,
        target="checkout-api",
        outcome=RemediationExecutionOutcome.SUCCEEDED,
    )
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == endpoint
    assert request.headers["content-type"] == "application/json"
    assert request.headers["idempotency-key"] == str(remediation_command.proposal_id)
    assert json.loads(request.content) == {
        "proposal_id": str(remediation_command.proposal_id)
    }
    assert request.extensions["timeout"] == {
        "connect": 5.0,
        "read": 5.0,
        "write": 5.0,
        "pool": 5.0,
    }


async def test_target_a_cannot_select_target_b_endpoint() -> None:
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = HttpRestartServiceAdapter(
            settings(
                {
                    "service-a": "http://control-a/internal/restart",
                    "service-b": "http://control-b/internal/restart",
                }
            ),
            client,
        )
        await adapter.restart(command("service-a"))

    assert requested_urls == ["http://control-a/internal/restart"]


async def test_redirect_is_not_followed() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "control":
            return httpx.Response(
                307,
                headers={"Location": "http://attacker.example/restart"},
            )
        return httpx.Response(204)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    ) as client:
        adapter = HttpRestartServiceAdapter(
            settings({"checkout-api": "http://control/internal/restart"}),
            client,
        )
        with pytest.raises(RemediationExecutionRejectedError) as raised:
            await adapter.restart(command())

    assert raised.value.status_code == 307
    assert len(requests) == 1
    assert requests[0].url.host == "control"


@pytest.mark.parametrize("status_code", [400, 503])
async def test_non_2xx_is_typed_and_does_not_expose_response_body(
    status_code: int,
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code, text="remote secret body")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = HttpRestartServiceAdapter(
            settings({"checkout-api": "http://control/internal/restart"}),
            client,
        )
        with pytest.raises(RemediationExecutionRejectedError) as raised:
            await adapter.restart(command())

    assert raised.value.status_code == status_code
    assert "remote secret body" not in str(raised.value)
    assert attempts == 1


async def test_timeout_is_typed_and_not_retried() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("remote detail", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = HttpRestartServiceAdapter(
            settings({"checkout-api": "http://control/internal/restart"}),
            client,
        )
        with pytest.raises(RemediationExecutionTimeoutError) as raised:
            await adapter.restart(command())

    assert "remote detail" not in str(raised.value)
    assert attempts == 1


async def test_transport_failure_is_typed_and_not_retried() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("remote detail", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = HttpRestartServiceAdapter(
            settings({"checkout-api": "http://control/internal/restart"}),
            client,
        )
        with pytest.raises(RemediationExecutionTransportError) as raised:
            await adapter.restart(command())

    assert "remote detail" not in str(raised.value)
    assert attempts == 1


def test_execution_module_imports_no_host_control_libraries() -> None:
    source = Path(__file__).parents[2] / "src/signalforge/remediation/execution.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(
                alias.name.split(".", maxsplit=1)[0] for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_roots.add(node.module.split(".", maxsplit=1)[0])

    assert imported_roots.isdisjoint({"subprocess", "os", "docker", "kubernetes"})


@pytest.mark.parametrize(
    ("timeout", "lease"),
    [(5.0, 5.0), (5.0, 4.9), (float("inf"), 45.0), (5.0, float("inf"))],
)
def test_execution_settings_require_finite_lease_longer_than_timeout(
    timeout: float,
    lease: float,
) -> None:
    with pytest.raises(ValidationError):
        RemediationExecutionSettings.model_validate(
            {
                "request_timeout_seconds": timeout,
                "attempt_lease_seconds": lease,
            }
        )


def test_execution_attempt_lease_defaults_to_45_seconds() -> None:
    configured = settings()
    assert configured.attempt_lease_seconds == 45.0
    assert configured.attempt_lease_seconds > configured.request_timeout_seconds


class RaisingRestartAdapter:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0

    async def restart(
        self, remediation_command: RemediationCommand
    ) -> RemediationExecutionResult:
        self.calls += 1
        raise self.error


def executor_metric_value(
    metrics: RemediationExecutorMetrics,
    name: str,
    result: RemediationExecutorMetricResult,
) -> float | None:
    return metrics.registry.get_sample_value(name, {"result": result.value})


async def test_executor_success_records_one_call_and_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = RemediationExecutorMetrics()
    adapter = RecordingRestartAdapter()
    executor = AllowlistedHttpRemediationExecutor(adapter, metrics)
    times = iter((10.0, 10.25))
    monkeypatch.setattr(
        "signalforge.remediation.execution.monotonic", lambda: next(times)
    )
    remediation_command = command()

    result = await executor.execute(remediation_command)

    assert result.outcome is RemediationExecutionOutcome.SUCCEEDED
    assert (
        executor_metric_value(
            metrics,
            "signalforge_remediation_executor_calls_total",
            RemediationExecutorMetricResult.SUCCESS,
        )
        == 1
    )
    assert (
        executor_metric_value(
            metrics,
            "signalforge_remediation_executor_duration_seconds_count",
            RemediationExecutorMetricResult.SUCCESS,
        )
        == 1
    )
    assert executor_metric_value(
        metrics,
        "signalforge_remediation_executor_duration_seconds_sum",
        RemediationExecutorMetricResult.SUCCESS,
    ) == pytest.approx(0.25)


@pytest.mark.parametrize(
    ("error", "metric_result"),
    [
        (
            RemediationTargetNotAllowedError("not allowed"),
            RemediationExecutorMetricResult.TARGET_NOT_ALLOWED,
        ),
        (
            RemediationExecutionRejectedError(400),
            RemediationExecutorMetricResult.HTTP_REJECTED,
        ),
        (
            RemediationExecutionRejectedError(500),
            RemediationExecutorMetricResult.HTTP_SERVER_ERROR,
        ),
        (
            RemediationExecutionTimeoutError("timeout"),
            RemediationExecutorMetricResult.TIMEOUT,
        ),
        (
            RemediationExecutionTransportError("transport"),
            RemediationExecutorMetricResult.TRANSPORT,
        ),
        (asyncio.CancelledError(), RemediationExecutorMetricResult.INTERRUPTED),
        (RuntimeError("unexpected"), RemediationExecutorMetricResult.UNEXPECTED),
    ],
)
async def test_executor_error_metrics_preserve_original_exception(
    error: BaseException,
    metric_result: RemediationExecutorMetricResult,
) -> None:
    metrics = RemediationExecutorMetrics()
    adapter = RaisingRestartAdapter(error)
    executor = AllowlistedHttpRemediationExecutor(adapter, metrics)

    with pytest.raises(type(error)) as caught:
        await executor.execute(command())

    assert caught.value is error
    assert adapter.calls == 1
    assert (
        executor_metric_value(
            metrics,
            "signalforge_remediation_executor_calls_total",
            metric_result,
        )
        == 1
    )
    assert (
        executor_metric_value(
            metrics,
            "signalforge_remediation_executor_duration_seconds_count",
            metric_result,
        )
        == 1
    )


async def test_unsupported_action_metric_preserves_fail_closed_behavior() -> None:
    metrics = RemediationExecutorMetrics()
    adapter = RecordingRestartAdapter()
    executor = AllowlistedHttpRemediationExecutor(adapter, metrics)
    unsupported = RemediationCommand(
        proposal_id=uuid4(),
        action_kind=cast(RemediationActionKind, "stop_service"),
        target="checkout-api",
    )

    with pytest.raises(UnsupportedRemediationActionError):
        await executor.execute(unsupported)

    assert adapter.commands == []
    assert (
        executor_metric_value(
            metrics,
            "signalforge_remediation_executor_calls_total",
            RemediationExecutorMetricResult.UNSUPPORTED_ACTION,
        )
        == 1
    )


async def test_executor_metric_failure_does_not_change_result_or_exception() -> None:
    class FailingMetrics:
        def record(self, **kwargs: object) -> None:
            raise RuntimeError("metrics unavailable")

    successful = AllowlistedHttpRemediationExecutor(
        RecordingRestartAdapter(),
        cast(RemediationExecutorMetrics, FailingMetrics()),
    )
    result = await successful.execute(command())
    assert result.outcome is RemediationExecutionOutcome.SUCCEEDED

    error = RemediationExecutionTimeoutError("original")
    failing = AllowlistedHttpRemediationExecutor(
        RaisingRestartAdapter(error),
        cast(RemediationExecutorMetrics, FailingMetrics()),
    )
    with pytest.raises(RemediationExecutionTimeoutError) as caught:
        await failing.execute(command())
    assert caught.value is error
