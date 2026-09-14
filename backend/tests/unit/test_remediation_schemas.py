from uuid import uuid4

import pytest
from pydantic import ValidationError

from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationProposalStatus,
)
from signalforge.remediation.schemas import (
    RemediationProposalCreate,
    RemediationProposalReject,
)


def proposal(**overrides: object) -> RemediationProposalCreate:
    values: dict[str, object] = {
        "incident_id": uuid4(),
        "action_kind": "restart_service",
        "target": "checkout-api",
        "reason": "Error rate is high",
    }
    values.update(overrides)
    return RemediationProposalCreate.model_validate(values)


def test_only_one_action_kind_and_three_approval_states_exist() -> None:
    assert list(RemediationActionKind) == [RemediationActionKind.RESTART_SERVICE]
    assert list(RemediationProposalStatus) == [
        RemediationProposalStatus.PENDING_APPROVAL,
        RemediationProposalStatus.APPROVED,
        RemediationProposalStatus.REJECTED,
    ]


def test_proposal_normalizes_human_text_and_target() -> None:
    parsed = proposal(target=" checkout-api ", reason="  Error rate is high  ")
    assert parsed.target == "checkout-api"
    assert parsed.reason == "Error rate is high"


@pytest.mark.parametrize(
    "target",
    [
        "",
        " ",
        "checkout api",
        "checkout; rm",
        "checkout\napi",
        "checkout-api\n",
        "\tcheckout-api",
        "checkout\x00api",
        "checkout\x7fapi",
        "checkout/api",
        "checkout$(id)",
        "-checkout",
        "checkout-",
        "Checkout",
        "a" * 101,
    ],
)
def test_invalid_service_targets_are_rejected(target: str) -> None:
    with pytest.raises(ValidationError):
        proposal(target=target)


@pytest.mark.parametrize("reason", ["", " \t ", "a" * 1001])
def test_invalid_proposal_reasons_are_rejected(reason: str) -> None:
    with pytest.raises(ValidationError):
        proposal(reason=reason)


@pytest.mark.parametrize("reason", ["", " \t ", "a" * 1001])
def test_invalid_rejection_reasons_are_rejected(reason: str) -> None:
    with pytest.raises(ValidationError):
        RemediationProposalReject(rejection_reason=reason)


def test_rejection_reason_is_trimmed() -> None:
    assert (
        RemediationProposalReject(
            rejection_reason="  No longer needed  "
        ).rejection_reason
        == "No longer needed"
    )


def test_unknown_action_and_executable_payload_are_rejected() -> None:
    with pytest.raises(ValidationError):
        proposal(action_kind="shell_command")
    with pytest.raises(ValidationError):
        proposal(command="rm -rf /")
