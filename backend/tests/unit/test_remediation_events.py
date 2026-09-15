from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from signalforge.remediation.events import (
    RemediationExecutionRequested,
    RemediationExecutionRequestedPayload,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationExecutionStatus,
)


def event() -> RemediationExecutionRequested:
    return RemediationExecutionRequested(
        event_id=uuid4(),
        occurred_at=datetime(2026, 9, 14, tzinfo=UTC),
        aggregate_id=uuid4(),
        payload=RemediationExecutionRequestedPayload(
            proposal_id=uuid4(),
            action_kind=RemediationActionKind.RESTART_SERVICE,
            target="checkout-api",
        ),
    )


def test_execution_status_has_phase_5d2a_states() -> None:
    assert list(RemediationExecutionStatus) == [
        RemediationExecutionStatus.REQUESTED,
        RemediationExecutionStatus.IN_PROGRESS,
        RemediationExecutionStatus.SUCCEEDED,
        RemediationExecutionStatus.FAILED,
        RemediationExecutionStatus.OUTCOME_UNKNOWN,
    ]


def test_v1_execution_request_is_frozen_and_roundtrips() -> None:
    original = event()
    serialized = original.model_dump_json()
    restored = RemediationExecutionRequested.model_validate_json(serialized)

    assert restored == original
    assert restored.event_type == "remediation.execution.requested"
    assert restored.event_version == 1
    assert restored.payload.action_kind is RemediationActionKind.RESTART_SERVICE
    assert set(restored.payload.model_dump()) == {
        "proposal_id",
        "action_kind",
        "target",
    }
    for forbidden in (
        "reason",
        "rejection_reason",
        "email",
        "jwt",
        "endpoint_url",
        "headers",
        "credentials",
        "command",
    ):
        assert forbidden not in serialized
    with pytest.raises(ValidationError):
        original.payload.target = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    "changes",
    [
        {"unexpected": "field"},
        {"action_kind": "shell_command"},
        {"target": "checkout-api; reboot"},
    ],
)
def test_execution_request_payload_rejects_extra_or_unbounded_fields(
    changes: dict[str, str],
) -> None:
    values = {
        "proposal_id": uuid4(),
        "action_kind": "restart_service",
        "target": "checkout-api",
        **changes,
    }
    with pytest.raises(ValidationError):
        RemediationExecutionRequestedPayload.model_validate(values)


@pytest.mark.parametrize(
    "changes",
    [
        {"event_type": "remediation.execution.completed"},
        {"event_version": 2},
        {"unexpected": "field"},
    ],
)
def test_execution_request_envelope_rejects_wrong_contract(
    changes: dict[str, object],
) -> None:
    values = event().model_dump()
    values.update(changes)
    with pytest.raises(ValidationError):
        RemediationExecutionRequested.model_validate(values)
