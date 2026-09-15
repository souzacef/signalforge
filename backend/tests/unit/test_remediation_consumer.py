import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from signalforge.remediation.consumer import InvalidEventError, decode_event


def valid_event() -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "event_type": "remediation.execution.requested",
        "event_version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "aggregate_id": str(uuid4()),
        "payload": {
            "proposal_id": str(uuid4()),
            "action_kind": "restart_service",
            "target": "checkout-api",
        },
    }


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"\xff", "invalid_encoding"),
        (b"{", "invalid_json"),
        (b"[]", "invalid_event"),
        (
            json.dumps(
                {**valid_event(), "event_type": "remediation.execution.completed"}
            ).encode(),
            "unsupported_type",
        ),
        (
            json.dumps({**valid_event(), "event_version": 2}).encode(),
            "unsupported_version",
        ),
        (
            json.dumps({**valid_event(), "payload": {}}).encode(),
            "invalid_event",
        ),
    ],
)
def test_decoder_rejects_malformed_or_unsupported_events(
    body: bytes,
    reason: str,
) -> None:
    with pytest.raises(InvalidEventError) as caught:
        decode_event(body)
    assert caught.value.reason == reason


def test_decoder_accepts_only_the_strict_v1_contract() -> None:
    values = valid_event()
    decoded = decode_event(json.dumps(values).encode())
    assert decoded.event_type == "remediation.execution.requested"
    assert decoded.event_version == 1
    assert decoded.aggregate_id.hex == str(values["aggregate_id"]).replace("-", "")
