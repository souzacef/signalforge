from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.events import (
    TriageEnrichmentRequested,
    TriageEnrichmentRequestedPayload,
)
from signalforge.triage.models import TriagePriority


def valid_payload() -> dict[str, Any]:
    return {
        "trigger_event_id": uuid4(),
        "source": "alertmanager",
        "title": "Checkout latency increased",
        "description": None,
        "original_severity": "critical",
        "priority": "P1",
        "requires_human_review": True,
        "incident_occurred_at": "2026-09-11T15:30:00-03:00",
    }


def test_enrichment_request_is_an_immutable_json_snapshot_with_distinct_id() -> None:
    trigger_event_id = uuid4()
    enrichment_event_id = uuid4()
    incident_id = uuid4()
    event = TriageEnrichmentRequested(
        event_id=enrichment_event_id,
        occurred_at=datetime(2026, 9, 13, 12, tzinfo=UTC),
        aggregate_id=incident_id,
        payload=TriageEnrichmentRequestedPayload(
            trigger_event_id=trigger_event_id,
            source="alertmanager",
            title="Checkout latency increased",
            description=None,
            original_severity=IncidentSeverity.CRITICAL,
            priority=TriagePriority.P1,
            requires_human_review=True,
            incident_occurred_at=datetime(
                2026, 9, 11, 15, 30, tzinfo=timezone(timedelta(hours=-3))
            ),
        ),
    )

    assert event.event_id == enrichment_event_id
    assert event.event_id != trigger_event_id
    assert event.aggregate_id == incident_id
    assert event.payload.trigger_event_id == trigger_event_id
    assert event.model_dump(mode="json") == {
        "event_id": str(enrichment_event_id),
        "event_type": "triage.enrichment.requested",
        "event_version": 1,
        "occurred_at": "2026-09-13T12:00:00Z",
        "aggregate_id": str(incident_id),
        "payload": {
            "trigger_event_id": str(trigger_event_id),
            "source": "alertmanager",
            "title": "Checkout latency increased",
            "description": None,
            "original_severity": "critical",
            "priority": "P1",
            "requires_human_review": True,
            "incident_occurred_at": "2026-09-11T15:30:00-03:00",
        },
    }
    assert (
        TriageEnrichmentRequested.model_validate_json(event.model_dump_json()) == event
    )
    with pytest.raises(ValidationError, match="frozen_instance"):
        event.event_id = uuid4()


@pytest.mark.parametrize(
    "changes",
    [
        {"trigger_event_id": "not-a-uuid"},
        {"original_severity": "urgent"},
        {"priority": "P0"},
        {"requires_human_review": "not-a-boolean"},
        {"incident_occurred_at": "2026-09-11T15:30:00"},
        {"prompt": "must not be accepted"},
    ],
)
def test_enrichment_payload_rejects_invalid_or_extra_data(
    changes: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        TriageEnrichmentRequestedPayload.model_validate({**valid_payload(), **changes})


@pytest.mark.parametrize(
    "changes",
    [
        {"event_type": "incident.created"},
        {"event_version": 2},
        {"occurred_at": "2026-09-13T12:00:00"},
    ],
)
def test_enrichment_envelope_rejects_invalid_contract_data(
    changes: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        TriageEnrichmentRequested.model_validate(
            {
                "event_id": uuid4(),
                "event_type": "triage.enrichment.requested",
                "event_version": 1,
                "occurred_at": "2026-09-13T12:00:00Z",
                "aggregate_id": uuid4(),
                "payload": valid_payload(),
                **changes,
            }
        )
