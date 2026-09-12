from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from signalforge.incidents.events import IncidentCreated, IncidentCreatedPayload
from signalforge.incidents.models import IncidentSeverity


def test_incident_created_is_an_immutable_json_serializable_snapshot() -> None:
    event = IncidentCreated(
        event_id=uuid4(),
        occurred_at=datetime(2026, 9, 12, 12, tzinfo=UTC),
        aggregate_id=uuid4(),
        payload=IncidentCreatedPayload(
            source="manual",
            title="Checkout alert",
            description=None,
            severity=IncidentSeverity.HIGH,
            incident_occurred_at=datetime(
                2026, 9, 11, 15, 30, tzinfo=timezone(timedelta(hours=-3))
            ),
        ),
    )

    assert event.model_dump(mode="json") == {
        "event_id": str(event.event_id),
        "event_type": "incident.created",
        "event_version": 1,
        "occurred_at": "2026-09-12T12:00:00Z",
        "aggregate_id": str(event.aggregate_id),
        "payload": {
            "source": "manual",
            "title": "Checkout alert",
            "description": None,
            "severity": "high",
            "incident_occurred_at": "2026-09-11T15:30:00-03:00",
        },
    }
    assert IncidentCreated.model_validate_json(event.model_dump_json()) == event

    with pytest.raises(ValidationError, match="frozen_instance"):
        event.event_id = uuid4()
    with pytest.raises(ValidationError, match="frozen_instance"):
        event.payload.title = "Changed later"
