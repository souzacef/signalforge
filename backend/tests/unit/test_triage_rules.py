import pytest

from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority
from signalforge.triage.rules import determine_triage


@pytest.mark.parametrize(
    ("severity", "priority", "requires_human_review"),
    [
        (IncidentSeverity.CRITICAL, TriagePriority.P1, True),
        (IncidentSeverity.HIGH, TriagePriority.P2, False),
        (IncidentSeverity.MEDIUM, TriagePriority.P3, False),
        (IncidentSeverity.LOW, TriagePriority.P4, False),
    ],
)
def test_deterministic_triage_mapping(
    severity: IncidentSeverity,
    priority: TriagePriority,
    requires_human_review: bool,
) -> None:
    decision = determine_triage(severity)

    assert decision.priority is priority
    assert decision.requires_human_review is requires_human_review
