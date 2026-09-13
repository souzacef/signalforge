"""Pure deterministic incident triage rules."""

from dataclasses import dataclass

from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority


@dataclass(frozen=True, slots=True)
class TriageDecision:
    priority: TriagePriority
    requires_human_review: bool


_PRIORITY_BY_SEVERITY = {
    IncidentSeverity.CRITICAL: TriagePriority.P1,
    IncidentSeverity.HIGH: TriagePriority.P2,
    IncidentSeverity.MEDIUM: TriagePriority.P3,
    IncidentSeverity.LOW: TriagePriority.P4,
}


def determine_triage(severity: IncidentSeverity) -> TriageDecision:
    """Map an event-snapshot severity to the deterministic baseline decision."""
    return TriageDecision(
        priority=_PRIORITY_BY_SEVERITY[severity],
        requires_human_review=severity is IncidentSeverity.CRITICAL,
    )
