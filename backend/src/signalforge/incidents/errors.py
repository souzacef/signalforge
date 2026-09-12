class IncidentNotFoundError(Exception):
    """Raised when a requested Incident does not exist."""


class InvalidIncidentTransitionError(Exception):
    """Raised when an Incident cannot enter the requested lifecycle state."""
