class RemediationProposalNotFoundError(Exception):
    """Raised when a requested remediation proposal does not exist."""


class IncidentNotFoundForRemediationError(Exception):
    """Raised when a remediation proposal references a missing Incident."""


class IncidentNotEligibleForRemediationError(Exception):
    """Raised when a resolved Incident cannot receive a new proposal."""


class DuplicatePendingRemediationProposalError(Exception):
    """Raised when the same action is already pending for the Incident target."""


class InvalidRemediationTransitionError(Exception):
    """Raised when a proposal cannot enter the requested state."""


class RemediationSelfApprovalError(Exception):
    """Raised when a proposer attempts to approve their own proposal."""


class RemediationExecutionError(Exception):
    """Base class for failures from the internal remediation executor."""


class RemediationNotApprovedForExecutionError(RemediationExecutionError):
    """Raised when command creation is attempted for a non-approved proposal."""


class UnsupportedRemediationActionError(RemediationExecutionError):
    """Raised when the executor receives an action it does not explicitly support."""


class RemediationTargetNotAllowedError(RemediationExecutionError):
    """Raised when no trusted endpoint is configured for a logical target."""


class RemediationExecutionTimeoutError(RemediationExecutionError):
    """Raised when the single actuator request exceeds its bounded timeout."""


class RemediationExecutionTransportError(RemediationExecutionError):
    """Raised when the single actuator request fails at the transport layer."""


class RemediationExecutionRejectedError(RemediationExecutionError):
    """Raised when the configured actuator endpoint returns a non-2xx response."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"remediation endpoint returned HTTP {status_code}")
