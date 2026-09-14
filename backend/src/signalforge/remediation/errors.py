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
