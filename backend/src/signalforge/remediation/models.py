from datetime import datetime
from enum import Enum as PythonEnum
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import text

from signalforge.db.base import Base


class RemediationActionKind(StrEnum):
    RESTART_SERVICE = "restart_service"


class RemediationProposalStatus(StrEnum):
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"


class RemediationExecutionStatus(StrEnum):
    REQUESTED = "requested"


def _enum_values(enum_class: type[PythonEnum]) -> list[str]:
    return [str(member.value) for member in enum_class]


class RemediationProposal(Base):
    __tablename__ = "remediation_proposals"
    __table_args__ = (
        CheckConstraint(
            "char_length(target) BETWEEN 1 AND 100 "
            "AND target ~ '^[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?$'",
            name="ck_remediation_proposals_target_logical_service",
        ),
        CheckConstraint(
            "char_length(reason) BETWEEN 1 AND 1000 "
            "AND reason = btrim(reason, E' \\t\\n\\r\\f\\v')",
            name="ck_remediation_proposals_reason_bounded_trimmed",
        ),
        CheckConstraint(
            "rejection_reason IS NULL OR "
            "(char_length(rejection_reason) BETWEEN 1 AND 1000 "
            "AND rejection_reason = "
            "btrim(rejection_reason, E' \\t\\n\\r\\f\\v'))",
            name="ck_remediation_proposals_rejection_reason_bounded_trimmed",
        ),
        CheckConstraint(
            "approved_by_user_id IS NULL OR approved_by_user_id <> proposed_by_user_id",
            name="ck_remediation_proposals_no_self_approval",
        ),
        CheckConstraint(
            "(status = 'pending_approval' "
            "AND approved_by_user_id IS NULL AND approved_at IS NULL "
            "AND rejected_by_user_id IS NULL AND rejected_at IS NULL "
            "AND rejection_reason IS NULL) "
            "OR (status = 'approved' "
            "AND approved_by_user_id IS NOT NULL AND approved_at IS NOT NULL "
            "AND rejected_by_user_id IS NULL AND rejected_at IS NULL "
            "AND rejection_reason IS NULL) "
            "OR (status = 'rejected' "
            "AND approved_by_user_id IS NULL AND approved_at IS NULL "
            "AND rejected_by_user_id IS NOT NULL AND rejected_at IS NOT NULL "
            "AND rejection_reason IS NOT NULL)",
            name="ck_remediation_proposals_status_attribution",
        ),
        Index(
            "uq_remediation_proposals_pending_incident_action_target",
            "incident_id",
            "action_kind",
            "target",
            unique=True,
            postgresql_where=text("status = 'pending_approval'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    incident_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "incidents.id",
            name="fk_remediation_proposals_incident_id_incidents",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    action_kind: Mapped[RemediationActionKind] = mapped_column(
        Enum(
            RemediationActionKind,
            name="remediation_action_kind",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    target: Mapped[str] = mapped_column(String(100), nullable=False)
    reason: Mapped[str] = mapped_column(String(1000), nullable=False)
    status: Mapped[RemediationProposalStatus] = mapped_column(
        Enum(
            RemediationProposalStatus,
            name="remediation_proposal_status",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
        default=RemediationProposalStatus.PENDING_APPROVAL,
        server_default=RemediationProposalStatus.PENDING_APPROVAL.value,
    )
    proposed_by_user_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "users.id",
            name="fk_remediation_proposals_proposed_by_user_id_users",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    approved_by_user_id: Mapped[UUID | None] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "users.id",
            name="fk_remediation_proposals_approved_by_user_id_users",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    approved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rejected_by_user_id: Mapped[UUID | None] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "users.id",
            name="fk_remediation_proposals_rejected_by_user_id_users",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    rejected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rejection_reason: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class RemediationExecution(Base):
    __tablename__ = "remediation_executions"
    __table_args__ = (
        CheckConstraint(
            "char_length(target) BETWEEN 1 AND 100 "
            "AND target ~ '^[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?$'",
            name="ck_remediation_executions_target_logical_service",
        ),
        UniqueConstraint(
            "proposal_id",
            name="uq_remediation_executions_proposal_id",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    proposal_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "remediation_proposals.id",
            name="fk_remediation_executions_proposal_id_remediation_proposals",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    action_kind: Mapped[RemediationActionKind] = mapped_column(
        Enum(
            RemediationActionKind,
            name="remediation_execution_action_kind",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    target: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[RemediationExecutionStatus] = mapped_column(
        Enum(
            RemediationExecutionStatus,
            name="remediation_execution_status",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
        default=RemediationExecutionStatus.REQUESTED,
        server_default=RemediationExecutionStatus.REQUESTED.value,
    )
    requested_by_user_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "users.id",
            name="fk_remediation_executions_requested_by_user_id_users",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
