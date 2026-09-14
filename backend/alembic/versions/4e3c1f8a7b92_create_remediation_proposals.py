"""create remediation proposals

Revision ID: 4e3c1f8a7b92
Revises: a19c7e4d2b63
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "4e3c1f8a7b92"
down_revision: str | Sequence[str] | None = "a19c7e4d2b63"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the controlled remediation proposal and approval domain."""
    op.create_table(
        "remediation_proposals",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("incident_id", sa.UUID(), nullable=False),
        sa.Column(
            "action_kind",
            sa.Enum(
                "restart_service",
                name="remediation_action_kind",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("target", sa.String(length=100), nullable=False),
        sa.Column("reason", sa.String(length=1000), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "pending_approval",
                "approved",
                "rejected",
                name="remediation_proposal_status",
                native_enum=False,
                create_constraint=True,
            ),
            server_default="pending_approval",
            nullable=False,
        ),
        sa.Column("proposed_by_user_id", sa.UUID(), nullable=False),
        sa.Column("approved_by_user_id", sa.UUID(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejected_by_user_id", sa.UUID(), nullable=True),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejection_reason", sa.String(length=1000), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(target) BETWEEN 1 AND 100 "
            "AND target ~ '^[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?$'",
            name="ck_remediation_proposals_target_logical_service",
        ),
        sa.CheckConstraint(
            "char_length(reason) BETWEEN 1 AND 1000 "
            "AND reason = btrim(reason, E' \\t\\n\\r\\f\\v')",
            name="ck_remediation_proposals_reason_bounded_trimmed",
        ),
        sa.CheckConstraint(
            "rejection_reason IS NULL OR "
            "(char_length(rejection_reason) BETWEEN 1 AND 1000 "
            "AND rejection_reason = "
            "btrim(rejection_reason, E' \\t\\n\\r\\f\\v'))",
            name="ck_remediation_proposals_rejection_reason_bounded_trimmed",
        ),
        sa.CheckConstraint(
            "approved_by_user_id IS NULL OR approved_by_user_id <> proposed_by_user_id",
            name="ck_remediation_proposals_no_self_approval",
        ),
        sa.CheckConstraint(
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
        sa.ForeignKeyConstraint(
            ["incident_id"],
            ["incidents.id"],
            name="fk_remediation_proposals_incident_id_incidents",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["proposed_by_user_id"],
            ["users.id"],
            name="fk_remediation_proposals_proposed_by_user_id_users",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["approved_by_user_id"],
            ["users.id"],
            name="fk_remediation_proposals_approved_by_user_id_users",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["rejected_by_user_id"],
            ["users.id"],
            name="fk_remediation_proposals_rejected_by_user_id_users",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_remediation_proposals"),
    )
    op.create_index(
        "uq_remediation_proposals_pending_incident_action_target",
        "remediation_proposals",
        ["incident_id", "action_kind", "target"],
        unique=True,
        postgresql_where=sa.text("status = 'pending_approval'"),
    )


def downgrade() -> None:
    """Remove the remediation proposal domain."""
    op.drop_index(
        "uq_remediation_proposals_pending_incident_action_target",
        table_name="remediation_proposals",
        postgresql_where=sa.text("status = 'pending_approval'"),
    )
    op.drop_table("remediation_proposals")
