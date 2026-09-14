"""create remediation executions

Revision ID: 625caaeb597a
Revises: 4e3c1f8a7b92
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "625caaeb597a"
down_revision: str | Sequence[str] | None = "4e3c1f8a7b92"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create durable logical remediation execution requests."""
    op.create_table(
        "remediation_executions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("proposal_id", sa.UUID(), nullable=False),
        sa.Column(
            "action_kind",
            sa.Enum(
                "restart_service",
                name="remediation_execution_action_kind",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("target", sa.String(length=100), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "requested",
                name="remediation_execution_status",
                native_enum=False,
                create_constraint=True,
            ),
            server_default="requested",
            nullable=False,
        ),
        sa.Column("requested_by_user_id", sa.UUID(), nullable=False),
        sa.Column(
            "requested_at",
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
            name="ck_remediation_executions_target_logical_service",
        ),
        sa.ForeignKeyConstraint(
            ["proposal_id"],
            ["remediation_proposals.id"],
            name="fk_remediation_executions_proposal_id_remediation_proposals",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["requested_by_user_id"],
            ["users.id"],
            name="fk_remediation_executions_requested_by_user_id_users",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_remediation_executions"),
        sa.UniqueConstraint(
            "proposal_id",
            name="uq_remediation_executions_proposal_id",
        ),
    )


def downgrade() -> None:
    """Remove durable logical remediation execution requests."""
    op.drop_table("remediation_executions")
