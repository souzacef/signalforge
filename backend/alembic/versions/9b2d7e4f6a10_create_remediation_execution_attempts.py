"""create remediation execution attempts

Revision ID: 9b2d7e4f6a10
Revises: 625caaeb597a
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9b2d7e4f6a10"
down_revision: str | Sequence[str] | None = "625caaeb597a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_EXECUTION_STATUSES = (
    "requested",
    "in_progress",
    "succeeded",
    "failed",
    "outcome_unknown",
)
_ATTEMPT_STATUSES = ("in_progress", "succeeded", "failed", "outcome_unknown")
_FAILURE_KINDS = (
    "target_not_allowed",
    "unsupported_action",
    "http_rejected",
    "http_server_error",
    "timeout",
    "transport",
    "interrupted",
    "unexpected",
)


def upgrade() -> None:
    op.drop_constraint(
        "remediation_execution_status", "remediation_executions", type_="check"
    )
    op.alter_column(
        "remediation_executions",
        "status",
        existing_type=sa.String(length=9),
        type_=sa.String(length=15),
        existing_nullable=False,
        existing_server_default="requested",
    )
    op.create_check_constraint(
        "remediation_execution_status",
        "remediation_executions",
        f"status IN {repr(_EXECUTION_STATUSES)}",
    )
    op.add_column(
        "remediation_executions",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_remediation_executions_status_completion",
        "remediation_executions",
        "status NOT IN ('requested', 'in_progress', 'succeeded', 'failed', "
        "'outcome_unknown') OR "
        "(status IN ('requested', 'in_progress') AND completed_at IS NULL) "
        "OR (status IN ('succeeded', 'failed', 'outcome_unknown') "
        "AND completed_at IS NOT NULL)",
    )
    op.create_table(
        "remediation_execution_attempts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("execution_id", sa.UUID(), nullable=False),
        sa.Column("request_event_id", sa.UUID(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                *_ATTEMPT_STATUSES,
                name="remediation_execution_attempt_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "failure_kind",
            sa.Enum(
                *_FAILURE_KINDS,
                name="remediation_failure_kind",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column("http_status_code", sa.SmallInteger(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempt_number > 0",
            name="ck_remediation_execution_attempts_number_positive",
        ),
        sa.CheckConstraint(
            "(status = 'in_progress' AND completed_at IS NULL "
            "AND failure_kind IS NULL AND http_status_code IS NULL "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status = 'succeeded' AND completed_at IS NOT NULL "
            "AND failure_kind IS NULL AND http_status_code IS NULL "
            "AND lease_expires_at IS NULL) OR "
            "(status IN ('failed', 'outcome_unknown') AND completed_at IS NOT NULL "
            "AND failure_kind IS NOT NULL AND lease_expires_at IS NULL)",
            name="ck_remediation_execution_attempts_status_attribution",
        ),
        sa.CheckConstraint(
            "(status = 'in_progress' AND failure_kind IS NULL) OR "
            "(status = 'succeeded' AND failure_kind IS NULL) OR "
            "(status = 'failed' AND failure_kind IN "
            "('target_not_allowed', 'unsupported_action', 'http_rejected')) OR "
            "(status = 'outcome_unknown' AND failure_kind IN "
            "('http_server_error', 'timeout', 'transport', 'interrupted', "
            "'unexpected'))",
            name="ck_remediation_execution_attempts_failure_attribution",
        ),
        sa.CheckConstraint(
            "(failure_kind = 'http_rejected' "
            "AND http_status_code IS NOT NULL "
            "AND http_status_code BETWEEN 300 AND 499) OR "
            "(failure_kind = 'http_server_error' "
            "AND http_status_code IS NOT NULL "
            "AND http_status_code BETWEEN 500 AND 999) OR "
            "(failure_kind IS DISTINCT FROM 'http_rejected' "
            "AND failure_kind IS DISTINCT FROM 'http_server_error' "
            "AND http_status_code IS NULL)",
            name="ck_remediation_execution_attempts_http_attribution",
        ),
        sa.ForeignKeyConstraint(
            ["execution_id"],
            ["remediation_executions.id"],
            name="fk_remediation_attempts_execution_id_executions",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_remediation_execution_attempts"),
        sa.UniqueConstraint(
            "execution_id",
            "attempt_number",
            name="uq_remediation_execution_attempts_execution_number",
        ),
        sa.UniqueConstraint(
            "request_event_id",
            name="uq_remediation_execution_attempts_request_event_id",
        ),
    )


def downgrade() -> None:
    op.drop_table("remediation_execution_attempts")
    op.drop_constraint(
        "ck_remediation_executions_status_completion",
        "remediation_executions",
        type_="check",
    )
    op.execute(
        "UPDATE remediation_executions SET status = 'requested', completed_at = NULL "
        "WHERE status <> 'requested'"
    )
    op.drop_column("remediation_executions", "completed_at")
    op.drop_constraint(
        "remediation_execution_status", "remediation_executions", type_="check"
    )
    op.alter_column(
        "remediation_executions",
        "status",
        existing_type=sa.String(length=15),
        type_=sa.String(length=9),
        existing_nullable=False,
        existing_server_default="requested",
    )
    op.create_check_constraint(
        "remediation_execution_status",
        "remediation_executions",
        "status IN ('requested')",
    )
