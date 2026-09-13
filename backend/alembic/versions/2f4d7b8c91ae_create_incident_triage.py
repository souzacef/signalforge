"""create incident triage

Revision ID: 2f4d7b8c91ae
Revises: e64c872d5e0a
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2f4d7b8c91ae"
down_revision: str | Sequence[str] | None = "e64c872d5e0a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add one deterministic triage result per Incident creation event."""
    op.create_table(
        "incident_triage",
        sa.Column("incident_id", sa.UUID(), nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column(
            "original_severity",
            sa.Enum(
                "low",
                "medium",
                "high",
                "critical",
                name="incident_triage_original_severity",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column(
            "priority",
            sa.Enum(
                "P1",
                "P2",
                "P3",
                "P4",
                name="incident_triage_priority",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("requires_human_review", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(trim(source)) > 0",
            name="ck_incident_triage_source_nonempty",
        ),
        sa.ForeignKeyConstraint(
            ["incident_id"],
            ["incidents.id"],
            name="fk_incident_triage_incident_id_incidents",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("incident_id"),
        sa.UniqueConstraint("event_id", name="uq_incident_triage_event_id"),
    )


def downgrade() -> None:
    """Remove only deterministic Incident triage records."""
    op.drop_table("incident_triage")
