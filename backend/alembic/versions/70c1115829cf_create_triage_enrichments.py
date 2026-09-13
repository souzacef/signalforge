"""create triage enrichments

Revision ID: 70c1115829cf
Revises: 2f4d7b8c91ae
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "70c1115829cf"
down_revision: str | Sequence[str] | None = "2f4d7b8c91ae"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create durable advisory enrichment results."""
    op.create_table(
        "triage_enrichments",
        sa.Column("request_event_id", sa.UUID(), nullable=False),
        sa.Column("incident_id", sa.UUID(), nullable=False),
        sa.Column("trigger_event_id", sa.UUID(), nullable=False),
        sa.Column("provider", sa.String(length=50), nullable=False),
        sa.Column("model", sa.String(length=100), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column(
            "category",
            sa.Enum(
                "availability",
                "performance",
                "security",
                "capacity",
                "dependency",
                "deployment",
                "data",
                "unknown",
                name="triage_enrichment_category",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("suspected_component", sa.String(length=200), nullable=True),
        sa.Column(
            "investigation_steps",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(investigation_steps) = 'array' AND jsonb_array_length(investigation_steps) BETWEEN 1 AND 5",
            name="ck_triage_enrichments_investigation_steps_count",
        ),
        sa.CheckConstraint(
            "length(trim(model)) > 0", name="ck_triage_enrichments_model_nonempty"
        ),
        sa.CheckConstraint(
            "length(trim(provider)) > 0", name="ck_triage_enrichments_provider_nonempty"
        ),
        sa.CheckConstraint(
            "length(trim(summary)) BETWEEN 1 AND 1000",
            name="ck_triage_enrichments_summary_length",
        ),
        sa.CheckConstraint(
            "suspected_component IS NULL OR length(trim(suspected_component)) BETWEEN 1 AND 200",
            name="ck_triage_enrichments_suspected_component_length",
        ),
        sa.ForeignKeyConstraint(
            ["incident_id"],
            ["incidents.id"],
            name="fk_triage_enrichments_incident_id_incidents",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("request_event_id"),
    )


def downgrade() -> None:
    """Remove only advisory enrichment results."""
    op.drop_table("triage_enrichments")
