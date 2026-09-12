"""create outbox events

Revision ID: c42b7d91e6a8
Revises: b6b023ebef03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c42b7d91e6a8"
down_revision: str | Sequence[str] | None = "b6b023ebef03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add delivery intent storage without backfilling historical incidents."""
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("event_version", sa.SmallInteger(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "event_version > 0", name="ck_outbox_events_event_version_positive"
        ),
        sa.CheckConstraint(
            "length(trim(event_type)) > 0", name="ck_outbox_events_event_type_nonempty"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name="ck_outbox_events_payload_object"
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Remove delivery intent storage."""
    op.drop_table("outbox_events")
