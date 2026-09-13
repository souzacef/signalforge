"""create processed events

Revision ID: e64c872d5e0a
Revises: d83e9a2f6b10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e64c872d5e0a"
down_revision: str | Sequence[str] | None = "d83e9a2f6b10"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add a per-consumer durable processing receipt."""
    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.String(length=100), nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("event_version", sa.SmallInteger(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(trim(consumer_name)) > 0",
            name="ck_processed_events_consumer_name_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(event_type)) > 0",
            name="ck_processed_events_event_type_nonempty",
        ),
        sa.CheckConstraint(
            "event_version > 0",
            name="ck_processed_events_event_version_positive",
        ),
        sa.PrimaryKeyConstraint("consumer_name", "event_id"),
    )


def downgrade() -> None:
    """Remove only consumer processing receipts."""
    op.drop_table("processed_events")
