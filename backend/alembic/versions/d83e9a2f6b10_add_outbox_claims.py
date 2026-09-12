"""add durable outbox claims

Revision ID: d83e9a2f6b10
Revises: c42b7d91e6a8
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d83e9a2f6b10"
down_revision: str | Sequence[str] | None = "c42b7d91e6a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Make existing unpublished intents due without creating any events."""
    op.add_column(
        "outbox_events",
        sa.Column(
            "attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column("outbox_events", sa.Column("claim_token", sa.UUID(), nullable=True))
    op.add_column(
        "outbox_events",
        sa.Column("claimed_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "outbox_events", sa.Column("last_error", sa.String(length=64), nullable=True)
    )
    op.create_check_constraint(
        "ck_outbox_events_attempt_count_nonnegative",
        "outbox_events",
        "attempt_count >= 0",
    )
    op.create_check_constraint(
        "ck_outbox_events_claim_pair",
        "outbox_events",
        "(claim_token IS NULL) = (claimed_until IS NULL)",
    )
    op.create_check_constraint(
        "ck_outbox_events_published_unclaimed",
        "outbox_events",
        "published_at IS NULL OR (claim_token IS NULL AND claimed_until IS NULL)",
    )
    op.create_index(
        "ix_outbox_events_pending_due",
        "outbox_events",
        ["next_attempt_at", "created_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Remove delivery metadata only; preserve all existing event snapshots."""
    op.drop_index("ix_outbox_events_pending_due", table_name="outbox_events")
    op.drop_constraint(
        "ck_outbox_events_published_unclaimed", "outbox_events", type_="check"
    )
    op.drop_constraint("ck_outbox_events_claim_pair", "outbox_events", type_="check")
    op.drop_constraint(
        "ck_outbox_events_attempt_count_nonnegative", "outbox_events", type_="check"
    )
    for column in (
        "last_error",
        "claimed_until",
        "claim_token",
        "next_attempt_at",
        "attempt_count",
    ):
        op.drop_column("outbox_events", column)
