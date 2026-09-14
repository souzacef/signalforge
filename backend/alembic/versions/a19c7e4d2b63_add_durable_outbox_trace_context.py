"""add durable outbox trace context

Revision ID: a19c7e4d2b63
Revises: 70c1115829cf
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a19c7e4d2b63"
down_revision: str | Sequence[str] | None = "70c1115829cf"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add optional bounded W3C propagation metadata without backfilling rows."""
    op.add_column(
        "outbox_events",
        sa.Column("traceparent", sa.String(length=512), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("tracestate", sa.String(length=512), nullable=True),
    )


def downgrade() -> None:
    """Remove only optional propagation metadata."""
    op.drop_column("outbox_events", "tracestate")
    op.drop_column("outbox_events", "traceparent")
