"""add incident query ordering index

Revision ID: b6b023ebef03
Revises: 1d75e5469376
Create Date: 2026-09-11 22:28:23.036281
"""

from collections.abc import Sequence

from alembic import op

revision: str = "b6b023ebef03"
down_revision: str | Sequence[str] | None = "1d75e5469376"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this revision."""
    op.create_index(
        "ix_incidents_occurred_at_id",
        "incidents",
        ["occurred_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    """Revert this revision."""
    op.drop_index("ix_incidents_occurred_at_id", table_name="incidents")
