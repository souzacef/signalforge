"""add incident lifecycle attribution

Revision ID: 1d75e5469376
Revises: bf6d93f696bd
Create Date: 2026-09-11 22:03:03.228267
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "1d75e5469376"
down_revision: str | Sequence[str] | None = "bf6d93f696bd"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this revision."""
    op.add_column(
        "incidents",
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "incidents",
        sa.Column("acknowledged_by_user_id", sa.UUID(), nullable=True),
    )
    op.add_column(
        "incidents",
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "incidents",
        sa.Column("resolved_by_user_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_incidents_resolved_by_user_id_users",
        "incidents",
        "users",
        ["resolved_by_user_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_incidents_acknowledged_by_user_id_users",
        "incidents",
        "users",
        ["acknowledged_by_user_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Revert this revision."""
    op.drop_constraint(
        "fk_incidents_acknowledged_by_user_id_users",
        "incidents",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_incidents_resolved_by_user_id_users",
        "incidents",
        type_="foreignkey",
    )
    op.drop_column("incidents", "resolved_by_user_id")
    op.drop_column("incidents", "resolved_at")
    op.drop_column("incidents", "acknowledged_by_user_id")
    op.drop_column("incidents", "acknowledged_at")
