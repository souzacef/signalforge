from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import CheckConstraint, DateTime, SmallInteger, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from signalforge.db.base import Base


class OutboxEvent(Base):
    """A durable event snapshot, separate from mutable delivery metadata."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "event_version > 0", name="ck_outbox_events_event_version_positive"
        ),
        CheckConstraint(
            "length(trim(event_type)) > 0", name="ck_outbox_events_event_type_nonempty"
        ),
        CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name="ck_outbox_events_payload_object"
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    event_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False
    )
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
