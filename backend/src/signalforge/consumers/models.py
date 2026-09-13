"""Durable per-consumer event processing receipts."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, SmallInteger, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from signalforge.db.base import Base


class ProcessedEvent(Base):
    """One committed receipt per logical consumer and durable event ID."""

    __tablename__ = "processed_events"
    __table_args__ = (
        CheckConstraint(
            "length(trim(consumer_name)) > 0",
            name="ck_processed_events_consumer_name_nonempty",
        ),
        CheckConstraint(
            "length(trim(event_type)) > 0",
            name="ck_processed_events_event_type_nonempty",
        ),
        CheckConstraint(
            "event_version > 0",
            name="ck_processed_events_event_version_positive",
        ),
    )

    consumer_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    event_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
