"""Persistence model for deterministic incident triage."""

from datetime import datetime
from enum import Enum as PythonEnum
from enum import StrEnum
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from signalforge.db.base import Base
from signalforge.incidents.models import IncidentSeverity


class TriagePriority(StrEnum):
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"
    P4 = "P4"


def _enum_values(enum_class: type[PythonEnum]) -> list[str]:
    return [str(member.value) for member in enum_class]


class IncidentTriage(Base):
    """One deterministic triage result per Incident creation event."""

    __tablename__ = "incident_triage"
    __table_args__ = (
        CheckConstraint(
            "length(trim(source)) > 0",
            name="ck_incident_triage_source_nonempty",
        ),
        UniqueConstraint("event_id", name="uq_incident_triage_event_id"),
    )

    incident_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "incidents.id",
            name="fk_incident_triage_incident_id_incidents",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
    event_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    original_severity: Mapped[IncidentSeverity] = mapped_column(
        Enum(
            IncidentSeverity,
            name="incident_triage_original_severity",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    priority: Mapped[TriagePriority] = mapped_column(
        Enum(
            TriagePriority,
            name="incident_triage_priority",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    requires_human_review: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
