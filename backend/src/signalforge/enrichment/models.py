"""Persistence model for advisory triage enrichment results."""

from datetime import datetime
from enum import Enum as PythonEnum
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Enum, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from signalforge.db.base import Base
from signalforge.enrichment.domain import EnrichmentCategory


def _enum_values(enum_class: type[PythonEnum]) -> list[str]:
    return [str(member.value) for member in enum_class]


class TriageEnrichment(Base):
    """One persisted advisory result per enrichment request event."""

    __tablename__ = "triage_enrichments"
    __table_args__ = (
        CheckConstraint(
            "length(trim(provider)) > 0",
            name="ck_triage_enrichments_provider_nonempty",
        ),
        CheckConstraint(
            "length(trim(model)) > 0",
            name="ck_triage_enrichments_model_nonempty",
        ),
        CheckConstraint(
            "length(trim(summary)) BETWEEN 1 AND 1000",
            name="ck_triage_enrichments_summary_length",
        ),
        CheckConstraint(
            "suspected_component IS NULL OR "
            "length(trim(suspected_component)) BETWEEN 1 AND 200",
            name="ck_triage_enrichments_suspected_component_length",
        ),
        CheckConstraint(
            "jsonb_typeof(investigation_steps) = 'array' AND "
            "jsonb_array_length(investigation_steps) BETWEEN 1 AND 5",
            name="ck_triage_enrichments_investigation_steps_count",
        ),
    )

    request_event_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True
    )
    incident_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "incidents.id",
            name="fk_triage_enrichments_incident_id_incidents",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    trigger_event_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False
    )
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[EnrichmentCategory] = mapped_column(
        Enum(
            EnrichmentCategory,
            name="triage_enrichment_category",
            native_enum=False,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    suspected_component: Mapped[str | None] = mapped_column(String(200), nullable=True)
    investigation_steps: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
