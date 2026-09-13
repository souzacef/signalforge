from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from signalforge.enrichment.domain import EnrichmentCategory
from signalforge.enrichment.schemas import (
    EnrichmentListQuery,
    GlobalEnrichmentListQuery,
)


def test_enrichment_query_defaults() -> None:
    query = EnrichmentListQuery()

    assert query.limit == 20
    assert query.offset == 0
    assert query.category is None
    assert query.provider is None
    assert query.model is None
    assert query.created_from is None
    assert query.created_to is None


@pytest.mark.parametrize("limit", [1, 100])
def test_limit_boundaries_are_valid(limit: int) -> None:
    assert EnrichmentListQuery(limit=limit).limit == limit


@pytest.mark.parametrize(
    ("field", "value"),
    [("limit", 0), ("limit", 101), ("offset", -1)],
)
def test_invalid_pagination_is_rejected(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        EnrichmentListQuery.model_validate({field: value})


def test_category_and_global_incident_id_are_typed() -> None:
    incident_id = uuid4()
    query = GlobalEnrichmentListQuery.model_validate(
        {"incident_id": str(incident_id), "category": "performance"}
    )

    assert query.incident_id == incident_id
    assert query.category is EnrichmentCategory.PERFORMANCE


def test_invalid_category_is_rejected() -> None:
    with pytest.raises(ValidationError):
        EnrichmentListQuery(category="not-a-category")  # type: ignore[arg-type]


def test_provider_and_model_filters_are_trimmed() -> None:
    query = EnrichmentListQuery(provider="  gemini  ", model="  model-v1  ")

    assert query.provider == "gemini"
    assert query.model == "model-v1"


@pytest.mark.parametrize("field", ["provider", "model"])
def test_blank_text_filter_is_rejected(field: str) -> None:
    with pytest.raises(ValidationError):
        EnrichmentListQuery.model_validate({field: "   "})


@pytest.mark.parametrize("field", ["created_from", "created_to"])
def test_created_filter_requires_aware_datetime(field: str) -> None:
    with pytest.raises(ValidationError):
        EnrichmentListQuery.model_validate({field: datetime(2026, 1, 1)})

    aware = datetime(2026, 1, 1, tzinfo=UTC)
    assert getattr(EnrichmentListQuery.model_validate({field: aware}), field) == aware


def test_reversed_created_range_is_rejected() -> None:
    with pytest.raises(ValidationError, match="created_from"):
        EnrichmentListQuery(
            created_from=datetime(2026, 1, 2, tzinfo=UTC),
            created_to=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_extra_query_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        EnrichmentListQuery.model_validate({"unexpected": "value"})
