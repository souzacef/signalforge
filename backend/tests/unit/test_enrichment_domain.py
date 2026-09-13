from typing import Any

import pytest
from pydantic import ValidationError

from signalforge.enrichment.domain import EnrichmentCategory, EnrichmentResult


def valid_result(**changes: Any) -> dict[str, object]:
    data: dict[str, object] = {
        "summary": "Elevated database latency is affecting request availability.",
        "category": "performance",
        "suspected_component": "primary database",
        "investigation_steps": [
            "Compare database query latency with the incident start time.",
            "Review connection-pool saturation for the affected service.",
        ],
    }
    data.update(changes)
    return data


def test_valid_result_is_strict_bounded_and_serializable() -> None:
    result = EnrichmentResult.model_validate(valid_result())

    assert result.category is EnrichmentCategory.PERFORMANCE
    assert result.model_dump(mode="json") == valid_result()
    assert EnrichmentResult.model_validate_json(result.model_dump_json()) == result


@pytest.mark.parametrize(
    "changes",
    [
        {"summary": ""},
        {"summary": "x" * 1001},
        {"category": "not-a-category"},
        {"suspected_component": ""},
        {"suspected_component": "x" * 201},
        {"investigation_steps": []},
        {"investigation_steps": ["step"] * 6},
        {"investigation_steps": [""]},
        {"investigation_steps": ["x" * 501]},
        {"unexpected": "not accepted"},
    ],
)
def test_invalid_or_unexpected_result_data_is_rejected(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        EnrichmentResult.model_validate(valid_result(**changes))


def test_result_text_is_trimmed_and_optional_component_can_be_absent() -> None:
    result = EnrichmentResult.model_validate(
        valid_result(
            summary="  concise summary  ",
            suspected_component=None,
            investigation_steps=["  inspect the dependency  "],
        )
    )

    assert result.summary == "concise summary"
    assert result.suspected_component is None
    assert result.investigation_steps == ["inspect the dependency"]
