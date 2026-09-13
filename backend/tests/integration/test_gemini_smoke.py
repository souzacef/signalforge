import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from signalforge.core.config import EnrichmentSettings
from signalforge.enrichment.domain import EnrichmentInput
from signalforge.enrichment.gemini import GeminiEnrichmentProvider
from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_real_gemini_structured_enrichment_smoke() -> None:
    if os.environ.get("SIGNALFORGE_RUN_GEMINI_SMOKE") != "1":
        pytest.skip("Set SIGNALFORGE_RUN_GEMINI_SMOKE=1 for the opt-in Gemini smoke")
    if not os.environ.get("SIGNALFORGE_GEMINI_API_KEY"):
        pytest.skip("Set SIGNALFORGE_GEMINI_API_KEY for the opt-in Gemini smoke")

    provider = GeminiEnrichmentProvider(EnrichmentSettings())  # type: ignore[call-arg]
    result = await provider.enrich(
        EnrichmentInput(
            incident_id=uuid4(),
            source="smoke-test",
            title="API latency above service objective",
            description="P95 latency rose for five minutes.",
            original_severity=IncidentSeverity.MEDIUM,
            priority=TriagePriority.P3,
            requires_human_review=False,
            incident_occurred_at=datetime.now(UTC),
        )
    )

    assert result.summary
    assert result.category
    assert 1 <= len(result.investigation_steps) <= 5
