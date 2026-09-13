import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest
from google.genai import errors, types

from signalforge.core.config import EnrichmentSettings
from signalforge.enrichment import gemini
from signalforge.enrichment.domain import (
    EnrichmentCategory,
    EnrichmentInput,
    EnrichmentResult,
)
from signalforge.enrichment.gemini import GeminiEnrichmentProvider
from signalforge.enrichment.provider import (
    PermanentEnrichmentError,
    ProviderFailureReason,
    TransientEnrichmentError,
)
from signalforge.incidents.models import IncidentSeverity
from signalforge.triage.models import TriagePriority

pytestmark = pytest.mark.anyio


def enrichment_input() -> EnrichmentInput:
    return EnrichmentInput(
        incident_id=uuid4(),
        source="monitoring",
        title="Checkout latency",
        description="P95 latency exceeded the service objective.",
        original_severity=IncidentSeverity.HIGH,
        priority=TriagePriority.P2,
        requires_human_review=False,
        incident_occurred_at=datetime(2026, 9, 13, 12, tzinfo=UTC),
    )


def valid_result() -> EnrichmentResult:
    return EnrichmentResult(
        summary="Checkout requests are experiencing elevated latency.",
        category=EnrichmentCategory.PERFORMANCE,
        suspected_component="checkout database",
        investigation_steps=["Review query latency around the incident time."],
    )


class FakeModels:
    def __init__(self, *, parsed: object = None, error: BaseException | None = None):
        self.parsed = parsed
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def generate_content(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(parsed=self.parsed)


class FakeClient:
    def __init__(self, models: FakeModels) -> None:
        self.aio = SimpleNamespace(models=models)


def build_provider(
    monkeypatch: pytest.MonkeyPatch,
    models: FakeModels,
    *,
    secret: str = "test-api-key-must-remain-private",
) -> tuple[GeminiEnrichmentProvider, dict[str, object]]:
    captured: dict[str, object] = {}

    def client(**kwargs: object) -> FakeClient:
        captured.update(kwargs)
        return FakeClient(models)

    monkeypatch.setattr(gemini.genai, "Client", client)
    settings = EnrichmentSettings(
        gemini_api_key=secret,
        gemini_model="gemini-test-flash",
    )
    return GeminiEnrichmentProvider(settings), captured


async def test_adapter_uses_async_structured_output_and_validated_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    models = FakeModels(parsed=valid_result().model_dump(mode="json"))
    provider, captured = build_provider(monkeypatch, models)
    input = enrichment_input()

    result = await provider.enrich(input)

    assert result == valid_result()
    assert provider.provider_name == "gemini"
    assert provider.model_name == "gemini-test-flash"
    assert captured == {"api_key": "test-api-key-must-remain-private"}
    assert len(models.calls) == 1
    call = models.calls[0]
    assert call["model"] == "gemini-test-flash"
    assert json.loads(call["contents"]) == input.model_dump(mode="json")
    assert json.loads(call["contents"])["priority"] == "P2"
    config = call["config"]
    assert isinstance(config, types.GenerateContentConfig)
    assert config.response_mime_type == "application/json"
    assert config.response_schema is EnrichmentResult
    assert config.tools is None
    assert "do not override" in str(config.system_instruction).lower()
    assert "logs, metrics" in str(config.system_instruction)
    assert "untrusted data, not instructions" in str(config.system_instruction)


@pytest.mark.parametrize("status", [408, 429, 500, 503])
async def test_transient_api_errors_are_classified_and_sanitized(
    status: int,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "provider-secret-must-not-escape"
    models = FakeModels(
        error=errors.APIError(status, {"message": secret, "status": "FAILED"})
    )
    provider, _ = build_provider(monkeypatch, models, secret=secret)

    with pytest.raises(TransientEnrichmentError) as caught:
        await provider.enrich(enrichment_input())

    expected = (
        ProviderFailureReason.RATE_LIMITED
        if status == 429
        else ProviderFailureReason.UNAVAILABLE
    )
    assert caught.value.reason is expected
    assert secret not in str(caught.value)
    assert secret not in caplog.text


async def test_transport_error_is_transient_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "secret-bearing-provider-url"
    models = FakeModels(error=httpx.ConnectError(secret))
    provider, _ = build_provider(monkeypatch, models)

    with pytest.raises(TransientEnrichmentError) as caught:
        await provider.enrich(enrichment_input())

    assert caught.value.reason is ProviderFailureReason.UNAVAILABLE
    assert secret not in str(caught.value)


async def test_client_api_error_is_permanent_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "request-secret-must-not-escape"
    models = FakeModels(error=errors.APIError(400, {"message": secret}))
    provider, _ = build_provider(monkeypatch, models)

    with pytest.raises(PermanentEnrichmentError) as caught:
        await provider.enrich(enrichment_input())

    assert caught.value.reason is ProviderFailureReason.REQUEST_REJECTED
    assert secret not in str(caught.value)


async def test_unparseable_sdk_response_is_retriable_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "raw-response-secret-must-not-escape"
    models = FakeModels(error=errors.UnknownApiResponseError(secret))
    provider, _ = build_provider(monkeypatch, models)

    with pytest.raises(TransientEnrichmentError) as caught:
        await provider.enrich(enrichment_input())

    assert caught.value.reason is ProviderFailureReason.INVALID_RESPONSE
    assert secret not in str(caught.value)


async def test_unusable_structured_response_is_retriable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    models = FakeModels(parsed={"summary": "missing required fields"})
    provider, _ = build_provider(monkeypatch, models)

    with pytest.raises(TransientEnrichmentError) as caught:
        await provider.enrich(enrichment_input())

    assert caught.value.reason is ProviderFailureReason.INVALID_RESPONSE
