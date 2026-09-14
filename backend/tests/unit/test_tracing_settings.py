import pytest
from pydantic import ValidationError

from signalforge.core.config import TracingSettings


def test_disabled_default_requires_no_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "SIGNALFORGE_TRACING_ENABLED",
        "SIGNALFORGE_OTLP_TRACES_ENDPOINT",
        "SIGNALFORGE_TRACING_SAMPLE_RATIO",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = TracingSettings()
    assert settings.tracing_enabled is False
    assert settings.otlp_traces_endpoint is None
    assert settings.tracing_sample_ratio == 1.0


def test_enabled_requires_endpoint() -> None:
    with pytest.raises(ValidationError, match="otlp_traces_endpoint"):
        TracingSettings(tracing_enabled=True)


@pytest.mark.parametrize(
    "endpoint", ["not-a-url", "ftp://localhost:4318/v1/traces", "http://"]
)
def test_invalid_endpoint_is_rejected(endpoint: str) -> None:
    with pytest.raises(ValidationError):
        TracingSettings(tracing_enabled=True, otlp_traces_endpoint=endpoint)


@pytest.mark.parametrize("ratio", [0.0, 1.0])
def test_sampling_boundaries_are_accepted(ratio: float) -> None:
    settings = TracingSettings(tracing_sample_ratio=ratio)
    assert settings.tracing_sample_ratio == ratio


@pytest.mark.parametrize("ratio", [-0.01, 1.01, "nan", "inf"])
def test_invalid_sample_ratios_are_rejected(ratio: float | str) -> None:
    with pytest.raises(ValidationError):
        TracingSettings(tracing_sample_ratio=ratio)
