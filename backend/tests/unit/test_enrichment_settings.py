import pytest
from pydantic import ValidationError

from signalforge.core.config import EnrichmentSettings


def test_gemini_settings_use_secret_and_current_flash_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "test-gemini-key-must-stay-private"
    monkeypatch.setenv("SIGNALFORGE_GEMINI_API_KEY", secret)
    monkeypatch.delenv("SIGNALFORGE_GEMINI_MODEL", raising=False)

    settings = EnrichmentSettings(_env_file=None)  # type: ignore[call-arg]

    assert settings.gemini_api_key.get_secret_value() == secret
    assert secret not in repr(settings)
    assert settings.gemini_model == "gemini-3.8-flash"


def test_gemini_settings_require_key_without_affecting_api_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SIGNALFORGE_GEMINI_API_KEY", raising=False)

    with pytest.raises(ValidationError):
        EnrichmentSettings(_env_file=None)  # type: ignore[call-arg]
