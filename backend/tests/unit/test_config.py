from pathlib import Path

import pytest
from pydantic import ValidationError

from signalforge.core.config import DatabaseSettings, RemediationExecutionSettings


def test_database_settings_require_asyncpg_driver() -> None:
    with pytest.raises(ValidationError, match=r"postgresql\+asyncpg"):
        DatabaseSettings(
            _env_file=None,
            database_url="postgresql://user:password@localhost/signalforge",
        )


def test_database_settings_hide_invalid_input() -> None:
    secret = "credential-that-must-not-leak"

    with pytest.raises(ValidationError) as caught:
        DatabaseSettings(_env_file=None, database_url=secret)

    assert secret not in str(caught.value)


@pytest.mark.parametrize(
    ("environment_value", "expected_targets"),
    [
        ('{"current-service":"http://localhost/current"}', {"current-service"}),
        ("{}", set()),
    ],
)
def test_remediation_environment_replaces_dotenv_allowlist(
    environment_value: str,
    expected_targets: set[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        'SIGNALFORGE_REMEDIATION_RESTART_ENDPOINTS={"stale-service":"http://localhost/stale"}\n'
    )
    monkeypatch.setenv("SIGNALFORGE_REMEDIATION_RESTART_ENDPOINTS", environment_value)

    settings = RemediationExecutionSettings(_env_file=env_file)

    assert set(settings.restart_endpoints) == expected_targets
