import pytest

from tests.database_safety import require_test_database_url


def test_accepts_explicit_test_database_url() -> None:
    database_url = (
        "postgresql+asyncpg://signalforge:secret@localhost:5432/signalforge_test"
    )

    assert require_test_database_url(database_url) == database_url


def test_rejects_development_database_url() -> None:
    database_url = "postgresql+asyncpg://signalforge:secret@localhost:5432/signalforge"

    with pytest.raises(RuntimeError, match="Refusing to run tests") as exc_info:
        require_test_database_url(database_url)

    assert "SIGNALFORGE_DATABASE_URL" in str(exc_info.value)
    assert "signalforge_test" in str(exc_info.value)


def test_rejects_missing_database_url() -> None:
    with pytest.raises(RuntimeError, match="Set SIGNALFORGE_DATABASE_URL"):
        require_test_database_url(None)
