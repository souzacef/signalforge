from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

DATABASE_ENV_VAR = "SIGNALFORGE_DATABASE_URL"
TEST_DATABASE_NAME = "signalforge_test"


def require_test_database_url(database_url: str | None) -> str:
    """Require an explicit PostgreSQL URL for the dedicated test database."""
    if database_url is None:
        raise RuntimeError(
            f"Set {DATABASE_ENV_VAR} explicitly to a PostgreSQL URL for the "
            f"'{TEST_DATABASE_NAME}' database before running tests. Dotenv and "
            "development-database fallbacks are not accepted."
        )

    try:
        url = make_url(database_url)
    except ArgumentError:
        raise RuntimeError(
            f"Set {DATABASE_ENV_VAR} to a valid PostgreSQL URL for the "
            f"'{TEST_DATABASE_NAME}' database before running tests."
        ) from None

    if url.get_backend_name() != "postgresql" or url.database != TEST_DATABASE_NAME:
        raise RuntimeError(
            "Refusing to run tests against an unsafe database. "
            f"Set {DATABASE_ENV_VAR} explicitly to a PostgreSQL URL whose database "
            f"name is exactly '{TEST_DATABASE_NAME}'."
        )

    return database_url
