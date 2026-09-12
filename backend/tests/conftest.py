import os
from collections.abc import AsyncIterator, Iterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from tests.database_safety import DATABASE_ENV_VAR, require_test_database_url

# Validate before application imports construct process-level database infrastructure.
os.environ[DATABASE_ENV_VAR] = require_test_database_url(
    os.environ.get(DATABASE_ENV_VAR)
)
os.environ.setdefault(
    "SIGNALFORGE_JWT_SECRET",
    "test-only-secret-not-for-production-0123456789",
)

from signalforge.main import create_app  # noqa: E402


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def app() -> Iterator[FastAPI]:
    application = create_app()
    yield application
    application.dependency_overrides.clear()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as test_client:
        yield test_client
