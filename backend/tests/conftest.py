import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

# Application imports construct process-level database infrastructure but do not
# connect. Use a fallback only when neither the environment nor a dotenv file is
# supplying the required setting.
if (
    "SIGNALFORGE_DATABASE_URL" not in os.environ
    and not Path(".env").is_file()
    and not Path("../.env").is_file()
):
    os.environ["SIGNALFORGE_DATABASE_URL"] = (
        "postgresql+asyncpg://signalforge:signalforge-local@localhost:5432/signalforge"
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
