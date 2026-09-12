from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.db.session import engine, get_session
from signalforge.users.models import User, UserRole
from tests.integration.factories import (
    AuthHeadersFactory,
    login,
    persist_user,
)


@pytest.fixture(autouse=True)
async def dispose_engine_connections() -> AsyncIterator[None]:
    yield
    await engine.dispose()


@pytest.fixture
async def database_session_factory(
    app: FastAPI,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async with engine.connect() as connection:
        transaction = await connection.begin()
        session_factory = async_sessionmaker(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )

        async def override_session() -> AsyncIterator[AsyncSession]:
            async with session_factory() as session:
                yield session

        app.dependency_overrides[get_session] = override_session
        yield session_factory
        await transaction.rollback()


@pytest.fixture
def auth_headers_factory(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> AuthHeadersFactory:
    async def create_headers(role: UserRole) -> tuple[dict[str, str], User]:
        email = f"{role.value}-{uuid4()}@example.com"
        user = await persist_user(
            database_session_factory,
            email=email,
            role=role,
        )
        response = await login(client, email=email)
        assert response.status_code == 200
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}, user

    return create_headers


@pytest.fixture
async def operator_headers(
    auth_headers_factory: AuthHeadersFactory,
) -> dict[str, str]:
    headers, _ = await auth_headers_factory(UserRole.OPERATOR)
    return headers
