from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.db.session import engine, get_session


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
