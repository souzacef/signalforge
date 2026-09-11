from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from signalforge.core.config import get_settings

settings = get_settings()
engine = create_async_engine(str(settings.database_url), pool_pre_ping=True)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Provide one database session per request or dependency use."""
    async with async_session_factory() as session:
        yield session
