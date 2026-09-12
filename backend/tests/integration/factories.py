from collections.abc import Awaitable, Callable

from httpx import AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.auth.passwords import hash_password
from signalforge.users.models import User, UserRole

VALID_PASSWORD = "correct horse battery staple"
VALID_PASSWORD_HASH = hash_password(VALID_PASSWORD)

AuthHeadersFactory = Callable[
    [UserRole],
    Awaitable[tuple[dict[str, str], User]],
]


async def persist_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    email: str = "operator@example.com",
    role: UserRole = UserRole.OPERATOR,
    is_active: bool = True,
) -> User:
    async with session_factory() as session:
        user = User(
            email=email,
            password_hash=VALID_PASSWORD_HASH,
            role=role,
            is_active=is_active,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


async def login(
    client: AsyncClient,
    *,
    email: str = "operator@example.com",
    password: str = VALID_PASSWORD,
) -> Response:
    return await client.post(
        "/api/v1/auth/token",
        data={"username": email, "password": password},
    )
