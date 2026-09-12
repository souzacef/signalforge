from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.passwords import (
    DUMMY_PASSWORD_HASH,
    hash_password,
    verify_password,
)
from signalforge.users.models import User
from signalforge.users.schemas import UserCreate


async def get_user_by_email(session: AsyncSession, email: str) -> User | None:
    normalized_email = email.strip().lower()
    result = await session.execute(
        select(User).where(User.email == normalized_email),
    )
    return result.scalar_one_or_none()


async def get_user_by_id(session: AsyncSession, user_id: UUID) -> User | None:
    return await session.get(User, user_id)


async def create_user(session: AsyncSession, user_data: UserCreate) -> User:
    user = User(
        email=user_data.email,
        password_hash=hash_password(user_data.password.get_secret_value()),
        role=user_data.role,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


async def authenticate_user(
    session: AsyncSession,
    email: str,
    password: str,
) -> User | None:
    user = await get_user_by_email(session, email)
    password_hash = user.password_hash if user is not None else DUMMY_PASSWORD_HASH
    password_is_valid = verify_password(password, password_hash)

    if user is None or not password_is_valid or not user.is_active:
        return None
    return user
