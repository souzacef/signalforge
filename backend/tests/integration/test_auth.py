from datetime import timedelta
from uuid import uuid4

import pytest
from httpx import AsyncClient
from pydantic import SecretStr
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from signalforge.auth.service import create_user
from signalforge.auth.tokens import create_access_token
from signalforge.core.config import get_settings
from signalforge.users.models import User, UserRole
from signalforge.users.schemas import UserCreate
from tests.integration.factories import VALID_PASSWORD, login, persist_user


@pytest.mark.integration
@pytest.mark.anyio
async def test_user_persists_normalized_email_hash_and_role(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_data = UserCreate(
        email="  Admin@Example.COM  ",
        password=SecretStr(VALID_PASSWORD),
        role=UserRole.ADMIN,
    )
    async with database_session_factory() as session:
        user = await create_user(session, user_data)
        user_id = user.id

    async with database_session_factory() as session:
        stored_user = await session.get(User, user_id)

    assert stored_user is not None
    assert stored_user.email == "admin@example.com"
    assert stored_user.password_hash != VALID_PASSWORD
    assert VALID_PASSWORD not in stored_user.password_hash
    assert stored_user.role is UserRole.ADMIN


@pytest.mark.integration
@pytest.mark.anyio
async def test_duplicate_normalized_email_is_rejected_by_database(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_user = UserCreate(
        email="admin@example.com",
        password=SecretStr(VALID_PASSWORD),
        role=UserRole.ADMIN,
    )
    duplicate_user = UserCreate(
        email="  ADMIN@EXAMPLE.COM ",
        password=SecretStr(VALID_PASSWORD),
        role=UserRole.VIEWER,
    )
    async with database_session_factory() as session:
        await create_user(session, first_user)

    async with database_session_factory() as session:
        with pytest.raises(IntegrityError):
            await create_user(session, duplicate_user)
        await session.rollback()


@pytest.mark.integration
@pytest.mark.anyio
async def test_valid_case_normalized_login_returns_bearer_token(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await persist_user(database_session_factory)

    response = await login(client, email="  Operator@Example.COM  ")

    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert isinstance(body["access_token"], str)
    assert body["access_token"]
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    assert "password_hash" not in body
    assert body["access_token"].count(".") == 2
    assert user.id


@pytest.mark.integration
@pytest.mark.anyio
async def test_wrong_password_and_unknown_email_share_public_failure(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await persist_user(database_session_factory)

    wrong_password = await login(client, password="incorrect password")
    unknown_email = await login(client, email="unknown@example.com")

    assert wrong_password.status_code == 401
    assert unknown_email.status_code == 401
    assert (
        wrong_password.json()
        == unknown_email.json()
        == {"detail": "Incorrect email or password"}
    )
    assert wrong_password.headers["www-authenticate"] == "Bearer"
    assert unknown_email.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
async def test_inactive_user_cannot_log_in(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await persist_user(database_session_factory, is_active=False)

    response = await login(client)

    assert response.status_code == 401
    assert response.json() == {"detail": "Incorrect email or password"}
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
async def test_me_returns_current_user_without_password_hash(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await persist_user(database_session_factory, role=UserRole.ADMIN)
    token_response = await login(client)
    token = token_response.json()["access_token"]

    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "id": str(user.id),
        "email": user.email,
        "role": "admin",
        "is_active": True,
        "created_at": user.created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": user.updated_at.isoformat().replace("+00:00", "Z"),
    }
    assert "password_hash" not in response.text


@pytest.mark.integration
@pytest.mark.anyio
async def test_me_without_token_returns_unauthorized(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.get("/api/v1/auth/me")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
async def test_me_with_invalid_token_returns_unauthorized(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": "Bearer not-a-jwt"},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Could not validate credentials"}
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.integration
@pytest.mark.anyio
async def test_me_rejects_token_for_nonexistent_user(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = get_settings()
    token = create_access_token(
        uuid4(),
        settings.jwt_secret,
        timedelta(minutes=settings.access_token_expire_minutes),
    )

    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Could not validate credentials"}


@pytest.mark.integration
@pytest.mark.anyio
async def test_me_rejects_inactive_user_with_valid_token(
    client: AsyncClient,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await persist_user(database_session_factory, is_active=False)
    settings = get_settings()
    token = create_access_token(
        user.id,
        settings.jwt_secret,
        timedelta(minutes=settings.access_token_expire_minutes),
    )

    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Could not validate credentials"}
