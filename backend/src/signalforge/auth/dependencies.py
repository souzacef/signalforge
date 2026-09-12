from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.service import get_user_by_id
from signalforge.auth.tokens import decode_access_token
from signalforge.core.config import Settings, get_settings
from signalforge.db.session import get_session
from signalforge.users.models import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")
BearerToken = Annotated[str, Depends(oauth2_scheme)]
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_current_user(
    token: BearerToken,
    session: DatabaseSession,
    settings: AppSettings,
) -> User:
    try:
        user_id = decode_access_token(token, settings.jwt_secret)
    except InvalidTokenError as error:
        raise _unauthorized() from error

    user = await get_user_by_id(session, user_id)
    if user is None or not user.is_active:
        raise _unauthorized()
    return user
