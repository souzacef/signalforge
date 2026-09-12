from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth.dependencies import get_current_user
from signalforge.auth.schemas import AccessTokenResponse
from signalforge.auth.service import authenticate_user
from signalforge.auth.tokens import create_access_token
from signalforge.core.config import Settings, get_settings
from signalforge.db.session import get_session
from signalforge.users.models import User
from signalforge.users.schemas import UserResponse

router = APIRouter(prefix="/api/v1/auth", tags=["authentication"])
PasswordForm = Annotated[OAuth2PasswordRequestForm, Depends()]
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post("/token", response_model=AccessTokenResponse)
async def login(
    response: Response,
    form_data: PasswordForm,
    session: DatabaseSession,
    settings: AppSettings,
) -> AccessTokenResponse:
    user = await authenticate_user(session, form_data.username, form_data.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token(
        user.id,
        settings.jwt_secret,
        timedelta(minutes=settings.access_token_expire_minutes),
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return AccessTokenResponse(access_token=token)


@router.get("/me", response_model=UserResponse)
async def current_user(user: CurrentUser) -> User:
    return user
