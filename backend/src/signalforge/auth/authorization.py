from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException, status

from signalforge.auth.dependencies import get_current_user
from signalforge.users.models import User, UserRole

CurrentUser = Annotated[User, Depends(get_current_user)]
RoleDependency = Callable[[User], User]


def require_roles(*allowed_roles: UserRole) -> RoleDependency:
    allowed = frozenset(allowed_roles)

    def authorize(current_user: CurrentUser) -> User:
        if current_user.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions",
            )
        return current_user

    return authorize
