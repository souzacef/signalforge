from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StringConstraints

from signalforge.users.models import UserRole

NormalizedEmail = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        to_lower=True,
        min_length=3,
        max_length=320,
        pattern=r"^[^@\s]+@[^@\s]+$",
    ),
]
Password = Annotated[SecretStr, Field(min_length=12, max_length=128)]


class UserCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: NormalizedEmail
    password: Password
    role: UserRole


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: str
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime
