from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import jwt
from jwt.exceptions import InvalidTokenError
from pydantic import SecretStr

ALGORITHM = "HS256"


def create_access_token(
    subject: UUID,
    secret: SecretStr,
    expires_in: timedelta,
) -> str:
    issued_at = datetime.now(UTC)
    return jwt.encode(
        {
            "sub": str(subject),
            "iat": issued_at,
            "exp": issued_at + expires_in,
        },
        secret.get_secret_value(),
        algorithm=ALGORITHM,
    )


def decode_access_token(token: str, secret: SecretStr) -> UUID:
    payload = cast(
        dict[str, object],
        jwt.decode(
            token,
            secret.get_secret_value(),
            algorithms=[ALGORITHM],
            options={"require": ["sub", "iat", "exp"]},
        ),
    )
    subject = payload["sub"]
    if not isinstance(subject, str):
        raise InvalidTokenError("Invalid subject")

    try:
        return UUID(subject)
    except ValueError as error:
        raise InvalidTokenError("Invalid subject") from error
