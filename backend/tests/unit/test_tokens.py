from datetime import timedelta
from uuid import uuid4

import jwt
import pytest
from jwt.exceptions import DecodeError, ExpiredSignatureError, InvalidSignatureError
from pydantic import SecretStr

from signalforge.auth.tokens import ALGORITHM, create_access_token, decode_access_token

JWT_SECRET = SecretStr("test-only-token-secret-0123456789abcdef")
OTHER_SECRET = SecretStr("different-test-only-secret-0123456789")


def test_valid_token_contains_only_expected_identity_claims() -> None:
    user_id = uuid4()
    token = create_access_token(user_id, JWT_SECRET, timedelta(minutes=30))

    assert decode_access_token(token, JWT_SECRET) == user_id
    payload = jwt.decode(
        token,
        JWT_SECRET.get_secret_value(),
        algorithms=[ALGORITHM],
    )
    assert payload["sub"] == str(user_id)
    assert {"sub", "iat", "exp"} <= payload.keys()
    assert "password_hash" not in payload
    assert "email" not in payload
    assert "role" not in payload


def test_expired_token_is_rejected() -> None:
    token = create_access_token(uuid4(), JWT_SECRET, timedelta(seconds=-1))

    with pytest.raises(ExpiredSignatureError):
        decode_access_token(token, JWT_SECRET)


def test_token_with_invalid_signature_is_rejected() -> None:
    token = create_access_token(uuid4(), JWT_SECRET, timedelta(minutes=30))

    with pytest.raises(InvalidSignatureError):
        decode_access_token(token, OTHER_SECRET)


def test_malformed_token_is_rejected() -> None:
    with pytest.raises(DecodeError):
        decode_access_token("not-a-jwt", JWT_SECRET)
