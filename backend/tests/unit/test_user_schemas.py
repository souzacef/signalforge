import pytest
from pydantic import SecretStr, ValidationError

from signalforge.users.models import UserRole
from signalforge.users.schemas import UserCreate


def test_user_email_is_trimmed_and_lowercased() -> None:
    user_data = UserCreate(
        email="  Admin@Example.COM  ",
        password=SecretStr("correct horse battery staple"),
        role=UserRole.ADMIN,
    )

    assert user_data.email == "admin@example.com"


@pytest.mark.parametrize("length", [11, 129])
def test_user_creation_rejects_password_outside_length_policy(length: int) -> None:
    with pytest.raises(ValidationError):
        UserCreate(
            email="admin@example.com",
            password=SecretStr("x" * length),
            role=UserRole.ADMIN,
        )
