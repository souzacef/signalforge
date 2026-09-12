from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.auth import service
from signalforge.auth.passwords import DUMMY_PASSWORD_HASH


@pytest.mark.anyio
async def test_unknown_email_still_performs_dummy_password_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_user = AsyncMock(return_value=None)
    verify = Mock(return_value=False)
    monkeypatch.setattr(service, "get_user_by_email", get_user)
    monkeypatch.setattr(service, "verify_password", verify)

    result = await service.authenticate_user(
        AsyncMock(spec=AsyncSession),
        "unknown@example.com",
        "incorrect password",
    )

    assert result is None
    verify.assert_called_once_with("incorrect password", DUMMY_PASSWORD_HASH)
