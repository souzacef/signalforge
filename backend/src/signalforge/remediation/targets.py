from typing import Annotated
from unicodedata import category

from pydantic import BeforeValidator, StringConstraints


def _reject_control_characters(value: object) -> object:
    if isinstance(value, str) and any(
        category(character) == "Cc" for character in value
    ):
        raise ValueError("target must not contain control characters")
    return value


ServiceTarget = Annotated[
    str,
    BeforeValidator(_reject_control_characters),
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?$",
    ),
]
