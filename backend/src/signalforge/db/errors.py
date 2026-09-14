"""Sanitized database failures shared across process boundaries."""


class DatabaseTransportError(RuntimeError):
    """A raw database-driver transport failure with no sensitive details."""

    def __init__(self) -> None:
        super().__init__("Database transport unavailable")
