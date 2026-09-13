"""Narrow domain-facing provider boundary and sanitized failures."""

from enum import StrEnum
from typing import Protocol

from signalforge.enrichment.domain import EnrichmentInput, EnrichmentResult


class ProviderFailureReason(StrEnum):
    UNAVAILABLE = "unavailable"
    RATE_LIMITED = "rate_limited"
    REQUEST_REJECTED = "request_rejected"
    INVALID_RESPONSE = "invalid_response"


class EnrichmentProviderError(RuntimeError):
    """Base sanitized provider failure carrying only a stable reason code."""

    def __init__(self, reason: ProviderFailureReason) -> None:
        self.reason = reason
        super().__init__(f"Enrichment provider failed: {reason}")


class TransientEnrichmentError(EnrichmentProviderError):
    """A provider failure that may succeed on a later delivery."""


class PermanentEnrichmentError(EnrichmentProviderError):
    """A provider failure that redelivery is not expected to repair."""


class EnrichmentProvider(Protocol):
    @property
    def provider_name(self) -> str: ...

    @property
    def model_name(self) -> str: ...

    async def enrich(self, input: EnrichmentInput) -> EnrichmentResult: ...
