"""Narrow Development-stage OpenTelemetry GenAI semantic attributes."""

from typing import Final

GEN_AI_OPERATION_NAME: Final = "gen_ai.operation.name"
GEN_AI_PROVIDER_NAME: Final = "gen_ai.provider.name"
GEN_AI_REQUEST_MODEL: Final = "gen_ai.request.model"
GEN_AI_OUTPUT_TYPE: Final = "gen_ai.output.type"


def genai_attributes(*, model: str) -> dict[str, str]:
    """Return safe Gemini operation metadata without prompt or response content."""
    return {
        GEN_AI_OPERATION_NAME: "generate_content",
        GEN_AI_PROVIDER_NAME: "gcp.gemini",
        GEN_AI_REQUEST_MODEL: model,
        GEN_AI_OUTPUT_TYPE: "json",
    }
