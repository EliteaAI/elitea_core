from typing import Optional


DEFAULT_MAX_TOKENS = None
DEFAULT_REASONING_MODEL_MAX_TOKENS = None


def normalize_runtime_max_tokens(value: Optional[int]) -> Optional[int]:
    """Map persisted/API Default values to the runtime no-custom-cap value."""
    return None if value in (None, -1) else value


def get_default_max_tokens(_supports_reasoning: bool) -> None:
    """Backward-compatible accessor for the no-custom-cap Default value."""
    return None
