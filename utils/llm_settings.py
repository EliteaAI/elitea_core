from tools import rpc_tools


DEFAULT_MAX_TOKENS: int = 4_000
DEFAULT_REASONING_MODEL_MAX_TOKENS: int = 16_000


def get_default_max_tokens(supports_reasoning: bool) -> int:
    """
    Get the default max tokens for a given model.
    :param supports_reasoning: Whether the model supports reasoning.
    :return: The default max tokens.
    """
    return DEFAULT_REASONING_MODEL_MAX_TOKENS if supports_reasoning else DEFAULT_MAX_TOKENS
