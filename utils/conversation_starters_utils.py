from typing import Optional

from pylon.core.tools import log
from tools import this


DEFAULT_MAX_CONVERSATION_STARTERS = 4
# Hard ceiling for every path, copies included; the admin setting can only move below it.
ABSOLUTE_MAX_CONVERSATION_STARTERS = 10

CONFIG_SECTION = 'conversation_starters_guardrail'
CONFIG_KEY = 'max_per_version'


class ConversationStartersLimitError(ValueError):
    pass


def count_conversation_starters(starters: Optional[list]) -> int:
    """Number of starters that would be stored: non-empty strings after strip."""
    if not starters:
        return 0
    return sum(1 for item in starters if isinstance(item, str) and item.strip())


def resolve_max_conversation_starters(config: Optional[dict]) -> int:
    section = (config or {}).get(CONFIG_SECTION) or {}
    if CONFIG_KEY not in section:
        return DEFAULT_MAX_CONVERSATION_STARTERS
    value = section.get(CONFIG_KEY)
    # bool is an int subclass, so True would otherwise read as a limit of 1
    if isinstance(value, bool) or not isinstance(value, int) \
            or not 1 <= value <= ABSOLUTE_MAX_CONVERSATION_STARTERS:
        log.warning(
            'Invalid %s.%s=%r, using the default %s',
            CONFIG_SECTION, CONFIG_KEY, value, DEFAULT_MAX_CONVERSATION_STARTERS,
        )
        return DEFAULT_MAX_CONVERSATION_STARTERS
    return value


def get_max_conversation_starters() -> int:
    # Read on every call, so an admin change applies without a restart.
    try:
        config = this.descriptor.config
    except Exception:  # pylint: disable=W0703
        return DEFAULT_MAX_CONVERSATION_STARTERS
    return resolve_max_conversation_starters(config)


def check_conversation_starters_limit(new_count: int, previous_count: int, limit: int) -> None:
    """A write may keep what the version already had, but never grow past the limit.

    A version above the limit (copied, or saved before the limit was lowered) keeps its starters
    and may edit or remove them; it just can't add more.
    """
    allowed = max(limit, previous_count)
    if new_count > allowed:
        raise ConversationStartersLimitError(
            f'conversation_starters cannot exceed {allowed} items, got {new_count}'
        )


def enforce_conversation_starters_limit(starters: Optional[list], previous_count: int = 0) -> None:
    check_conversation_starters_limit(
        count_conversation_starters(starters), previous_count, get_max_conversation_starters(),
    )


def import_starters_errors(agent_name: str, versions: list) -> list:
    """Per-version messages for an import above the absolute cap, so the user sees which version failed.

    Counts the raw list like the model validator does, so both agree on what is rejected.
    """
    errors = []
    for version in versions or []:
        starters = version.get('conversation_starters')
        if isinstance(starters, list) and len(starters) > ABSOLUTE_MAX_CONVERSATION_STARTERS:
            errors.append(
                f"{agent_name} / {version.get('name') or 'base'}: conversation_starters cannot exceed "
                f"{ABSOLUTE_MAX_CONVERSATION_STARTERS} items, got {len(starters)}"
            )
    return errors


def limit_error_response(error: ConversationStartersLimitError) -> list:
    """The pydantic-style error list the version endpoints already return for this field."""
    return [{'type': 'value_error', 'loc': ['conversation_starters'], 'msg': str(error)}]
