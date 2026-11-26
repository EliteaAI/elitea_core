from typing import Optional

from pylon.core.tools import log
from tools import this


DEFAULT_MAX_CONVERSATION_STARTERS = 4
# Hard ceiling for every path, copies included; the admin setting can only move below it.
ABSOLUTE_MAX_CONVERSATION_STARTERS = 10
MAX_CONVERSATION_STARTER_LENGTH = 768

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
    # Coerced like the skill limits, so "6" from a YAML/env override still works. bool is an int
    # subclass, so True would otherwise read as a limit of 1.
    try:
        if isinstance(value, bool) or isinstance(value, float):
            raise ValueError
        limit = int(value)
    except (TypeError, ValueError):
        limit = None
    if limit is None or not 1 <= limit <= ABSOLUTE_MAX_CONVERSATION_STARTERS:
        log.warning(
            'Invalid %s.%s=%r, using the default %s',
            CONFIG_SECTION, CONFIG_KEY, value, DEFAULT_MAX_CONVERSATION_STARTERS,
        )
        return DEFAULT_MAX_CONVERSATION_STARTERS
    return limit


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


def check_conversation_starters_length(starters: Optional[list], previous_starters: Optional[list]) -> None:
    """Same keep-what-it-had rule as the count: a starter saved over the length before the limit
    existed may stay unchanged, but a new or edited one must fit."""
    kept = {item.strip() for item in previous_starters or [] if isinstance(item, str)}
    for item in starters or []:
        if len(item) > MAX_CONVERSATION_STARTER_LENGTH and item not in kept:
            raise ConversationStartersLimitError(starter_length_message(len(item)))


def starter_length_message(length: int) -> str:
    return (f'a conversation starter must be {MAX_CONVERSATION_STARTER_LENGTH} characters or less, '
            f'got {length}')


def enforce_conversation_starters_limit(starters: Optional[list], previous_starters: Optional[list] = None) -> None:
    check_conversation_starters_limit(
        count_conversation_starters(starters),
        count_conversation_starters(previous_starters),
        get_max_conversation_starters(),
    )
    check_conversation_starters_length(starters, previous_starters)


def fit_fork_starters(applications: list) -> None:
    """Cut fork starters to the length limit in place, so a published agent saved over it before the
    limit stays forkable: the forker can't edit the source, and its owner can't edit a published version."""
    for application in applications or []:
        for version in application.get('versions') or []:
            starters = version.get('conversation_starters')
            if isinstance(starters, list):
                version['conversation_starters'] = [
                    item.strip()[:MAX_CONVERSATION_STARTER_LENGTH] if isinstance(item, str) else item
                    for item in starters
                ]


def import_starters_errors(agent_name: str, versions: list) -> list:
    """Per-version messages for an import or fork above the absolute cap or the starter length,
    so the user sees which version failed.

    Counts the raw list like the model validator does, so both agree on what is rejected. The body
    comes from the client, so there is no saved row whose over-length starters could be kept.
    """
    errors = []
    for version in versions or []:
        starters = version.get('conversation_starters')
        if not isinstance(starters, list):
            continue
        label = f"{agent_name} / {version.get('name') or 'base'}"
        if len(starters) > ABSOLUTE_MAX_CONVERSATION_STARTERS:
            errors.append(
                f"{label}: conversation_starters cannot exceed "
                f"{ABSOLUTE_MAX_CONVERSATION_STARTERS} items, got {len(starters)}"
            )
        longest = max((len(item.strip()) for item in starters if isinstance(item, str)), default=0)
        if longest > MAX_CONVERSATION_STARTER_LENGTH:
            errors.append(f"{label}: {starter_length_message(longest)}")
    return errors


def limit_error_response(error: ConversationStartersLimitError) -> list:
    """The pydantic-style error list the version endpoints already return for this field."""
    return [{'type': 'value_error', 'loc': ['conversation_starters'], 'msg': str(error)}]
