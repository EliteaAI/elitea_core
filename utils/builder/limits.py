"""Platform limits the Builder checks before a write (#7383 AC-19). Pure: numbers mirror the pydantic models."""
import re
from collections import Counter

SKILL_NAME_MAX = 64
SKILL_DESCRIPTION_MAX = 2304
SKILL_NAME_RE = re.compile(r'^[a-z0-9]+(-[a-z0-9]+)*$')
RESERVED_NAME_WORDS = ('claude', 'anthropic')
PROJECT_CONTEXT_MAX = 2500
ACTIVATION_DESCRIPTION_MAX = 300
DEFAULT_MAX_STARTERS = 4
STARTER_LENGTH_MAX = 768


def _over(value: str, limit: int) -> str:
    over = len(value) - limit
    return f"{over} character{'s' if over != 1 else ''} over"


def _error(field, limit, message, suggestion=None):
    error = {'field': field, 'limit': limit, 'message': message}
    if suggestion is not None:
        error['suggestion'] = suggestion
    return error


def suggest_skill_name(value: str) -> str:
    slug = re.sub(r'[^a-z0-9]+', '-', (value or '').lower())
    for word in RESERVED_NAME_WORDS:
        slug = slug.replace(word, '')
    slug = re.sub(r'-+', '-', slug).strip('-')
    if len(slug) > SKILL_NAME_MAX:
        slug = slug[:SKILL_NAME_MAX].rsplit('-', 1)[0] if '-' in slug[:SKILL_NAME_MAX] else slug[:SKILL_NAME_MAX]
    return slug.strip('-') or 'skill'


def _check_skill(fields: dict) -> list:
    errors = []
    name = fields.get('name')
    if name is not None:
        bad_shape = len(name) > SKILL_NAME_MAX or not SKILL_NAME_RE.match(name)
        reserved = any(word in name for word in RESERVED_NAME_WORDS)
        if bad_shape or reserved:
            if len(name) > SKILL_NAME_MAX:
                message = f"name is {len(name)} characters; the limit is {SKILL_NAME_MAX}."
            elif reserved and not bad_shape:
                message = f"name can't contain {' or '.join(RESERVED_NAME_WORDS)}."
            else:
                message = "name must use only lowercase letters, digits and single hyphens."
            errors.append(_error(
                'name', SKILL_NAME_MAX, message,
                suggest_skill_name(name),
            ))
    description = fields.get('description')
    if description is not None and len(description) > SKILL_DESCRIPTION_MAX:
        errors.append(_error('description', SKILL_DESCRIPTION_MAX,
                             f"description is {_over(description, SKILL_DESCRIPTION_MAX)} "
                             f"the {SKILL_DESCRIPTION_MAX}-character limit."))
    return errors


def _check_project_context(fields: dict) -> list:
    errors = []
    for field, limit in (('content', PROJECT_CONTEXT_MAX), ('activation_description', ACTIVATION_DESCRIPTION_MAX)):
        value = fields.get(field)
        if value is not None and len(value) > limit:
            errors.append(_error(field, limit, f"{field} is {_over(value, limit)} the {limit}-character limit."))
    return errors


def _new_over_length_starters(starters: list, previous_starters) -> list:
    kept = Counter(item.strip() for item in previous_starters or [] if isinstance(item, str))
    too_long = []
    for item in (item.strip() for item in starters if isinstance(item, str)):
        if len(item) > STARTER_LENGTH_MAX:
            if kept[item]:
                kept[item] -= 1
            else:
                too_long.append(item)
    return too_long


def _check_application(fields: dict, max_starters: int, previous_starters) -> list:
    errors = []
    if 'name' in fields and not str(fields['name'] or '').strip():
        errors.append(_error('name', 1, 'name must not be empty.'))
    starters = fields.get('conversation_starters')
    if starters is not None:
        count = sum(1 for item in starters if isinstance(item, str) and item.strip())
        if count > max_starters:
            errors.append(_error('conversation_starters', max_starters,
                                 f"{count} conversation starters given; at most {max_starters} are allowed."))
        too_long = _new_over_length_starters(starters, previous_starters)
        if too_long:
            errors.append(_error('conversation_starters', STARTER_LENGTH_MAX,
                                 f"a conversation starter is {_over(too_long[0], STARTER_LENGTH_MAX)} "
                                 f"the {STARTER_LENGTH_MAX}-character limit."))
    return errors


def check(entity_type: str, fields: dict, max_starters: int = DEFAULT_MAX_STARTERS,
          previous_starters=None) -> list:
    """Errors for the fields a write would store: [{field, limit, message, suggestion?}]."""
    fields = fields or {}
    if entity_type == 'skill':
        return _check_skill(fields)
    if entity_type == 'project_context':
        return _check_project_context(fields)
    if entity_type in ('agent', 'pipeline'):
        return _check_application(fields, max_starters, previous_starters)
    return []
