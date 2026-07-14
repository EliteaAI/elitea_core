"""Skill category helpers.

Skills reuse the exact same category vocabulary as agents (there is one shared
category list platform-wide). These helpers therefore delegate to
``category_utils`` — they exist only to keep the skill-side call sites and
function names stable.
"""

from typing import List, Optional

from . import category_utils


def get_builtin_skill_categories() -> List[str]:
    from .constants import DEFAULT_AGENT_CATEGORIES
    return list(DEFAULT_AGENT_CATEGORIES)


def get_active_skill_categories() -> List[str]:
    return category_utils.get_active_categories()


def get_skill_categories_detailed() -> List[dict]:
    return category_utils.get_active_categories_detailed()


def validate_skill_category(name: Optional[str]) -> bool:
    # Empty is allowed (category is optional at the skill level); a provided
    # value must be one of the active agent categories.
    if not name:
        return True
    return category_utils.is_valid_category(name)


def resolve_skill_category(name: Optional[str]) -> str:
    return category_utils.resolve_category(name)


def apply_skill_category_to_tag_dicts(tags: Optional[List[dict]], category: str) -> List[dict]:
    return category_utils.apply_category_to_tag_dicts(tags, category)
