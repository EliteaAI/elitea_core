"""Skill category helpers — built-in defaults only."""
from typing import Optional

from .constants import DEFAULT_SKILL_CATEGORIES


def get_builtin_skill_categories() -> list[str]:
    return list(DEFAULT_SKILL_CATEGORIES)


def validate_skill_category(name: Optional[str]) -> bool:
    if not name:
        return True
    return name.lower() in {c.lower() for c in get_builtin_skill_categories()}


def get_skill_categories_detailed() -> list[dict]:
    return [{"name": name, "is_default": True} for name in get_builtin_skill_categories()]
