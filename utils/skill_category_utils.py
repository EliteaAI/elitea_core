"""Skill category helpers — built-in defaults only."""
from typing import Optional

from .constants import DEFAULT_AGENT_CATEGORIES


def get_builtin_skill_categories() -> list[str]:
    return list(DEFAULT_AGENT_CATEGORIES)


def validate_skill_category(name: Optional[str]) -> bool:
    if not name:
        return True
    return name.lower() in {c.lower() for c in get_builtin_skill_categories()}
