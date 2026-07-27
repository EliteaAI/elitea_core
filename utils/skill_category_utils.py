"""Skill category helpers.

Skills have their OWN category list, managed independently of agents (seeded as
a duplicate of the agent defaults — see ``DEFAULT_SKILL_CATEGORIES``). The active
list is the union of those defaults and any extras an admin adds via the skill
guardrail configuration. "Other" is a permanent, non-removable fallback.
"""

from typing import List, Optional

from tools import this

from .constants import DEFAULT_SKILL_CATEGORIES, DEFAULT_FALLBACK_CATEGORY


def _get_extra_skill_categories() -> List[str]:
    """Return admin-added skill categories from live plugin config (no restart)."""
    guardrail = this.descriptor.config.get('skill_publishing_guardrail', {})
    extras = guardrail.get('skill_categories', []) or []
    return [str(c).strip() for c in extras if str(c).strip()]


def get_builtin_skill_categories() -> List[str]:
    return list(DEFAULT_SKILL_CATEGORIES)


def get_active_skill_categories() -> List[str]:
    """Return the active category names: defaults first, then deduped admin extras.

    Deduplication is case-insensitive; the first-seen spelling wins. "Other" is
    always forced to be last, even if re-added by an admin extra.
    """
    result: List[str] = []
    seen = set()
    fallback_key = DEFAULT_FALLBACK_CATEGORY.lower()
    candidates = [c for c in DEFAULT_SKILL_CATEGORIES if c != DEFAULT_FALLBACK_CATEGORY]
    candidates += _get_extra_skill_categories()
    for name in candidates:
        key = name.lower()
        if key == fallback_key or key in seen:
            continue
        seen.add(key)
        result.append(name)
    result.append(DEFAULT_FALLBACK_CATEGORY)
    return result


def get_skill_categories_detailed() -> List[dict]:
    """Return active categories as ``[{"name": str, "is_default": bool}]``."""
    default_keys = {c.lower() for c in DEFAULT_SKILL_CATEGORIES}
    return [
        {"name": name, "is_default": name.lower() in default_keys}
        for name in get_active_skill_categories()
    ]


def validate_skill_category(name: Optional[str]) -> bool:
    if not name:
        return True
    return name.lower() in {c.lower() for c in get_active_skill_categories()}


def resolve_skill_category(name: Optional[str]) -> str:
    """Return a valid category name, falling back to "Other" when invalid/empty.

    Preserves the canonical spelling from the active list.
    """
    if name:
        for active in get_active_skill_categories():
            if active.lower() == name.lower():
                return active
    return DEFAULT_FALLBACK_CATEGORY


def apply_skill_category_to_tag_dicts(tags: Optional[List[dict]], category: str) -> List[dict]:
    """Return a tag-dict list with all active-category entries replaced by ``category``.

    Operates on the plain dicts produced by publish snapshots
    (``[{"name": str, "data": dict}, ...]``). Non-category tags are preserved.
    """
    category = resolve_skill_category(category)
    active_lower = {c.lower() for c in get_active_skill_categories()}
    result = [
        t for t in (tags or [])
        if str(t.get('name', '')).lower() not in active_lower
    ]
    result.append({"name": category, "data": {}})
    return result
