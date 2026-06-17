"""Agent category helpers.

Categories are implemented as a constrained, predefined set of tag names. The
active list is the union of the hardcoded ``DEFAULT_AGENT_CATEGORIES`` and any
extra categories an admin adds via the guardrails configuration. "Other" is a
permanent, non-removable fallback category.
"""

from typing import List, Optional

from tools import this

from ..models.all import Tag
from .constants import DEFAULT_AGENT_CATEGORIES, DEFAULT_FALLBACK_CATEGORY


def _get_extra_categories() -> List[str]:
    """Return admin-added categories from live plugin config (no restart needed)."""
    guardrail = this.descriptor.config.get('publishing_guardrail', {})
    extras = guardrail.get('agent_categories', []) or []
    return [str(c).strip() for c in extras if str(c).strip()]


def get_active_categories() -> List[str]:
    """Return the active category names: defaults first, then deduped admin extras.

    Deduplication is case-insensitive; the first-seen spelling wins.
    """
    result: List[str] = []
    seen = set()
    for name in list(DEFAULT_AGENT_CATEGORIES) + _get_extra_categories():
        key = name.lower()
        if key not in seen:
            seen.add(key)
            result.append(name)
    return result


def get_active_categories_detailed() -> List[dict]:
    """Return active categories as ``[{"name": str, "is_default": bool}]``."""
    default_keys = {c.lower() for c in DEFAULT_AGENT_CATEGORIES}
    return [
        {"name": name, "is_default": name.lower() in default_keys}
        for name in get_active_categories()
    ]


def is_valid_category(name: Optional[str]) -> bool:
    """Check whether ``name`` is one of the active categories (case-insensitive)."""
    if not name:
        return False
    return name.lower() in {c.lower() for c in get_active_categories()}


def resolve_category(name: Optional[str]) -> str:
    """Return a valid category name, falling back to "Other" when invalid/empty.

    Preserves the canonical spelling from the active list.
    """
    if name:
        for active in get_active_categories():
            if active.lower() == name.lower():
                return active
    return DEFAULT_FALLBACK_CATEGORY


def set_version_category(session, version, category: str) -> None:
    """Replace any active-category tags on ``version`` with the single ``category``.

    Non-category tags on the version are preserved. The tag row is reused if it
    already exists in the project schema, otherwise it is created.
    """
    category = resolve_category(category)
    active_lower = {c.lower() for c in get_active_categories()}

    # Drop existing tags that belong to the category vocabulary.
    version.tags = [t for t in version.tags if t.name.lower() not in active_lower]

    existing = session.query(Tag).filter(Tag.name == category).first()
    version.tags.append(existing or Tag(name=category, data={}))


def apply_category_to_tag_dicts(tags: Optional[List[dict]], category: str) -> List[dict]:
    """Return a tag-dict list with all active-category entries replaced by ``category``.

    Operates on the plain dicts produced by ``ApplicationVersion.to_dict()`` /
    publish snapshots (``[{"name": str, "data": dict}, ...]``). Non-category tags
    are preserved.
    """
    category = resolve_category(category)
    active_lower = {c.lower() for c in get_active_categories()}
    result = [
        t for t in (tags or [])
        if str(t.get('name', '')).lower() not in active_lower
    ]
    result.append({"name": category, "data": {}})
    return result

