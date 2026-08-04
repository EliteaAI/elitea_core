"""Platform guardrail for mid-turn user input injection.

Config is read live on each call (as skill_publish_utils does) so admin changes
apply without a reload. Blocklist semantics, mirroring publishing: off means
allowed everywhere; on means allowed only for whitelisted projects.
"""

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from tools import this


def _guardrail_config() -> dict:
    return this.descriptor.config.get('midturn_injection_guardrail', {}) or {}


def get_midturn_injection_blocked() -> bool:
    return bool(_guardrail_config().get('is_blocked', False))


def get_midturn_injection_whitelist() -> set:
    raw = _guardrail_config().get('whitelist_project_ids', []) or []
    return set(
        int(x) for x in raw
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit())
    )


def is_midturn_injection_blocked_for_project(project_id: int) -> bool:
    """Platform guardrail; defaults to not-blocked until admin config exists."""
    if not get_midturn_injection_blocked():
        return False
    try:
        project_id = int(project_id)
    except (TypeError, ValueError):
        log.warning("Non-numeric project_id %r for mid-turn injection gate", project_id)
        return True
    return project_id not in get_midturn_injection_whitelist()
