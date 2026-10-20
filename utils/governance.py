"""Credential governance mode of the platform (C-1009, #6754, #7120).

The mode is ``off``, ``audit_only`` or ``enforce``. It is stored in the database and set from the Admin UI; the
deployment only sets a ceiling (``utils/governance_settings.py``). Every reader asks the module, which answers from an
in-memory snapshot: ``module.governance_profile`` (``enabled`` means ``enforce``) and ``module.governance_effective``.
"""

from dataclasses import dataclass
from typing import Optional

from pylon.core.tools import log

CONFIG_SECTION = "enterprise_credential_governance"

PROFILE_LEGACY = "legacy"
PROFILE_GOVERNED = "governed"

MODE_OFF = "off"
MODE_AUDIT_ONLY = "audit_only"
MODE_ENFORCE = "enforce"
MODES = (MODE_OFF, MODE_AUDIT_ONLY, MODE_ENFORCE)
MODE_RANK = {MODE_OFF: 0, MODE_AUDIT_ONLY: 1, MODE_ENFORCE: 2}


@dataclass(frozen=True)
class GovernanceProfile:
    enabled: bool
    policy_revision: Optional[str]
    profile: str


def init_governance(module):
    """Build the governance state of this module (the ceiling from the environment, the first read of the stored
    switch). Misconfiguration of the deployment keys raises, so the plugin does not start."""
    from .governance_settings import create_state  # pylint: disable=C0415
    state = create_state(module)
    module.governance_state = state
    effective = state.effective
    log.info(
        "Credential governance: mode=%s ceiling=%s policy_revision=%s",
        effective.mode, state.ceiling.max_mode, effective.policy_revision,
    )
    return state


def is_governed(module) -> bool:
    """Enforce: governed runs are admitted or refused."""
    return module.governance_profile.enabled


def is_auditing(module) -> bool:
    """Audit only: every run is checked as in enforce and the result is recorded, nothing is refused."""
    return getattr(getattr(module, "governance_effective", None), "mode", MODE_OFF) == MODE_AUDIT_ONLY


def is_unavailable(module) -> bool:
    """The mode is on and a required service is not (the preflight fails, or the stored switch cannot be read)."""
    return bool(getattr(getattr(module, "governance_effective", None), "unavailable", False))


def governance_lifecycle_active(module) -> bool:
    """Is there governed work to look after: enforce now, or enforce earlier in this process (its runs keep their
    admission after a switch to off)."""
    return is_governed(module) or bool(getattr(module, "governed_handlers_registered", False))
