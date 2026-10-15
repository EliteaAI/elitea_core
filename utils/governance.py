"""Operator-owned enterprise credential governance profile (C-1009, #6754).

Read from the plugin config only (never Vault, never the admin editor) so the
legacy profile has no external dependency. Misconfiguration raises at startup.
"""

from dataclasses import dataclass
from typing import Mapping, Optional

from pylon.core.tools import log

CONFIG_SECTION = "enterprise_credential_governance"

PROFILE_LEGACY = "legacy"
PROFILE_GOVERNED = "governed"


@dataclass(frozen=True)
class GovernanceProfile:
    enabled: bool
    policy_revision: Optional[str]
    profile: str


def load_governance_profile(config: Mapping) -> GovernanceProfile:
    section = config.get(CONFIG_SECTION)
    if section is None:
        return GovernanceProfile(False, None, PROFILE_LEGACY)
    if not isinstance(section, Mapping):
        raise ValueError(f"{CONFIG_SECTION} must be a mapping, got {type(section).__name__}")
    #
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError(
            f"{CONFIG_SECTION}.enabled must be a boolean (true/false), got {enabled!r}"
        )
    if not enabled:
        return GovernanceProfile(False, None, PROFILE_LEGACY)
    #
    revision = section.get("policy_revision")
    if not isinstance(revision, str) or not revision.strip():
        raise ValueError(
            f"{CONFIG_SECTION}.policy_revision must be a non-empty string when enabled"
        )
    return GovernanceProfile(True, revision, PROFILE_GOVERNED)


def init_governance(module) -> GovernanceProfile:
    profile = load_governance_profile(module.descriptor.config)
    module.governance_profile = profile
    log.info(
        "Credential governance: profile=%s policy_revision=%s",
        profile.profile, profile.policy_revision,
    )
    return profile


def is_governed(module) -> bool:
    return module.governance_profile.enabled
