"""Who a scheduled or webhook run acts for (#6947, C-1009).

The principal of an agent-version trigger is declared by its autonomous mandate row (not by the trigger JSON,
which any editor of the version can rewrite): the latest revision for the agent version, whatever its state, so
a refusal can say why. No mandate row means the trigger still runs as the system user (``legacy_system``).
Reads only; nothing here changes behaviour until the admission layer asks.
"""
from dataclasses import dataclass
from typing import Literal, Optional

from sqlalchemy import select

from ..models.agent_identity import AutonomousMandate

TRIGGER_KINDS = ("pipeline_schedule", "pipeline_webhook", "agent_webhook", "index_schedule")
# The operation a mandate must allow for each agent-version trigger
OPERATION_OF = {"pipeline_schedule": "run:schedule", "pipeline_webhook": "run:webhook", "agent_webhook": "run:webhook"}
TEAM_SCHEDULE_KEY = "-1"
_COLUMNS = (AutonomousMandate.id, AutonomousMandate.agent_version, AutonomousMandate.revision,
            AutonomousMandate.state, AutonomousMandate.owner_user_id, AutonomousMandate.allowed_operations,
            AutonomousMandate.valid_until)


@dataclass(frozen=True)
class TriggerPrincipal:
    kind: Literal["mandate", "delegation", "legacy_system"]
    ref: Optional[str]              # "mandate:<uuid>" | "user:<id>" | None
    mandate_state: Optional[str]    # state of the latest mandate revision, for readable refusals and the report
    owner_user_id: Optional[int]


LEGACY = TriggerPrincipal("legacy_system", None, None, None)


def latest_mandates(session, project_id, version_ids=None) -> dict:
    """The latest revision row of each agent version that has a mandate, by version id. One read."""
    statement = select(*_COLUMNS).where(AutonomousMandate.project_id == project_id)
    if version_ids is not None:
        statement = statement.where(AutonomousMandate.agent_version.in_(list(version_ids)))
    latest = {}
    for row in session.execute(statement.order_by(AutonomousMandate.agent_version, AutonomousMandate.revision.desc())):
        latest.setdefault(row.agent_version, row)
    return latest


def principal_of(trigger_kind, mandate=None, schedule_key=None) -> TriggerPrincipal:
    """The principal for one trigger, given the latest mandate row of its agent version (if any)."""
    if trigger_kind not in TRIGGER_KINDS:
        raise ValueError("unknown trigger kind")
    if trigger_kind == "index_schedule":
        key = str(schedule_key)
        if schedule_key is None or key == TEAM_SCHEDULE_KEY:
            return LEGACY
        return TriggerPrincipal("delegation", f"user:{key}", None, int(key) if key.isdigit() else None)
    if mandate is None:
        return LEGACY
    return TriggerPrincipal("mandate", f"mandate:{mandate.id}", mandate.state, mandate.owner_user_id)


def resolve_trigger_principal(
    session, *, trigger_kind, project_id, version_id=None, schedule_key=None,
) -> TriggerPrincipal:
    """One indexed read for an agent-version trigger, none for an index schedule. Never writes."""
    mandate = None
    if trigger_kind != "index_schedule" and version_id is not None:
        mandate = latest_mandates(session, project_id, [version_id]).get(version_id)
    return principal_of(trigger_kind, mandate, schedule_key)
