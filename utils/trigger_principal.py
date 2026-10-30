"""Who a scheduled or webhook run acts for (#6947, C-1009).

The principal of an agent-version trigger is declared by its autonomous mandate row (not by the trigger JSON,
which any editor of the version can rewrite): the latest revision for the agent version, whatever its state, so
a refusal can say why. No mandate row means the trigger still runs as the system user (``legacy_system``).
Reads only; nothing here changes behaviour until the admission layer asks.
"""
from dataclasses import dataclass
from typing import Literal, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import aliased

from ..models.agent_identity import AutonomousMandate

TRIGGER_KINDS = ("pipeline_schedule", "pipeline_webhook", "agent_webhook", "index_schedule")
# The operation a mandate must allow for each agent-version trigger
OPERATION_OF = {"pipeline_schedule": "run:schedule", "pipeline_webhook": "run:webhook", "agent_webhook": "run:webhook",
                "index_schedule": "run:index_schedule"}   # the index schedules of a toolkit mandate (#7305)
AGENT_OPERATIONS = frozenset({"run:schedule", "run:webhook"})
TOOLKIT_OPERATIONS = frozenset({OPERATION_OF["index_schedule"]})
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
    """The latest revision row of each agent version that has a mandate, by version id. One query: a correlated
    ``max(revision)`` over the unique ``(project_id, agent_version, revision)`` index."""
    other = aliased(AutonomousMandate)
    newest = select(func.max(other.revision)).where(
        other.project_id == AutonomousMandate.project_id, other.agent_version == AutonomousMandate.agent_version,
    ).scalar_subquery()
    statement = select(*_COLUMNS).where(AutonomousMandate.project_id == project_id,
                                        AutonomousMandate.revision == newest)
    if version_ids is not None:
        statement = statement.where(AutonomousMandate.agent_version.in_(list(version_ids)))
    return {row.agent_version: row for row in session.execute(statement)}


_TOOLKIT_COLUMNS = (AutonomousMandate.id, AutonomousMandate.toolkit_id, AutonomousMandate.owner_user_id,
                    AutonomousMandate.revision, AutonomousMandate.state, AutonomousMandate.allowed_operations,
                    AutonomousMandate.valid_until)


def latest_toolkit_mandates(session, project_id) -> dict:
    """The latest revision row of each (toolkit, owner) that has a toolkit mandate (#7305), keyed so. One query for the
    whole project (the index tick asks it once per project, never per schedule): a correlated ``max(revision)``."""
    other = aliased(AutonomousMandate)
    newest = select(func.max(other.revision)).where(
        other.project_id == AutonomousMandate.project_id, other.toolkit_id == AutonomousMandate.toolkit_id,
        other.owner_user_id == AutonomousMandate.owner_user_id,
    ).scalar_subquery()
    statement = select(*_TOOLKIT_COLUMNS).where(
        AutonomousMandate.project_id == project_id, AutonomousMandate.toolkit_id.isnot(None),
        AutonomousMandate.revision == newest)
    return {(row.toolkit_id, row.owner_user_id): row for row in session.execute(statement)}


def index_schedule_owner(schedule_key, created_by):
    """Whose index schedule this is, for a toolkit mandate (#7305), or ``None`` when no mandate can cover it. A personal
    schedule (key ``<user id>``) is that user's only while they made its last change: every save of the schedule
    endpoints stamps ``created_by`` with the saver, so a schedule someone else saved for them, or one saved before the
    field existed (``None``), is not theirs to vouch for until they save it again (C9-SEC-72). A team schedule (key
    ``-1``) is the person who saved it last."""
    key = str(schedule_key)
    if not isinstance(created_by, int) or isinstance(created_by, bool) or created_by <= 0:
        return None
    if key == TEAM_SCHEDULE_KEY:
        return created_by
    if not key.isdigit():
        return None
    return created_by if created_by == int(key) else None


def resave_needed(mandates, toolkit_id, schedule_key, created_by) -> bool:
    """A personal schedule whose key's user holds a toolkit mandate on it, but which they did not save last (or saved
    before the field existed): saving it again is the remedy. Pure: no read."""
    key = str(schedule_key)
    return key.isdigit() and index_schedule_owner(key, created_by) is None and (toolkit_id, int(key)) in mandates


def covering_toolkit_mandate(mandates, toolkit_id, schedule_key, created_by):
    """The latest toolkit mandate of the schedule's owner on this toolkit (whatever its state, so a refusal can say
    why), or ``None``. ``mandates``: ``latest_toolkit_mandates`` of the project. Pure: no read."""
    owner = index_schedule_owner(schedule_key, created_by)
    return None if owner is None else mandates.get((toolkit_id, owner))


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
