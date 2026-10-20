"""Read-only report of the triggers that run on the system user (#6947, C-1009): which schedules and webhooks
would be admitted under a mandate, and which would be refused once governance is switched on.

Lists, per project, the pipeline schedule and webhook triggers, the agent webhooks (non-pipeline versions of an
application with a webhook secret) and the toolkit index schedules, each with its principal and the outcome the
admission layer would give it. Works with governance off: it is the driver of the migration to mandates.
Never selects or returns a cron, a webhook secret, a credential title or a schedule body.
"""
from datetime import datetime, timezone

from pylon.core.tools import log
from sqlalchemy import String, select
from tools import db

from .trigger_principal import OPERATION_OF, TEAM_SCHEDULE_KEY, latest_mandates, principal_of

PRINCIPALS = ("legacy_system", "delegation", "mandate")
PIPELINE_TRIGGER_KINDS = {"schedule": "pipeline_schedule", "webhook": "pipeline_webhook"}


def governed_outcome(principal, trigger_kind, mandate, now) -> str:
    """What admission would answer for this trigger. ``mandate`` is the latest row of its agent version."""
    if principal.kind == "delegation":
        return "AUTHORIZATION_REQUIRED"
    if principal.kind == "legacy_system":
        return "MANDATE_REQUIRED"
    valid_until = mandate.valid_until
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)
    if mandate.state == "expired" or (mandate.state in ("active", "suspended") and valid_until <= now):
        return "MANDATE_EXPIRED"
    if mandate.state != "active":
        return "MANDATE_INACTIVE"
    if OPERATION_OF[trigger_kind] not in mandate.allowed_operations:
        return "DENIED_OPERATION"
    return "ADMISSIBLE"


def _pipeline_triggers(session):
    """``(version_id, application_id, trigger type, created_by)``; the trigger body is not read beyond those."""
    from ..models.all import ApplicationVersion  # pylint: disable=C0415
    from ..models.enums.all import AgentTypes  # pylint: disable=C0415
    trigger = ApplicationVersion.pipeline_settings.op('->')('trigger')
    kind = trigger.op('->>', return_type=String)('type')
    created_by = trigger.op('->>', return_type=String)('created_by')
    return session.execute(
        select(ApplicationVersion.id, ApplicationVersion.application_id, kind, created_by)
        .where(ApplicationVersion.agent_type == AgentTypes.pipeline.value,
               ApplicationVersion.pipeline_settings.isnot(None), kind.in_(tuple(PIPELINE_TRIGGER_KINDS)))
        .order_by(ApplicationVersion.id)
    ).all()


def _agent_webhooks(session):
    """``(version_id, application_id, application owner)``: only the presence of a secret is tested."""
    from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415
    from ..models.enums.all import AgentTypes  # pylint: disable=C0415
    return session.execute(
        select(ApplicationVersion.id, ApplicationVersion.application_id, Application.owner_id)
        .join(Application, Application.id == ApplicationVersion.application_id)
        .where(Application.webhook_secret.isnot(None), Application.webhook_secret != "",
               ApplicationVersion.agent_type != AgentTypes.pipeline.value)
        .order_by(ApplicationVersion.id)
    ).all()


def _index_schedules(session):
    """``(toolkit_id, index name, schedule key, enabled, created_by)``; the schedule body is not passed on."""
    from ..models.elitea_tools import EliteATool  # pylint: disable=C0415
    found = []
    for toolkit_id, indexes_meta in session.execute(
            select(EliteATool.id, EliteATool.meta['indexes_meta']).where(EliteATool.meta['indexes_meta'].isnot(None))
            .order_by(EliteATool.id)):
        for index_name, entry in sorted((indexes_meta or {}).items()):
            schedules = entry.get('schedules') if isinstance(entry, dict) else None
            for key, body in sorted((schedules or {}).items(), key=lambda item: str(item[0])):
                body = body if isinstance(body, dict) else {}
                found.append((toolkit_id, index_name, str(key), bool(body.get('enabled')), body.get('created_by')))
    return found


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _row(project_id, trigger_kind, principal, mandate, now, **fields):
    row = {"project_id": project_id, "trigger_kind": trigger_kind, "application_id": None, "version_id": None,
           "toolkit_id": None, "index_name": None, "owner_user_id": None, "enabled": True}
    row.update(fields)
    row.update(
        principal=principal.kind, mandate_ref=str(mandate.id) if mandate is not None else None,
        mandate_state=principal.mandate_state, governed_outcome=governed_outcome(principal, trigger_kind, mandate, now))
    return row


def _project_rows(session, project_id, now):
    pipelines, webhooks, indexes = _pipeline_triggers(session), _agent_webhooks(session), _index_schedules(session)
    mandates = latest_mandates(session, project_id, {item[0] for item in pipelines + webhooks})
    rows = []
    for version_id, application_id, trigger_type, created_by in pipelines:
        kind, mandate = PIPELINE_TRIGGER_KINDS[trigger_type], mandates.get(version_id)
        rows.append(_row(project_id, kind, principal_of(kind, mandate), mandate, now, application_id=application_id,
                         version_id=version_id, owner_user_id=_int_or_none(created_by)))
    for version_id, application_id, owner_id in webhooks:
        mandate = mandates.get(version_id)
        rows.append(_row(project_id, "agent_webhook", principal_of("agent_webhook", mandate), mandate, now,
                         application_id=application_id, version_id=version_id, owner_user_id=owner_id))
    for toolkit_id, index_name, key, enabled, created_by in indexes:
        principal = principal_of("index_schedule", schedule_key=key)
        owner = _int_or_none(created_by) if key == TEAM_SCHEDULE_KEY else principal.owner_user_id
        rows.append(_row(project_id, "index_schedule", principal, None, now, toolkit_id=toolkit_id,
                         index_name=index_name, enabled=enabled, owner_user_id=owner))
    return rows


def build_trigger_report(project_ids, principal=None):
    """``(rows, failed project ids)``. ``principal`` keeps only rows of that principal kind."""
    now = datetime.now(timezone.utc)
    rows, failed = [], []
    for project_id in project_ids:
        try:
            with db.get_session(project_id) as session:
                rows.extend(_project_rows(session, project_id, now))
        except Exception:  # pylint: disable=W0703
            log.exception("trigger report: failed to read project %s", project_id)
            failed.append(project_id)
    if principal is not None:
        rows = [row for row in rows if row["principal"] == principal]
    return rows, failed
