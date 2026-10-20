"""Check the fields of a Build with AI schedule draft one by one (EL-7011, US-21)."""
import json
from datetime import date

from pydantic import ValidationError
from pylon.core.tools import log

from ..models.pd.entity_schedule import ScheduleTiming
from ..models.pd.generate_schedule_draft import ScheduleDraft

RUN_MODES = ('entry_point', 'prompt')
RUN_WITHOUT_ASKING = 'run_without_asking'
PAUSE_AND_ASK = 'pause_and_ask'
ALLOW_SECRET = 'schedules_allow_run_without_asking'
TRUE_VALUES = ('true', '1', 'yes')
# Fields with no rule of their own: the response model decides whether the value fits
PLAIN_FIELDS = ('name', 'description', 'prompt', 'variables', 'expires_at', 'approval_rule')


def schedule_roster(project_id: int) -> list[dict]:
    """One row per agent or pipeline version in the project."""
    # Imported here: the ORM tables are not needed by callers that only check fields.
    from tools import db  # pylint: disable=E0401,C0415

    from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415

    with db.with_project_schema_session(project_id) as session:
        rows = session.query(
            Application.id, Application.name, ApplicationVersion.agent_type,
            ApplicationVersion.id, ApplicationVersion.name,
        ).join(ApplicationVersion, ApplicationVersion.application_id == Application.id).all()
    return [
        {'application_id': app_id, 'name': name, 'agent_type': agent_type,
         'version_id': version_id, 'version_name': version_name}
        for app_id, name, agent_type, version_id, version_name in rows
    ]


class _KeepUnknown(dict):
    """An admin may edit the service prompt; a placeholder this helper does not know stays as it is."""

    def __missing__(self, key):
        return '{' + key + '}'


def _entity_lines(roster: list[dict], pipeline: bool) -> str:
    names: dict[str, list[str]] = {}
    for row in roster:
        if (row['agent_type'] == 'pipeline') == pipeline:
            names.setdefault(row['name'], []).append(row['version_name'])
    return '\n'.join(f"- {name} (versions: {', '.join(versions)})" for name, versions in names.items()) or '(none)'


def build_schedule_draft_prompt(
        template: str, current: dict, roster: list[dict], timezone: str | None, locked_entity: dict | None = None,
) -> str:
    """Fill the placeholders of the generate_schedule_draft service prompt (US-20) with this request's data."""
    locked = 'none'
    if locked_entity:
        name = next((row['name'] for row in roster if row['application_id'] == locked_entity['application_id']), None)
        locked = f"{locked_entity['entity_type']} {name or locked_entity['application_id']}"
    values = _KeepUnknown(
        today=date.today().isoformat(),
        timezone=timezone or 'UTC',
        agents=_entity_lines(roster, pipeline=False),
        pipelines=_entity_lines(roster, pipeline=True),
        current_values=json.dumps(current or {}, ensure_ascii=False, default=str),
        locked_entity=locked,
    )
    try:
        return template.format_map(values)
    except (ValueError, IndexError):  # an admin-edited prompt with single braces, such as a JSON example
        for key, value in values.items():
            template = template.replace('{' + key + '}', value)
        return template


def _allows_run_without_asking(project_id: int) -> bool:
    from tools import VaultClient  # pylint: disable=E0401,C0415

    try:
        secret = VaultClient(project_id).get_all_secrets().get(ALLOW_SECRET)
    except Exception:  # noqa: BLE001 -- an unreadable secret never allows "Run without asking" (R2.4)
        log.exception('Project %s: cannot read the %s secret', project_id, ALLOW_SECRET)
        return False
    return str(secret).strip().lower() in TRUE_VALUES


def _same_name(left, right) -> bool:
    return str(left).strip().casefold() == str(right).strip().casefold()


def _check_schedule(raw: dict, out: dict, warn) -> None:
    """Cron (with the interval floor) and time zone, each on its own."""
    for field in ('cron', 'timezone'):
        if raw.get(field) is None:
            continue
        try:
            out[field] = getattr(ScheduleTiming, f'validate_{field}')(str(raw[field]))
        except ValueError as error:
            warn(field, str(error))


def _resolve_entity(rows: list[dict], raw: dict, locked: dict | None, out: dict, warn) -> None:
    """Names map to ids from the project roster; the model's own ids are never trusted."""
    entity_type = locked['entity_type'] if locked else raw.get('entity_type')
    if entity_type not in ('agent', 'pipeline'):
        if entity_type is not None:
            warn('entity_type', 'The type must be agent or pipeline')
        return
    out['entity_type'] = entity_type
    if locked:
        out['application_id'] = locked['application_id']
        rows = [row for row in rows if row['application_id'] == out['application_id']]
    elif raw.get('entity_name') is not None:
        rows = [row for row in rows
                if (row['agent_type'] == 'pipeline') == (entity_type == 'pipeline')
                and _same_name(row['name'], raw['entity_name'])]
        if not rows:
            warn('application_id', f"No {entity_type} named '{raw['entity_name']}' in this project")
            return
        out['application_id'] = rows[0]['application_id']
    else:
        if raw.get('version_name') is not None:
            warn('version_id', 'A version needs an agent or pipeline name')
        return
    if raw.get('version_name') is not None:
        version = next((row for row in rows if _same_name(row['version_name'], raw['version_name'])), None)
        if version:
            out['version_id'] = version['version_id']
        else:
            warn('version_id', f"No version named '{raw['version_name']}' for this entity")


def _apply_run_mode(raw: dict, out: dict, warn) -> None:
    """Agents have no run mode; Entry point mode takes no prompt and no variables."""
    run_mode = raw.get('run_mode')
    if out.get('entity_type') == 'pipeline' and run_mode in RUN_MODES:
        out['run_mode'] = run_mode
    elif run_mode is not None:
        warn('run_mode', 'Only a pipeline has a run mode: Entry point or Prompt')
    if out.get('run_mode') == 'entry_point' and (out.get('prompt') or out.get('variables')):
        out['prompt'], out['variables'] = None, []
        warn('prompt', 'Entry point mode takes no prompt and no variables')


def _check_stop_date(out: dict, warn) -> None:
    """The stop date with the save rules of US-03: not past, and the schedule runs before it."""
    if out.get('expires_at') is None or not (out.get('cron') and out.get('timezone')):
        return
    try:
        ScheduleTiming(cron=out['cron'], timezone=out['timezone'], expires_at=out['expires_at'])
    except ValidationError as error:
        out.pop('expires_at')
        warn('expires_at', error.errors()[0]['msg'].removeprefix('Value error, '))


def _check_approval_rule(out: dict, warn) -> None:
    """Only the two rules of the form; any other answer becomes the safe "Pause and ask me"."""
    if out.get('approval_rule') is None:
        return
    rule = str(out['approval_rule']).strip().lower().replace(' ', '_')
    if rule not in (PAUSE_AND_ASK, RUN_WITHOUT_ASKING):
        warn('approval_rule', f"Unknown approval rule '{out['approval_rule']}'; set \"Pause and ask me\"")
        rule = PAUSE_AND_ASK
    out['approval_rule'] = rule


def validate_schedule_draft(
        project_id: int, raw: dict, locked_entity: dict | None, roster: list[dict] | None = None,
) -> tuple[ScheduleDraft, list[dict]]:
    """Return the draft with each unusable field cleared, and one warning per change."""
    warnings: list[dict] = []

    def warn(field: str, reason: str) -> None:
        warnings.append({'field': field, 'reason': reason})

    raw = dict(raw)
    if isinstance(raw.get('variables'), dict):  # the service prompt answers {name: value}
        raw['variables'] = [{'name': name, 'value': value} for name, value in raw['variables'].items()]
    if isinstance(raw.get('expires_at'), str):  # the service prompt asks for an ISO date-time; the form keeps the date
        raw['expires_at'] = raw['expires_at'][:10]
    out: dict = {}
    for field in PLAIN_FIELDS:
        if raw.get(field) is None:
            continue
        try:
            out[field] = getattr(ScheduleDraft(**{field: raw[field]}), field)
        except ValidationError as error:
            warn(field, error.errors()[0]['msg'])
    _check_schedule(raw, out, warn)
    _check_stop_date(out, warn)
    _resolve_entity(schedule_roster(project_id) if roster is None else roster, raw, locked_entity, out, warn)
    _apply_run_mode(raw, out, warn)
    _check_approval_rule(out, warn)
    if out.get('approval_rule') == RUN_WITHOUT_ASKING and not _allows_run_without_asking(project_id):
        out['approval_rule'] = PAUSE_AND_ASK
        warn('approval_rule', 'This project does not allow "Run without asking"; set "Pause and ask me"')
    return ScheduleDraft(**out), warnings
