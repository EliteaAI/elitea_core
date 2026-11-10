"""Check the fields of a Build with AI schedule draft one by one (EL-7011, US-21)."""
import json
from datetime import date

from pydantic import ValidationError

from ..models.pd.entity_schedule import ScheduleTiming
from ..models.pd.generate_schedule_draft import ScheduleDraft
from .schedule_settings import is_run_without_asking_allowed

TOOLKIT_INDEX = 'toolkit_index'
INDEX_SLOT = '{toolkit_indexes}'  # the placeholder of the service prompt that lists the pairs below
# lean: 300 pairs in the prompt; add a search step when a project passes it
INDEX_PAIRS_MAX = 300
RUN_MODES = ('entry_point', 'prompt')
RUN_WITHOUT_ASKING = 'run_without_asking'
PAUSE_AND_ASK = 'pause_and_ask'
# Fields with no rule of their own: the response model decides whether the value fits
PLAIN_FIELDS = ('name', 'description', 'prompt', 'variables', 'expires_at', 'approval_rule')


def schedule_roster(project_id: int, user_id: int) -> list[dict]:
    """One row per agent or pipeline version in the project that the user may open (folder access)."""
    # Imported here: the ORM tables are not needed by callers that only check fields.
    from tools import db  # pylint: disable=E0401,C0415

    from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415
    from .folder_access import APPLICATION_ENTITY_TYPES, folder_exclusion_clause  # pylint: disable=C0415

    folder_filter = folder_exclusion_clause(project_id, APPLICATION_ENTITY_TYPES, Application.id, user_id)
    with db.with_project_schema_session(project_id) as session:
        query = session.query(
            Application.id, Application.name, ApplicationVersion.agent_type,
            ApplicationVersion.id, ApplicationVersion.name,
        ).join(ApplicationVersion, ApplicationVersion.application_id == Application.id)
        if folder_filter is not None:
            query = query.filter(folder_filter)
        rows = query.all()
    return [
        {'application_id': app_id, 'name': name, 'agent_type': agent_type,
         'version_id': version_id, 'version_name': version_name}
        for app_id, name, agent_type, version_id, version_name in rows
    ]


def schedule_index_roster(project_id: int, user_id: int) -> list[dict]:
    """One row per index of a toolkit the user may open: `toolkit_id`, `toolkit_name`, `index_name`, `scheduled`.

    Only toolkits whose vector store holds an index count, as the picker lists them (`has_indexes`). An index
    counts as scheduled when a schedule row or the old settings key already holds a schedule for it.
    """
    # Imported here: the ORM tables and the vector store helpers are not needed by callers that only check fields.
    from tools import db  # pylint: disable=E0401,C0415

    from ..models.all import EliteATool  # pylint: disable=C0415
    from ..models.entity_schedule import EntitySchedule  # pylint: disable=C0415
    from .application_tools import _toolkit_ids_with_indexes  # pylint: disable=C0415
    from .folder_access import TOOLKIT_ENTITY_TYPES, folder_exclusion_clause  # pylint: disable=C0415

    folder_filter = folder_exclusion_clause(project_id, TOOLKIT_ENTITY_TYPES, EliteATool.id, user_id)
    with db.with_project_schema_session(project_id) as session:
        query = session.query(EliteATool)
        if folder_filter is not None:
            query = query.filter(folder_filter)
        toolkit_ids = _toolkit_ids_with_indexes(project_id, query)
        if not toolkit_ids:
            return []
        toolkits = session.query(EliteATool.id, EliteATool.name, EliteATool.meta).filter(
            EliteATool.id.in_(toolkit_ids)).order_by(EliteATool.name, EliteATool.id).all()
        rows = set(session.query(EntitySchedule.toolkit_id, EntitySchedule.index_name).filter(
            EntitySchedule.toolkit_id.in_(toolkit_ids)).all())
    roster = []
    for toolkit_id, toolkit_name, meta in toolkits:
        for index_name, entry in sorted(((meta or {}).get('indexes_meta') or {}).items()):
            legacy = isinstance(entry, dict) and bool(entry.get('schedules'))
            roster.append({'toolkit_id': toolkit_id, 'toolkit_name': toolkit_name, 'index_name': index_name,
                           'scheduled': legacy or (toolkit_id, index_name) in rows})
    return roster


def index_roster_for(project_id: int, user_id: int, template: str, locked_entity: dict | None) -> list[dict] | None:
    """The index roster when the service prompt has the slot or the form is locked to an index, else None.

    The roster opens a vector store connection, so a draft for an agent or a pipeline does not pay for it.
    """
    if INDEX_SLOT in template or (locked_entity or {}).get('entity_type') == TOOLKIT_INDEX:
        return schedule_index_roster(project_id, user_id)
    return None


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


def _index_lines(index_roster: list[dict]) -> str:
    names: dict[str, list[str]] = {}
    for row in index_roster:
        if not row['scheduled']:
            names.setdefault(row['toolkit_name'], []).append(row['index_name'])
    lines, pairs = [], 0
    for toolkit, indexes in names.items():
        shown = indexes[:INDEX_PAIRS_MAX - pairs]
        if not shown:
            break
        pairs += len(shown)
        lines.append(f"- {toolkit} (indexes: {', '.join(shown)})")
    return '\n'.join(lines) or '(none)'


def _locked_text(locked_entity: dict, roster: list[dict], index_roster: list[dict]) -> str:
    if locked_entity['entity_type'] == TOOLKIT_INDEX:
        name = next((row['toolkit_name'] for row in index_roster if row['toolkit_id'] == locked_entity['toolkit_id']),
                    None)
        return f"{TOOLKIT_INDEX} {name or locked_entity['toolkit_id']}/{locked_entity['index_name']}"
    name = next((row['name'] for row in roster if row['application_id'] == locked_entity['application_id']), None)
    return f"{locked_entity['entity_type']} {name or locked_entity['application_id']}"


def build_schedule_draft_prompt(
        template: str, current: dict, roster: list[dict], timezone: str | None, locked_entity: dict | None = None,
        index_roster: list[dict] | None = None,
) -> str:
    """Fill the placeholders of the generate_schedule_draft service prompt (US-20) with this request's data."""
    index_roster = index_roster or []
    locked = _locked_text(locked_entity, roster, index_roster) if locked_entity else 'none'
    values = _KeepUnknown(
        today=date.today().isoformat(),
        timezone=timezone or 'UTC',
        agents=_entity_lines(roster, pipeline=False),
        pipelines=_entity_lines(roster, pipeline=True),
        toolkit_indexes=_index_lines(index_roster),
        current_values=json.dumps(current or {}, ensure_ascii=False, default=str),
        locked_entity=locked,
    )
    try:
        return template.format_map(values)
    except (ValueError, IndexError):  # an admin-edited prompt with single braces, such as a JSON example
        for key, value in values.items():
            template = template.replace('{' + key + '}', value)
        return template


def _same_name(left, right) -> bool:
    return str(left).strip().casefold() == str(right).strip().casefold()


def _check_schedule(raw: dict, out: dict, warn, index: bool = False) -> None:
    """Cron (with the interval floor) and time zone, each on its own; a Toolkit Index has its own floor."""
    for field in ('cron', 'timezone'):
        if raw.get(field) is None:
            continue
        try:
            out[field] = getattr(ScheduleTiming, f'validate_{field}')(str(raw[field]))
        except ValueError as error:
            warn(field, str(error))
    if index and out.get('cron'):
        from .schedule_service import ScheduleError, check_index_interval  # late: the service loads the ORM models
        try:
            check_index_interval(out['cron'])  # the one floor rule of the save path
        except ScheduleError as error:
            out.pop('cron')
            warn('cron', str(error))


def _resolve_index(index_rows: list[dict], raw: dict, locked: dict | None, out: dict, warn,
                   project_id: int, user_id: int) -> None:
    """A toolkit name and an index name map to ids from the roster the user may open, like an agent name.

    The target is cleared, with a warning, when a name is missing, the toolkit name is found twice or the index
    already has a schedule. A locked index wins and is only checked for access.
    """
    from .folder_access import TOOLKIT_ENTITY_TYPES, entity_access_error  # late: as in _resolve_entity

    out['entity_type'] = TOOLKIT_INDEX
    if locked:
        toolkit_id, index_name = locked['toolkit_id'], locked['index_name']
    else:
        toolkit_name, index_name = raw.get('entity_name'), raw.get('index_name')
        if toolkit_name is None or index_name is None:
            warn('toolkit_id', 'A toolkit index needs a toolkit name and an index name')
            return
        rows = [row for row in index_rows
                if _same_name(row['toolkit_name'], toolkit_name) and row['index_name'] == str(index_name).strip()]
        if not rows:
            warn('toolkit_id', f"No index '{index_name}' in a toolkit named '{toolkit_name}' in this project")
            return
        if len({row['toolkit_id'] for row in rows}) > 1:
            warn('toolkit_id', f"More than one toolkit is named '{toolkit_name}' and has this index")
            return
        if rows[0]['scheduled']:
            warn('index_name', 'This index already has a schedule')
            return
        toolkit_id, index_name = rows[0]['toolkit_id'], rows[0]['index_name']
    if entity_access_error(project_id, TOOLKIT_ENTITY_TYPES, toolkit_id, user_id=user_id):
        warn('toolkit_id', 'No toolkit with this id in this project')
        return
    out['toolkit_id'], out['index_name'] = toolkit_id, index_name


def _resolve_entity(rows: list[dict], raw: dict, locked: dict | None, out: dict, warn,
                    project_id: int, user_id: int, index_rows: list[dict] | None = None) -> None:
    """Names map to ids from the roster the user may open; the model's own ids are never trusted.

    A resolved id the user cannot open in its folder is dropped, the locked id of the request included.
    """
    from .folder_access import entity_access_error  # late: the social RPCs are not needed by the other checks

    entity_type = locked['entity_type'] if locked else raw.get('entity_type')
    if entity_type == TOOLKIT_INDEX:
        _resolve_index(index_rows or [], raw, locked, out, warn, project_id, user_id)
        return
    if entity_type not in ('agent', 'pipeline'):
        if entity_type is not None:
            warn('entity_type', 'The type must be agent, pipeline or toolkit_index')
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
    if entity_access_error(project_id, entity_type, out['application_id'], user_id=user_id):
        out.pop('application_id')
        warn('application_id', f'No {entity_type} with this id in this project')
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


def _clear_index_run_fields(out: dict, warn) -> None:
    """A Toolkit Index runs the index itself: no prompt, no variables and no approval rule."""
    if out.get('entity_type') != TOOLKIT_INDEX:
        return
    for field in ('prompt', 'variables', 'approval_rule'):
        if out.get(field):
            out[field] = [] if field == 'variables' else None
            warn(field, f'A Toolkit Index schedule takes no {field}')


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


def _check_risks(project_id: int, owner_id: int, out: dict, warn) -> None:
    """A draft never confirms sensitive tools (owner CR 2026-10-21): "Run without asking" with sensitive
    tools, or with tools that could not be checked, becomes "Pause and ask me"."""
    if out.get('approval_rule') != RUN_WITHOUT_ASKING or out.get('version_id') is None:
        return
    # late: the scan pulls in the toolkit expansion
    from .schedule_risk import scan_complete, scan_schedule
    review = scan_schedule(project_id, out['version_id'], owner_id)
    if review['sensitive'] or not scan_complete(review):
        out['approval_rule'] = PAUSE_AND_ASK
        warn('approval_rule', 'Sensitive tools need a confirmation when you save; set "Pause and ask me"')


def validate_schedule_draft(
        project_id: int, raw: dict, locked_entity: dict | None, roster: list[dict] | None = None, *, owner_id: int,
        index_roster: list[dict] | None = None,
) -> tuple[ScheduleDraft, list[dict]]:
    """Return the draft with each unusable field cleared, and one warning per change.

    `owner_id` is the caller: the risk review scans the drafted version as the owner who will save it.
    `index_roster` is read only when the draft names a Toolkit Index and the caller passed none.
    """
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
    entity_type = locked_entity['entity_type'] if locked_entity else raw.get('entity_type')
    if entity_type == TOOLKIT_INDEX and index_roster is None:
        index_roster = schedule_index_roster(project_id, owner_id)
    _check_schedule(raw, out, warn, index=entity_type == TOOLKIT_INDEX)
    _check_stop_date(out, warn)
    _resolve_entity(schedule_roster(project_id, owner_id) if roster is None else roster, raw, locked_entity, out, warn,
                    project_id, owner_id, index_roster)
    _apply_run_mode(raw, out, warn)
    _clear_index_run_fields(out, warn)
    _check_approval_rule(out, warn)
    if out.get('approval_rule') == RUN_WITHOUT_ASKING and not is_run_without_asking_allowed(project_id):
        out['approval_rule'] = PAUSE_AND_ASK
        warn('approval_rule', 'This project does not allow "Run without asking"; set "Pause and ask me"')
    _check_risks(project_id, owner_id, out, warn)
    return ScheduleDraft(**out), warnings
