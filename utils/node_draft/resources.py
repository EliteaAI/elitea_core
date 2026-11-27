"""Project toolkits, MCPs and agents as draft resources.

Visibility matches the project toolkit and agent lists (AC-30a): every resource in the project,
minus toolkit types blocked by ``toolkit_security``. The pure mappers are separate from the DB read.
"""
from typing import Callable, Dict, Iterable, List, Optional, Set

_PARAM_TYPES = {'string': 'str', 'integer': 'int', 'number': 'float', 'boolean': 'bool',
                'array': 'list', 'object': 'dict'}


def toolkit_candidate(row: dict, schema: dict, attached_ids: Set[int],
                      blocked: Callable[[str], bool]) -> Optional[dict]:
    kind_type = (row.get('type') or '').lower()
    if not row.get('name') or kind_type == 'application' or blocked(kind_type):
        return None
    args = dict((((schema or {}).get('properties') or {}).get('selected_tools') or {}).get('args_schemas') or {})
    # A remote MCP has no type schema; its tools' schemas are stored on the row when the tools are loaded
    for remote in (row.get('settings') or {}).get('available_mcp_tools') or []:
        if isinstance(remote, dict) and remote.get('value'):
            args.setdefault(remote['value'], {**(remote.get('args_schema') or {}),
                                              'description': remote.get('description', '')})
    tools = []
    for entry in (row.get('settings') or {}).get('selected_tools') or []:
        # Legacy OpenAPI toolkits store selected tools as objects ({'name': ...}), like the UI normalizes
        tool = entry.get('name') if isinstance(entry, dict) else entry
        if not isinstance(tool, str) or not tool:
            continue
        item = {'name': tool, 'description': (args.get(tool) or {}).get('description', '')}
        params = _params(args.get(tool))
        if params is not None:
            item['params'] = params
        tools.append(item)
    is_mcp = kind_type == 'mcp' or bool((row.get('meta') or {}).get('mcp'))
    return {'kind': 'mcp' if is_mcp else 'toolkit', 'entity_type': 'mcp' if is_mcp else 'toolkit',
            'id': row['id'], 'name': row['name'], 'type': row.get('type'),
            'description': row.get('description') or '', 'attached': row['id'] in attached_ids, 'tools': tools}


def _params(schema: Optional[dict]) -> Optional[Dict[str, str]]:
    """None when the tool's schema is unknown, so validation keeps its mapping as is."""
    if not isinstance(schema, dict) or 'properties' not in schema:
        return None
    required = set(schema.get('required') or [])
    return {name: _PARAM_TYPES.get((spec or {}).get('type'), 'str') + ('*' if name in required else '')
            for name, spec in (schema.get('properties') or {}).items()}


def agent_candidate(row: dict, attached_ids: Set[int]) -> dict:
    is_pipeline = row.get('agent_type') == 'pipeline'
    return {'kind': 'agent', 'entity_type': 'pipeline' if is_pipeline else 'agent', 'id': row['application_id'],
            'name': row['name'], 'description': row.get('description') or '',
            'attached': row['application_id'] in attached_ids, 'variables': list(row.get('variables') or []),
            'tools': []}


def index_resources(candidates: Iterable[dict]) -> Dict[str, Dict[str, dict]]:
    """``{kind: {name: {id, attached, needs_login, entity_type, tools: {tool: {param: required} | None}}}}``.

    A tool maps to None when its parameters are unknown (no schema).

    ``needs_login`` stays False: an MCP login lives in the browser, so the UI shows "Log in required"
    with its existing MCP status (PickerMcpStatus) when it lists the attachment.
    """
    index: Dict[str, Dict[str, dict]] = {'toolkit': {}, 'mcp': {}, 'agent': {}}
    for cand in candidates:
        index[cand['kind']][cand['name']] = {
            'id': cand['id'], 'attached': cand['attached'], 'needs_login': False,
            'entity_type': cand['entity_type'],
            'tools': {t['name']: ({p: kind.endswith('*') for p, kind in t['params'].items()}
                                  if 'params' in t else None)
                      for t in cand['tools']},
        }
    return index


def prompt_view(candidates: Iterable[dict]) -> dict:
    view: Dict[str, List[dict]] = {'toolkits': [], 'mcps': [], 'agents': []}
    for cand in candidates:
        if cand['kind'] == 'agent':
            view['agents'].append({'name': cand['name'], 'type': cand['entity_type'], 'attached': cand['attached'],
                                   'description': cand['description'], 'variables': cand['variables']})
        else:
            view['mcps' if cand['kind'] == 'mcp' else 'toolkits'].append({
                'name': cand['name'], 'type': cand['type'], 'attached': cand['attached'],
                'description': cand['description'], 'tools': cand['tools']})
    return view


def load_candidates(project_id: int, user_id: int, version_id: int, node_type: str,
                    exclude_application_id: int) -> List[dict]:
    """DB read: the resources of the kind this node type uses."""
    # pylint: disable=C0415
    from tools import db
    from ...models.all import Application, ApplicationVersion
    from ...models.elitea_tools import EliteATool
    from ..toolkit_security import is_toolkit_blocked
    from ..toolkits_utils import get_toolkit_schemas

    with db.with_project_schema_session(project_id) as session:
        version = session.query(ApplicationVersion).filter(ApplicationVersion.id == version_id).first()
        attached = {t.id for t in (version.tools if version else [])}
        attached_apps = {t.settings.get('application_id') for t in (version.tools if version else [])
                         if t.type == 'application'}
        candidates: List[dict] = []
        if node_type in ('llm', 'toolkit', 'mcp'):
            schemas = get_toolkit_schemas(project_id, user_id)
            for tool in session.query(EliteATool).filter(EliteATool.type != 'application').all():
                cand = toolkit_candidate(
                    {'id': tool.id, 'type': tool.type, 'name': tool.name, 'description': tool.description,
                     'settings': tool.settings or {}, 'meta': tool.meta or {}},
                    schemas.get(tool.type) or {}, attached, is_toolkit_blocked,
                )
                if cand and (node_type == 'llm' or cand['kind'] == node_type):
                    candidates.append(cand)
        if node_type == 'agent':
            rows = session.query(Application, ApplicationVersion).join(
                ApplicationVersion,
                (ApplicationVersion.application_id == Application.id) & (ApplicationVersion.name == 'base'),
            ).filter(Application.id != exclude_application_id).all()
            for app, base in rows:
                candidates.append(agent_candidate({
                    'application_id': app.id, 'name': app.name, 'description': app.description,
                    'agent_type': base.agent_type, 'variables': [v.name for v in base.variables],
                }, attached_apps))
        return candidates
