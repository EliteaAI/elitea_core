"""In-process dispatch of Builder calls to the existing endpoints (#7383).

Every call goes through ``McpApiToolExecutor`` (in-process WSGI with the caller's auth and the internal-MCP marker), so
each endpoint's own permission decorator, folder access, run-token scope and audit apply unchanged.
"""
from ..mcp_service import McpApiToolExecutor

BASE = '/api/v2/elitea_core'
MODE = 'prompt_lib'

# op -> (method, path template, where the args go: 'query' | 'body')
ROUTES = {
    'list_applications': ('GET', f'{BASE}/applications/{{mode}}/{{project_id}}', 'query'),
    'get_application': ('GET', f'{BASE}/application/{{mode}}/{{project_id}}/{{application_id}}', 'query'),
    'get_version': ('GET', f'{BASE}/version/{{mode}}/{{project_id}}/{{application_id}}/{{version_id}}', 'query'),
    'create_application': ('POST', f'{BASE}/applications/{{mode}}/{{project_id}}', 'body'),
    'update_application': ('PUT', f'{BASE}/application/{{mode}}/{{project_id}}/{{application_id}}', 'body'),
    'update_version': ('PUT', f'{BASE}/version/{{mode}}/{{project_id}}/{{application_id}}/{{version_id}}', 'body'),
    'patch_instructions': (
        'POST', f'{BASE}/version_instruction_patch/{{mode}}/{{project_id}}/{{application_id}}/{{version_id}}', 'body'),
    'delete_application': ('DELETE', f'{BASE}/application/{{mode}}/{{project_id}}/{{application_id}}', 'query'),
    'list_skills': ('GET', f'{BASE}/skills/{{mode}}/{{project_id}}', 'query'),
    'get_skill': ('GET', f'{BASE}/skill/{{mode}}/{{project_id}}/{{skill_id}}', 'query'),
    'create_skill': ('POST', f'{BASE}/skills/{{mode}}/{{project_id}}', 'body'),
    'update_skill': ('PUT', f'{BASE}/skill/{{mode}}/{{project_id}}/{{skill_id}}', 'body'),
    'delete_skill': ('DELETE', f'{BASE}/skill/{{mode}}/{{project_id}}/{{skill_id}}', 'query'),
    'get_project_context': ('GET', f'{BASE}/project_context/{{mode}}/{{project_id}}/project-context', 'query'),
    'put_project_context': ('PUT', f'{BASE}/project_context/{{mode}}/{{project_id}}/project-context', 'body'),
    'delete_project_context': ('DELETE', f'{BASE}/project_context/{{mode}}/{{project_id}}/project-context', 'query'),
    'list_toolkits': ('GET', f'{BASE}/tools/{{mode}}/{{project_id}}', 'query'),
    'list_models': ('GET', '/api/v2/configurations/models/{project_id}', 'query'),
    'list_configurations': ('GET', '/api/v2/configurations/configurations/{project_id}', 'query'),
}


def _api_tool(method, path, keys, where):
    params = [{'name': 'mode', 'in': 'path', 'schema': {'default': MODE}},
              {'name': 'project_id', 'in': 'path'}]
    params += [{'name': k, 'in': 'path'} for k in keys if '{' + k + '}' in path]
    if where == 'query':
        params += [{'name': k, 'in': 'query'} for k in keys if '{' + k + '}' not in path]
    return {'method': method, 'path': path, 'parameters': params, 'label': f'builder:{method} {path}'}


def make_call(project_id: int, catalog):
    """``call(op, args)`` for BuilderService. ``op:<op_id>`` runs a catalog operation with validated args."""
    scoped = {'project_id': project_id}

    def call(op, args):
        args = {k: v for k, v in (args or {}).items() if v is not None}
        if op.startswith('op:'):
            api_tool = catalog.tool(op[3:])
        else:
            method, path, where = ROUTES[op]
            api_tool = _api_tool(method, path, list(args), where)
            if op == 'update_skill' and args.get('version_id') is not None:
                api_tool['parameters'].append({'name': 'version_id', 'in': 'query'})
            if op == 'get_skill' and args.get('version_id') is not None:
                api_tool['parameters'].append({'name': 'version_id', 'in': 'query'})
        result = McpApiToolExecutor.execute(api_tool, args, default_path_params=scoped, scoped_path_params=scoped)
        if 'error' in result:
            return {'error': result['error']}
        answer, governance = result.get('result'), result.get('governance_requests')
        if isinstance(governance, dict) and isinstance(answer, dict):
            return {**answer, 'governance_requests': governance}
        return answer

    return call
