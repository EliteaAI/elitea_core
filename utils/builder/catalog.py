"""Operation catalog (#7383 AC-17): every endpoint the old builder modules exposed, reachable through
discover -> exact schema -> validated call. Pure: takes the MCP API tool list from the openapi registry."""
import re

BUILDER_TAGS = (
    'elitea_core/applications', 'elitea_core/chat', 'elitea_core/toolkits', 'elitea_core/analytics',
    'secrets', 'configurations', 'artifacts', 'elitea_core/skills', 'elitea_core/project_context',
)
SESSION_PARAMS = ('project_id',)
# Non-DELETE writes that destroy or replace something the user can't get back.
DESTRUCTIVE_OPERATIONS = frozenset({
    ('PUT', 'secrets/secret/{mode}/{project_id}/{secret}'),
    ('PUT', 'artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}'),
    ('PUT', 'artifacts/buckets/{mode}/{project_id}'),
})
READ_METHODS = frozenset({'GET', 'HEAD', 'OPTIONS'})
_JSON_TYPES = {
    'integer': int, 'number': (int, float), 'string': str, 'boolean': bool, 'array': list, 'object': dict,
}
_WORD = re.compile(r'[a-z0-9]+')


class UnknownOperation(KeyError):
    pass


def endpoint_key(path) -> str:
    text = str(path or '').strip('/')
    return text[len('api/v2/'):] if text.startswith('api/v2/') else text


def kind_of(method: str, path: str) -> str:
    verb = str(method or '').upper()
    if verb in READ_METHODS:
        return 'read'
    if verb == 'DELETE' or (verb, endpoint_key(path)) in DESTRUCTIVE_OPERATIONS:
        return 'destructive'
    return 'write'


def _words(text) -> set:
    return set(_WORD.findall(str(text or '').lower()))


def _type_ok(value, json_type) -> bool:
    if json_type is None:
        return True
    types_ = json_type if isinstance(json_type, list) else [json_type]
    if value is None:
        return 'null' in types_
    for name in types_:
        expected = _JSON_TYPES.get(name)
        if expected is None:
            return True
        if isinstance(value, bool) and name in ('integer', 'number'):
            continue
        if isinstance(value, expected):
            return True
    return False


class Catalog:
    def __init__(self, api_tools, tags=BUILDER_TAGS):
        self._ops = {}
        for tool in api_tools or []:
            if not set(tool.get('tags') or []) & set(tags):
                continue
            op_id = tool.get('value') or tool.get('label')
            if op_id:
                self._ops[op_id] = tool

    def _get(self, op_id):
        if op_id not in self._ops:
            raise UnknownOperation(op_id)
        return self._ops[op_id]

    def summary(self, op_id) -> dict:
        tool = self._get(op_id)
        description = (tool.get('description') or '').strip().split('\n')[0]
        return {
            'op_id': op_id, 'description': description[:200],
            'kind': kind_of(tool.get('method'), tool.get('path')), 'tag': (tool.get('tags') or [None])[0],
        }

    def find(self, query: str, limit: int = 10) -> list:
        wanted = _words(query)
        scored = []
        for op_id, tool in self._ops.items():
            haystack = (_words(op_id) | _words(tool.get('description')) | _words(tool.get('path'))
                        | _words(' '.join(tool.get('tags') or [])))
            score = len(wanted & haystack) if wanted else 0
            if wanted and not score:
                continue
            scored.append((-score, op_id))
        return [self.summary(op_id) for _, op_id in sorted(scored)[:limit]]

    def tool(self, op_id) -> dict:
        return self._get(op_id)

    def kind(self, op_id) -> str:
        tool = self._get(op_id)
        return kind_of(tool.get('method'), tool.get('path'))

    def schema(self, op_id) -> dict:
        schema = dict(self._get(op_id).get('args_schema') or {})
        properties = {k: v for k, v in (schema.get('properties') or {}).items() if k not in SESSION_PARAMS}
        required = [k for k in (schema.get('required') or []) if k not in SESSION_PARAMS]
        return {**schema, 'properties': properties, 'required': required}

    def validate(self, op_id, args: dict, project_id: int):
        """(args to dispatch, errors). Session params are filled from the session; another project is refused."""
        args = dict(args or {})
        errors = []
        supplied = args.pop('project_id', None)
        if supplied is not None and supplied != project_id:
            return args, [{'field': 'project_id',
                           'message': f'only the current project ({project_id}) can be changed'}]
        schema = self.schema(op_id)
        properties = schema.get('properties') or {}
        for field in schema.get('required') or []:
            if field not in args:
                errors.append({'field': field, 'message': 'required'})
        for field, value in args.items():
            spec = properties.get(field)
            if spec is None:
                if schema.get('additionalProperties') is False:
                    errors.append({'field': field, 'message': 'not allowed'})
                continue
            if not _type_ok(value, spec.get('type')):
                errors.append({'field': field, 'message': f"must be {spec.get('type')}"})
        full_schema = self._get(op_id).get('args_schema') or {}
        if 'project_id' in (full_schema.get('properties') or {}):
            args['project_id'] = project_id
        return args, errors
