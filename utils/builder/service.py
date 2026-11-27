"""Elitea Builder service (#7383): find / read / propose / apply.

Pure orchestration over an injected ``call(op, args)`` (the existing endpoints, dispatched in-process) and a changeset
``store``. Typed handling for agent, pipeline, skill and project context; every other old builder endpoint goes
through the operation ``catalog``. The model sends only the fields that change; the server builds the full request.
"""
import re
import time
import uuid

from . import changeset as cs
from . import limits
from . import pipelines
from .catalog import UnknownOperation
from .prompts import playbook

TYPED = ('agent', 'pipeline', 'skill', 'project_context')
LOOKUP = ('toolkit', 'model', 'configuration')
ENTITY_TYPES = TYPED + LOOKUP + ('operation',)
CHANGESET_TTL_S = 30 * 60
LIST_FIELDS = frozenset({'tags', 'conversation_starters', 'variables'})
# Agent / pipeline: `name` and `description` belong to the application (PUT application); everything else to the
# version (PUT version, instructions through the safe patch). The version's own name is read-only (`version_name`).
APPLICATION_LEVEL_FIELDS = frozenset({'name', 'description'})
VERSION_FIELDS = frozenset({
    'instructions', 'llm_settings', 'variables', 'conversation_starters', 'welcome_message',
    'pipeline_settings', 'tags', 'meta', 'agent_type', 'notes',
})
APPLICATION_FIELDS = APPLICATION_LEVEL_FIELDS | VERSION_FIELDS
# Skill: `name` / `description` belong to the skill; version content goes under `version` in the same PUT.
SKILL_LEVEL_FIELDS = frozenset({'name', 'description'})
SKILL_VERSION_FIELDS = frozenset({'instructions', 'tags', 'meta', 'run_settings'})
SKILL_FIELDS = SKILL_LEVEL_FIELDS | SKILL_VERSION_FIELDS
PROJECT_CONTEXT_FIELDS = frozenset({'content', 'enabled', 'activation_description'})
LOOKUP_OPS = {'toolkit': 'list_toolkits', 'model': 'list_models', 'configuration': 'list_configurations'}


# A key is secret when one of its words is a marker, or two adjacent words form one (``api_key``, ``authToken``);
# ``primary_key`` / ``project_key`` are not. ``{{secret.…}}`` references name a stored secret, never its value.
_SECRET_WORDS = frozenset({'password', 'passwd', 'passphrase', 'secret', 'token', 'authorization', 'bearer',
                           'credential', 'credentials', 'cookie', 'apikey'})
_SECRET_PAIRS = frozenset({('api', 'key'), ('private', 'key'), ('access', 'key'), ('secret', 'key'),
                           ('client', 'secret'), ('connection', 'string')})
_AUTH_SCHEMES = ('bearer ', 'basic ')
_WORDS = re.compile(r'[A-Z]?[a-z0-9]+|[A-Z]+(?![a-z])')
MASK = '****'


def _secret_name(key) -> bool:
    words = [w.lower() for w in _WORDS.findall(str(key))]
    return bool(_SECRET_WORDS & set(words)) or any(pair in _SECRET_PAIRS for pair in zip(words, words[1:]))


def _is_reference(value) -> bool:
    return isinstance(value, str) and value.startswith('{{secret.')


def masked(value, key=None, depth=0):
    """``value`` as the chat and the model may see it: every secret-named key, at any depth, and every
    ``Bearer …`` / ``Basic …`` string are masked; ``{{secret.…}}`` references stay."""
    if value in (None, '') or _is_reference(value) or depth > 8:
        return value
    if isinstance(value, dict):
        return {k: masked(v, k, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [masked(item, key, depth + 1) for item in value]
    if (key is not None and _secret_name(key)) or (isinstance(value, str) and value.lower().startswith(_AUTH_SCHEMES)):
        return MASK
    return value


def _masked(args, path=''):
    """Operation arguments as the chat shows them. A secrets operation shows only the secret's name (``name``, else
    the ``secret`` path argument): any other text argument may hold the value."""
    if not isinstance(args, dict):
        return args
    if '/secrets/' not in f'/{path or ""}':
        return masked(args)
    shown = 'name' if 'name' in args else 'secret'

    def one(key, value):
        if key in (shown, 'project_id'):
            return value
        return MASK if isinstance(value, str) and value and not _is_reference(value) else masked(value, key)
    return {k: one(k, v) for k, v in args.items()}


def _is_mcp(row) -> bool:
    return row.get('type') == 'mcp' or bool((row.get('meta') or {}).get('mcp'))


def _invalid(message, errors=None):
    return {'status': 'invalid', 'message': message, 'errors': errors or []}


class MemoryStore:
    """Changeset store for tests and single-process use; production uses the per-project table."""

    def __init__(self):
        self._rows = {}

    def put(self, row):
        self._rows[row['id']] = dict(row)

    def get(self, changeset_id):
        row = self._rows.get(changeset_id)
        return dict(row) if row else None

    def update(self, changeset_id, **fields):
        if changeset_id in self._rows:
            self._rows[changeset_id].update(fields)

    def find_pending(self, user_id, fingerprint, not_before):
        for row in self._rows.values():
            if (row['user_id'] == user_id and row.get('fingerprint') == fingerprint and not row['applied']
                    and row['created_at'] >= not_before):
                return dict(row)
        return None

    def claim_apply(self, changeset_id) -> bool:
        row = self._rows.get(changeset_id)
        if not row or row['applied']:
            return False
        row['applied'] = True
        return True


class BuilderService:
    def __init__(self, project_id, user_id, call, store, catalog, can_write, seen_playbooks=None,
                 max_starters=limits.DEFAULT_MAX_STARTERS, now=time.time, material=None, tool_schemas=None):
        # tool_schemas(toolkit_types) -> {toolkit_type: {tool_name: json_schema}}; lets the pipeline check
        # compare tool parameters with the attached toolkits (#6388). None: parameters are not checked.
        self.tool_schemas = tool_schemas
        self._attached = {}  # (application_id, version_id) -> the version's attached tools, read in _state
        self.project_id = project_id
        self.user_id = user_id
        self.call = call
        self.store = store
        self.catalog = catalog
        self.can_write = can_write
        self.seen_playbooks = seen_playbooks if seen_playbooks is not None else set()
        self.max_starters = max_starters
        self.now = now
        self.material = material

    # --- find ---------------------------------------------------------------------------------------------------

    def find(self, entity_type, query='', limit=10):
        if entity_type not in ENTITY_TYPES:
            return _invalid(f"unknown type '{entity_type}'; use one of: {', '.join(ENTITY_TYPES)}")
        if entity_type == 'operation':
            return self._with_playbook('operation', {'status': 'ok', 'candidates': self.catalog.find(query, limit)})
        if entity_type == 'project_context':
            return self._with_playbook(entity_type, {'status': 'ok', 'candidates': [{'id': 'project'}],
                                                     'ambiguous': False})
        if entity_type in LOOKUP:
            args = {'query': query}
            if entity_type == 'model':
                args = {'section': 'embedding' if 'embedding' in (query or '').lower() else 'llm',
                        'include_shared': 'true'}
            # The toolkit listing hides MCP toolkits unless asked (mcp=true), so a toolkit lookup asks both ways.
            calls = [args, {**args, 'mcp': 'true'}] if entity_type == 'toolkit' else [args]
            rows = []
            for call_args in calls:
                answer = self.call(LOOKUP_OPS[entity_type], call_args)
                if isinstance(answer, dict):
                    if answer.get('error'):
                        return {'status': 'error', 'message': str(answer['error'])}
                    answer = answer.get('rows', answer.get('items', []))
                rows += answer or []
            wanted = (query or '').lower()
            candidates = [
                {'id': r.get('id'), 'name': r.get('name') or r.get('label') or r.get('display_name'),
                 **({'mcp': True} if entity_type == 'toolkit' and _is_mcp(r) else {})}
                for r in rows if isinstance(r, dict)
                and (not wanted or wanted in ' '.join(str(r.get(k) or '') for k in ('name', 'label', 'display_name')).lower()
                     or (entity_type == 'model' and wanted in ('embedding', 'embeddings')))
            ][:limit]
            return {'status': 'ok', 'candidates': candidates, 'ambiguous': False}
        op = 'list_skills' if entity_type == 'skill' else 'list_applications'
        args = {'query': query, 'limit': limit}
        if entity_type in ('agent', 'pipeline'):
            args['agents_type'] = 'pipeline' if entity_type == 'pipeline' else 'classic'
        rows = self.call(op, args)
        rows = rows.get('rows', rows) if isinstance(rows, dict) else rows
        candidates = [self._candidate(r) for r in rows or []][:limit]
        exact = [c for c in candidates if str(c['name']).lower() == (query or '').lower()]
        return self._with_playbook(entity_type, {'status': 'ok', 'candidates': candidates,
                                                 'ambiguous': len(exact) > 1})

    @staticmethod
    def _candidate(row):
        versions = row.get('versions') or []
        default = versions[0] if versions else {}
        return {'id': row.get('id'), 'name': row.get('name'), 'version_id': default.get('id'),
                'version': default.get('name'), 'status': default.get('status')}

    # --- read ---------------------------------------------------------------------------------------------------

    def _application(self, entity_id, version_id):
        """(application row, version id): the default version when none is named."""
        app = self.call('get_application', {'application_id': entity_id}) or {}
        versions = app.get('versions') or []
        if version_id is None and versions:
            default_id = (app.get('meta') or {}).get('default_version_id')
            version_id = default_id or versions[0].get('id')
        return app, version_id

    def _state(self, entity_type, entity_id, version_id=None):
        """(raw endpoint answer, flat editable state). The flat state is what the model reads and changes."""
        if entity_type in ('agent', 'pipeline'):
            app, version_id = self._application(entity_id, version_id)
            raw = self.call('get_version', {'application_id': entity_id, 'version_id': version_id}) or {}
            flat = {k: v for k, v in raw.items() if k in VERSION_FIELDS or k in ('id', 'status')}
            if entity_type == 'pipeline':
                # read-only, for the pipeline check (#6388): which toolkits the YAML's tool nodes can call
                self._attached[(entity_id, version_id)] = raw.get('tools') or []
            flat.update({'name': app.get('name'), 'description': app.get('description'),
                         'version_name': raw.get('name'), 'version_id': version_id})
            return raw, flat
        if entity_type == 'skill':
            raw = self.call('get_skill', {'skill_id': entity_id, 'version_id': version_id}) or {}
            version = raw.get('version_details') or {}
            flat = {'id': raw.get('id'), 'name': raw.get('name'), 'description': raw.get('description'),
                    'version_id': version.get('id'), 'version_name': version.get('name'),
                    **{k: version.get(k) for k in SKILL_VERSION_FIELDS}}
            return raw, flat
        if entity_type == 'project_context':
            raw = self.call('get_project_context', {}) or {}
            return raw, {k: raw.get(k) for k in PROJECT_CONTEXT_FIELDS}
        raise ValueError(entity_type)

    def read(self, entity_type, entity_id=None, version_id=None):
        if entity_type == 'operation':
            try:
                return self._with_playbook('operation', {
                    'status': 'ok', 'operation': self.catalog.summary(entity_id),
                    'schema': self.catalog.schema(entity_id),
                })
            except UnknownOperation:
                return _invalid(f"unknown operation '{entity_id}'; use post_elitea_core_builder_find with type 'operation'")
        if entity_type in LOOKUP:
            return _invalid(f"{entity_type} can't be read; the id from post_elitea_core_builder_find is all you need "
                            f"(for a toolkit or MCP it is the tool_id to link)")
        if entity_type not in TYPED:
            return _invalid(f"read supports: {', '.join(TYPED + ('operation',))}")
        _, state = self._state(entity_type, entity_id, version_id)
        result = {'status': 'ok', 'state': state, 'revision': cs.revision(state)}
        if entity_type == 'pipeline' and state.get('instructions'):
            # #6388: what is already wrong, so the Builder can fix it while it is there
            result['pipeline_check'] = pipelines.health(state['instructions'],
                                                        self._pipeline_context(state, entity_id))
        return self._with_playbook(entity_type, result)

    def _with_playbook(self, entity_type, result):
        text = playbook(entity_type)
        if text and entity_type not in self.seen_playbooks:
            self.seen_playbooks.add(entity_type)
            result['playbook'] = text
        return result

    # --- propose ------------------------------------------------------------------------------------------------

    def _resolve_target(self, entity_type, entity_id, version_id, entity_name):
        if entity_id is not None or entity_type == 'project_context':
            return entity_id, version_id, None
        if not entity_name:
            return None, None, {'status': 'needs_input', 'questions': [
                {'header': 'Which one', 'question': f'Which {entity_type} should be changed?', 'options': []}]}
        found = self.find(entity_type, entity_name)
        exact = [c for c in found.get('candidates', []) if str(c['name']).lower() == entity_name.lower()]
        if len(exact) == 1:
            return exact[0]['id'], version_id or exact[0]['version_id'], None
        candidates = exact or found.get('candidates', [])
        if not candidates:
            return None, None, _invalid(f"no {entity_type} named '{entity_name}' in this project")
        return None, None, {'status': 'needs_input', 'questions': [{
            'header': 'Which one',
            'question': f'Several {entity_type}s match "{entity_name}". Which one should be changed?',
            'options': [{'label': f"{c['name']} (id {c['id']})", 'value': c['id']} for c in candidates[:4]],
        }]}

    def propose(self, entity_type, op, changes=None, entity_id=None, version_id=None, entity_name=None,
                base_revision=None):
        changes = changes or {}
        if not self.can_write:
            return {'status': 'forbidden', 'message': "You don't have permission to change entities in this project."}
        if entity_type == 'operation':
            return self._propose_operation(entity_id, changes)
        if entity_type not in TYPED:
            return _invalid(f"propose supports: {', '.join(TYPED + ('operation',))}")
        if op not in ('create', 'update', 'delete'):
            return _invalid("op must be create, update or delete")
        if op == 'create':
            if entity_name and not changes.get('name'):
                changes = {**changes, 'name': entity_name}
            if entity_type == 'skill' and 'body' in changes and not changes.get('instructions'):
                changes = {**{k: v for k, v in changes.items() if k != 'body'}, 'instructions': changes['body']}
            return self._propose_create(entity_type, changes)

        entity_id, version_id, stop = self._resolve_target(entity_type, entity_id, version_id, entity_name)
        if stop:
            return stop
        raw, state = self._state(entity_type, entity_id, version_id)
        version_id = state.get('version_id', version_id)
        display_name = state.get('name')
        if base_revision and base_revision != cs.revision(state):
            return {'status': 'conflict', 'message': 'The entity changed since you read it. Read it again and redo '
                                                     'the change once.', 'revision': cs.revision(state)}
        if op == 'delete':
            return self._store({'entity_type': entity_type, 'op': 'delete', 'entity_id': entity_id,
                                'version_id': version_id, 'before': state, 'after': None,
                                'destructive': True, 'name': display_name or state.get('name')})

        if entity_type in ('agent', 'pipeline') and cs.is_locked_version(state.get('status')):
            return _invalid(f"Version {version_id} is {state.get('status')} and can not be updated.", [{
                'field': 'version', 'message': f"version is {state.get('status')}",
                'suggestion': 'create a new draft version with this change'}])
        allowed = {'agent': APPLICATION_FIELDS, 'pipeline': APPLICATION_FIELDS, 'skill': SKILL_FIELDS,
                   'project_context': PROJECT_CONTEXT_FIELDS}[entity_type]
        if 'version_name' in changes:
            return _invalid("version_name can't be changed; create a new version instead",
                            [{'field': 'version_name', 'message': 'read-only'}])
        try:
            after = cs.apply_changes(state, changes, allowed_fields=allowed)
        except cs.ChangeError as exc:
            return _invalid(str(exc), [{'field': str(exc).split(':')[0], 'message': str(exc)}])
        touched = {k: after.get(k) for k in changes}
        errors = limits.check(entity_type, touched, max_starters=self.max_starters,
                              previous_starters=state.get('conversation_starters'))
        if errors:
            return _invalid('The change breaks a platform limit; nothing was saved.', errors)
        refusal = self._pipeline_refusal(entity_type, state, after, changes, entity_id)
        if refusal:
            return refusal
        delta = cs.diff(state, after)
        if not delta:
            return {'status': 'unchanged', 'message': 'Nothing to change.'}
        return self._store({'entity_type': entity_type, 'op': 'update', 'entity_id': entity_id,
                            'version_id': version_id, 'before': state, 'after': after, 'fields': sorted(changes),
                            'raw_sha': (raw or {}).get('instructions_sha256'), 'destructive': False,
                            'name': display_name or after.get('name') or state.get('name')})

    # A change to either one can introduce a validator error: the YAML itself, or the trigger it runs on (R31).
    _PIPELINE_CHECKED_FIELDS = ('instructions', 'pipeline_settings')

    def _pipeline_refusal(self, entity_type, before, after, changes, entity_id=None):
        """#6388: a pipeline change that introduces validator errors is refused before the Changes card."""
        if entity_type != 'pipeline' or not any(f in (changes or {}) for f in self._PIPELINE_CHECKED_FIELDS):
            return None
        schemas = self._tool_schemas_for(after, entity_id)  # the attached toolkits are the same on both sides
        errors = pipelines.new_errors(
            (before or {}).get('instructions'), (after or {}).get('instructions'),
            after_context=pipelines.context_for(pipelines.trigger_of(after), schemas),
            before_context=pipelines.context_for(pipelines.trigger_of(before), schemas))
        return _invalid(pipelines.REFUSAL, pipelines.as_errors(errors)) if errors else None

    def _pipeline_context(self, state, entity_id=None):
        """The trigger and the attached toolkits' tool schemas, for the checks that need them."""
        return pipelines.context_for(pipelines.trigger_of(state), self._tool_schemas_for(state, entity_id))

    def _tool_schemas_for(self, state, entity_id=None):
        """``{toolkit_name: {tool: json_schema}}`` for the version's attached built-in toolkits, or None."""
        attached = self._attached.get((entity_id, (state or {}).get('version_id'))) or []
        types_by_name = {t.get('name'): t.get('type') for t in attached
                         if isinstance(t, dict) and t.get('type') and t.get('name')}
        if not (self.tool_schemas and types_by_name):
            return None
        try:
            by_type = self.tool_schemas(sorted(set(types_by_name.values()))) or {}
        except Exception:  # pylint: disable=W0703
            return None  # a lookup failure never blocks a change; parameters are just not checked
        return {name: by_type[kind] for name, kind in types_by_name.items() if by_type.get(kind)} or None

    def _propose_create(self, entity_type, fields):
        try:
            fields = cs.apply_changes({f: [] for f in LIST_FIELDS if f in (fields or {})}, fields)
        except cs.ChangeError as exc:
            return _invalid(str(exc), [{'field': str(exc).split(':')[0], 'message': str(exc)}])
        if entity_type in ('agent', 'pipeline'):
            fields.setdefault('llm_settings', {})
        required = {'agent': ('name',), 'pipeline': ('name', 'instructions'), 'skill': ('name', 'description',
                                                                                          'instructions'),
                    'project_context': ('content',)}[entity_type]
        missing = [f for f in required if not fields.get(f)]
        if missing:
            return {'status': 'needs_input', 'questions': [
                {'header': f.replace('_', ' ').capitalize(), 'question': f'What should the {entity_type} {f} be?',
                 'options': []} for f in missing[:3]]}
        errors = limits.check(entity_type, fields, max_starters=self.max_starters)
        if errors:
            return _invalid('The change breaks a platform limit; nothing was saved.', errors)
        refusal = self._pipeline_refusal(entity_type, None, fields, fields)
        if refusal:
            return refusal
        return self._store({'entity_type': entity_type, 'op': 'create', 'entity_id': None, 'before': None,
                            'after': dict(fields), 'destructive': False, 'name': fields.get('name')})

    def _propose_operation(self, op_id, args):
        try:
            dispatch_args, errors = self.catalog.validate(op_id, args, project_id=self.project_id)
            kind = self.catalog.kind(op_id)
        except UnknownOperation:
            return _invalid(f"unknown operation '{op_id}'; use post_elitea_core_builder_find with type 'operation'")
        if errors:
            return _invalid('The arguments do not match the operation schema; nothing was called.', errors)
        mismatch = self._version_mismatch(dispatch_args)
        if mismatch:
            return _invalid(mismatch, [{'field': 'entity_version_id', 'message': mismatch}])
        if self.material is not None and self.material(self.catalog.tool(op_id), dispatch_args):
            # (#7228) a credential value never goes through the model: refused before anything is stored
            return {'status': 'refused', 'code': 'CREDENTIALS_VIA_FORM',
                    'message': 'Credentials are entered by the user in the credential form, never passed as '
                               'arguments; nothing was saved.'}
        return self._store({'entity_type': 'operation', 'op': 'call', 'entity_id': op_id, 'before': None,
                            'after': dispatch_args, 'destructive': kind == 'destructive', 'name': op_id})

    def _store(self, row):
        # The same pending proposal (e.g. re-issued when a HITL card resumes the run) keeps its changeset, so a
        # confirmation recorded on it still applies and no orphan rows pile up.
        fingerprint = cs.revision({k: row.get(k) for k in ('entity_type', 'op', 'entity_id', 'version_id',
                                                            'before', 'after')})
        existing = self.store.find_pending(self.user_id, fingerprint, self.now() - CHANGESET_TTL_S)
        if existing:
            row = existing
        else:
            row = {**row, 'id': uuid.uuid4().hex, 'project_id': self.project_id, 'user_id': self.user_id,
                   'created_at': self.now(), 'confirmed_by': None, 'applied': False, 'fingerprint': fingerprint}
            self.store.put(row)
        needs_click = cs.requires_confirmation(row['op'], row['destructive'])
        proposal = {
            'status': 'needs_confirmation' if needs_click else 'ready',
            'changeset_id': row['id'],
            'project_id': self.project_id,
            'diff': cs.diff(row['before'], row['after']) if row['before'] is not None and row['after'] is not None
            else [],
            'summary': self._describe(row),
        }
        if row['op'] == 'call':
            proposal['arguments'] = _masked(row['after'], self._op_path(row['entity_id']))
        return proposal

    def _op_path(self, op_id):
        try:
            return self.catalog.tool(op_id).get('path') or ''
        except UnknownOperation:
            return ''

    # --- confirm / apply ----------------------------------------------------------------------------------------

    def confirm(self, changeset_id, user_id):
        row = self.store.get(changeset_id)
        if not row or row['user_id'] != user_id or row['applied']:
            return False
        self.store.update(changeset_id, confirmed_by=user_id)
        return True

    def apply(self, changeset_id):
        row = self.store.get(changeset_id)
        if not row or row['project_id'] != self.project_id or row['user_id'] != self.user_id:
            return _invalid('Unknown changeset; propose the change again.')
        if row['applied']:
            return _invalid('This changeset was already applied.')
        if self.now() - row['created_at'] > CHANGESET_TTL_S:
            return _invalid('This changeset expired; propose the change again.')
        if cs.requires_confirmation(row['op'], row['destructive']) and row['confirmed_by'] != self.user_id:
            return {'status': 'forbidden', 'message': 'This change needs the user to confirm it in the chat first.'}
        if row['op'] == 'update':
            _, current = self._state(row['entity_type'], row['entity_id'], row['version_id'])
            if cs.revision(current) != cs.revision(row['before']):
                return {'status': 'conflict', 'message': 'The entity changed since the proposal. Read it again and '
                                                         'redo the change once.'}
        if not self.store.claim_apply(changeset_id):
            return _invalid('This changeset was already applied.')
        response = self._dispatch(row)
        return self._result(row, response)

    def _dispatch(self, row):
        kind, op, after = row['entity_type'], row['op'], row['after']
        if kind == 'operation':
            return self.call(f"op:{row['entity_id']}", after)
        if op == 'create':
            return self._dispatch_create(kind, after)
        if op == 'delete':
            if kind == 'project_context':
                return self.call('delete_project_context', {})
            if kind == 'skill':
                return self.call('delete_skill', {'skill_id': row['entity_id']})
            return self.call('delete_application', {'application_id': row['entity_id']})
        if kind == 'project_context':
            return self.call('put_project_context', {f: after.get(f) for f in PROJECT_CONTEXT_FIELDS})
        if kind == 'skill':
            return self._dispatch_skill_update(row)
        return self._dispatch_application_update(row)

    def _dispatch_create(self, kind, fields):
        if kind == 'project_context':
            return self.call('put_project_context', {f: fields.get(f) for f in PROJECT_CONTEXT_FIELDS
                                                     if f in fields})
        if kind == 'skill':
            version = {'name': 'base', **{f: fields[f] for f in SKILL_VERSION_FIELDS if f in fields}}
            return self.call('create_skill', {**{f: fields[f] for f in SKILL_LEVEL_FIELDS if f in fields},
                                              'versions': [version]})
        version = {'name': 'base', **{f: fields[f] for f in VERSION_FIELDS if f in fields}}
        if kind == 'pipeline':
            version['agent_type'] = 'pipeline'
        return self.call('create_application', {**{f: fields[f] for f in APPLICATION_LEVEL_FIELDS if f in fields},
                                                'versions': [version]})

    def _dispatch_skill_update(self, row):
        after, fields = row['after'], row['fields']
        payload = {'skill_id': row['entity_id'], **{f: after[f] for f in fields if f in SKILL_LEVEL_FIELDS}}
        version_fields = {f: after[f] for f in fields if f in SKILL_VERSION_FIELDS}
        if version_fields:
            payload['version'] = {'id': row['version_id'], **version_fields}
        return self.call('update_skill', payload)

    def _dispatch_application_update(self, row):
        """Application fields, then version settings, then instructions: the safe patch (with its backup) runs
        last, so a refused settings write leaves the instructions as they were."""
        after, fields = row['after'], row['fields']
        responses = []
        app_fields = {f: after[f] for f in fields if f in APPLICATION_LEVEL_FIELDS}
        if app_fields:
            responses.append(self.call('update_application', {'application_id': row['entity_id'], **app_fields}))
        settings = {f: after[f] for f in fields if f in VERSION_FIELDS and f != 'instructions'}
        if settings and not self._failed(responses):
            responses.append(self.call('update_version', {'application_id': row['entity_id'],
                                                          'version_id': row['version_id'], **settings}))
        if 'instructions' in fields and not self._failed(responses):
            responses.append(self.call('patch_instructions', {
                'application_id': row['entity_id'], 'version_id': row['version_id'],
                'expected_instructions_sha256': row['raw_sha'], 'old_text': row['before'].get('instructions') or None,
                'replacement': after['instructions'], 'replace_all': not row['before'].get('instructions'),
            }))
        merged = {}
        for response in responses:
            merged.update(response or {})
        if app_fields and isinstance(responses[0], dict) and responses[0].get('name'):
            merged['name'] = responses[0]['name']
        elif len(responses) > 1 or not app_fields:
            merged.pop('name', None)
        return merged

    @staticmethod
    def _failed(responses):
        return any(isinstance(r, dict) and r.get('error') for r in responses)

    def _describe(self, row):
        verb = {'create': 'Create', 'update': 'Update', 'delete': 'Delete', 'call': 'Run'}[row['op']]
        label = 'operation' if row['entity_type'] == 'operation' else row['entity_type'].replace('_', ' ')
        name = row.get('name')
        return f'{verb} {label} "{name}"' if name else f'{verb} {label}'

    def _result(self, row, response):
        response = response if isinstance(response, dict) else {}
        if response.get('error'):
            return {'status': 'error', 'message': str(response['error'])}
        verb = {'create': 'Created', 'update': 'Updated', 'delete': 'Deleted', 'call': 'Ran'}[row['op']]
        label = 'operation' if row['entity_type'] == 'operation' else row['entity_type'].replace('_', ' ')
        name = response.get('name') or row.get('name')
        lines = [f'**{verb} {label} "{name}"**' if name else f'**{verb} {label}**']
        for entry in cs.diff(row['before'], row['after']) if row['before'] and row['after'] else []:
            if 'added_lines' in entry:
                lines.append(f"- {entry['field']}: +{len(entry['added_lines'])} / -{len(entry['removed_lines'])} lines")
            else:
                lines.append(f"- {entry['field']}: {entry['before']} → {entry['after']}")
        backup = (response.get('mcp_backup_version') or {}).get('name')
        if backup:
            lines.append(f'- Backup: `{backup}`')
        result = {'status': 'applied', 'summary': '\n'.join(lines), 'result': masked(response)}
        if response.get('governance_requests'):
            result['governance_requests'] = response['governance_requests']
        if backup:
            result['backup'] = backup
        if row['entity_type'] in ('agent', 'pipeline') and row['op'] != 'delete':
            created_version = (response.get('version_details') or {}).get('id')
            if created_version is None and response.get('versions'):
                created_version = response['versions'][0].get('id')
            entity_id = row['entity_id'] if row['op'] != 'create' else response.get('id')
            result['chip'] = {'type': row['entity_type'], 'id': entity_id,
                              'version_id': created_version or row.get('version_id'), 'name': name}
        elif row['op'] == 'call':
            chip = self._linked_entity_chip(row['after'] or {})
            if chip:
                result['chip'] = chip
        return result

    def _version_mismatch(self, args):
        """A link names the agent (entity_id) and one of its versions (entity_version_id); both must match."""
        entity_id, version_id = args.get('entity_id'), args.get('entity_version_id')
        if args.get('entity_type') not in ('agent', 'pipeline', 'application') or not entity_id or not version_id:
            return None
        app = self.call('get_application', {'application_id': entity_id})
        versions = (app or {}).get('versions') if isinstance(app, dict) and not app.get('error') else None
        if versions is None:
            return f'Agent {entity_id} not found; entity_id is the agent id from builder_find, not a version id.'
        if version_id not in {v.get('id') for v in versions}:
            return (f'Version {version_id} does not belong to agent {entity_id}; read the agent and use its '
                    f'version_id as entity_version_id.')
        return None

    def _linked_entity_chip(self, args):
        """An operation that changed an agent or pipeline version (e.g. linking a toolkit) refreshes its editor."""
        entity_type = args.get('entity_type')
        entity_id, version_id = args.get('entity_id'), args.get('entity_version_id')
        if entity_type not in ('agent', 'pipeline', 'application') or not entity_id:
            return None
        app = self.call('get_application', {'application_id': entity_id}) or {}
        if not isinstance(app, dict) or app.get('error'):
            return None
        agent_type = (app.get('version_details') or {}).get('agent_type')
        return {'type': 'pipeline' if agent_type == 'pipeline' else 'agent', 'id': entity_id,
                'version_id': version_id or (app.get('version_details') or {}).get('id'),
                'name': app.get('name') or ''}
