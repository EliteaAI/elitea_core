"""Static validation of a pipeline spec before it is saved or run.

``validate_pipeline`` checks a pipeline (YAML/JSON text or a parsed dict) against what
``create_graph`` and the node implementations actually do, and returns findings the
author or an LLM can act on: each names the node, the field, the problem and a fix.

Node types, their status and the fields they need come from ``node_registry.yaml``;
everything else here is behaviour the runtime has but does not report, so a pipeline
that passes these checks still builds and does not silently lose data. Rules R1-R12 are
carried over from the ELITEA Agents Development Kit validator (same ids and logic);
R20+ are new or registry-driven.

Like ``registry.py`` this module imports nothing else from the SDK, so the platform can
keep a byte-identical copy.
"""

import ast
import re
from typing import Any, Dict, List, Optional, Tuple

import yaml

from .registry import END, inline_construct, node_spec, node_types, required_paths

# State channels create_graph always defines; never "undeclared", never "unused".
RUNTIME_STATE_VARS = frozenset({'input', 'messages', 'router_output'})
MAPPING_TYPES = ('fixed', 'variable', 'fstring')
HITL_ACTIONS = ('approve', 'reject', 'edit')
TRIGGERS_WITHOUT_A_PERSON = frozenset({'schedule', 'scheduled', 'webhook'})

# Node-level fields that belong next to input_mapping; one appearing inside it is an
# indentation slip (and crashes the runtime when it is not a {type, value} block).
_NODE_LEVEL_FIELDS = frozenset({
    'id', 'type', 'input', 'output', 'transition', 'structured_output', 'routes',
    'default_output', 'final_message', 'condition', 'template', 'code', 'user_message',
    'edit_state_key', 'tool', 'toolkit_name', 'variables_to_clean', 'nodes',
})
_FSTRING_PLACEHOLDER = re.compile(r'(?<!\{)\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}(?!\})')
_STATE_READ = re.compile(r"\b(?:elitea_state|alita_state)\s*(?:\.get\s*\(|\[)\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]")


class ValidationContext:
    """What the validator can check only when the caller knows it.

    ``trigger``: how the pipeline is started (``chat``, ``schedule``, ``webhook``).
    ``tool_schemas``: ``{toolkit_name: {tool_name: json_schema}}`` for attached toolkits.
    """

    def __init__(self, trigger: Optional[str] = None,
                 tool_schemas: Optional[Dict[str, Dict[str, dict]]] = None):
        self.trigger = (trigger or '').strip().lower() or None
        self.tool_schemas = tool_schemas or {}


class _Findings:
    def __init__(self):
        self.errors: List[dict] = []
        self.warnings: List[dict] = []

    def add(self, severity: str, rule: str, message: str, suggestion: str = '',
            node_id: Optional[str] = None, field: Optional[str] = None) -> None:
        finding = {'rule': rule, 'node_id': node_id, 'field': field,
                   'message': message, 'suggestion': suggestion}
        (self.errors if severity == 'error' else self.warnings).append(finding)

    def error(self, *args, **kwargs):
        self.add('error', *args, **kwargs)

    def warning(self, *args, **kwargs):
        self.add('warning', *args, **kwargs)


def validate_pipeline(spec: Any, context: Optional[ValidationContext] = None) -> dict:
    """Check a pipeline spec; never raises.

    Returns ``{'ok': bool, 'errors': [...], 'warnings': [...]}``; ``ok`` is True when there
    are no errors. Each finding is ``{rule, node_id, field, message, suggestion}``.
    """
    findings = _Findings()
    context = context or ValidationContext()
    doc = _parse(spec, findings)
    if doc is None:
        return _result(findings)
    try:
        _run_rules(doc, context, findings)
    except Exception as exc:  # a validator bug must not block a save with a traceback
        findings.warning('R00', f'validator could not finish: {type(exc).__name__}: {exc}',
                         'Report this; the findings above are still valid.')
    return _result(findings)


def _result(findings: _Findings) -> dict:
    return {'ok': not findings.errors, 'errors': findings.errors, 'warnings': findings.warnings}


# ---- parsing and shape (R20) -------------------------------------------------------------

def _parse(spec: Any, findings: _Findings) -> Optional[dict]:
    if isinstance(spec, (str, bytes)):
        try:
            doc = yaml.safe_load(spec)
        except yaml.YAMLError as exc:
            findings.error('R20', f'The pipeline is not valid YAML/JSON: {exc}',
                           'Fix the syntax; JSON is valid YAML, so either format works.')
            return None
    else:
        doc = spec
    if not isinstance(doc, dict):
        findings.error('R20', 'The pipeline must be a mapping with `entry_point` and `nodes`.',
                       'Start from: entry_point: <first node id>\\nnodes:\\n  - id: ...')
        return None
    nodes = doc.get('nodes')
    if not isinstance(nodes, list) or not nodes:
        findings.error('R20', '`nodes` must be a non-empty list.', 'Add at least one node.', field='nodes')
        return None
    state = doc.get('state')
    if state is not None and not isinstance(state, dict):
        findings.error('R20', '`state` must be a mapping of variable name to {type, value}.',
                       'Example: state:\\n  summary: {type: str, value: ""}', field='state')
        return None
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            findings.error('R20', f'nodes[{index}] is not a mapping.',
                           'Each node is a mapping with at least `id` and `type`.', field=f'nodes[{index}]')
            return None
    return doc


def _node_id(node: dict, index: int) -> str:
    value = node.get('id')
    return value.strip() if isinstance(value, str) and value.strip() else f'nodes[{index}]'


def _is_end(value: Any) -> bool:
    return isinstance(value, str) and value.strip().upper() == END


def _as_list(value: Any) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _get_path(node: dict, path: str) -> Any:
    current: Any = node
    for part in path.split('.'):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    if isinstance(current, dict) and set(current) >= {'type', 'value'} and current.get('type') in MAPPING_TYPES:
        return current.get('value')
    return current


def _is_filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _mapping_entries(node: dict) -> List[Tuple[str, Any]]:
    mapping = node.get('input_mapping')
    if isinstance(mapping, dict):
        return list(mapping.items())
    if isinstance(mapping, list):
        return [(entry.get('name', str(i)) if isinstance(entry, dict) else str(i), entry)
                for i, entry in enumerate(mapping)]
    return []


def _code_source(node: dict) -> str:
    block = node.get('code')
    if isinstance(block, dict) and isinstance(block.get('value'), str):
        return block['value']
    return ''


# ---- driver --------------------------------------------------------------------------------

def _run_rules(doc: dict, context: ValidationContext, findings: _Findings) -> None:
    nodes: List[dict] = doc['nodes']
    state: dict = doc.get('state') or {}
    ids = [_node_id(n, i) for i, n in enumerate(nodes)]
    by_id = {nid: node for nid, node in zip(ids, nodes)}

    _r21_unique_ids(nodes, ids, findings)
    _r22_entry_point(doc, by_id, findings)
    for nid, node in zip(ids, nodes):
        known = _r23_node_status(nid, node, findings)
        if not known:
            continue
        _r24_required_fields(nid, node, findings)
        _r27_code_shape(nid, node, findings)
        _r28_code_reads_unlisted_state(nid, node, findings)
        _r33_fstring_inputs(nid, node, findings)
        _r35_hitl_routes(nid, node, state, findings)
        _r36_tool_parameters(nid, node, context, findings)
        _r31_blocks_unattended(nid, node, context, findings)
    _r2_route_targets(doc, nodes, ids, findings)
    _r25_reachability(doc, nodes, ids, findings)
    _r26_custom_state(state, findings)
    _r4_declared_state(state, nodes, ids, findings)
    _r1_unused_state(state, nodes, findings)
    _r5_mapping_blocks(nodes, ids, findings)
    _r6_structured_llm_output(nodes, ids, findings)
    _r7_code_result_captured(nodes, ids, findings)
    _r12_state_modifier_single_output(nodes, ids, findings)
    _r29_messages_discipline(nodes, ids, findings)


# ---- R21-R26: graph and registry -------------------------------------------------------------

def _r21_unique_ids(nodes, ids, findings):
    seen = set()
    for index, node in enumerate(nodes):
        raw = node.get('id')
        if not isinstance(raw, str) or not raw.strip():
            findings.error('R21', f'nodes[{index}] has no `id`.', 'Give every node a unique string id.',
                           field=f'nodes[{index}].id')
            continue
        if ids[index] in seen:
            findings.error('R21', f'Node id `{ids[index]}` is used more than once.',
                           'Node ids must be unique; rename one of them and update the transitions.',
                           node_id=ids[index], field='id')
        seen.add(ids[index])


def _r22_entry_point(doc, by_id, findings):
    entry = doc.get('entry_point')
    first = next(iter(by_id), None)
    if not isinstance(entry, str) or not entry.strip():
        legacy = [nid for nid, node in by_id.items() if node.get('entrypoint') is True]
        hint = legacy[0] if legacy else first
        findings.error('R22', 'The pipeline has no top-level `entry_point`; the runtime refuses to build it.',
                       f'Add `entry_point: {hint}` at the top level (a node-level `entrypoint: true` is not read).',
                       field='entry_point')
        return
    if _is_end(entry):
        findings.error('R22', '`entry_point` cannot be END.', f'Set it to the first node, e.g. `{first}`.',
                       field='entry_point')
        return
    node = by_id.get(entry.strip())
    if node is None:
        findings.error('R22', f'`entry_point` `{entry}` is not a node id.',
                       f'Set it to one of: {", ".join(list(by_id)[:10])}.', field='entry_point')
        return
    spec = node_spec(node.get('type')) or {}
    if spec.get('routing') == 'own':
        findings.error('R22', f'`entry_point` is a `{node.get("type")}` node; routing nodes cannot start a pipeline.',
                       'Start with a node that does work (llm, code, toolkit, ...) and route after it.',
                       node_id=entry.strip(), field='entry_point')


def _r23_node_status(nid, node, findings) -> bool:
    """Unknown, removed, UI-only and deprecated types. Returns whether the type is known."""
    node_type = node.get('type')
    current = ', '.join(node_types('current'))
    if not isinstance(node_type, str) or not node_type.strip():
        findings.error('R23', f'Node `{nid}` has no `type`.', f'Set `type` to one of: {current}.',
                       node_id=nid, field='type')
        return False
    if node_type.strip().lower() == 'end':
        findings.error('R23', f'Node `{nid}` is a `type: END` node; END is a transition target, not a node.',
                       'Remove this node and set `transition: END` on the nodes that pointed to it.',
                       node_id=nid, field='type')
        return False
    spec = node_spec(node_type)
    if spec is None:
        findings.error('R23', f'Node `{nid}` has unknown type `{node_type}`.', f'Use one of: {current}.',
                       node_id=nid, field='type')
        return False
    status = spec['status']
    if status in ('removed', 'ui_only'):
        what = 'is removed' if status == 'removed' else 'is an editor marker the runtime refuses to build'
        findings.error('R23', f'Node `{nid}` uses type `{node_type}`, which {what}.',
                       f'Use instead: {spec["replacement"]}.', node_id=nid, field='type')
    elif status == 'deprecated':
        findings.warning('R23', f'Node `{nid}` uses type `{node_type}`, which is deprecated.',
                         f'Prefer: {spec["replacement"]}.', node_id=nid, field='type')
    for name in ('condition', 'decision'):
        if name in node and not (name == 'condition' and node_type == 'router'):
            construct = inline_construct(name) or {}
            add = findings.error if construct.get('status') == 'removed' else findings.warning
            why = (' (the runtime accepts it but does not route on it)' if name == 'condition' else '')
            add('R23', f'Node `{nid}` has an inline `{name}:` block{why}.',
                f'Use instead: {construct.get("replacement", "a separate routing node")}.',
                node_id=nid, field=name)
    return status not in ('removed', 'ui_only')


def _r24_required_fields(nid, node, findings):
    node_type = node.get('type')
    for alternatives in required_paths(node_type):
        if any(_is_filled(_get_path(node, path)) for path in alternatives):
            continue
        names = ' or '.join(f'`{p}`' for p in alternatives)
        hint = _REQUIRED_HINTS.get((node_type, alternatives[0]), f'Set {names}.')
        if node_type in ('toolkit', 'mcp', 'agent') and alternatives == ['tool'] and _is_filled(node.get('name')):
            hint = f'The runtime reads `tool`, not `name`: set `tool: {node["name"]}`' + (
                ' and `toolkit_name` to the attached toolkit.' if node_type != 'agent' else '.')
        findings.error('R24', f'`{node_type}` node `{nid}` is missing {names}.', hint,
                       node_id=nid, field=alternatives[0])


_REQUIRED_HINTS = {
    ('llm', 'input_mapping.system'): 'Give the node a `system` and/or `task` input_mapping entry; '
                                     'without one the model gets only the chat history.',
    ('toolkit', 'tool'): 'Set `tool` to the tool name and `toolkit_name` to the attached toolkit.',
    ('mcp', 'tool'): 'Set `tool` to the MCP tool name and `toolkit_name` to the attached MCP server.',
    ('agent', 'tool'): 'Set `tool` to the name of the attached agent or pipeline.',
    ('agent', 'input_mapping.task'): 'Add `input_mapping.task`; the child sees only this text.',
    ('code', 'code.value'): 'Put the Python in `code: {type: fixed, value: |...}`.',
    ('router', 'condition'): 'Write a Jinja2 `condition` that renders one of the route ids.',
    ('router', 'routes'): 'List the possible target node ids in `routes`.',
    ('decision', 'nodes'): 'List the candidate target node ids in `nodes`.',
    ('decision', 'description'): 'Describe in `description` when to pick each target.',
    ('hitl', 'user_message'): 'Set `user_message: {type: fstring, value: ...}` to what the person reviews.',
    ('hitl', 'routes'): 'Set `routes: {approve: <id>, reject: END}` (add `edit` with `edit_state_key`).',
    ('state_modifier', 'template'): 'Write the Jinja2 `template` that produces the new value.',
    ('state_modifier', 'output'): 'Name the one state variable the template writes, e.g. `output: [summary]`.',
    ('printer', 'input_mapping.printer'): 'Set `input_mapping.printer` to the text to show.',
    ('printer', 'transition'): 'Printer nodes need `transition:` to the next node or END.',
}


def _route_targets(node: dict) -> List[Tuple[str, Any]]:
    """(field, target) for every edge the runtime builds from this node."""
    node_type = node.get('type')
    spec = node_spec(node_type) or {}
    targets: List[Tuple[str, Any]] = []
    if spec.get('routing') == 'own':
        routes = node.get('routes')
        if node_type == 'hitl':
            if isinstance(routes, dict):
                targets += [(f'routes.{k}', v) for k, v in routes.items()]
        else:
            targets += [(f'routes[{i}]', v) for i, v in enumerate(_as_list(routes))]
            if node_type == 'decision':
                targets += [(f'nodes[{i}]', v) for i, v in enumerate(_as_list(node.get('nodes')))]
            targets.append(('default_output', node.get('default_output', END)))
        return targets
    if 'transition' in node:
        targets.append(('transition', node.get('transition')))
    decision = node.get('decision')
    if isinstance(decision, dict):
        targets += [(f'decision.nodes[{i}]', v) for i, v in enumerate(_as_list(decision.get('nodes')))]
        targets.append(('decision.default_output', decision.get('default_output', END)))
    return targets


def _r2_route_targets(doc, nodes, ids, findings):
    known = set(ids)
    for nid, node in zip(ids, nodes):
        for field, target in _route_targets(node):
            if _is_end(target):
                continue
            if not isinstance(target, str) or not target.strip():
                findings.error('R2', f'Node `{nid}` has an empty or non-text target in `{field}`.',
                               'Use a node id or END.', node_id=nid, field=field)
            elif target.strip() not in known:
                # Not a build error: the run just stops after this node, without a message.
                findings.error('R2', f'Node `{nid}` points to `{target}` in `{field}`, which is not a node id; '
                                     'the run silently stops after this node.',
                               'Fix the spelling, add that node, or use END.', node_id=nid, field=field)
        if node.get('type') == 'router' and isinstance(node.get('routes'), (list, str)):
            for route in _as_list(node.get('routes')):
                if _is_end(route):
                    findings.warning('R2', f'Router `{nid}` lists END in `routes`.',
                                     'Use `default_output: END` for the exit instead.', node_id=nid, field='routes')


def _r25_reachability(doc, nodes, ids, findings):
    entry = doc.get('entry_point')
    if not isinstance(entry, str) or entry.strip() not in ids:
        return
    edges: Dict[str, List[str]] = {}
    ends: set = set()
    for nid, node in zip(ids, nodes):
        spec = node_spec(node.get('type')) or {}
        outs = [t.strip() for _, t in _route_targets(node) if isinstance(t, str)]
        if spec.get('routing') != 'own' and not any(f == 'transition' or f.startswith('decision.')
                                                    for f, _ in _route_targets(node)):
            outs.append(END)  # no transition: the runtime adds no edge, LangGraph ends the run here
        edges[nid] = [o for o in outs if not _is_end(o)]
        if any(_is_end(o) for o in outs):
            ends.add(nid)
    seen, stack = set(), [entry.strip()]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack += [n for n in edges.get(current, []) if n in edges and n not in seen]
    for nid in ids:
        if nid not in seen:
            findings.warning('R25', f'Node `{nid}` cannot be reached from `entry_point`.',
                             'Point a transition or route at it, or delete it.', node_id=nid)
    if not ends & seen:
        findings.error('R25', 'No path from `entry_point` reaches END, so the run can never finish.',
                       'Set `transition: END` on the last node of each path (and give loops an exit).')


def _r26_custom_state(state, findings):
    if state and 'messages' not in state:
        findings.warning('R26', 'The custom `state` does not declare `messages`.',
                         'Add `messages: {type: list}`; nodes that read or write chat history expect it.',
                         field='state')


# ---- R1, R4: state declarations ----------------------------------------------------------------

def _r4_declared_state(state, nodes, ids, findings):
    declared = set(state) | RUNTIME_STATE_VARS
    for nid, node in zip(ids, nodes):
        for field in ('input', 'output'):
            values = node.get(field)
            if values is None:
                continue
            if not isinstance(values, list):
                findings.error('R4', f'Node `{nid}` `{field}` must be a list of state variable names.',
                               f'Write `{field}: [{values}]`.' if isinstance(values, str) else
                               f'Write `{field}: [name, ...]`.', node_id=nid, field=field)
                continue
            if not all(isinstance(name, str) for name in values):
                findings.error('R4', f'Node `{nid}` `{field}` must be a flat list of state variable names; '
                                     'the graph build fails on objects.',
                               f'Write `{field}: [name1, name2]`, not a list of objects.', node_id=nid, field=field)
                continue
            for name in values:
                if name not in declared:
                    findings.error('R4', f'Node `{nid}` uses `{name}` in `{field}`, which is not declared in `state`.',
                                   f'Add `{name}: {{type: str}}` (or its real type) under `state`.',
                                   node_id=nid, field=field)
        for key, entry in _mapping_entries(node):
            if isinstance(entry, dict) and entry.get('type') == 'variable':
                name = entry.get('value')
                if isinstance(name, str) and name and name not in declared:
                    findings.error('R4', f'Node `{nid}` maps `{key}` to state variable `{name}`, which is not declared.',
                                   f'Declare `{name}` under `state`, or use `fixed`/`fstring`.',
                                   node_id=nid, field=f'input_mapping.{key}')


def _r1_unused_state(state, nodes, findings):
    if not state:
        return
    used = set()
    for node in nodes:
        used.update(v for v in _as_list(node.get('input')) if isinstance(v, str))
        used.update(v for v in _as_list(node.get('output')) if isinstance(v, str))
        used.update(v for v in _as_list(node.get('variables_to_clean')) if isinstance(v, str))
        if isinstance(node.get('edit_state_key'), str):
            used.add(node['edit_state_key'])
        texts = [node.get('condition'), node.get('template'), _code_source(node)]
        for _, entry in _mapping_entries(node):
            if isinstance(entry, dict):
                if entry.get('type') == 'variable':
                    used.add(entry.get('value'))
                texts.append(entry.get('value'))
        user_message = node.get('user_message')
        if isinstance(user_message, dict):
            texts.append(user_message.get('value'))
        for text in texts:
            if isinstance(text, str):
                used.update(name for name in state if re.search(rf'\b{re.escape(name)}\b', text))
    for name in sorted(set(state) - used - RUNTIME_STATE_VARS):
        findings.warning('R1', f'State variable `{name}` is declared but no node uses it.',
                         'Remove it, or wire it into the node that should read or write it.', field=f'state.{name}')


# ---- R5, R6, R7, R12, R27-R29, R33: node content ---------------------------------------------------

def _r5_mapping_blocks(nodes, ids, findings):
    for nid, node in zip(ids, nodes):
        block = node.get('code')
        if node.get('type') == 'code' and block is not None:
            if not isinstance(block, dict):
                findings.error('R5', f'Code node `{nid}`: `code` must be a block, not a bare string.',
                               'Write `code: {type: fixed, value: |\\n  <python>}`.', node_id=nid, field='code')
            else:
                if block.get('type') not in MAPPING_TYPES:
                    findings.error('R5', f'Code node `{nid}`: `code.type` is missing or invalid; '
                                         'the graph build fails with KeyError: \'type\'.',
                                   'Add `type: fixed`.', node_id=nid, field='code.type')
        mapping = node.get('input_mapping')
        if mapping is not None and not isinstance(mapping, (dict, list)):
            findings.error('R5', f'Node `{nid}`: `input_mapping` must be a mapping of name to {{type, value}}.',
                           'Example: input_mapping:\\n  task: {type: fstring, value: "..."}',
                           node_id=nid, field='input_mapping')
            continue
        for key, entry in _mapping_entries(node):
            if not isinstance(entry, dict):
                hint = (f'`{key}` is a node-level field; move it out of input_mapping (indentation).'
                        if key in _NODE_LEVEL_FIELDS else f'Write `{key}: {{type: fixed, value: ...}}`.')
                findings.error('R5', f'Node `{nid}`: input_mapping `{key}` is not a {{type, value}} block.',
                               hint, node_id=nid, field=f'input_mapping.{key}')
                continue
            if entry.get('type') not in MAPPING_TYPES:
                findings.error('R5', f'Node `{nid}`: input_mapping `{key}` has type `{entry.get("type")}`.',
                               'Use fixed, variable or fstring.', node_id=nid, field=f'input_mapping.{key}.type')
            if 'value' not in entry:
                findings.error('R5', f'Node `{nid}`: input_mapping `{key}` has no `value`.',
                               'Add `value:`.', node_id=nid, field=f'input_mapping.{key}.value')


def _r6_structured_llm_output(nodes, ids, findings):
    for nid, node in zip(ids, nodes):
        if node.get('type') != 'llm' or not node.get('structured_output'):
            continue
        for name in _as_list(node.get('output')):
            if name in ('messages', 'input'):
                findings.error('R6', f'Structured llm node `{nid}` lists `{name}` in `output`; the model never '
                                     'returns it, so the whole structured result is dropped.',
                               f'Remove `{name}` from `output`; add a final node to write the answer to messages.',
                               node_id=nid, field='output')


def _r27_code_shape(nid, node, findings):
    if node.get('type') != 'code':
        return
    source = _code_source(node)
    if not source.strip():
        return
    try:
        # compile, not just parse: `return` outside a function parses but does not compile
        compile(source, f'<code node {nid}>', 'exec', flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT, dont_inherit=True)
    except SyntaxError as exc:
        hint = ('The code runs as a module, so `return` is not allowed at top level; end with the '
                'result dict as a bare expression, e.g. `{"summary": summary}`.'
                if 'return' in str(exc.msg) else 'Fix the Python syntax.')
        findings.error('R27', f'Code node `{nid}` has a syntax error: {exc.msg} (line {exc.lineno}).',
                       hint, node_id=nid, field='code.value')
        return
    if node.get('structured_output') and not isinstance(node.get('output'), list):
        findings.warning('R27', f'Structured code node `{nid}` has no `output` list.',
                         'List the keys of the returned dict in `output` so they land in state.',
                         node_id=nid, field='output')


def _r28_code_reads_unlisted_state(nid, node, findings):
    if node.get('type') != 'code':
        return
    inputs = node.get('input')
    if not isinstance(inputs, list) or inputs in ([], ['messages']):
        return  # the runtime injects every state variable in this case
    for name in sorted(set(_STATE_READ.findall(_code_source(node)))):
        if name not in inputs and name != 'messages':
            findings.warning('R28', f'Code node `{nid}` reads `{name}` from elitea_state but `{name}` is not '
                                    'in its `input`, so it always gets the default.',
                             f'Add `{name}` to `input`.', node_id=nid, field='input')


def _r33_fstring_inputs(nid, node, findings):
    inputs = node.get('input')
    if not isinstance(inputs, list):
        return
    for key, entry in _mapping_entries(node):
        if not (isinstance(entry, dict) and entry.get('type') == 'fstring' and isinstance(entry.get('value'), str)):
            continue
        for name in sorted(set(_FSTRING_PLACEHOLDER.findall(entry['value']))):
            if name not in inputs:
                findings.warning('R33', f'Node `{nid}` uses `{{{name}}}` in input_mapping `{key}` but `{name}` is '
                                        'not in its `input`.',
                                 f'Add `{name}` to `input` so the node declares what it reads.',
                                 node_id=nid, field=f'input_mapping.{key}')


def _r35_hitl_routes(nid, node, state, findings):
    if node.get('type') != 'hitl':
        return
    routes = node.get('routes')
    if routes is not None and not isinstance(routes, dict):
        findings.error('R35', f'HITL node `{nid}`: `routes` must map actions to node ids.',
                       'Write `routes: {approve: <id>, reject: END, edit: <id>}`.', node_id=nid, field='routes')
        return
    routes = routes or {}
    for action in routes:
        if action not in HITL_ACTIONS:
            findings.error('R35', f'HITL node `{nid}` has unknown action `{action}`.',
                           f'Use only: {", ".join(HITL_ACTIONS)}.', node_id=nid, field=f'routes.{action}')
    if routes and not routes.get('approve'):
        findings.warning('R35', f'HITL node `{nid}` has no `approve` route, so approving is not offered.',
                         'Add `approve: <next node id>`.', node_id=nid, field='routes.approve')
    if routes.get('edit'):
        key = node.get('edit_state_key')
        if not key:
            findings.error('R35', f'HITL node `{nid}` has an `edit` route but no `edit_state_key`; '
                                  'the edit action fails at run time.',
                           'Set `edit_state_key` to the state variable the person edits.',
                           node_id=nid, field='edit_state_key')
        elif state and key not in state and key not in RUNTIME_STATE_VARS:
            findings.error('R35', f'HITL node `{nid}` edits `{key}`, which is not declared in `state`.',
                           f'Declare `{key}` under `state`.', node_id=nid, field='edit_state_key')
        if _is_end(routes.get('edit')):
            findings.error('R35', f'HITL node `{nid}` routes `edit` to END; the edited value would be discarded.',
                           'Route `edit` to the node that should use the edited value.',
                           node_id=nid, field='routes.edit')


def _r36_tool_parameters(nid, node, context, findings):
    if node.get('type') not in ('toolkit', 'mcp') or not context.tool_schemas:
        return
    toolkit = node.get('toolkit_name')
    tool = node.get('tool')
    schema = (context.tool_schemas.get(toolkit) or {}).get(tool) if toolkit else None
    if schema is None:
        candidates = [tools[tool] for tools in context.tool_schemas.values() if tool in tools]
        schema = candidates[0] if len(candidates) == 1 else None
    if not isinstance(schema, dict):
        return
    properties = set((schema.get('properties') or {}).keys())
    required = set(schema.get('required') or [])
    mapped = {key for key, _ in _mapping_entries(node)}
    for key in sorted(mapped - properties):
        if properties:
            findings.warning('R36', f'Node `{nid}` maps `{key}`, which tool `{tool}` does not accept.',
                             f'Parameters of `{tool}`: {", ".join(sorted(properties))}.',
                             node_id=nid, field=f'input_mapping.{key}')
    for key in sorted(required - mapped):
        findings.error('R36', f'Node `{nid}` does not set required parameter `{key}` of tool `{tool}`.',
                       f'Add `input_mapping.{key}`.', node_id=nid, field='input_mapping')


def _r31_blocks_unattended(nid, node, context, findings):
    spec = node_spec(node.get('type')) or {}
    if spec.get('blocks_unattended') and context.trigger in TRIGGERS_WITHOUT_A_PERSON:
        findings.error('R31', f'`{node.get("type")}` node `{nid}` waits for a person, but this pipeline runs '
                              f'on a {context.trigger} trigger, so the run never finishes.',
                       'Remove it, or move the human step to a pipeline started from chat.', node_id=nid)


def _r12_state_modifier_single_output(nodes, ids, findings):
    for nid, node in zip(ids, nodes):
        output = node.get('output')
        if node.get('type') == 'state_modifier' and isinstance(output, list) and len(output) > 1:
            findings.error('R12', f'state_modifier `{nid}` lists {len(output)} outputs; only `{output[0]}` is '
                                  'written and the rest stay empty.',
                           'Keep one output; use a code node to set several variables.', node_id=nid, field='output')


def _r29_messages_discipline(nodes, ids, findings):
    writers = [nid for nid, node in zip(ids, nodes)
               if 'messages' in _as_list(node.get('output')) and node.get('type') != 'agent']
    if len(writers) > 2:
        findings.warning('R29', f'{len(writers)} nodes write to `messages` ({", ".join(writers)}); every later '
                                'llm/agent call re-sends that history, which makes the pipeline slow and costly.',
                         'Pass intermediate results through typed state variables; write `messages` only in '
                         'the node that produces the final answer.')


# ---- R7: the code node's result must be what the sandbox captures ------------------------------------
# The sandbox captures the value of the last top-level expression. These detectors fire only
# where that value provably is not the result dict; anything unknown passes, because a false
# positive blocks a pipeline that works.

_NON_DICT_BUILTINS = frozenset({
    'len', 'sum', 'int', 'str', 'float', 'bool', 'sorted', 'list', 'tuple', 'set', 'min', 'max',
    'any', 'all', 'round', 'abs', 'print', 'repr', 'enumerate', 'range', 'type', 'id', 'format',
})
_NON_DICT_LITERALS = (ast.Constant, ast.List, ast.Tuple, ast.Set, ast.JoinedStr)
_NON_DICT_EXPRESSIONS = (ast.Constant, ast.List, ast.Tuple, ast.Set, ast.ListComp, ast.SetComp,
                         ast.GeneratorExp, ast.Compare, ast.JoinedStr)
_R7_FIX = ('The sandbox keeps only the value of the last top-level expression. Build the result at top '
           'level and end with it, e.g. `result = {...}` on every branch, then a final line `result`.')


def _bound_names(tree) -> set:
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            names.add(node.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update((alias.asname or alias.name).split('.')[0] for alias in node.names)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
    return names


def _names_bound_only_to_non_dicts(tree) -> set:
    bindings: Dict[str, list] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bindings.setdefault(target.id, []).append(node.value)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)) and isinstance(node.target, ast.Name):
            bindings.setdefault(node.target.id, []).append(getattr(node, 'value', None))
        elif isinstance(node, (ast.For, ast.comprehension)) and isinstance(getattr(node, 'target', None), ast.Name):
            bindings.setdefault(node.target.id, []).append(None)
    return {name for name, values in bindings.items()
            if values and all(isinstance(v, _NON_DICT_LITERALS) for v in values)}


def _r7_problem(tree) -> Optional[str]:
    body = [s for s in tree.body if not isinstance(s, (ast.Import, ast.ImportFrom))]
    if not body or not any(isinstance(n, ast.Dict) for n in ast.walk(tree)):
        return None
    last = body[-1]
    if not isinstance(last, ast.Expr):
        return (f'the last top-level statement is a `{type(last).__name__}` statement, so nothing is captured, '
                'although the code builds a dict')
    value = last.value
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) \
            and value.func.id in _NON_DICT_BUILTINS and value.func.id not in _bound_names(tree):
        return f'the code ends with `{value.func.id}(...)`, which cannot return the result dict'
    if isinstance(value, ast.Name):
        if value.id in _names_bound_only_to_non_dicts(tree):
            return f'the code ends with `{value.id}`, which only ever holds a non-dict value'
        return None
    if isinstance(value, _NON_DICT_EXPRESSIONS):
        return f'the code ends with a `{type(value).__name__}` expression, which cannot be a dict'
    return None


def _r7_code_result_captured(nodes, ids, findings):
    for nid, node in zip(ids, nodes):
        if node.get('type') != 'code' or not node.get('structured_output'):
            continue
        try:
            tree = ast.parse(_code_source(node))
            compile(tree, '<code>', 'exec', flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT, dont_inherit=True)
        except SyntaxError:
            continue  # reported by R27
        problem = _r7_problem(tree)
        if problem:
            findings.error('R7', f'Structured code node `{nid}`: {problem}, so its outputs never reach state.',
                           _R7_FIX, node_id=nid, field='code.value')
