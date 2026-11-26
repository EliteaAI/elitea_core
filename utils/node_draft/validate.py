"""Turn the model's answer into a node the editor can apply, or say why it can't.

Blocking problems raise :class:`DraftInvalid` (the caller repairs once, then answers 422).
Fixable ones (a reference that doesn't exist, an unknown key) are removed and reported as warnings.
"""
import re
from functools import lru_cache
from typing import List, Optional, Tuple

import jinja2

from ...models.pd.node_draft import LIMITS, STATE_TYPES, DraftTarget, GeneratePipelineNodeDraftResponse
from ..pipeline_authoring import validate_pipeline
from ..pipeline_authoring.registry import END, required_paths
from ..pipeline_authoring.validator import MAPPING_TYPES
from .merge import changed_paths, merge_patch
from .schema import apply_defaults, drop_unknown

_STATE_NAME = re.compile(r'^[a-z][a-z0-9_]*$')
_PLACEHOLDER = re.compile(r'\{([A-Za-z_][A-Za-z0-9_]*)\}')
# Required registry paths that are references, not text: they may stay empty with a warning
_REFERENCE_KEYS = frozenset({'tool', 'routes', 'nodes', 'transition', 'output'})
_JINJA_FIELDS = {'router': 'condition', 'state_modifier': 'template'}
_STUB_RULES = ('R27', 'R35')
_JINJA = jinja2.Environment()


@lru_cache(maxsize=None)
def required_text(node_type: str) -> Tuple[Tuple[str, ...], ...]:
    """Registry-required text the model can always write (blocking when empty)."""
    return _required_groups(node_type)[0]


@lru_cache(maxsize=None)
def required_references(node_type: str) -> Tuple[Tuple[str, ...], ...]:
    """Registry-required references (tool, routes, ...): left empty, they only get a warning."""
    return _required_groups(node_type)[1]


@lru_cache(maxsize=None)
def _required_groups(node_type: str):
    text, references = [], []
    for alternatives in required_paths(node_type):
        paths = tuple(p for p in alternatives if p.split('.')[0] not in _REFERENCE_KEYS)
        refs = tuple(p for p in alternatives if p.split('.')[0] in _REFERENCE_KEYS)
        if paths:
            text.append(paths)
        elif refs:
            references.append(refs)
    return tuple(text), tuple(references)


class DraftInvalid(Exception):
    def __init__(self, details: List[dict]):
        self.details = details[:LIMITS.max_details]
        super().__init__('; '.join(f"{d['path']}: {d['message']}" for d in self.details))


def finalize(parsed, target: DraftTarget) -> GeneratePipelineNodeDraftResponse:
    if not isinstance(parsed, dict) or not isinstance(parsed.get('node'), dict):
        raise DraftInvalid([{'path': 'node', 'message': 'Return one JSON object with a "node" object.'}])
    warnings = [w for w in parsed.get('warnings') or [] if isinstance(w, str) and w.strip()]

    patch, _dropped = drop_unknown(target.node_type, parsed['node'])
    errors = _shape_errors(patch)
    if errors:
        raise DraftInvalid(errors)

    if target.mode == 'edit':
        node = merge_patch(target.base or target.current, patch)
    else:
        node = apply_defaults(target.node_type, patch)
    node = {'id': target.node_id, 'type': target.node_type,
            **{k: v for k, v in node.items() if k not in ('id', 'type')}}

    errors = _shape_errors(node)
    if errors:
        raise DraftInvalid(errors)

    errors = _content_errors(node)
    if errors:
        raise DraftInvalid(errors)

    # Before the repairs: a reference they clear already gets its own warning
    warnings.extend(_empty_reference_warnings(node))
    _repair_targets(node, target, warnings)
    attachments = _repair_resources(node, target, warnings)
    new_state = _new_state(parsed.get('new_state'), node, target, warnings)

    errors = _stub_pipeline_errors(node, target, new_state)
    if errors:
        raise DraftInvalid(errors)

    summary = parsed.get('summary') if isinstance(parsed.get('summary'), str) else ''
    return GeneratePipelineNodeDraftResponse(
        summary=summary.strip()[:LIMITS.summary_limit],
        node=node,
        changed_paths=changed_paths(target.current, node),
        attachments=attachments,
        new_state=new_state,
        warnings=list(dict.fromkeys(warnings)),
    )


# ---- shape -----------------------------------------------------------------------------------

def _value_objects(node: dict):
    for key, entry in (node.get('input_mapping') or {}).items() if isinstance(node.get('input_mapping'), dict) else ():
        yield f'input_mapping.{key}', entry
    for key in ('user_message', 'code'):
        if key in node:
            yield key, node[key]


def _shape_errors(node: dict) -> List[dict]:
    errors = []
    if 'input_mapping' in node and node['input_mapping'] is not None and not isinstance(node['input_mapping'], dict):
        errors.append(_detail('input_mapping', 'must be an object of value objects'))
    for key in ('input', 'output', 'variables_to_clean', 'nodes'):
        if node.get(key) is not None and not _is_str_list(node[key]):
            errors.append(_detail(key, 'must be a list of names'))
    for key in ('tool', 'toolkit_name', 'transition', 'default_output', 'edit_state_key', 'condition',
                'template', 'description', 'final_message'):
        if node.get(key) is not None and not isinstance(node[key], str):
            errors.append(_detail(key, 'must be text'))
    tool_names = node.get('tool_names')
    if tool_names is not None and not (isinstance(tool_names, dict)
                                       and all(isinstance(k, str) and _is_str_list(v) for k, v in tool_names.items())):
        errors.append(_detail('tool_names', 'must be an object of toolkit name to a list of tool names'))
    routes = node.get('routes')
    if isinstance(routes, dict) and not all(isinstance(v, str) for v in routes.values()):
        errors.append(_detail('routes', 'route targets must be node ids'))
    for path, entry in _value_objects(node):
        if entry is None:
            continue
        if not isinstance(entry, dict) or 'value' not in entry:
            errors.append(_detail(path, 'must be {"type": "fixed|variable|fstring", "value": ...}'))
        elif entry.get('type') not in MAPPING_TYPES:
            errors.append(_detail(f'{path}.type', f'must be one of {", ".join(MAPPING_TYPES)}'))
        elif entry['type'] != 'fixed' and not isinstance(entry['value'], str):
            errors.append(_detail(f'{path}.value', f'a {entry["type"]} value must be text'))
    return errors


def _content_errors(node: dict) -> List[dict]:
    errors = []
    for alternatives in required_text(node['type']):
        if not any(_has_text(_value_text(_get(node, path))) for path in alternatives):
            errors.append(_detail(alternatives[0] if len(alternatives) == 1 else ' or '.join(alternatives),
                                  'is required and must not be empty'))
    # A printer has no `input`: the runtime formats its fstring over the whole state (propagate_the_input_mapping)
    inputs = None if node['type'] == 'printer' else (node.get('input') if isinstance(node.get('input'), list) else [])
    for path, entry in _value_objects(node):
        if isinstance(entry, dict) and entry.get('type') == 'fstring' and isinstance(entry.get('value'), str):
            errors.extend(_fstring_errors(path, entry['value'], inputs))
    jinja_key = _JINJA_FIELDS.get(node['type'])
    if jinja_key and _has_text(node.get(jinja_key)):
        try:
            _JINJA.parse(node[jinja_key])
        except jinja2.TemplateSyntaxError as exc:
            errors.append(_detail(jinja_key, f'is not a valid Jinja2 template: {exc.message}'))
    return errors


def _empty_reference_warnings(node: dict) -> List[str]:
    warnings = []
    for alternatives in required_references(node['type']):
        if not any(_has_reference(_get(node, path)) for path in alternatives):
            warnings.append(f"`{' or '.join(alternatives)}` is required but empty. Set it before running the pipeline.")
    return warnings


def _fstring_errors(path: str, text: str, inputs: Optional[List[str]]) -> List[dict]:
    errors = []
    stripped = text.replace('{{', '').replace('}}', '')
    names = _PLACEHOLDER.findall(stripped)
    if '{' in _PLACEHOLDER.sub('', stripped) or '}' in _PLACEHOLDER.sub('', stripped):
        errors.append(_detail(f'{path}.value', 'has a literal brace; write it as {{ or }}, or use type fixed'))
    for name in sorted(set(names) - set(inputs)) if inputs is not None else ():
        errors.append(_detail(f'{path}.value', f'uses {{{name}}} but "{name}" is not in input'))
    return errors


def _stub_pipeline_errors(node: dict, target: DraftTarget, new_state: List[dict]) -> List[dict]:
    """Per-node runtime rules (code syntax, HITL edit route) from the shared pipeline validator."""
    state = {name: {'type': kind} for name, kind in target.state.items()}
    state.update({v['name']: {'type': v['type']} for v in new_state})
    stubs = [{'id': nid, 'type': 'printer', 'input_mapping': {'printer': {'type': 'fixed', 'value': '.'}},
              'transition': END} for nid in sorted(target.node_ids - {END, target.node_id})]
    result = validate_pipeline({'entry_point': target.node_id, 'state': state, 'nodes': [node, *stubs]})
    return [_detail(f['field'] or 'node', f['message']) for f in result['errors']
            if f.get('node_id') == target.node_id and f.get('rule') in _STUB_RULES]


# ---- references (fixable) ---------------------------------------------------------------------

def _missing_target(name: str) -> str:
    return f"Route target `{name}` doesn't exist. Add the node, or pick another target."


def _repair_targets(node: dict, target: DraftTarget, warnings: List[str]) -> None:
    def valid(name) -> bool:
        return isinstance(name, str) and (name in target.node_ids or name == END)

    for key in ('transition', 'default_output', 'edit_state_key'):
        if key == 'edit_state_key':
            continue
        if _has_text(node.get(key)) and not valid(node[key]):
            warnings.append(_missing_target(node[key]))
            node[key] = ''
    for key in ('routes', 'nodes'):
        value = node.get(key)
        if isinstance(value, list):
            kept = [name for name in value if valid(name)]
            warnings.extend(_missing_target(name) for name in value if not valid(name))
            node[key] = kept
        elif isinstance(value, dict):
            for action, name in value.items():
                if _has_text(name) and not valid(name):
                    warnings.append(_missing_target(name))
                    value[action] = ''


def _repair_resources(node: dict, target: DraftTarget, warnings: List[str]) -> List[dict]:
    used: List[Tuple[str, str]] = []
    kind = node['type']
    if kind in ('toolkit', 'mcp'):
        used.extend(_repair_tool_node(node, kind, target, warnings))
    elif kind == 'agent' and _has_text(node.get('tool')):
        if node['tool'] in target.resources.get('agent', {}):
            used.append(('agent', node['tool']))
        else:
            warnings.append(f"Agent `{node['tool']}` isn't available in this project. Pick an existing agent.")
            node['tool'] = ''
    elif kind == 'llm' and isinstance(node.get('tool_names'), dict):
        used.extend(_repair_tool_names(node, target, warnings))

    attachments = []
    for res_kind, name in dict.fromkeys(used):
        resource = target.resources[res_kind][name]
        if not resource.get('attached'):
            attachments.append({'entity_type': resource.get('entity_type', res_kind), 'id': resource.get('id'),
                                'name': name, 'needs_login': bool(resource.get('needs_login'))})
    return attachments


def _repair_tool_node(node: dict, kind: str, target: DraftTarget, warnings: List[str]):
    name = node.get('toolkit_name')
    if not _has_text(name):
        return []
    resource = target.resources.get(kind, {}).get(name)
    if resource is None:
        label = 'MCP' if kind == 'mcp' else 'toolkit'
        warnings.append(f"The {label} `{name}` isn't available in this project. Add it, then regenerate.")
        node['toolkit_name'], node['tool'] = '', ''
        return []
    tool = node.get('tool')
    tools = resource.get('tools') or {}
    if _has_text(tool) and tool not in tools:
        warnings.append(f"`{name}` has no tool `{tool}`. Pick one of: {', '.join(sorted(tools)) or 'none'}.")
        node['tool'] = ''
    elif _has_text(tool) and tools[tool] is not None:
        params = tools[tool]
        mapping = node.get('input_mapping') if isinstance(node.get('input_mapping'), dict) else {}
        for key in [k for k in mapping if k not in params]:
            del mapping[key]
            warnings.append(f"Tool `{tool}` has no parameter `{key}`; it was removed.")
        for key in sorted(k for k, required in params.items() if required):
            if key not in mapping or not _has_text(_value_text(mapping[key])):
                warnings.append(f"Tool parameter `{key}` needs a value.")
        node['input_mapping'] = mapping
    return [(kind, name)]


def _repair_tool_names(node: dict, target: DraftTarget, warnings: List[str]):
    used, kept = [], {}
    for name, tools in node['tool_names'].items():
        kind = next((k for k in ('toolkit', 'mcp') if name in target.resources.get(k, {})), None)
        if kind is None:
            warnings.append(f"The toolkit `{name}` isn't available in this project. Add it, then regenerate.")
            continue
        available = target.resources[kind][name].get('tools') or {}
        tools = tools if isinstance(tools, list) else []
        for tool in tools:
            if tool not in available:
                warnings.append(f"`{name}` has no tool `{tool}`; it was removed.")
        valid_tools = [t for t in tools if t in available]
        if valid_tools:
            kept[name] = valid_tools
            used.append((kind, name))
    if kept:
        node['tool_names'] = kept
    else:
        node.pop('tool_names', None)
    return used


# ---- state -----------------------------------------------------------------------------------

def _new_state(raw, node: dict, target: DraftTarget, warnings: List[str]) -> List[dict]:
    result, names = [], set(target.state)
    for entry in raw if isinstance(raw, list) else []:
        name = entry.get('name') if isinstance(entry, dict) else None
        kind = entry.get('type') if isinstance(entry, dict) else None
        if isinstance(name, str) and name in names:
            continue
        if not isinstance(name, str) or not _STATE_NAME.match(name) or kind not in STATE_TYPES:
            warnings.append(f"State variable {name!r} ({kind}) was skipped: use a snake_case name and one of "
                            f"{', '.join(STATE_TYPES)}.")
            continue
        result.append({'name': name, 'type': kind})
        names.add(name)
    for key in ('input', 'output'):
        for name in node.get(key) or []:
            if isinstance(name, str) and name not in names and _STATE_NAME.match(name):
                result.append({'name': name, 'type': 'str'})
                names.add(name)
                warnings.append(f"State variable `{name}` was added as str; change its type if needed.")
    return result


# ---- helpers ---------------------------------------------------------------------------------

def _detail(path: str, message: str) -> dict:
    return {'path': f'node.{path}', 'message': message}


def _get(node: dict, path: str):
    value = node
    for part in path.split('.'):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _value_text(entry):
    return entry.get('value') if isinstance(entry, dict) else entry


def _has_text(value) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return value not in (None, [], {})


def _has_reference(value) -> bool:
    if isinstance(value, dict):
        return any(_has_text(v) for v in value.values())
    return _has_text(value)


def _is_str_list(value) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)
