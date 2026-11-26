"""Context the model sees when drafting one node: the pipeline around it and the resources it may use."""
import re
from typing import Callable, Iterable, List, Optional, Set

import yaml

from ...models.pd.node_draft import LIMITS, ParsedPipeline
from ..pipeline_authoring.registry import END
from ..pipeline_authoring.validator import TRIGGERS_WITHOUT_A_PERSON

# The default state every pipeline has (UI DefaultState); always shown to the model
DEFAULT_STATE = {'input': 'str', 'messages': 'list'}
# Which project resources each node type may reference
RESOURCE_KINDS = {'llm': ('toolkit', 'mcp'), 'agent': ('agent',), 'toolkit': ('toolkit',), 'mcp': ('mcp',)}
_WORD = re.compile(r'[a-z0-9]+')


def parse_pipeline(pipeline_yaml: str, node_id: str, trigger: Optional[str] = None) -> ParsedPipeline:
    try:
        doc = yaml.safe_load(pipeline_yaml or '') or {}
    except yaml.YAMLError as exc:
        raise ValueError(f'The pipeline YAML is not valid: {exc}') from exc
    if not isinstance(doc, dict):
        raise ValueError('The pipeline YAML must be a mapping.')
    raw_nodes = doc.get('nodes') if isinstance(doc.get('nodes'), list) else []
    nodes = [{**n, 'id': str(n['id'])} for n in raw_nodes if isinstance(n, dict) and n.get('id') not in (None, '')]

    state = dict(DEFAULT_STATE)
    raw_state = doc.get('state') if isinstance(doc.get('state'), dict) else {}
    for name, spec in raw_state.items():
        kind = spec.get('type', 'str') if isinstance(spec, dict) else 'str'
        state[str(name)] = kind if isinstance(kind, str) else 'str'
    entry_point = doc.get('entry_point')

    return ParsedPipeline(
        state=[{'name': name, 'type': kind} for name, kind in state.items()],
        node_ids=[n['id'] for n in nodes] + [END],
        incoming=sorted({n['id'] for n in nodes if n['id'] != node_id and node_id in _targets(n)}),
        entry_point=str(entry_point) if entry_point is not None else None,
        unattended=(trigger or '').strip().lower() in TRIGGERS_WITHOUT_A_PERSON,
        nodes=nodes,
    )


def _targets(node: dict) -> Set[str]:
    found: Set[str] = set()
    for key in ('transition', 'default_output'):
        if isinstance(node.get(key), str):
            found.add(node[key])
    routes = node.get('routes')
    if isinstance(routes, list):
        found.update(r for r in routes if isinstance(r, str))
    elif isinstance(routes, dict):
        found.update(r for r in routes.values() if isinstance(r, str))
    found.update(n for n in (node.get('nodes') or []) if isinstance(n, str))
    return found


def needs_resources(node_type: str) -> bool:
    return node_type in RESOURCE_KINDS


def _tokens(text: str) -> Set[str]:
    return {t for t in _WORD.findall((text or '').lower()) if len(t) >= LIMITS.min_token_length}


def _is_named(resource: dict, requirement_tokens: Set[str]) -> bool:
    """A resource is named when a word of its name appears in the requirement ("the Jira toolkit")."""
    return bool(_tokens(resource.get('name', '')) & requirement_tokens)


def _score(resource: dict, requirement_tokens: Set[str]) -> int:
    text = ' '.join([resource.get('name') or '', resource.get('description') or '']
                    + [t.get('name', '') + ' ' + (t.get('description') or '') for t in resource.get('tools') or []])
    return len(_tokens(text) & requirement_tokens)


def select_resources(
    candidates: Iterable[dict],
    requirement: str,
    attached: Set[str],
    is_accessible: Callable[[dict], bool] = lambda _r: True,
    cap: int = LIMITS.resource_cap,
    params_for_top: int = LIMITS.params_for_top,
) -> List[dict]:
    """Ranked resources for the prompt: up to ``cap`` by relevance, plus attached and named ones."""
    requirement_tokens = _tokens(requirement)
    visible = [r for r in candidates if is_accessible(r)]
    ranked = sorted(visible, key=lambda r: _score(r, requirement_tokens), reverse=True)  # stable on ties

    picked, seen = [], set()
    for resource in ranked:
        name = resource.get('name')
        must = name in attached or _is_named(resource, requirement_tokens)
        if name in seen or not (must or len([p for p in picked if not p['_must']]) < cap):
            continue
        seen.add(name)
        picked.append({**resource, '_must': must})

    return [_trimmed(r, with_params=i < params_for_top) for i, r in enumerate(picked)]


def _trimmed(resource: dict, with_params: bool) -> dict:
    out = {k: v for k, v in resource.items() if not k.startswith('_')}
    out['description'] = _trim(out.get('description'))
    tools = []
    for tool in resource.get('tools') or []:
        item = {'name': tool.get('name'), 'description': _trim(tool.get('description'))}
        if with_params and tool.get('params'):
            item['params'] = tool['params']
        tools.append(item)
    out['tools'] = tools
    return out


def _trim(text: Optional[str]) -> str:
    text, limit = (text or '').strip(), LIMITS.description_limit
    return text if len(text) <= limit else text[:limit - 1].rstrip() + '…'
