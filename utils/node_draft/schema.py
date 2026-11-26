"""Node types the AI can draft, the keys each one may have and the defaults of a new node.

Supported types and allowed keys come from the runtime registry (``pipeline_authoring/node_registry.yaml``):
the current types plus Printer (deprecated in the registry, still offered in the editor), and the top-level
keys of ``required`` + ``optional``. Only the keys the editor stores beyond the registry and the new-node
defaults are listed here; the defaults mirror the UI's ``InitialNodeData`` (EliteaUI
``flowEditor.constants.js``), change both together.
"""
import copy
from functools import lru_cache
from typing import Dict, List, Tuple

from ..pipeline_authoring.registry import END, node_spec, node_types

# Editor-offered types the registry has moved to `deprecated`
_EDITOR_TYPES = ('printer',)
# Registry keys the editor never sets for these types (code debug flag, legacy inline decision inputs)
HIDDEN_KEYS = frozenset({'debug', 'decisional_inputs'})

_IO = {'input': [], 'output': []}
_TRANSITION = {**_IO, 'transition': END}
_STRUCTURED = {**_TRANSITION, 'structured_output': False}
_TOOL = {'tool': '', **_STRUCTURED, 'input_mapping': {}}

_DEFAULTS: Dict[str, dict] = {
    'llm': _STRUCTURED,
    'agent': _TRANSITION,
    'toolkit': _TOOL,
    'mcp': _TOOL,
    'code': {'code': {'type': 'fixed', 'value': ''}, **_STRUCTURED},
    'router': {'default_output': '', 'routes': [], 'input': [], 'condition': ''},
    'decision': {'input': [], 'description': '', 'nodes': [], 'default_output': ''},
    'hitl': {'input': [], 'user_message': {'type': 'fixed', 'value': ''},
             'routes': {'approve': '', 'edit': '', 'reject': END}, 'edit_state_key': ''},
    'state_modifier': {'template': '', 'variables_to_clean': [], **_IO},
    'printer': {'transition': END},
}

SUPPORTED_TYPES: Tuple[str, ...] = tuple(
    t for t in (*node_types('current'), *_EDITOR_TYPES) if t in _DEFAULTS
)


@lru_cache(maxsize=None)
def allowed_keys(node_type: str) -> frozenset:
    spec = node_spec(node_type) or {}
    paths = [part for entry in (*(spec.get('required') or []), *(spec.get('optional') or []))
             for part in entry.split('|')]
    keys = {path.split('.')[0] for path in paths}
    if spec.get('routing') == 'transition':
        keys.add('transition')
    return frozenset(keys - HIDDEN_KEYS)


def defaults(node_type: str) -> dict:
    return copy.deepcopy(_DEFAULTS[node_type])


def drop_unknown(node_type: str, node: dict) -> Tuple[dict, List[str]]:
    """Keep the keys this type allows. ``id``/``type`` are removed without being reported."""
    allowed = allowed_keys(node_type)
    cleaned, dropped = {}, []
    for key, value in node.items():
        if key in allowed:
            cleaned[key] = value
        elif key not in ('id', 'type'):
            dropped.append(key)
    return cleaned, dropped


def apply_defaults(node_type: str, node: dict) -> dict:
    """Build mode: keys the model left out get the defaults of a new node."""
    return {**defaults(node_type), **node}
