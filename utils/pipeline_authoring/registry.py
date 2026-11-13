"""Pipeline node registry: node types, their lifecycle status and the fields they need.

The data lives in ``node_registry.yaml`` next to this module. The module imports nothing
from the SDK so that the platform can keep a byte-identical copy of both files (it does not
import the SDK at runtime).
"""

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import yaml

REGISTRY_PATH = Path(__file__).with_name('node_registry.yaml')

STATUSES = ('current', 'deprecated', 'removed', 'ui_only')
ROUTING_KINDS = ('transition', 'own')
END = 'END'


@lru_cache(maxsize=1)
def _load() -> dict:
    data = yaml.safe_load(REGISTRY_PATH.read_text(encoding='utf-8'))
    _check(data)
    return data


def _check(data: dict) -> None:
    """Fail loudly on a malformed registry: every consumer trusts its shape."""
    for name, spec in (data.get('nodes') or {}).items():
        if spec.get('status') not in STATUSES:
            raise ValueError(f"node_registry: '{name}' has invalid status {spec.get('status')!r}")
        if spec.get('routing') not in ROUTING_KINDS:
            raise ValueError(f"node_registry: '{name}' has invalid routing {spec.get('routing')!r}")
        if spec['status'] != 'current' and not spec.get('replacement'):
            raise ValueError(f"node_registry: '{name}' is {spec['status']} but names no replacement")
    for name, spec in (data.get('inline_constructs') or {}).items():
        if spec.get('status') not in STATUSES:
            raise ValueError(f"node_registry: inline '{name}' has invalid status {spec.get('status')!r}")


def get_node_registry() -> dict:
    """The whole registry as plain JSON-serialisable data (for the UI and the platform)."""
    data = _load()
    return {
        'schema_version': data['schema_version'],
        'nodes': {name: dict(spec) for name, spec in data['nodes'].items()},
        'inline_constructs': {name: dict(spec) for name, spec in (data.get('inline_constructs') or {}).items()},
    }


def node_spec(node_type: str) -> Optional[dict]:
    """The registry entry for one node type, or None when the type is unknown."""
    spec = _load()['nodes'].get(node_type)
    return dict(spec) if spec is not None else None


def node_types(*statuses: str) -> List[str]:
    """Node type names, optionally only those with one of the given statuses."""
    nodes = _load()['nodes']
    return [name for name, spec in nodes.items() if not statuses or spec['status'] in statuses]


def inline_construct(name: str) -> Optional[dict]:
    spec = (_load().get('inline_constructs') or {}).get(name)
    return dict(spec) if spec is not None else None


def required_paths(node_type: str) -> List[List[str]]:
    """Required fields as alternatives: [['input_mapping.system', 'input_mapping.task'], ['tool']]."""
    spec = node_spec(node_type) or {}
    return [entry.split('|') for entry in spec.get('required') or []]


def registry_entries_hash(names: List[str]) -> str:
    """Stable short hash of the given registry entries, used to notice when knowledge goes stale."""
    nodes = _load()['nodes']
    inline = _load().get('inline_constructs') or {}
    picked: Dict[str, object] = {name: nodes.get(name, inline.get(name)) for name in sorted(names)}
    blob = json.dumps(picked, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()[:12]
