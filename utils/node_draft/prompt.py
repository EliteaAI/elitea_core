"""Service prompts for node drafting and the messages sent to the model.

Prompts resolve project override -> Public -> shipped default and are concatenated, never
``.format``-ed, so an admin edit with braces can't break the build (AC-38, AC-39).
"""
import json
import re
from typing import Callable, Dict, Mapping, Optional, Tuple

from pylon.core.tools import log

_SPEC_SEPARATOR = '\n\n## Node type\n'
GENERIC_SPEC_KEY = 'pipeline_node_spec_generic'


def prompt_keys(mode: str, node_type: str) -> Tuple[str, str]:
    return f'pipeline_node_{mode}', f'pipeline_node_spec_{node_type}'


def resolve_prompt(
    key: str,
    project_lookup: Callable[[str], Optional[str]],
    public_lookup: Callable[[str], Optional[str]],
    shipped: Mapping[str, str],
) -> str:
    for source in (project_lookup, public_lookup):
        try:
            text = source(key)
        except Exception:  # pylint: disable=W0703
            log.warning('node draft: prompt lookup for %s failed, falling back', key)
            continue
        if text and text.strip():
            return text
    return shipped.get(key, '')


def resolve_spec(
    node_type: str,
    project_lookup: Callable[[str], Optional[str]],
    public_lookup: Callable[[str], Optional[str]],
    shipped: Mapping[str, str],
    registry_lookup: Callable[[str], Optional[dict]],
) -> str:
    """The node type's own spec; for a type that has none yet, the registry facts plus the generic rules."""
    own = resolve_prompt(f'pipeline_node_spec_{node_type}', project_lookup, public_lookup, shipped)
    if own.strip():
        return own
    log.warning('node draft: no spec prompt for node type %s, using the generic spec', node_type)
    generic = resolve_prompt(GENERIC_SPEC_KEY, project_lookup, public_lookup, shipped)
    return '\n'.join(filter(None, [_registry_facts(node_type, registry_lookup(node_type)), generic]))


def _registry_facts(node_type: str, entry: Optional[dict]) -> str:
    entry = entry or {}
    lines = [f'Node type: {node_type}. {entry.get("summary") or ""}'.rstrip()]
    required = [' or '.join(item.split('|')) for item in entry.get('required') or []]
    if required:
        lines.append(f'Required keys: {"; ".join(required)}.')
    optional = entry.get('optional') or []
    if optional:
        lines.append(f'Optional keys: {", ".join(optional)}.')
    if entry.get('routing') == 'transition':
        lines.append('transition: an existing node id or "END".')
    return '\n'.join(lines)


def registry_entry(node_type: str) -> Optional[dict]:
    from ..pipeline_authoring.registry import node_spec  # pylint: disable=C0415
    return node_spec(node_type)


def build_messages(core: str, spec: str, context: dict, requirement: str) -> Tuple[str, str]:
    system = f'{core}{_SPEC_SEPARATOR}{spec}'
    context_json = json.dumps(context, ensure_ascii=False, separators=(',', ':'))
    user = (f'<context>{_neutralise(context_json)}</context>\n'
            f'<requirement>{_neutralise(requirement)}</requirement>')
    return system, user


_TAG = re.compile(r'<(\s*/?\s*)(requirement|context)\b', re.IGNORECASE)


def _neutralise(text: str) -> str:
    """Break the tags this layout uses, in any case or spacing, so user data can't close or open one."""
    return _TAG.sub(lambda m: f'<\u200b{m.group(1)}{m.group(2)}', text)


def default_lookups(project_id: int, rpc) -> Tuple[Callable[[str], Optional[str]], Callable[[str], Optional[str]]]:
    """The two runtime lookups: a project service_prompt row, then the Public one."""
    from ..service_prompt_utils import get_service_prompt  # pylint: disable=C0415

    def project_lookup(key: str) -> Optional[str]:
        row = rpc.timeout(5).configurations_get_first_filtered_project(
            project_id=project_id, filter_fields={'type': 'service_prompt', 'elitea_title': key},
        )
        return ((row or {}).get('data') or {}).get('prompt')

    return project_lookup, get_service_prompt


def shipped_defaults() -> Dict[str, str]:
    try:
        from plugins.configurations.models.pd.service_prompt_defaults import (  # pylint: disable=C0415,E0401
            SERVICE_PROMPT_DEFAULTS,
        )
    except Exception:  # pylint: disable=W0703
        log.warning('node draft: shipped service prompt defaults are not importable')
        return {}
    return SERVICE_PROMPT_DEFAULTS
