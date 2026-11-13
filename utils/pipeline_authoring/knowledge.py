"""Pipeline authoring knowledge: short Markdown entries served by key.

Entries live under ``knowledge/``: ``overview``, ``design``, ``constraints``,
``troubleshooting``, ``node:<type>`` and ``pattern:<name>``. Each file starts with
frontmatter naming its ``key`` and the registry entries it ``depends_on``.

Facts that the node registry owns (status, required and optional fields, defaults, the
replacement for a retired type) are not written in the files; ``get_reference`` adds them
when it serves a ``node:`` entry, so changing the registry changes what the model reads.

Imports nothing else from the SDK, like ``registry.py``.
"""

from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml

from .registry import node_spec, node_types

KNOWLEDGE_DIR = Path(__file__).with_name('knowledge')
_STATUS_NOTE = {
    'current': 'current',
    'deprecated': 'deprecated (keep it working in existing pipelines; do not add it to new ones)',
    'removed': 'removed (the runtime no longer supports it)',
    'ui_only': 'editor only (the runtime refuses to build it)',
}


class KnowledgeEntry:
    def __init__(self, key: str, depends_on: List[str], body: str, path: Path):
        self.key = key
        self.depends_on = depends_on
        self.body = body
        self.path = path

    @property
    def title(self) -> str:
        first = next((line for line in self.body.splitlines() if line.startswith('# ')), self.key)
        return first.lstrip('# ').strip()


def _parse(path: Path) -> KnowledgeEntry:
    text = path.read_text(encoding='utf-8')
    if not text.startswith('---\n'):
        raise ValueError(f'{path.name}: missing frontmatter')
    header, body = text[4:].split('\n---\n', 1)
    meta = yaml.safe_load(header) or {}
    if not meta.get('key'):
        raise ValueError(f'{path.name}: frontmatter has no key')
    return KnowledgeEntry(meta['key'], list(meta.get('depends_on') or []), body.strip() + '\n', path)


@lru_cache(maxsize=1)
def _entries() -> Dict[str, KnowledgeEntry]:
    entries = {}
    for path in sorted(KNOWLEDGE_DIR.rglob('*.md')):
        entry = _parse(path)
        if entry.key in entries:
            raise ValueError(f'knowledge: key {entry.key!r} is defined twice')
        entries[entry.key] = entry
    return entries


def list_topics() -> List[dict]:
    """Every servable key with a one-line title, in a stable order."""
    order = {'overview': 0, 'design': 1}
    return [{'key': e.key, 'title': e.title}
            for e in sorted(_entries().values(), key=lambda e: (order.get(e.key, 2), e.key))]


def get_entry(key: str) -> Optional[KnowledgeEntry]:
    return _entries().get(key.strip())


def node_facts(node_type: str) -> str:
    """The registry's facts about one node type, as a short Markdown block."""
    spec = node_spec(node_type)
    if spec is None:
        return ''
    lines = [f'**Status:** {_STATUS_NOTE.get(spec["status"], spec["status"])}']
    if spec.get('replacement'):
        lines.append(f'**Use instead:** {spec["replacement"]}')
    if spec.get('required'):
        lines.append('**Required:** ' + ', '.join(f'`{r.replace("|", "` or `")}`' for r in spec['required']))
    if spec.get('optional'):
        lines.append('**Optional:** ' + ', '.join(f'`{o}`' for o in spec['optional']))
    if spec.get('defaults'):
        lines.append('**Defaults:** ' + ', '.join(
            f'`{k}: {yaml.safe_dump(v, default_flow_style=True).strip().removesuffix("...").strip()}`'
            for k, v in spec['defaults'].items()))
    routing = 'its own `routes`/`nodes` (no `transition`)' if spec['routing'] == 'own' else '`transition`'
    lines.append(f'**Next node via:** {routing}')
    if spec.get('blocks_unattended'):
        lines.append('**Waits for a person:** never use it in scheduled, webhook or tool-called pipelines.')
    return '\n'.join(lines)


def _overview_node_list() -> str:
    rows = [f'- `{name}`: {node_spec(name)["summary"]}' for name in node_types('current')]
    retired = [f'`{name}`' for name in node_types('deprecated', 'removed', 'ui_only')]
    return ('## Node types\n\n' + '\n'.join(rows)
            + '\n\nDo not use in new pipelines: ' + ', '.join(retired)
            + '. Load `node:<type>` for any of them to see what to use instead.\n')


def render_entry(key: str) -> Optional[str]:
    """One entry ready to show a model: the file, plus registry facts where they apply."""
    entry = get_entry(key)
    if entry is None:
        return None
    body = entry.body
    if key == 'overview':
        return body.rstrip() + '\n\n' + _overview_node_list()
    if key.startswith('node:'):
        facts = node_facts(key.split(':', 1)[1])
        if facts:
            title, _, rest = body.partition('\n')
            return f'{title}\n\n{facts}\n{rest}'
    return body


def get_reference(topics: Union[str, List[str]]) -> str:
    """One or more entries joined for a single tool result; unknown keys list the valid ones."""
    keys = [topics] if isinstance(topics, str) else list(topics or [])
    parts, unknown = [], []
    for key in keys:
        rendered = render_entry(str(key))
        if rendered is None:
            unknown.append(str(key))
        else:
            parts.append(rendered.rstrip())
    if unknown:
        valid = ', '.join(t['key'] for t in list_topics())
        parts.append(f'Unknown topic(s): {", ".join(unknown)}. Valid topics: {valid}.')
    return '\n\n---\n\n'.join(parts) + '\n'
