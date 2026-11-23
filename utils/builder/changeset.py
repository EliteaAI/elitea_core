"""Field-level change merge, diff, revision and confirmation policy for the Builder (#7383). Pure."""
import copy
import hashlib
import json

LOCKED_VERSION_STATUSES = frozenset({'published', 'embedded'})
TEXT_PATCH_KEYS = frozenset({'old_text', 'new_text', 'append', 'replace'})


class ChangeError(ValueError):
    pass


def revision(state) -> str:
    return hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode('utf-8')).hexdigest()


def _is_text_patch(value) -> bool:
    return isinstance(value, dict) and value and set(value) <= TEXT_PATCH_KEYS


def _apply_text_patch(field: str, current, patch: dict) -> str:
    current = current or ''
    if 'replace' in patch:
        return patch['replace']
    if 'append' in patch:
        return f"{current}\n{patch['append']}" if current else patch['append']
    old_text = patch.get('old_text')
    if not old_text or 'new_text' not in patch:
        raise ChangeError(f"{field}: a text change needs old_text and new_text, or append, or replace")
    count = current.count(old_text)
    if count == 0:
        raise ChangeError(f"{field}: old_text not found; read the entity again and copy the text exactly")
    if count > 1:
        raise ChangeError(f"{field}: old_text occurs more than once; include more surrounding text")
    return current.replace(old_text, patch['new_text'], 1)


def _merge(field: str, current, change):
    if _is_text_patch(change) and isinstance(current, list):
        if set(change) == {'append'}:
            added = change['append'] if isinstance(change['append'], list) else [change['append']]
            return [*current, *added]
        if set(change) == {'replace'}:
            return copy.deepcopy(change['replace'])
        raise ChangeError(f"{field}: a list change is the full list, or {{append}} / {{replace}}")
    if _is_text_patch(change) and (current is None or isinstance(current, str)):
        return _apply_text_patch(field, current, change)
    if isinstance(change, dict) and isinstance(current, dict):
        merged = dict(current)
        for key, value in change.items():
            merged[key] = _merge(f'{field}.{key}', current.get(key), value)
        return merged
    return copy.deepcopy(change)


def apply_changes(state: dict, changes: dict, allowed_fields=None) -> dict:
    """New state = state with `changes` merged: dicts merge per key, lists and scalars replace,
    text fields accept {old_text,new_text} | {append} | {replace}. `state` is not mutated."""
    result = copy.deepcopy(state or {})
    for field, change in (changes or {}).items():
        if allowed_fields is not None and field not in allowed_fields:
            raise ChangeError(f"{field}: unknown field; allowed: {', '.join(sorted(allowed_fields))}")
        result[field] = _merge(field, result.get(field), change)
    return result


def _lines(text):
    return [line for line in (text or '').splitlines()]


def _text_entry(path, before, after):
    before_lines, after_lines = _lines(before), _lines(after)
    before_set, after_set = set(before_lines), set(after_lines)
    return {
        'field': path,
        'added_lines': [line for line in after_lines if line not in before_set],
        'removed_lines': [line for line in before_lines if line not in after_set],
    }


def diff(before, after, path: str = '') -> list:
    """Changed leaves as [{field, before, after}]; multi-line text as added/removed lines."""
    if isinstance(before, dict) and isinstance(after, dict):
        entries = []
        for key in sorted(set(before) | set(after)):
            entries.extend(diff(before.get(key), after.get(key), f'{path}.{key}' if path else key))
        return entries
    if before == after:
        return []
    if isinstance(before, str) and isinstance(after, str) and ('\n' in before or '\n' in after):
        return [_text_entry(path, before, after)]
    return [{'field': path, 'before': before, 'after': after}]


def requires_confirmation(op: str, destructive: bool) -> bool:
    """Policy (decided 2026-11-17): only destructive changes need a click."""
    return op == 'delete' or bool(destructive)


def is_locked_version(status) -> bool:
    return str(status or '').lower() in LOCKED_VERSION_STATUSES
