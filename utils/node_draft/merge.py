"""Edit-mode patch merge: objects merge by key, lists and scalars replace, ``None`` removes the key."""
import copy
from typing import List

_PROTECTED = ('id', 'type')


def merge_patch(current: dict, patch: dict) -> dict:
    merged = copy.deepcopy(current)
    for key, value in patch.items():
        if key in _PROTECTED:
            continue
        _merge_into(merged, key, value)
    return merged


def _merge_into(target: dict, key: str, value) -> None:
    if value is None:
        target.pop(key, None)
    elif isinstance(value, dict) and isinstance(target.get(key), dict) and not _is_value_object(value):
        for sub_key, sub_value in value.items():
            _merge_into(target[key], sub_key, sub_value)
    else:
        target[key] = copy.deepcopy(value)


def _is_value_object(value) -> bool:
    """``{type, value}`` mapping objects are one field: replaced whole, reported as one path."""
    return isinstance(value, dict) and 'type' in value and 'value' in value


def changed_paths(before: dict, after: dict) -> List[str]:
    paths: List[str] = []
    _diff(before, after, '', paths)
    return sorted(paths)


def _diff(before, after, prefix: str, paths: List[str]) -> None:
    for key in set(before) | set(after):
        if not prefix and key in _PROTECTED:
            continue
        path = f'{prefix}{key}'
        old, new = before.get(key, _MISSING), after.get(key, _MISSING)
        if old == new:
            continue
        # a new object is reported per field (each gets a marker); a removed one as one path
        if _is_plain_dict(new) and old is _MISSING:
            old = {}
        if _is_plain_dict(old) and _is_plain_dict(new):
            _diff(old, new, f'{path}.', paths)
        else:
            paths.append(path)


def _is_plain_dict(value) -> bool:
    return isinstance(value, dict) and not _is_value_object(value)


_MISSING = object()
