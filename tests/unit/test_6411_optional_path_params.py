"""Issue #6411 - a path parameter declared `required: False` is published as required.

`extract_path_params_from_url` (shared/tools/openapi_tools.py) hard-codes `"required": True` for
every path parameter, and `register_api_class` merges explicit `parameters` entries by name only -
so `{"in": "path", "required": False}` is silently discarded. OpenAPI forbids an optional path
parameter anyway, so the declaration can never be honoured; it only ever produces a spec that
contradicts its own description.

This scans the decorator source directly rather than the generated spec: it needs no stubbing, and
it covers every endpoint in `api/v2/` including ones added later.
"""
import ast
import pathlib

import pytest

API_V2 = pathlib.Path(__file__).resolve().parents[2] / 'api' / 'v2'


def _literal(node):
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return None


def _optional_path_params(source: str):
    """Yield every dict literal declaring an `in: path` parameter that is not required."""
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Dict):
            continue
        entry = _literal(node)
        if not isinstance(entry, dict):
            continue
        if entry.get('in') == 'path' and entry.get('required') is False:
            yield node.lineno, entry.get('name')


def _api_modules():
    return sorted(p for p in API_V2.glob('*.py') if not p.name.startswith('_'))


def test_api_v2_has_modules_to_scan():
    """Guards the scan itself - an empty glob would make every assertion below vacuous."""
    assert len(_api_modules()) > 50


@pytest.mark.parametrize('path', _api_modules(), ids=lambda p: p.name)
def test_no_path_parameter_is_declared_optional(path):
    offenders = list(_optional_path_params(path.read_text()))
    assert not offenders, (
        f'{path.name} declares optional path parameter(s) '
        f'{[f"{name} (line {line})" for line, name in offenders]}. '
        'A path parameter is always published as required - pin the short path with '
        'path_suffix_override and declare the selector as an "in": "query" parameter instead.'
    )
