"""``_apply_dimension_target_defaults``: a binding with no target pair of its own runs against the
dimension's current default, so a target set on the dimension after attach reaches the run snapshot.
The function is extracted by AST because the module's relative imports need the full plugin."""
import ast
import pathlib
import types
from typing import List

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load():
    src = (PLUGIN_ROOT / 'utils' / 'evaluation_run_utils.py').read_text()
    node = next(n for n in ast.parse(src).body
                if isinstance(n, ast.FunctionDef) and n.name == '_apply_dimension_target_defaults')
    ns = {'List': List}
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'x', 'exec'), ns)
    return ns['_apply_dimension_target_defaults']


apply_defaults = _load()


def _dim(i, target, op):
    return types.SimpleNamespace(id=i, default_target=target, default_target_operator=op)


def _binding(dim_id, target=None, op=None):
    return {'dimension_id': dim_id, 'target': target, 'target_operator': op}


def test_empty_binding_inherits_dimension_default():
    b = [_binding(1)]
    apply_defaults(b, [_dim(1, 0.8, '>=')])
    assert (b[0]['target'], b[0]['target_operator']) == (0.8, '>=')


def test_binding_override_wins():
    b = [_binding(1, 0.5, '>')]
    apply_defaults(b, [_dim(1, 0.8, '>=')])
    assert (b[0]['target'], b[0]['target_operator']) == (0.5, '>')


def test_half_pair_default_is_not_inherited():
    b = [_binding(1)]
    apply_defaults(b, [_dim(1, 0.8, None)])
    assert b[0]['target'] is None and b[0]['target_operator'] is None


def test_no_default_stays_empty():
    b = [_binding(1), _binding(None)]
    apply_defaults(b, [_dim(1, None, None)])
    assert all(x['target'] is None for x in b)
