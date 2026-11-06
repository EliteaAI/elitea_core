"""The batch pause flag goes from the starter to the payload of the predict RPC, as an RPC argument only.

Contract under test: utils/pipeline_execution.py `execute_pipeline_via_predict_sio` (loaded from source with the
fakes of its other tests) and rpc/application.py `predict_sio` (read as source: the plugin does not load in the
unit env).
"""
import ast
import pathlib

from unit.utils import test_7011_pipeline_execution_owner as owner_fixtures

world = owner_fixtures.world  # the fixture of the owner run tests, bound as an attribute
_predict = owner_fixtures._predict  # pylint: disable=W0212

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
FLAG = "batch_sensitive_approvals"


def test_the_predict_call_carries_the_flag_when_it_is_set(world):
    assert _predict(world, batch_sensitive_approvals=True)[FLAG] is True


def test_the_predict_call_has_no_flag_by_default_or_when_false(world):
    assert FLAG not in _predict(world)
    assert FLAG not in _predict(world, batch_sensitive_approvals=False)


def test_the_flag_does_not_change_the_other_predict_arguments(world):
    plain = _predict(world, unattended=True)
    batch = _predict(world, unattended=True, batch_sensitive_approvals=True)

    assert {key: value for key, value in batch.items() if key != FLAG} == plain


def _predict_sio():
    tree = ast.parse((PLUGIN_ROOT / "rpc" / "application.py").read_text())
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "predict_sio")


def test_predict_sio_takes_the_flag_as_a_false_keyword_argument():
    function = _predict_sio()
    names = [arg.arg for arg in function.args.args]
    default = function.args.defaults[names.index(FLAG) - len(names)]

    assert (FLAG in names, ast.literal_eval(default)) == (True, False)


def test_predict_sio_puts_the_flag_in_the_payload_and_never_in_the_data_model():
    function = _predict_sio()
    stored = [node for node in ast.walk(function) if isinstance(node, ast.Assign)
              and isinstance(node.targets[0], ast.Subscript) and ast.unparse(node.targets[0]) == f"payload['{FLAG}']"]
    payload_call = next(node for node in ast.walk(function)
                        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "generate_predict_payload")

    assert len(stored) == 1
    assert FLAG not in {keyword.arg for keyword in payload_call.keywords}
