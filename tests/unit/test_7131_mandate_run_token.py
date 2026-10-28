"""A run admitted under an autonomous mandate (scheduled or webhook) looks up no token of its owner (C9-SEC-47).

Under enforce its only platform identity is the run token minted at admission (the payload strip and the token itself
are pinned in ``test_6947_mandate_admission``). Here: the payload builder is told the run is a mandate run before it
builds anything (the ticket is taken first), and then neither lists nor creates the owner's token. Under off or audit
only nothing changes: no ticket is minted, the builder looks the token up as before.
"""
import ast
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
PAT, SESSION = "pat-value-not-a-secret", "session-reference-not-a-secret"


def _function(path, name, namespace):
    source = (PLUGIN_ROOT / path).read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), namespace)  # pylint: disable=W0122
    return namespace[name]


def _lookup(applies):
    looked_up = []
    namespace = {"Optional": __import__("typing").Optional, "PredictPayloadError": RuntimeError,
                 "get_user_token": lambda user_id: looked_up.append(user_id) or PAT,
                 "resolve_auth_session": lambda user_id, sid: SESSION,
                 "run_identity_applies": applies}
    return _function("utils/predict_utils.py", "get_predict_token_and_session", namespace), looked_up


def test_a_mandate_run_under_enforce_looks_no_token_of_its_owner_up():
    fn, looked_up = _lookup(lambda sid, mandate=False: mandate)   # enforce, signing key: only the mandate applies
    assert fn(1, 5, None, run_identity=True, mandate_run=True) == (None, None) and looked_up == []
    assert fn(1, 5, None, run_identity=True) == (PAT, None) and looked_up == [5]   # no ticket: the old system run


def test_off_or_audit_only_looks_the_owner_token_up_as_before():
    fn, looked_up = _lookup(lambda sid, mandate=False: False)   # not governed (or no key): nothing applies
    assert fn(1, 5, None, run_identity=True, mandate_run=True) == (PAT, None) and looked_up == [5]


@pytest.fixture
def identity():
    """``run_identity_applies`` with its two dependencies in hand: governance and the signing key."""
    state = {"governed": True, "key": b"k", "holder": None}
    pkg = types.ModuleType("c9pkg")
    pkg.__path__ = []
    utils = types.ModuleType("c9pkg.utils")
    utils.__path__ = []
    obo = types.ModuleType("c9pkg.utils.internal_obo")
    obo.signing_key = lambda: state["key"]
    admission = types.ModuleType("c9pkg.models.pd.admission")
    admission._holder = lambda sid: state["holder"]  # pylint: disable=W0212
    modules = {"c9pkg": pkg, "c9pkg.utils": utils, "c9pkg.utils.internal_obo": obo,
               "c9pkg.models": types.ModuleType("c9pkg.models"), "c9pkg.models.pd": types.ModuleType("c9pkg.models.pd"),
               "c9pkg.models.pd.admission": admission}
    for name in ("c9pkg.models", "c9pkg.models.pd"):
        modules[name].__path__ = []
    utils.internal_obo = obo
    saved = {name: sys.modules.get(name) for name in modules}
    sys.modules.update(modules)
    namespace = {"__package__": "c9pkg.utils", "__name__": "c9pkg.utils.internal_tools",
                 "run_token_applies": lambda: state["governed"]}
    try:
        yield _function("utils/internal_tools.py", "run_identity_applies", namespace), state
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def test_a_mandate_run_has_the_run_identity_only_when_the_run_token_exists(identity):
    applies, state = identity
    assert applies(None, mandate=True) is True and applies(None) is False   # no session holder: not a person's run
    state["governed"] = False
    assert applies(None, mandate=True) is False   # off / audit only: unchanged
    state["governed"], state["key"] = True, None
    assert applies(None, mandate=True) is False   # no signing key: no run token, the token stays


def test_the_ticket_is_taken_before_the_payload_is_built_and_tells_the_builder():
    tree = ast.parse((PLUGIN_ROOT / "rpc" / "application.py").read_text())
    predict = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "predict_sio")
    calls = [n for n in ast.walk(predict) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
    takes = [n for n in calls if n.func.id == "take_trigger_admission"]
    builds = [n for n in calls if n.func.id == "generate_predict_payload"]
    assert len(takes) == 1 and len(builds) == 1 and takes[0].lineno < builds[0].lineno   # spent once, before the build
    flag = next(k.value for k in builds[0].keywords if k.arg == "mandate_run")
    assert ast.unparse(flag) == "trigger_admission is not None"
