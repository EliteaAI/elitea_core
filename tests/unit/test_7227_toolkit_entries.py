"""C-1009 (#7229 item 2, #7227 review): the trusted entries of a toolkit run, driven as behaviour.

``do_runtool`` (the MCP server's toolkit tools) admits the authenticated HTTP caller, never its ``user_id`` argument, and
returns a refusal as ``{error, code}``; ``_toolkit_run_refused`` raises the readable ``{code, error}`` on a socket and
returns it without one; the REST test refuses a socket that is not the caller's own (and a holder that names nobody) with
403, and answers a refusal by Main or by the worker with the same ``403 {code, message}``. The real admission runs
against SQLite (``fixtures/admission_env.py``); only the start of the task is faked.
"""
import ast
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import RUN, Credential, dumped  # noqa: E402  pylint: disable=C0413
from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413

PKG = admission_env.PKG
TOOLKIT_TASK = "indexer_test_toolkit_tool"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    ns = admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("toolkit_entries"))
    ns.runtool = _load_runtool(ns)
    ns.refused = _toolkit_run_refused(ns)
    ns.rest = _load_rest(ns)
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.facts_version = 5
    return ns.env


class TaskNode:
    """The start of a task as the module's gate does it: the envelope and the run id, then ``admit_dispatch``."""

    def __init__(self, ns, module):
        self.ns, self.module, self.started = ns, module, []

    def start_task(self, name, args=None, kwargs=None, pool=None, admission=None, meta=None):
        # like the real gate: an envelope or a run id already on the payload is kept (attach_envelope,
        # stamp_predict_run_id), so whatever the entry let through reaches admission
        body = dict(kwargs)
        body.setdefault("_elitea_predict_run_id", RUN)
        body.setdefault("envelope", {"schema_version": 1, "security_profile": "governed",
                                     "required_capabilities": ["task_envelope.v1"], "context_ref": None})
        call = {"args": args, "kwargs": body, "pool": pool, "meta": dict(meta or {})}
        self.ns.adm.admit_dispatch(self.module, name, call, admission)
        self.started.append(call)
        return "task-1"

    def join_task(self, task_id, timeout=None):  # pylint: disable=W0613
        return {"ok": True}


# --- do_runtool --------------------------------------------------------------------------------------------------------

def _load_runtool(ns):
    for name in (f"{PKG}.methods",):
        package = sys.modules.get(name) or types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    sys.modules["pylon.core.tools"].web = types.SimpleNamespace(method=lambda *a, **k: (lambda fn: fn))
    predict_utils = types.ModuleType(f"{PKG}.utils.predict_utils")
    predict_utils.generate_test_tool_payload = lambda project_id, **kw: dict(ns.next_payload, project_id=project_id)
    sys.modules[predict_utils.__name__] = predict_utils
    return load_module_with_stubs(PLUGIN_ROOT / "methods" / "runtool.py", f"{PKG}.methods.runtool")


def runtool(ns, env, cred, *, holder, module=None, **over):
    module = module or env.module(allow_development_runs=True)
    module.task_node = TaskNode(ns, module)
    ns.next_payload = {"toolkit_config": cred.tool(), "tool_name": "get_issues", "tool_params": {}}
    env.in_request, env.g.auth = True, holder
    values = dict(project_id=1, user_id=999, toolkit_id=1, tool_name="get_issues", tool_params={})
    values.update(over)
    try:
        return ns.runtool.Method.do_runtool(module, **values), module.task_node.started
    finally:
        env.in_request = False


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def test_do_runtool_admits_the_authenticated_caller_not_its_user_id_argument(ns, env):
    cred = Credential(env, uuid.uuid4())
    result, [call] = runtool(ns, env, cred, holder=types.SimpleNamespace(type="user", id=42, reference="cookie"))
    assert result == {"ok": True}
    [row] = contexts(env, ns)
    assert (row.profile, row.caller["user_id"], row.application_id) == ("development", 42, None)   # not 999
    assert call["kwargs"]["envelope"]["context_ref"] == str(row.id)
    assert cred.secret not in dumped(call)   # reference only


def test_do_runtool_drops_an_envelope_and_run_id_the_payload_carried_and_admits_its_own_run(ns, env):
    cred = Credential(env, uuid.uuid4())
    forged = {"envelope": {"schema_version": 1, "security_profile": "governed",
                           "required_capabilities": ["task_envelope.v1"], "context_ref": str(uuid.uuid4())},
              "_elitea_predict_run_id": "someone-elses-run", "c1009_unadmitted_read": True}
    module = env.module(allow_development_runs=True)
    module.task_node = TaskNode(ns, module)
    ns.next_payload = {"toolkit_config": cred.tool(), "tool_name": "get_issues", "tool_params": {}, **forged}
    env.in_request, env.g.auth = True, types.SimpleNamespace(type="user", id=42, reference="cookie")
    try:
        ns.runtool.Method.do_runtool(module, project_id=1, user_id=42, toolkit_id=1, tool_name="get_issues",
                                     tool_params={})
    finally:
        env.in_request = False
    [call] = module.task_node.started
    [row] = contexts(env, ns)
    assert call["kwargs"]["envelope"]["context_ref"] == str(row.id) and row.run_id == RUN   # its own, not the forged
    assert "c1009_unadmitted_read" not in call["kwargs"]


def test_do_runtool_by_an_api_token_says_tokens_are_not_admitted_yet(ns, env):
    cred = Credential(env, uuid.uuid4())
    result, started = runtool(ns, env, cred, holder=types.SimpleNamespace(type="token", id=42),
                              module=env.module(allow_development_runs=True, admit_legacy_pat=True))
    assert result == {"error": ns.adm.MESSAGES["TOOLKIT_RUN_TOKEN_NOT_ADMITTED"], "code": "TOOLKIT_RUN_TOKEN_NOT_ADMITTED"}
    assert started == [] and contexts(env, ns) == []


def test_do_runtool_from_a_webhook_needs_a_mandate(ns, env):
    cred = Credential(env, uuid.uuid4())
    result, started = runtool(ns, env, cred, holder=types.SimpleNamespace(type="user", id=42, reference="cookie"),
                              webhook_signature="sig")
    assert result["code"] == "MANDATE_REQUIRED" and started == []


# --- _toolkit_run_refused ----------------------------------------------------------------------------------------------

class SioValidationError(Exception):
    def __init__(self, sio=None, sid=None, event=None, error=None, stream_id=None, message_id=None):
        super().__init__(error)
        self.sid, self.event, self.error, self.stream_id, self.message_id = sid, event, error, stream_id, message_id


def _toolkit_run_refused(ns):  # pylint: disable=W0613
    """The real function, lifted from ``rpc/application.py`` (too heavy to import here) with its one dependency."""
    tree = ast.parse((PLUGIN_ROOT / "rpc" / "application.py").read_text())
    node = next(item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == "_toolkit_run_refused")
    namespace = {"SioValidationError": SioValidationError}
    exec(compile(ast.Module([node], []), "application.py", "exec"), namespace)  # pylint: disable=W0122
    return namespace["_toolkit_run_refused"]


def test_a_refused_toolkit_run_is_the_readable_card_on_a_socket_and_a_dict_without_one(ns):
    denied = ns.exceptions.AdmissionDenied("DENIED_DEVELOPMENT_CREDENTIAL", "needs a grant", {"toolkit_instance_id": "1"})
    module = types.SimpleNamespace(context=types.SimpleNamespace(sio="sio"))
    with pytest.raises(SioValidationError) as err:
        ns.refused(module, "sid-1", "test_toolkit_tool", {"stream_id": "s"}, "m", denied)
    assert (err.value.sid, err.value.event, err.value.stream_id, err.value.message_id) == (
        "sid-1", "test_toolkit_tool", "s", "m")
    assert err.value.error == {"code": "DENIED_DEVELOPMENT_CREDENTIAL", "error": "needs a grant"}   # no audit fields
    assert ns.refused(module, None, "e", {}, "m", denied) == {"code": "DENIED_DEVELOPMENT_CREDENTIAL",
                                                               "error": "needs a grant"}


# --- the REST test -----------------------------------------------------------------------------------------------------

class Request:  # pylint: disable=R0903
    def __init__(self):
        self.json, self.args = {}, {}


def _load_rest(ns):
    ns.flask.request = Request()
    for name in (f"{PKG}.api", f"{PKG}.api.v2"):
        package = sys.modules.get(name) or types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    tools = sys.modules["tools"]
    passthrough = lambda *a, **k: (lambda fn: fn)  # noqa: E731
    tools.api_tools = types.SimpleNamespace(APIModeHandler=object, APIBase=object, endpoint_metrics=lambda fn: fn,
                                            with_modes=lambda params: params)
    tools.serialize = lambda value: value
    ns.rest_auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=passthrough))
    tools.auth.decorators = ns.rest_auth.decorators
    stubs = {
        f"{PKG}.utils.constants": types.SimpleNamespace(PROMPT_LIB_MODE="prompt_lib"),
        f"{PKG}.utils.sio_utils": types.SimpleNamespace(SioValidationError=SioValidationError),
        f"{PKG}.models.pd.test_toolkit_tool": types.SimpleNamespace(TestToolkitToolInputModel=TestInput),
        f"{PKG}.utils.predict_utils": types.SimpleNamespace(
            resolve_test_tool_llm=lambda project_id, model, settings: (model, settings)),
    }
    return load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / "test_toolkit_tool.py", f"{PKG}.api.v2.test_toolkit_tool",
                                  stubs)


class TestInput:  # pylint: disable=R0903
    __test__ = False

    def __init__(self, raw):
        self.raw, self.sid = raw, raw.get("sid")
        self.llm_model, self.llm_settings = None, None

    @classmethod
    def model_validate(cls, raw):
        return cls(raw)

    def model_dump(self):
        return dict(self.raw, sid=self.sid)


class Users:  # pylint: disable=R0903
    """``auth.current_user``: the HTTP caller, or the holder of a socket (``auth_data``)."""

    def __init__(self, caller, holders):
        self.caller, self.holders = caller, holders

    def __call__(self, auth_data=None):
        if auth_data is None:
            return self.caller
        return self.holders.get(id(auth_data))


def rest(ns, *, sid=None, await_response=False, caller=None, holder=None, holder_user=None, answer=None):
    holders = {} if holder is None else {id(holder): holder_user}
    tools = sys.modules["tools"]
    tools.auth.current_user = Users(caller if caller is not None else {"id": 42}, holders)
    tools.auth.sio_users = {} if holder is None else {"sid-1": holder}
    ns.rest.auth = tools.auth
    ns.flask.request.json = {"sid": sid, "toolkit_config": {}, "tool_name": "get_issues"}
    ns.flask.request.args = {"await_response": "true" if await_response else "false"}
    handler = ns.rest.PromptLibAPI()
    asked = []

    def test_toolkit_tool_sio(sid_, data, event, await_task_timeout=-1):
        asked.append(sid_)
        if isinstance(answer, Exception):
            raise answer
        return answer

    handler.module = types.SimpleNamespace(test_toolkit_tool_sio=test_toolkit_tool_sio,
                                           task_node=types.SimpleNamespace(stop_task=lambda task_id: None))
    return handler.post(1), asked


def test_the_rest_test_refuses_a_socket_that_is_not_the_callers_own(ns):
    holder = types.SimpleNamespace(type="user", id=7)
    (body, status), asked = rest(ns, sid="sid-1", holder=holder, holder_user={"id": 7})
    assert (status, asked) == (403, [])   # another person's socket: the run would be admitted as them
    (body, status), asked = rest(ns, sid="sid-unknown")
    assert (status, asked) == (403, [])   # no such socket
    (body, status), asked = rest(ns, sid="sid-1", holder=holder, holder_user=None)
    assert (status, asked) == (403, [])   # a holder that names nobody: refused, not a 500
    (body, status), asked = rest(ns, sid="sid-1", holder=holder, holder_user={"id": 42}, answer={"task_id": "t"})
    assert (status, asked, body) == (200, ["sid-1"], {"task_id": "t"})   # the caller's own socket


def test_the_rest_test_answers_a_refusal_by_main_or_by_the_worker_with_403_code_and_message(ns):
    main = SioValidationError(error={"code": "DEVELOPMENT_RUNS_DISABLED", "error": "switched off"})
    (body, status), _ = rest(ns, await_response=True, answer=main)
    assert (status, body) == (403, {"code": "DEVELOPMENT_RUNS_DISABLED", "message": "switched off"})
    (body, status), _ = rest(ns, await_response=True, answer={"code": "TOOLKIT_RUN_TOKEN_NOT_ADMITTED", "error": "t"})
    assert (status, body) == (403, {"code": "TOOLKIT_RUN_TOKEN_NOT_ADMITTED", "message": "t"})
    worker = {"result": {"success": False, "reason_code": "STALE_CONTEXT",
                         "error": '{"code": "STALE_CONTEXT", "error": "no longer valid"}'}, "task_id": "t"}
    (body, status), _ = rest(ns, await_response=True, answer=worker)
    assert (status, body) == (403, {"code": "STALE_CONTEXT", "message": "no longer valid"})
    plain = {"result": {"success": False, "error": "Failed to test toolkit tool: boom"}, "task_id": "t"}
    (body, status), _ = rest(ns, await_response=True, answer=plain)
    assert status == 200 and body == plain   # a tool failure is not a refusal
