"""C-1009 (#7120): what a switch of the mode does to the runs that are already admitted, and to a dispatch in flight.

A run admitted under enforce keeps its admission when the mode becomes off or audit only: its children and its
reconcile run are sealed against the parent's decisions and carry its context, its stash stays sealed, its workers still
validate and release, and its audit events carry the policy revision it was admitted with. Nothing of that costs off
anything: a legacy dispatch in off still reads dicts only.
"""
import ast
import copy
import importlib.abc
import json
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.dispatch_wrapper import Original, build_wrapper  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, RUN, SECRET, call_kwargs, dispatch, payload_of, run_to_running, sealed_tool_settings, seeded,
)


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("continuity"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def child_dispatch(ns, env, module, payload, parent):
    """A child (or reconcile) dispatch of a parent through the real wrapper: what it sends to the worker."""
    original = Original()
    wrapper = build_wrapper(ns, module, original)
    body = call_kwargs(payload, parent)
    assert wrapper("indexer_agent", args=body["args"], kwargs=body["kwargs"], pool="agents", meta=body["meta"]) == "task-1"
    [(_, sent)] = original.calls
    return sent


# --- a parent admitted under enforce, then the switch -----------------------------------------------------------------

@pytest.mark.parametrize("mode", ["off", "audit_only"])
@pytest.mark.parametrize("handlers", [True, False], ids=["handlers registered", "booted after the switch"])
def test_a_child_of_a_run_admitted_under_enforce_is_sealed_and_carries_the_context_after_the_switch(
        ns, env, mode, handlers):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)   # admitted under enforce
    [entry] = contexts(env, ns)[0].allowed_bindings
    module = env.module(governed=False, audit=mode == "audit_only")
    module.governed_handlers_registered = handlers
    env.verify_calls.clear()
    sent = child_dispatch(ns, env, module, payload_of(cred), parent)   # a child payload built from an unsealed source
    config = sealed_tool_settings(sent)
    assert config["credential_slot"]["decision_id"] == entry["decision_id"] and "token" not in config
    assert SECRET not in json.dumps(sent, default=str)
    assert sent["meta"]["context_ref"] == parent and sent["kwargs"]["envelope"]["context_ref"] == parent
    assert sent["kwargs"]["envelope"]["security_profile"] == "governed"
    assert env.verify_calls == [] and env.denied == [] and len(contexts(env, ns)) == 1   # nothing was admitted again


def test_a_reconcile_dispatch_of_an_already_sealed_parent_payload_passes_unchanged_after_the_switch(ns, env):
    cred = seeded(env)
    first, parent = dispatch(env, ns, cred)
    reconcile = call_kwargs(copy.deepcopy(first["kwargs"]), parent)
    expected = copy.deepcopy(reconcile["kwargs"])
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", reconcile, None) is None
    assert reconcile["kwargs"] == expected and reconcile["meta"]["context_ref"] == parent


def test_a_child_with_a_foreign_credential_is_still_refused_after_the_switch(ns, env):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    foreign = type(cred)(env, cred.binding, tool_id=41, config_id=77, reference=False, grants=False)
    child = call_kwargs(payload_of(foreign), parent)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", child, None)
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED" and "context_ref" not in child["meta"]


def test_a_legacy_child_is_untouched_and_reads_nothing_in_off(ns, env):
    cred = seeded(env)
    child = call_kwargs(payload_of(cred), None)
    child["kwargs"]["envelope"] = {"schema_version": 1, "security_profile": "legacy", "required_capabilities": [],
                                   "context_ref": None}
    before = copy.deepcopy(child)
    env.poison()
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", child, env.request()) is None
    env.unpoison()
    assert child == before and SECRET in json.dumps(child)


def test_a_governed_envelope_without_a_context_is_not_a_parent_and_is_left_alone_in_off(ns, env):
    child = call_kwargs(payload_of(seeded(env)), None)   # governed envelope, context_ref None
    before = copy.deepcopy(child)
    env.poison()
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", child, env.request()) is None
    env.unpoison()
    assert child == before   # off never creates an admission


def test_the_inherit_path_costs_off_dict_reads_only():
    text = (PLUGIN_ROOT / "utils" / "admission.py").read_text()
    carries = text[text.index("def _carries_parent_context"):text.index("def admit_dispatch(")]
    assert "db." not in carries and "this." not in carries and "auth." not in carries
    head = text[text.index("def admit_dispatch("):text.index('    payload = call_kwargs.setdefault("kwargs", {})')]
    assert head.index("if not governed:") < head.index("_carries_parent_context(call_kwargs)")


# --- the stash of a fan-out child --------------------------------------------------------------------------------------

def parallel_method():
    """``_parallel_governance_active`` from methods/parallel_dispatch.py, without the rest of the mixin."""
    tree = ast.parse((PLUGIN_ROOT / "methods" / "parallel_dispatch.py").read_text())
    function = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef) and node.name == "_parallel_governance_active")
    function.decorator_list = []
    namespace = {}
    exec(compile(ast.Module([function], []), "parallel_dispatch.py", "exec"), namespace)  # pylint: disable=W0122
    return namespace["_parallel_governance_active"]


@pytest.mark.parametrize("enabled, registered, expected", [
    (True, False, True), (False, True, True), (False, False, False), (True, True, True)])
def test_the_fan_out_looks_after_a_parent_admitted_under_enforce_after_the_switch(enabled, registered, expected):
    module = types.SimpleNamespace(governance_profile=types.SimpleNamespace(enabled=enabled),
                                   governed_handlers_registered=registered)
    assert parallel_method()(module) is expected
    assert parallel_method()(types.SimpleNamespace()) is False   # a module with no governance at all


def test_the_child_stash_is_written_after_the_seal_when_the_child_carries_a_governed_envelope():
    text = (PLUGIN_ROOT / "methods" / "parallel_dispatch.py").read_text()
    part = text[text.index("governed = self._parallel_governance_active() or ("):][:260]
    assert "envelope.get('security_profile') == 'governed'" in part
    stash = text[text.index("governed = self._parallel_governance_active() or ("):]
    before_start = stash[:stash.index("child_task_id = self.task_node.start_task(")]
    after_start = stash[stash.index("child_task_id = self.task_node.start_task("):][:900]
    assert "if not governed:" in before_start and "if governed and child_task_id is not None:" in after_start


# --- the state is read once per dispatch ------------------------------------------------------------------------------

class Flipping:
    """A module whose switch flips after the first read of its state: a dispatch that read it twice would see both."""

    def __init__(self, base, first, then):
        self._base, self._states, self.reads = base, (first, then), 0

    def __getattr__(self, name):
        return getattr(self._base, name)

    @property
    def governance_effective(self):
        self.reads += 1
        return self._states[0] if self.reads == 1 else self._states[1]

    @property
    def governance_profile(self):
        return self.governance_effective.profile


def test_the_wrapper_reads_the_state_once_so_the_envelope_and_the_admission_agree(ns, env):
    env.seed_agent()
    enforce, off = env.module().governance_effective, env.module(governed=False).governance_effective
    module = Flipping(env.module(), enforce, off)   # enforce for the first read, off for every later one
    original = Original()
    wrapper = build_wrapper(ns, module, original)
    wrapper("indexer_agent", args=["s", "m"], kwargs={"project_id": 1, "message_id": 5}, pool="agents",
            meta={"project_id": 1}, admission=env.request())
    assert module.reads == 1
    [(_, sent)] = original.calls
    [row] = contexts(env, ns)   # governed envelope and a governed admission, not one without the other
    assert sent["kwargs"]["envelope"]["security_profile"] == "governed"
    assert sent["kwargs"]["envelope"]["context_ref"] == str(row.id) == sent["meta"]["context_ref"]
    # and the other way round: off first, enforce later - nothing governed at all
    env.reset()
    env.seed_agent()
    module = Flipping(env.module(governed=False), off, enforce)
    original = Original()
    build_wrapper(ns, module, original)("indexer_agent", args=["s", "m"], kwargs={"project_id": 1, "message_id": 5},
                                        pool="agents", meta={"project_id": 1}, admission=env.request())
    [(_, sent)] = original.calls
    assert module.reads == 1 and sent["kwargs"]["envelope"]["security_profile"] == "legacy" and contexts(env, ns) == []


def test_the_wrapper_passes_the_state_it_read_to_the_gate_and_to_admission():
    text = (PLUGIN_ROOT / "module.py").read_text()
    wrapper = text[text.index("def _maintenance_gated_start_task"):text.index("self.task_node.start_task = _maintenance_gated_start_task")]
    assert wrapper.count("self.governance_effective") == 1 and "self.governance_profile" not in wrapper
    assert "effective.profile, self.worker_capabilities" in wrapper and "kwargs, admission, effective)" in wrapper


# --- the events of a run that was admitted earlier --------------------------------------------------------------------

def test_a_refusal_of_an_admitted_run_after_the_switch_carries_the_revision_it_was_admitted_with(ns, env):
    cred = seeded(env)
    _, ref = dispatch(env, ns, cred)   # admitted under enforce with policy revision rev-1
    off = env.module(governed=False)
    off.governed_handlers_registered = True
    assert off.governance_profile.policy_revision is None   # today's profile has none
    env.epochs[42] = {"active": True, "user": 9, "token": 0}   # the user was revoked since
    result = ns.adm.validate_context(off, 1, ref, CALLER)
    assert result["ok"] is False and result["reason_code"] == "EPOCH_MISMATCH"
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["policy_revision"]) == ("deny", "EPOCH_MISMATCH", "rev-1")


def test_a_release_refusal_after_the_switch_carries_the_revision_of_the_run(ns, env):
    cred = seeded(env)
    call, ref = dispatch(env, ns, cred)
    run_to_running(env, ns, ref)
    slot = dict(sealed_tool_settings(call)["credential_slot"])
    env.denied.clear()
    off = env.module(governed=False)
    off.governed_handlers_registered = True
    _, public = ns.keypair()
    request = dict(project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
                   field="credentials", slot={**slot, "decision_id": "11111111-1111-4111-8111-111111111111"},
                   recipient_key=public)
    reply = ns.credential_release.release_credential(off, **request)
    assert reply["ok"] is False and reply["reason_code"] == "CREDENTIAL_REFERENCE_REQUIRED"
    [event] = env.denied
    assert (event["decision"], event["policy_revision"]) == ("deny", "rev-1")


def test_a_release_is_still_served_after_the_switch_only_where_enforce_ran(ns, env):
    cred = seeded(env)
    call, ref = dispatch(env, ns, cred)
    run_to_running(env, ns, ref)
    slot = dict(sealed_tool_settings(call)["credential_slot"])
    _, public = ns.keypair()
    request = dict(project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
                   field="credentials", slot=slot, recipient_key=public)
    never = env.module(governed=False)   # a process that never ran enforce and has nothing live
    assert ns.credential_release.release_credential(never, **request)["reason_code"] == "STALE_CONTEXT"
    served = env.module(governed=False)
    served.governed_handlers_registered = True
    ns.need_transport()
    assert ns.credential_release.release_credential(served, **request)["ok"] is True


def test_a_run_deadline_event_carries_the_revision_of_the_run(ns, env):
    cred = seeded(env)
    _, ref = dispatch(env, ns, cred)
    run_to_running(env, ns, ref)
    module = env.module(governed=False)
    module.governed_handlers_registered = True
    ns.adm._revoke_overrun(module, 1, ref, "RUNTIME_LIMIT_EXCEEDED")   # pylint: disable=W0212
    [event] = env.denied
    assert (event["reason_code"], event["policy_revision"]) == ("RUNTIME_LIMIT_EXCEEDED", "rev-1")


def test_a_deny_of_today_still_carries_todays_revision_and_a_stamped_field_wins(ns, env):
    module = env.module()
    ns.adm._record_deny(module, "IDENTITY_REVOKED", project_id=1)   # pylint: disable=W0212
    ns.adm._record_deny(module, "IDENTITY_REVOKED", project_id=1, policy_revision="rev-0")   # pylint: disable=W0212
    assert [event["policy_revision"] for event in env.denied] == ["rev-1", "rev-0"]


# --- the corporate Vault is never imported in off ----------------------------------------------------------------------

class ForbidCorporateVault(importlib.abc.MetaPathFinder):
    """Fails the import of the corporate Vault module, wherever it comes from."""

    def __init__(self):
        self.attempts = []

    def find_spec(self, fullname, path=None, target=None):
        if "corporate_vault" in fullname:
            self.attempts.append(fullname)
            raise AssertionError(f"{fullname} was imported")
        return None


def test_the_import_hook_really_catches_an_import_of_the_corporate_vault():
    hook = ForbidCorporateVault()
    sys.meta_path.insert(0, hook)
    try:
        with pytest.raises(AssertionError):
            importlib.import_module("corporate_vault")
    finally:
        sys.meta_path.remove(hook)
    assert hook.attempts == ["corporate_vault"]


def test_an_off_dispatch_imports_nothing_of_the_corporate_vault(ns, env):
    from fixtures.dispatch_wrapper import run_matrix  # pylint: disable=C0415
    hook = ForbidCorporateVault()
    sys.meta_path.insert(0, hook)
    try:
        run_matrix(ns, env.module(governed=False), admission=env.request())
    finally:
        sys.meta_path.remove(hook)
    assert hook.attempts == [] and not [name for name in sys.modules if "corporate_vault" in name]
