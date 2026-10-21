"""The real ``_maintenance_gated_start_task`` of module.py, run against the real gates (#6756, #7120).

``build_wrapper`` cuts the wrapper source out of ``module.py`` and runs it with the plugin's own gate, run-id and
admission code, so the tests prove what the shipped wrapper does. ``dispatch_matrix`` builds the four payload kinds
the off invariant is proven on (chat, predict, pipeline, test tool).
"""
import copy
import sys
import textwrap
import types
from pathlib import Path

from fixtures import admission_env

PLUGIN_ROOT = Path(__file__).resolve().parents[2]


class Original:
    """Stands in for the arbiter ``start_task``: records every call it receives, deep-copied."""

    def __init__(self, task_id="task-1"):
        self.calls, self.task_id = [], task_id

    def __call__(self, *args, **kwargs):
        self.calls.append((args, copy.deepcopy(kwargs)))
        return self.task_id


def build_wrapper(ns, module, original):
    source = (PLUGIN_ROOT / "module.py").read_text()
    text = source[source.index("def _maintenance_gated_start_task"):
                  source.index("self.task_node.start_task = _maintenance_gated_start_task")]
    text = textwrap.dedent("        " + text).replace("from .utils.", f"from {admission_env.PKG}.utils.")
    gate = types.ModuleType(f"{admission_env.PKG}.utils.maintenance_gate")
    gate.is_maintenance_active = lambda: False
    budget = types.ModuleType(f"{admission_env.PKG}.utils.budget_door")
    budget.closed_budget_scope = lambda *a, **k: None
    budget.dispatch_may_use_llm = lambda kwargs: False
    budget.dispatch_owner = lambda kwargs: (None, None)
    budget.dispatch_uses_own_model = lambda *a, **k: False
    guardrail = types.ModuleType(f"{admission_env.PKG}.utils.input_guardrail")
    guardrail.apply_input_guardrail = lambda kwargs: None   # inert, as with input_guardrail.enabled false (#6886)
    sys.modules[gate.__name__], sys.modules[budget.__name__] = gate, budget
    sys.modules[guardrail.__name__] = guardrail
    module.worker_capabilities = types.SimpleNamespace(has_capable_worker=lambda required: True)
    namespace = {
        "self": module, "log": ns.log, "_original_start_task": original,
        "apply_envelope_gate": ns.caps.apply_envelope_gate, "stamp_predict_run_id": ns.run_id.stamp_predict_run_id,
        "admit_dispatch": ns.adm.admit_dispatch, "track_context": ns.adm.track_context,
    }
    exec(compile(text, "module.py", "exec"), namespace)  # pylint: disable=W0122
    return namespace["_maintenance_gated_start_task"]


def _toolkit(tool_id, kind, settings):
    return {"id": tool_id, "type": kind, "name": f"{kind}-{tool_id}", "settings": settings,
            "selected_tools": ["run"], "toolkit_name": f"{kind}-{tool_id}"}


def dispatch_matrix():
    """``{name: (args, kwargs)}``: a dispatch of each kind, every id fixed so the run id is deterministic."""
    configuration = {"configuration_uuid": "11111111-1111-4111-8111-111111111111", "configuration_project_id": 1,
                     "configuration_type": "jira", "elitea_title": "jira-main"}
    chat = {"project_id": 1, "message_id": 5, "stream_id": "s-1", "version_id": 70, "application_id": 7,
            "instructions": "Be brief.", "tools": [_toolkit(3, "jira", {"jira_configuration": configuration})]}
    predict = {"project_id": 1, "message_id": 6, "execution_generation": 1, "version_id": 70,
               "tools": [_toolkit(4, "github", {"token": "{{secret.gh}}"})], "user_input": "hi"}
    pipeline = {"project_id": 1, "message_id": 7, "version_id": 71, "yaml": "nodes: []", "tools": []}
    test_tool = {"project_id": 1, "message_id": 8, "tool_name": "run", "toolkit_config": {"settings": {"k": "v"}}}
    return {
        "chat": (("indexer_agent",), {"args": ["thread", 5], "kwargs": chat, "pool": "agents",
                                      "meta": {"project_id": 1, "task_name": "indexer_agent"}}),
        "predict": (("indexer_predict_agent",), {"args": [], "kwargs": predict, "pool": "agents",
                                                 "meta": {"project_id": 1, "task_name": "indexer_predict_agent"}}),
        "pipeline": (("indexer_agent",), {"args": ["thread", 7], "kwargs": pipeline, "pool": "agents",
                                          "meta": {"project_id": 1, "task_name": "indexer_agent",
                                                   "entity": "pipeline"}}),
        "test_tool": (("indexer_test_toolkit_tool",), {"args": [], "kwargs": test_tool, "pool": "indexer",
                                                       "meta": {"project_id": 1}}),
    }


def run_matrix(ns, module, *, admission=None):
    """Dispatch every kind through the wrapper; ``{name: [(args, kwargs), ...]}`` as the arbiter would see it."""
    out = {}
    for name, (args, kwargs) in dispatch_matrix().items():
        original = Original()
        wrapper = build_wrapper(ns, module, original)
        kwargs = dict(kwargs)
        if admission is not None:
            kwargs["admission"] = admission
        assert wrapper(*args, **kwargs) == "task-1"
        out[name] = original.calls
    return out
