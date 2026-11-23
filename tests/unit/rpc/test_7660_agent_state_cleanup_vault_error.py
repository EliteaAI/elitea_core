"""elitea_issues#7660: the connstr lookup now also reads a project's hidden secrets; a vault error there must skip
that project, not abort the nightly cleanup of every project after it."""
import sys
import types

from unit._tool_spend_loader import load


def test_a_vault_error_skips_only_that_project(monkeypatch):
    emitted = []

    def lookup(project_id):
        if project_id == 1:
            raise RuntimeError("kv-for-hidden-1: forbidden")
        return f"conn-{project_id}"

    def stub(name, **attrs):
        module = types.ModuleType(name)
        vars(module).update(attrs)
        monkeypatch.setitem(sys.modules, name, module)

    stub("plugins.elitea_core.utils.project_utils", get_all_project_ids=lambda: [1, 2])
    stub("plugins.elitea_core.utils.vectorstore", get_pgvector_connection_string=lookup)
    stub("plugins.elitea_core.utils.schedule_runs", has_waiting_runs=lambda _id: False,
         approval_wait_hours=lambda: 24)
    stub("pylon.core.tools", web=types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f)),
         log=types.SimpleNamespace(exception=lambda *a, **k: None))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.rpc.agent_state_cleanup", None)
    rpc = load("rpc/agent_state_cleanup.py", fresh=True).RPC()
    rpc.event_node = types.SimpleNamespace(emit=lambda _name, payload: emitted.append(payload))

    rpc.empty_state()

    assert [payload["pgvector_connstr"] for payload in emitted] == ["conn-2"]
