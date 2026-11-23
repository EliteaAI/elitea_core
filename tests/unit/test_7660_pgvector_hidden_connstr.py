"""elitea_issues#7660: the worker now trusts Main's pre-fork connstr, None included, so Main must find the secret
wherever the secrets API does: a hidden ``pgvector_project_connstr`` used to be missed and its runs lost their memory.
"""

import importlib.util
import pathlib
import sys
import types

import pytest

VECTORSTORE = pathlib.Path(__file__).resolve().parents[2] / "utils" / "vectorstore.py"
CONNSTR = "postgresql://project"


class InvalidPath(Exception):
    """hvac's "nothing at this path"; CI has no hvac."""


def _lookup(monkeypatch, secrets, hidden, **options):
    reads = []

    class Vault:
        def __init__(self, project_id):
            self.project_id = project_id

        def get_secrets(self):
            reads.append("secrets")
            if isinstance(secrets, Exception):
                raise secrets
            return secrets

        def get_project_hidden_secrets(self):
            reads.append("hidden")
            if isinstance(hidden, Exception):
                raise hidden
            return hidden

    monkeypatch.setitem(sys.modules, "tools", types.SimpleNamespace(VaultClient=Vault))
    monkeypatch.setitem(sys.modules, "pylon.core.tools",
                        types.SimpleNamespace(log=types.SimpleNamespace(debug=lambda *a, **k: None, warning=lambda *a, **k: None)))
    monkeypatch.setitem(sys.modules, "hvac", types.ModuleType("hvac"))
    monkeypatch.setitem(sys.modules, "hvac.exceptions", types.SimpleNamespace(InvalidPath=InvalidPath))
    spec = importlib.util.spec_from_file_location("vectorstore_7660", VECTORSTORE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.get_pgvector_connection_string(2, **options), reads


@pytest.mark.unit
def test_a_hidden_connstr_is_found(monkeypatch):
    assert _lookup(monkeypatch, {}, {"pgvector_project_connstr": CONNSTR})[0] == CONNSTR


@pytest.mark.unit
def test_a_regular_connstr_never_reads_the_hidden_store(monkeypatch):
    assert _lookup(monkeypatch, {"pgvector_project_connstr": CONNSTR}, {}) == (CONNSTR, ["secrets"])


@pytest.mark.unit
def test_no_connstr_anywhere_is_none(monkeypatch):
    assert _lookup(monkeypatch, {}, {})[0] is None


@pytest.mark.unit
def test_a_never_written_hidden_path_is_no_connstr(monkeypatch):
    assert _lookup(monkeypatch, {}, InvalidPath("kv-for-hidden-2"))[0] is None


@pytest.mark.unit
@pytest.mark.parametrize("failure", [ConnectionError("vault timeout"), PermissionError("forbidden"), KeyError("x")])
def test_any_other_hidden_read_failure_raises_so_dispatch_omits_the_key(monkeypatch, failure):
    with pytest.raises(type(failure)):
        _lookup(monkeypatch, {}, failure)


@pytest.mark.unit
def test_an_unreadable_project_vault_still_raises(monkeypatch):
    with pytest.raises(RuntimeError):
        _lookup(monkeypatch, RuntimeError("vault down"), {})


@pytest.mark.unit
def test_a_checkpoint_delete_falls_back_to_the_default_store(monkeypatch):
    assert _lookup(monkeypatch, {}, ConnectionError("vault timeout"), default_on_hidden_failure=True)[0] is None


@pytest.mark.unit
def test_a_checkpoint_delete_still_raises_when_the_project_vault_is_unreadable(monkeypatch):
    with pytest.raises(RuntimeError):
        _lookup(monkeypatch, RuntimeError("vault down"), {}, default_on_hidden_failure=True)
