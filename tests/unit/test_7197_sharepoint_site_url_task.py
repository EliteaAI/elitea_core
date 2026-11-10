"""#7197: migrate_sharepoint_site_url_to_toolkit copies the credential's site_url onto SharePoint toolkits."""
import importlib.util
import os
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_6724_chat_templates import _make_migration_stubs  # noqa: E402  pylint: disable=C0413


@pytest.fixture
def admin_mod(monkeypatch):
    for name, module in _make_migration_stubs({}).items():
        monkeypatch.setitem(sys.modules, name, module)
    for pkg in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                "elitea_core.methods"):
        if pkg not in sys.modules:
            package = types.ModuleType(pkg)
            package.__path__ = [str(PLUGIN_ROOT)]
            monkeypatch.setitem(sys.modules, pkg, package)
    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"))
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "elitea_core.methods"
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    return mod


class Col:
    """``Model.type`` in a filter: the fake session ignores the expression and returns its rows."""
    type = "sharepoint"


class Toolkit(Col):
    def __init__(self, id_, settings):
        self.id, self.settings = id_, settings


class Credential(Col):
    def __init__(self, title, data):
        self.elitea_title, self.data = title, data


class Session:
    def __init__(self, toolkits, credentials):
        self.rows = {Toolkit: toolkits, Credential: credentials}
        self.committed = False

    def query(self, model):
        rows = self.rows[model]
        return types.SimpleNamespace(filter=lambda *_: types.SimpleNamespace(all=lambda: rows))

    def commit(self):
        self.committed = True


@pytest.fixture(autouse=True)
def _no_orm_state(monkeypatch):
    """The fakes are not mapped instances, so flag_modified has nothing to flag."""
    monkeypatch.setattr("sqlalchemy.orm.attributes.flag_modified", lambda *_: None)


def _run(admin_mod, toolkits, credentials, dry_run=False):
    session = Session(toolkits, credentials)

    @contextmanager
    def project_session(_pid):
        yield session

    db = types.SimpleNamespace(with_project_schema_session=project_session)
    result = admin_mod._run_sharepoint_site_url_migration(1, dry_run, "", db, Toolkit, Credential)
    return result, session


SITE = "https://t.sharepoint.com/sites/a"


def _toolkit(id_, **settings):
    return Toolkit(id_, {"sharepoint_configuration": {"elitea_title": "cred"}, **settings})


def test_copies_the_credential_site_url_and_leaves_the_credential_alone(admin_mod):
    toolkit, cred = _toolkit(1), Credential("cred", {"site_url": SITE, "client_id": "c"})
    result, session = _run(admin_mod, [toolkit], [cred])
    assert result == {"toolkits_migrated": 1, "toolkits_skipped": 0}
    assert toolkit.settings["site_url"] == SITE and session.committed
    assert cred.data["site_url"] == SITE   # other toolkits and the run-time fallback still need it


def test_two_toolkits_of_one_credential_both_get_the_site(admin_mod):
    a, b = _toolkit(1), _toolkit(2)
    result, _ = _run(admin_mod, [a, b], [Credential("cred", {"site_url": SITE})])
    assert result["toolkits_migrated"] == 2 and a.settings["site_url"] == b.settings["site_url"] == SITE


def test_idempotent_and_never_overwrites_a_toolkit_site(admin_mod):
    toolkit = _toolkit(1, site_url="https://t.sharepoint.com/sites/mine")
    result, _ = _run(admin_mod, [toolkit], [Credential("cred", {"site_url": SITE})])
    assert result == {"toolkits_migrated": 0, "toolkits_skipped": 1}
    assert toolkit.settings["site_url"].endswith("/mine")


@pytest.mark.parametrize("credentials", [[], [Credential("cred", {})], [Credential("cred", {"site_url": "  "})]])
def test_missing_credential_or_site_is_skipped(admin_mod, credentials):
    toolkit = _toolkit(1)
    result, _ = _run(admin_mod, [toolkit], credentials)
    assert result == {"toolkits_migrated": 0, "toolkits_skipped": 1} and "site_url" not in toolkit.settings


def test_dry_run_changes_and_commits_nothing(admin_mod):
    toolkit = _toolkit(1)
    result, session = _run(admin_mod, [toolkit], [Credential("cred", {"site_url": SITE})], dry_run=True)
    assert result["toolkits_migrated"] == 1
    assert "site_url" not in toolkit.settings and not session.committed
