"""Load the real agent identity modules (#6756) against a real SQLAlchemy ``Base``.

The model, digest, state and schema files are loaded from disk, not re-implemented, so the tests
compile the statements and DDL the plugin would send. Only the Pylon runtime (``pylon``, ``tools``)
is faked. The caller saves ``sys.modules`` before and restores it after (see the fixtures in the
``test_6756_*`` files), because the stubs here must not leak into other suites.
"""
import importlib
import sys
import types
from pathlib import Path

from fixtures.helpers import load_module_with_stubs

PKG = "plugins.elitea_core"
LOG = types.SimpleNamespace(
    info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None,
    debug=lambda *a, **k: None, exception=lambda *a, **k: None,
)


_REAL_SQLALCHEMY: dict = {}
_SQLALCHEMY_MODULES = (
    "sqlalchemy", "sqlalchemy.orm", "sqlalchemy.schema", "sqlalchemy.sql.dml", "sqlalchemy.sql.selectable",
    "sqlalchemy.dialects.postgresql", "sqlalchemy.dialects.sqlite", "sqlalchemy.ext.compiler",
)


def snapshot_sqlalchemy():
    """Remember the real SQLAlchemy package while it is still intact (called from ``conftest.py`` before any
    suite installs stand-ins). The package is never imported twice: its compiled extensions keep references to
    the first import's classes (``InternalTraversal`` and friends), so a second import is inconsistent with
    them and every statement fails at cache-key generation on SQLAlchemy 2.1."""
    if _REAL_SQLALCHEMY:
        return
    for name in _SQLALCHEMY_MODULES:
        importlib.import_module(name)
    _REAL_SQLALCHEMY.update(
        (name, module) for name, module in sys.modules.items()
        if name == "sqlalchemy" or name.startswith("sqlalchemy."))


def fresh_sqlalchemy():
    """A clean, consistent SQLAlchemy for these tests. Other suites leave ``types.ModuleType`` stand-ins
    for ``sqlalchemy.*`` in ``sys.modules`` and evict half of the real package, so every ``sqlalchemy*``
    entry is dropped and the snapshot of the real package is put back (the caller restores afterwards)."""
    snapshot_sqlalchemy()
    for name in [n for n in sys.modules if n == "sqlalchemy" or n.startswith("sqlalchemy.")]:
        del sys.modules[name]
    sys.modules.update(_REAL_SQLALCHEMY)
    modules = {name: sys.modules[name] for name in (
        "sqlalchemy", "sqlalchemy.orm", "sqlalchemy.dialects.postgresql", "sqlalchemy.schema")}
    return types.SimpleNamespace(
        Table=modules["sqlalchemy"].Table, postgresql=modules["sqlalchemy.dialects.postgresql"],
        CreateTable=modules["sqlalchemy.schema"].CreateTable, CreateIndex=modules["sqlalchemy.schema"].CreateIndex,
        declarative_base=modules["sqlalchemy.orm"].declarative_base,
    )


def install(plugin_root: Path) -> types.SimpleNamespace:
    sa = fresh_sqlalchemy()
    for name in ("plugins", PKG, f"{PKG}.models", f"{PKG}.utils"):
        pkg = types.ModuleType(name)
        pkg.__path__ = []
        sys.modules[name] = pkg
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = LOG
    sys.modules.update({
        "pylon": types.ModuleType("pylon"), "pylon.core": types.ModuleType("pylon.core"),
        "pylon.core.tools": pylon_tools,
    })
    tools = types.ModuleType("tools")
    tools.db = types.SimpleNamespace(Base=sa.declarative_base(), get_session=None)
    tools.db_tools = types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {}))
    tools.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant", POSTGRES_SCHEMA="public")
    sys.modules["tools"] = tools

    def load(kind, name):
        return load_module_with_stubs(plugin_root / kind / f"{name}.py", f"{PKG}.{kind}.{name}")

    return types.SimpleNamespace(
        sa=sa, tools=tools,
        models=load("models", "agent_identity"),
        digest=load("utils", "definition_digest"),
        state=load("utils", "binding_state"),
        schema=load("utils", "agent_identity_schema"),
    )
