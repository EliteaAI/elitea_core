"""#7305 (C9-SEC-72): index schedules are written only by the schedule endpoints, which record who saved each one.

Every toolkit create, import or update validated by ``ToolCreateModel`` (the agent version create with ``tools``, the
``add_application_tool`` RPC, the import wizard, the toolkit PUT, which then puts the stored schedules back) drops the
``indexes_meta`` a caller sent. The real ``models/pd/tool.py`` with its heavy imports stubbed.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    return mod


@pytest.fixture(scope="module")
def tool_models():
    saved = dict(sys.modules)
    noop = lambda *a, **k: None  # noqa: E731
    for pkg in ("plugins", "plugins.elitea_core", "plugins.elitea_core.models", "plugins.elitea_core.models.pd",
                "plugins.elitea_core.models.enums", "plugins.elitea_core.utils"):
        package = types.ModuleType(pkg)
        package.__path__ = []
        sys.modules[pkg] = package
    log = types.SimpleNamespace(info=noop, error=noop, warning=noop, debug=noop, exception=noop)
    sys.modules["pylon"] = _module("pylon")
    sys.modules["pylon.core"] = _module("pylon.core")
    sys.modules["pylon.core.tools"] = _module("pylon.core.tools", log=log)
    sys.modules["tools"] = _module(
        "tools", auth=types.SimpleNamespace(current_user=lambda: {"id": 1}), serialize=lambda value: value,
        db=types.SimpleNamespace(get_session=noop),
        this=types.SimpleNamespace(module=types.SimpleNamespace(toolkit_settings_validator=lambda *a, **k: {"ok": True})))
    from pydantic import BaseModel, ConfigDict  # pylint: disable=C0415

    class _Permissive(BaseModel):
        model_config = ConfigDict(extra="allow", from_attributes=True)

    stubs = {
        "plugins.elitea_core.models.all": {"Application": type("A", (), {}), "ApplicationVersion": type("V", (), {})},
        "plugins.elitea_core.models.pd.collection_base": {"AuthorBaseModel": _Permissive},
        "plugins.elitea_core.utils.authors": {"get_authors_data": lambda **k: []},
        "plugins.elitea_core.utils.toolkits_utils": {"get_mcp_schemas": lambda *a, **k: {}},
        "plugins.elitea_core.utils.application_tools": {
            "expand_toolkit_settings": lambda type_, settings, project_id, user_id: settings,
            "ValidatorNotSupportedError": type("ValidatorNotSupportedError", (Exception,), {}),
            "ConfigurationExpandError": type("ConfigurationExpandError", (Exception,), {}),
            "raise_validation_error_if_any": noop,
            "find_suggested_toolkit_name_field": lambda **k: None,
            "find_suggested_toolkit_max_length": lambda **k: None,
        },
    }
    for name, attrs in stubs.items():
        sys.modules[name] = _module(name, **attrs)
    for dotted, relative in (("plugins.elitea_core.models.enums.all", "models/enums/all.py"),
                             ("plugins.elitea_core.models.pd.tool", "models/pd/tool.py")):
        spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / relative)
        module = importlib.util.module_from_spec(spec)
        sys.modules[dotted] = module
        spec.loader.exec_module(module)
    yield sys.modules["plugins.elitea_core.models.pd.tool"]
    sys.modules.clear()
    sys.modules.update(saved)


PLANTED = {"mcp": True, "indexes_meta": {"docs": {"schedules": {"5": {"cron": "* * * * *", "created_by": 5}}}}}


@pytest.mark.parametrize("model", ["ToolCreateModel", "ToolImportModel", "ToolAPIUpdateModel"])
def test_a_created_imported_or_updated_toolkit_carries_no_index_schedules_a_caller_sent(tool_models, model):
    tool = getattr(tool_models, model).model_validate({
        "type": "github", "name": "Github", "settings": {"repository": "EliteaAI/x"}, "meta": dict(PLANTED),
        "author_id": 3, "project_id": 2, "user_id": 3})
    assert tool.meta == {"mcp": True}
    assert PLANTED["indexes_meta"]   # the caller's dict is not changed in place


def test_the_import_wizard_fallback_drops_them_too():
    source = (PLUGIN_ROOT / "rpc" / "import_wizzard.py").read_text()
    body = source[source.index("def _create_minimal_toolkit("):source.index("toolkit = EliteATool(",
                                                                          source.index("def _create_minimal_toolkit("))]
    assert "if key != 'indexes_meta'" in body
