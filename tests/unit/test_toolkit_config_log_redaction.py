"""Debug logging of toolkit configs in ``rpc/application.py`` must never carry credential values.

By the time ``Expanded toolkit_config`` is logged, ``expand_toolkit_settings`` has resolved
``{{secret.*}}`` references, so the settings hold plain client secrets, API keys and refresh
tokens. The helper is lifted out of the module with ``ast`` (the module itself cannot be imported
without Pylon) and the two log call sites are checked to route through it.
"""

import ast
import pathlib
import textwrap

import pytest

pytestmark = pytest.mark.unit

APPLICATION_PY = pathlib.Path(__file__).resolve().parents[2] / "rpc" / "application.py"
SOURCE = APPLICATION_PY.read_text()
TREE = ast.parse(SOURCE)


def _load_helper():
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_toolkit_config_log_summary":
            namespace = {}
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
            return namespace["_toolkit_config_log_summary"]
    raise AssertionError("_toolkit_config_log_summary not found in rpc/application.py")


summary = _load_helper()


def test_expanded_config_keeps_identity_and_setting_names_only():
    expanded = {
        "id": 16123,
        "type": "dataverse",
        "toolkit_name": "OneCRM Dataverse",
        "settings": {
            "selected_tools": ["get_record"],
            "dataverse_configuration": {
                "url": "https://org.crm4.dynamics.com",
                "client_secret": "plain-client-secret",
                "refresh_token": "plain-refresh-token",
            },
        },
    }
    result = summary(expanded)

    assert result == {
        "id": 16123,
        "type": "dataverse",
        "toolkit_name": "OneCRM Dataverse",
        "settings_keys": ["dataverse_configuration", "selected_tools"],
    }
    rendered = repr(result)
    assert "plain-client-secret" not in rendered
    assert "plain-refresh-token" not in rendered


def test_original_config_uses_toolkit_id_key():
    # The incoming (pre-expansion) config carries toolkit_id rather than id.
    result = summary({"toolkit_id": 7, "type": "github", "settings": {"github_configuration": {"token": "x"}}})
    assert result["id"] == 7
    assert result["settings_keys"] == ["github_configuration"]
    assert "'x'" not in repr(result)


@pytest.mark.parametrize("value", [None, {}, "not-a-dict", {"settings": None}, {"settings": ["a"]}])
def test_malformed_configs_do_not_raise(value):
    result = summary(value)
    assert result["settings_keys"] == []


def _toolkit_config_debug_calls():
    calls = []
    for node in ast.walk(TREE):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "debug" and node.args
                and isinstance(node.args[0], ast.Constant)
                and str(node.args[0].value).endswith("toolkit_config: %s")):
            calls.append(node)
    return calls


def test_expanded_version_details_are_not_logged_whole():
    # get_application_version_details_expanded returns tools with unsecreted settings; its debug
    # log must name identifiers only, never the whole result dict.
    utils_source = (APPLICATION_PY.parents[1] / "utils" / "application_utils.py").read_text()
    func = next(n for n in ast.walk(ast.parse(utils_source))
                if isinstance(n, ast.FunctionDef) and n.name == "get_application_version_details_expanded")
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "debug":
            for arg in node.args[1:]:
                assert not (isinstance(arg, ast.Name) and arg.id == "result"), f"line {node.lineno} logs result"
            first = node.args[0] if node.args else None
            if isinstance(first, ast.JoinedStr):
                names = {v.value.id for v in first.values
                         if isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name)}
                assert "result" not in names, f"line {node.lineno} logs result"


def test_toolkit_config_debug_logs_go_through_the_summary():
    calls = _toolkit_config_debug_calls()
    assert {c.args[0].value for c in calls} == {"Original toolkit_config: %s", "Expanded toolkit_config: %s"}
    for call in calls:
        arg = call.args[1]
        assert isinstance(arg, ast.Call) and getattr(arg.func, "id", None) == "_toolkit_config_log_summary", (
            f"line {call.lineno} logs a toolkit config without _toolkit_config_log_summary"
        )
