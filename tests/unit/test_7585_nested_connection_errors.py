"""#7585: a sub-agent's connection errors reach its parent's validation in the structured shape a toolkit's own have.

A sub-agent whose toolkit only needs a sign-in (or cannot reach its service) used to surface on the parent as
"Application misconfiguration error …" text, which the agent card shows as a generic misconfiguration. The real
toolkit loop and the nested wrap are lifted out of the module with ``ast`` (it cannot be imported without Pylon);
only the version lookup is replaced by a tree of made-up agents.
"""
import ast
import json
import pathlib
import textwrap
import types

import pytest
from pydantic import ValidationError

pytestmark = pytest.mark.unit

SOURCE_PATH = pathlib.Path(__file__).resolve().parents[2] / "utils" / "application_utils.py"
SOURCE = SOURCE_PATH.read_text()
TREE = ast.parse(SOURCE)
LIFTED = ("governed_refusal_of", "_nested_governed_refusal", "_nested_connection_errors", "_toolkit_connection_entry",
          "_validate_toolkit_type_application", "_validate_version_toolkits")
SENTINEL = "__connection_errors__"
PREFIX = "Value error, "

SIGN_IN = {"configuration_title": "repro_dataverse", "configuration_type": "dataverse",
           "message": "Sign in to Dataverse to connect this credential.", "requires_authorization": True,
           "auth_metadata": {"server_url": "https://login.example.test/tenant"}}
WRONG_SECRET = {"configuration_title": "repro_jira", "configuration_type": "jira",
                "message": "The client secret is wrong or expired.", "requires_authorization": False,
                "auth_metadata": None}
REFUSED = {"configuration_title": "repro_sharepoint", "configuration_type": "sharepoint",
           "message": "Sign in with Microsoft again, then retry.", "code": "DENIED_ASSERTION_UNAVAILABLE",
           "requires_authorization": False, "auth_metadata": None}


class ToolValidatedDetails:
    CONNECTION_ERROR_SENTINEL = SENTINEL


def _namespace(tree):
    namespace = {"json": json, "ValidationError": ValidationError, "ToolValidatedDetails": ToolValidatedDetails,
                 "log": types.SimpleNamespace(debug=lambda *_a, **_k: None),
                 "application_ids_to_names": lambda _session, app_id, _version_id: (tree[app_id]["name"], "base")}
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in LIFTED:
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102

    def validate_application_version_details(_project_id, application_id, _version_id, _user_id, _visited=None):
        node = tree[application_id]
        if "raises" in node:
            raise node["raises"]
        entries = [namespace["_toolkit_connection_entry"](tool_id, None, errors)
                   for tool_id, errors in node.get("connection_errors", {}).items()]
        entries += node.get("settings_errors", [])
        if "child" in node:
            tool = {"id": node["tool_id"], "type": "application",
                    "settings": {"application_id": node["child"], "application_version_id": 1}}
            namespace["_validate_version_toolkits"](None, [tool], 5, 9, _visited if _visited is not None else set(),
                                                    entries, [])
        if entries:
            raise ValidationError.from_exception_data("ApplicationVersionDetailToolValidatedModel", entries)
        return True

    namespace["validate_application_version_details"] = validate_application_version_details
    return namespace


def _api_messages(tree, application_id):
    """The ``toolkit_errors[].msg`` the version validator API returns for an agent of the tree."""
    try:
        _namespace(tree)["validate_application_version_details"](5, application_id, 1, 9)
    except ValidationError as ex:
        return [err["msg"] for err in ex.errors(include_url=False, include_context=False, include_input=False)]
    return []


def _payload(message):
    assert message.startswith(PREFIX)
    return json.loads(message[len(PREFIX):])


def _tree(child, parent_name="Parent agent", grandparent_name=None):
    tree = {1: {"name": "Dataverse child", **child}, 2: {"name": parent_name, "child": 1, "tool_id": 149}}
    if grandparent_name:
        tree[3] = {"name": grandparent_name, "child": 2, "tool_id": 150}
    return tree


def _signed_message(*names):
    return ": ".join([*names, SIGN_IN["message"]])


def test_the_sub_agents_own_validation_is_unchanged():
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN]}}), 1)
    assert message == PREFIX + json.dumps({SENTINEL: [SIGN_IN]})


def test_a_parent_gets_the_sub_agents_sign_in_error_with_the_sub_agents_name():
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN]}}), 2)
    assert _payload(message) == {SENTINEL: [{**SIGN_IN, "message": _signed_message("Dataverse child")}]}


def test_every_nesting_level_adds_its_agents_name_and_keeps_the_payload_json():
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN]}}, grandparent_name="Grandparent"), 3)
    assert _payload(message) == {SENTINEL: [{**SIGN_IN, "message": _signed_message("Parent agent", "Dataverse child")}]}


@pytest.mark.parametrize("name", ["O'Brien's agent", 'The "quoted" agent', "Агент 7585 ✓"])
def test_any_agent_name_keeps_the_payload_json(name):
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN]}}, parent_name=name, grandparent_name="G"), 3)
    assert _payload(message)[SENTINEL][0]["message"] == _signed_message(name, "Dataverse child")


def test_every_connection_error_of_every_toolkit_is_carried_in_order():
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN, WRONG_SECRET], 151: [SIGN_IN]}}), 2)
    assert [(error["configuration_type"], error["requires_authorization"]) for error in _payload(message)[SENTINEL]] \
        == [("dataverse", True), ("jira", False), ("dataverse", True)]
    assert _payload(message)[SENTINEL][1]["message"] == "Dataverse child: The client secret is wrong or expired."


@pytest.mark.parametrize("missing", [{"message": ""}, {"message": None}, {}])
def test_a_connection_error_without_text_reads_like_the_toolkit_cards_fallback(missing):
    error = {key: value for key, value in SIGN_IN.items() if key != "message"} | missing
    [message] = _api_messages(_tree({"connection_errors": {148: [error]}}), 2)
    assert _payload(message)[SENTINEL][0]["message"] == "Dataverse child: Connection error"


def test_a_real_misconfiguration_beside_a_sign_in_error_stays_a_misconfiguration():
    settings_error = {"type": "value_error", "loc": ("tools", 152, "__root__"), "input": None,
                      "ctx": {"error": "settings.url Field required"}}
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN]}, "settings_errors": [settings_error]}), 2)
    assert message.startswith(PREFIX + "Application misconfiguration error for agent_name='Dataverse child'")


def test_a_governed_refusal_still_wins_over_a_sign_in_error():
    [message] = _api_messages(_tree({"connection_errors": {148: [SIGN_IN], 153: [REFUSED]}}), 2)
    assert _payload(message) == {"code": REFUSED["code"], "error": f"Dataverse child: {REFUSED['message']}"}


@pytest.mark.parametrize("raised", [RuntimeError("Application version data validation error: bad"),
                                    ValueError("Application toolkit missing application_id")])
def test_a_sub_agent_that_fails_without_a_validation_error_stays_a_misconfiguration(raised):
    [message] = _api_messages(_tree({"raises": raised}), 2)
    assert message.startswith(PREFIX + "Application misconfiguration error for agent_name='Dataverse child'")
