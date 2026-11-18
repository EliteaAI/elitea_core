"""#7585: a sub-agent's connection errors reach its parent's validation in the structured shape a toolkit's own have.

A sub-agent whose toolkit only needs a sign-in (or cannot reach its service) used to surface on the parent as
"Application misconfiguration error …" text, which the agent card shows as a generic misconfiguration. The real
version validation, its toolkit loop and the nested wrap are lifted out of the module with ``ast`` (it cannot be
imported without Pylon); only the version lookup and the toolkit model are replaced, by a tree of made-up agents and a
model that fails the way ``ToolValidatedDetails`` does.
"""
import ast
import contextvars
import json
import pathlib
import textwrap
import types
from typing import ClassVar

import pydantic
import pytest
from pydantic import ValidationError

pytestmark = pytest.mark.unit

SOURCE_PATH = pathlib.Path(__file__).resolve().parents[2] / "utils" / "application_utils.py"
SOURCE = SOURCE_PATH.read_text()
TREE = ast.parse(SOURCE)
LIFTED = ("connection_errors_from", "governed_refusal_of", "_nested_governed_refusal", "_nested_connection_errors",
          "_toolkit_connection_entry", "_resolved", "_DeferredProbe", "_ProbeBatch", "_validate_toolkit_type_application",
          "_validate_version_toolkits", "validate_application_version_details")
ASSIGNED = ("NOT_CHECKED", "_PROBE_BATCH", "PROBE_BATCH_SIZE")
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


class ToolValidatedDetails(pydantic.BaseModel):
    """Fails like the real model: a settings error first, else the connection check's sentinel ``ValueError``."""
    model_config = pydantic.ConfigDict(extra="allow")
    CONNECTION_ERROR_SENTINEL: ClassVar[str] = SENTINEL
    settings: dict
    project_id: int
    user_id: int

    @pydantic.model_validator(mode="after")
    def check_connection(self, info):
        if self.settings.get("missing"):
            raise ValueError("settings.url Field required")
        errors = self.settings.get("connection_errors")
        if errors and info.context and info.context.get("check_connection"):
            raise ValueError({self.CONNECTION_ERROR_SENTINEL: errors})
        return self


class Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return self.name, other

    __hash__ = object.__hash__


class ApplicationVersion:
    id = Column("id")
    application_id = Column("application_id")
    tools = tool_mappings = variables = tags = skill_mappings = None


class VersionNotFound(RuntimeError):
    def __init__(self, application_id, version_id):
        super().__init__(f"No version {version_id} of application {application_id}")


def _session(agents):
    class Version:
        def __init__(self, tools):
            self.tools = tools

        def to_dict(self):
            return {"tools": [json.loads(json.dumps(tool)) for tool in self.tools]}

    class Query:
        def filter(self, *conditions):
            self.version_id = dict(conditions)["id"]
            return self

        def options(self, *_a):
            return self

        def first(self):
            agent = agents[self.version_id]
            return None if agent["tools"] is None else Version(agent["tools"])

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def query(self, _model):
            return Query()

    return Session()


def _api_messages(agents, application_id):
    """The ``toolkit_errors[].msg`` the version validator API returns for an agent of the tree."""
    namespace = {"json": json, "contextvars": contextvars, "ValidationError": ValidationError,
                 "ToolValidatedDetails": ToolValidatedDetails, "ApplicationVersion": ApplicationVersion,
                 "ApplicationVersionNonFoundError": VersionNotFound, "selectinload": lambda column: column,
                 "ApplicationVersionDetailToolValidatedModel": types.SimpleNamespace(model_validate=lambda data: data),
                 "this": types.SimpleNamespace(module=types.SimpleNamespace(governance_profile=None)),
                 "db": types.SimpleNamespace(get_session=lambda _project_id: _session(agents)),
                 "log": types.SimpleNamespace(debug=lambda *_a, **_k: None, warning=lambda *_a, **_k: None),
                 "application_ids_to_names": lambda _session, app_id, _version_id: (agents[app_id]["name"], "base")}
    for node in TREE.body:
        named = isinstance(node, ast.Assign) and any(getattr(t, "id", None) in ASSIGNED for t in node.targets)
        if named or (isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in LIFTED):
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
    try:
        # an empty chain skips the top-level sub-agent tree check, which lives in another module
        namespace["validate_application_version_details"](5, application_id, application_id, 9, _visited=set())
    except ValidationError as ex:
        return [err["msg"] for err in ex.errors(include_url=False, include_context=False, include_input=False)]
    return []


def _payload(message):
    assert message.startswith(PREFIX)
    return json.loads(message[len(PREFIX):])


def toolkit(tool_id, *connection_errors, missing=False):
    settings = {"connection_errors": list(connection_errors), "missing": missing}
    return {"id": tool_id, "type": "dataverse", "settings": settings}


def agent_tool(tool_id, application_id):
    return {"id": tool_id, "type": "application",
            "settings": {"application_id": application_id, "application_version_id": application_id}}


def _tree(*child_tools, parent_name="Parent agent", grandparent_name=None):
    agents = {1: {"name": "Dataverse child", "tools": list(child_tools)},
              2: {"name": parent_name, "tools": [agent_tool(149, 1)]}}
    if grandparent_name:
        agents[3] = {"name": grandparent_name, "tools": [agent_tool(150, 2)]}
    return agents


def _signed_message(*names):
    return ": ".join([*names, SIGN_IN["message"]])


def test_the_sub_agents_own_validation_is_unchanged():
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN)), 1)
    assert message == PREFIX + json.dumps({SENTINEL: [SIGN_IN]})


def test_a_parent_gets_the_sub_agents_sign_in_error_with_the_sub_agents_name():
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN)), 2)
    assert _payload(message) == {SENTINEL: [{**SIGN_IN, "message": _signed_message("Dataverse child")}]}


def test_every_nesting_level_adds_its_agents_name_and_keeps_the_payload_json():
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN), grandparent_name="Grandparent"), 3)
    assert _payload(message) == {SENTINEL: [{**SIGN_IN, "message": _signed_message("Parent agent", "Dataverse child")}]}


@pytest.mark.parametrize("name", ["O'Brien's agent", 'The "quoted" agent', "Агент 7585 ✓"])
def test_any_agent_name_keeps_the_payload_json(name):
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN), parent_name=name, grandparent_name="G"), 3)
    assert _payload(message)[SENTINEL][0]["message"] == _signed_message(name, "Dataverse child")


def test_every_connection_error_of_every_toolkit_is_carried_in_order():
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN, WRONG_SECRET), toolkit(151, SIGN_IN)), 2)
    assert [(error["configuration_type"], error["requires_authorization"]) for error in _payload(message)[SENTINEL]] \
        == [("dataverse", True), ("jira", False), ("dataverse", True)]
    assert _payload(message)[SENTINEL][1]["message"] == "Dataverse child: The client secret is wrong or expired."


@pytest.mark.parametrize("missing", [{"message": ""}, {"message": None}, {}])
def test_a_connection_error_without_text_reads_like_the_toolkit_cards_fallback(missing):
    error = {key: value for key, value in SIGN_IN.items() if key != "message"} | missing
    [message] = _api_messages(_tree(toolkit(148, error)), 2)
    assert _payload(message)[SENTINEL][0]["message"] == "Dataverse child: Connection error"


def test_a_real_misconfiguration_beside_a_sign_in_error_stays_a_misconfiguration():
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN), toolkit(152, missing=True)), 2)
    assert message.startswith(PREFIX + "Application misconfiguration error for agent_name='Dataverse child'")


def test_a_governed_refusal_still_wins_over_a_sign_in_error():
    [message] = _api_messages(_tree(toolkit(148, SIGN_IN), toolkit(153, REFUSED)), 2)
    assert _payload(message) == {"code": REFUSED["code"], "error": f"Dataverse child: {REFUSED['message']}"}


def test_a_sub_agent_that_fails_without_a_validation_error_stays_a_misconfiguration():
    agents = _tree(toolkit(148, SIGN_IN))
    agents[1]["tools"] = None
    [message] = _api_messages(agents, 2)
    assert message.startswith(PREFIX + "Application misconfiguration error for agent_name='Dataverse child'")
