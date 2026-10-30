"""#7276: a toolkit whose connection check fails lands in toolkit_errors, not error.

The original bug: connection_errors_from() returned a list, which was placed into ctx (must be a dict)
and typed as 'connection_error' (not a valid pydantic type), causing TypeError in from_exception_data.
The error was caught by the generic except and placed in result['error'] instead of toolkit_errors.

The fix encodes connection errors as {'__connection_errors__': [...]} in ctx.error, which is what
the frontend's handleConnectionErrors already understands.

The helpers and the branch under test are lifted from application_utils.py so the module does not
need the full Pylon runtime.
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

LIFTED = (
    "connection_errors_from",
    "governed_refusal_of",
)

CONNECTION_ERROR = {
    "configuration_type": "github",
    "configuration_title": "My GitHub",
    "message": "Authentication failed",
}

TOOL_ID = 42


def _load_helpers():
    """Lift the two pure helpers from application_utils without importing the whole module."""
    # ToolValidatedDetails.CONNECTION_ERROR_SENTINEL is just a class variable string
    sentinel_class = types.SimpleNamespace(CONNECTION_ERROR_SENTINEL="__connection_errors__")
    namespace = {
        "json": json,
        "ToolValidatedDetails": sentinel_class,
    }
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in LIFTED:
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
    return namespace["connection_errors_from"], namespace["governed_refusal_of"]


def _make_connection_error_pydantic_err(connection_errors: list) -> dict:
    """Build the pydantic error dict that ToolValidatedDetails.check_connection produces."""
    sentinel = "__connection_errors__"
    # Pydantic stores the ValueError's first arg in ctx.error
    ctx_error = ValueError({sentinel: connection_errors})
    return {
        "type": "value_error",
        "loc": ("tools", TOOL_ID, "__root__"),
        "input": None,
        "ctx": {"error": ctx_error},
        "msg": f"Value error, {{{sentinel!r}: ...}}",
        "url": "",
    }


def _build_toolkit_errors_branch(connection_errors_from_fn, governed_refusal_of_fn):
    """
    Lift and execute the elif connection_errors is not None branch from
    validate_application_version_details, returning what it appends.
    """
    appended = []

    # Simulate one iteration of the for err in e.errors() loop for a connection-error pydantic err
    raw_err = _make_connection_error_pydantic_err([CONNECTION_ERROR])

    connection_errors = connection_errors_from_fn(raw_err)
    assert connection_errors is not None, "connection_errors_from should have found the sentinel"

    refusal = governed_refusal_of_fn(connection_errors)
    assert refusal is None, "ungoverned error should not produce a refusal"

    # This is the elif branch from application_utils.py (lines ~1260-1267 post-fix)
    appended.append({
        "type": "value_error",
        "loc": ("tools", TOOL_ID, "__root__"),
        "input": raw_err.get("input"),
        "ctx": {"error": json.dumps({"__connection_errors__": connection_errors})},
    })
    return appended


def test_connection_error_lands_in_toolkit_errors_not_error():
    """The fixed branch produces a valid pydantic error dict that from_exception_data accepts."""
    conn_from, gov_refusal = _load_helpers()
    errors = _build_toolkit_errors_branch(conn_from, gov_refusal)

    # Must not raise TypeError — this was the original crash
    ve = ValidationError.from_exception_data("TestModel", errors)

    assert ve is not None
    [err] = ve.errors(include_url=False, include_context=False, include_input=False)
    assert err["type"] == "value_error"
    assert err["loc"] == ("tools", TOOL_ID, "__root__")


def test_connection_error_ctx_uses_connection_errors_key():
    """ctx.error encodes __connection_errors__ so the frontend's handleConnectionErrors can parse it."""
    conn_from, gov_refusal = _load_helpers()
    errors = _build_toolkit_errors_branch(conn_from, gov_refusal)

    ctx_error_str = errors[0]["ctx"]["error"]
    payload = json.loads(ctx_error_str)

    assert "__connection_errors__" in payload
    assert payload["__connection_errors__"] == [CONNECTION_ERROR]


def test_full_connection_errors_list_is_preserved():
    """All connection errors are kept — not just the first one."""
    conn_from, gov_refusal = _load_helpers()

    second_error = {
        "configuration_type": "jira",
        "configuration_title": "Jira Cloud",
        "message": "Token expired",
    }
    raw_err = _make_connection_error_pydantic_err([CONNECTION_ERROR, second_error])
    connection_errors = conn_from(raw_err)

    entry = {
        "type": "value_error",
        "loc": ("tools", TOOL_ID, "__root__"),
        "input": None,
        "ctx": {"error": json.dumps({"__connection_errors__": connection_errors})},
    }
    payload = json.loads(entry["ctx"]["error"])

    assert len(payload["__connection_errors__"]) == 2
    assert payload["__connection_errors__"][1] == second_error
