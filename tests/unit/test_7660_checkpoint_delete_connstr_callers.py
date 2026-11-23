"""elitea_issues#7660: checkpoint deletes fall back to the default store when the hidden secrets are unreadable, as
they did before the hidden read existed; task dispatch must keep raising, so the worker never trusts that None."""
import ast
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
DELETES = ["api/v2/messages.py", "rpc/chat_conversation.py", "utils/schedule_runs.py", "utils/schedule_test_runs.py"]
DISPATCH = ["methods/predict.py", "rpc/application.py"]


def lookups(path):
    calls = [node for node in ast.walk(ast.parse((PLUGIN_ROOT / path).read_text()))
             if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "get_pgvector_connection_string"]
    assert calls, path
    return [{kw.arg: getattr(kw.value, "value", None) for kw in call.keywords} for call in calls]


@pytest.mark.unit
@pytest.mark.parametrize("path", DELETES)
def test_checkpoint_deletes_fall_back_to_the_default_store(path):
    assert all(keywords.get("default_on_hidden_failure") is True for keywords in lookups(path))


@pytest.mark.unit
@pytest.mark.parametrize("path", DISPATCH)
def test_dispatch_keeps_raising(path):
    assert all("default_on_hidden_failure" not in keywords for keywords in lookups(path))
