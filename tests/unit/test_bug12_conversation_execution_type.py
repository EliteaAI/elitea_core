"""EL-7011 US-16 R1.5 (BUG-12): each entry point stores its execution_type on the conversations it runs in.

Contract under test: utils/pipeline_execution.py `with_execution_type` and `create_trigger_run_conversation`
(loaded from source), the chat run in rpc/chat_all.py `predict_sio` (read from its source, the module does
not load in the unit env), and the skill REST predict in utils/skill_run_utils.py `dispatch_skill_run`.
REST agent predict and MCP create no conversation; their trigger sources are covered by
test_bug12_entry_point_sources.py.
"""
import ast
import pathlib

import pytest
from unit.utils.test_6920_skill_run import FakeModule, _run, env, own_skill  # noqa: F401 -- fixtures
from unit.utils.test_7011_pipeline_execution_owner import _create, world  # noqa: F401 -- the real module

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
CHAT_ALL = ast.parse((PLUGIN_ROOT / "rpc" / "chat_all.py").read_text())


def _method(tree, name):
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)


@pytest.mark.parametrize("meta", [None, {}, {"persona": "generic"}])
def test_el_7011_us16_r1_5_meta_without_a_value_gets_the_entry_point_value(world, meta):  # noqa: F811
    marked = world.execution.with_execution_type(meta, "chat")

    assert marked == {**(meta or {}), "execution_type": "chat"}


@pytest.mark.parametrize("stored", ["scheduled", "schedule_manual", "webhook"])
def test_el_7011_us16_r1_5_a_stored_value_is_never_overwritten(world, stored):  # noqa: F811
    meta = {"schedule_id": 3, "execution_type": stored}

    assert world.execution.with_execution_type(meta, "chat") == meta


def test_el_7011_us16_r1_5_webhook_conversation_stores_webhook(world):  # noqa: F811
    conversation = _create(world, world.execution.TriggerType.WEBHOOK)

    assert conversation.meta["execution_type"] == "webhook"
    assert conversation.meta["webhook_trigger"] is True


def test_el_7011_us16_r1_5_cron_conversation_stores_scheduled(world):  # noqa: F811
    conversation = _create(world, world.execution.TriggerType.SCHEDULED)

    assert conversation.meta["execution_type"] == "scheduled"


def test_el_7011_us16_r1_5_run_now_conversation_keeps_schedule_manual(world):  # noqa: F811
    conversation = _create(world, world.execution.TriggerType.SCHEDULE_MANUAL,
                           extra_meta={"execution_type": "schedule_manual"})

    assert conversation.meta["execution_type"] == "schedule_manual"


def test_el_7011_us16_r1_5_chat_run_marks_its_conversation_chat():
    method = _method(CHAT_ALL, "predict_sio")
    marks = [node for node in ast.walk(method)
             if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
             and getattr(node.value.func, "id", None) == "with_execution_type"]

    assert len(marks) == 1
    mark = marks[0]
    assert ast.unparse(mark.targets[0]) == "conversation.meta"
    assert [ast.unparse(arg) for arg in mark.value.args] == ["conversation.meta", "TriggerType.CHAT"]
    access_check = next(node for node in ast.walk(method) if isinstance(node, ast.Call)
                        and getattr(node.func, "id", None) == "check_post_access")
    assert mark.lineno > access_check.lineno  # a refused sender leaves the conversation as it is


def test_el_7011_us16_r1_5_skill_rest_predict_sends_api(env, own_skill):  # noqa: F811
    module = FakeModule()

    _, status = _run(env, module, {"user_input": "hi"})

    assert status == 200
    assert module.calls[0]["trigger_source"] == "api"


@pytest.mark.parametrize("path", ["utils/llm_judge.py", "utils/skill_publish_utils.py"])
def test_el_7011_us16_r1_5_internal_runs_send_no_trigger_source(path):
    tree = ast.parse((PLUGIN_ROOT / path).read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and getattr(node.func, "attr", None) == "predict_sio"]

    assert calls
    assert all(kw.arg != "trigger_source" for call in calls for kw in call.keywords)
