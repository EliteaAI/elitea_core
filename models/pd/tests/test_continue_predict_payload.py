"""Unit tests for ContinuePredictPayload (HITL resume / continue).

The model imports only pydantic + stdlib, so it loads in isolation; we add the
parent ``models/pd/`` dir to sys.path and import it directly.

    python3 -m pytest --rootdir=models/pd/tests --import-mode=importlib \
        models/pd/tests/test_continue_predict_payload.py -v
"""

import importlib
import pathlib
import sys

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

ContinuePredictPayload = importlib.import_module("continue_predict").ContinuePredictPayload

UUID_STR = "550e8400-e29b-41d4-a716-446655440000"
MSG_ID = "a1b2c3d4-e29b-41d4-a716-446655440000"


def _base(**over):
    data = {"conversation_uuid": UUID_STR, "message_id": MSG_ID, "hitl_action": "approve"}
    data.update(over)
    return data


def test_valid_approve_defaults():
    m = ContinuePredictPayload.model_validate(_base())
    assert m.hitl_resume is True
    assert m.hitl_action == "approve"
    assert m.await_task_timeout == 30


def test_edit_with_value():
    m = ContinuePredictPayload.model_validate(_base(hitl_action="edit", hitl_value="use v2"))
    assert m.hitl_action == "edit"
    assert m.hitl_value == "use v2"


@pytest.mark.parametrize("action", ["approve", "reject", "edit", "block_with_comment"])
def test_all_valid_actions(action):
    assert ContinuePredictPayload.model_validate(_base(hitl_action=action)).hitl_action == action


def test_invalid_action_rejected():
    with pytest.raises(ValidationError):
        ContinuePredictPayload.model_validate(_base(hitl_action="foo"))


def test_hitl_action_required_when_resuming():
    with pytest.raises(ValidationError):
        ContinuePredictPayload.model_validate(
            {"conversation_uuid": UUID_STR, "message_id": MSG_ID})  # hitl_resume defaults True


def test_action_optional_when_not_resuming():
    m = ContinuePredictPayload.model_validate(
        {"conversation_uuid": UUID_STR, "message_id": MSG_ID, "hitl_resume": False})
    assert m.hitl_action is None


def test_message_id_required():
    with pytest.raises(ValidationError):
        ContinuePredictPayload.model_validate(
            {"conversation_uuid": UUID_STR, "hitl_action": "approve"})


@pytest.mark.parametrize("bad", [-2, 301, 1000])
def test_await_timeout_bounds(bad):
    with pytest.raises(ValidationError):
        ContinuePredictPayload.model_validate(_base(await_task_timeout=bad))


@pytest.mark.parametrize("ok", [-1, 0, 30, 300])
def test_await_timeout_valid(ok):
    assert ContinuePredictPayload.model_validate(_base(await_task_timeout=ok)).await_task_timeout == ok
