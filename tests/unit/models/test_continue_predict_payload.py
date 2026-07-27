"""Unit tests for ContinuePredictPayload (HITL resume / continue).

The model imports only pydantic + stdlib, so it loads without stubs.

Run from plugin root:
    python3 tests/run_tests.py unit/models/test_continue_predict_payload.py -v
"""
import pathlib
import sys

import pytest
from pydantic import ValidationError

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs

UUID_STR = "550e8400-e29b-41d4-a716-446655440000"
MSG_ID = "a1b2c3d4-e29b-41d4-a716-446655440000"


@pytest.fixture(scope="module")
def payload_model(models_path):
    module = load_module_with_stubs(
        models_path / "pd" / "continue_predict.py",
        "plugins.elitea_core.models.pd.continue_predict",
    )
    return module.ContinuePredictPayload


def _base(**over):
    data = {"conversation_uuid": UUID_STR, "message_id": MSG_ID, "hitl_action": "approve"}
    data.update(over)
    return data


class TestContinuePredictPayload:
    def test_valid_approve_defaults(self, payload_model):
        m = payload_model.model_validate(_base())
        assert m.hitl_resume is True
        assert m.hitl_action == "approve"
        assert m.await_task_timeout == 30

    @pytest.mark.parametrize("action", ["approve", "reject"])
    def test_value_free_actions_ok_without_value(self, payload_model, action):
        assert payload_model.model_validate(_base(hitl_action=action)).hitl_action == action

    @pytest.mark.parametrize("action", ["edit", "block_with_comment"])
    def test_text_actions_require_value(self, payload_model, action):
        # without hitl_value -> rejected
        with pytest.raises(ValidationError):
            payload_model.model_validate(_base(hitl_action=action))
        # with hitl_value -> ok
        m = payload_model.model_validate(_base(hitl_action=action, hitl_value="some text"))
        assert m.hitl_value == "some text"

    def test_invalid_action_rejected(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate(_base(hitl_action="foo"))

    def test_hitl_action_required_when_resuming(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate(
                {"conversation_uuid": UUID_STR, "message_id": MSG_ID})  # hitl_resume defaults True

    def test_action_optional_when_not_resuming(self, payload_model):
        m = payload_model.model_validate(
            {"conversation_uuid": UUID_STR, "message_id": MSG_ID, "hitl_resume": False})
        assert m.hitl_action is None

    def test_message_id_required(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate(
                {"conversation_uuid": UUID_STR, "hitl_action": "approve"})

    @pytest.mark.parametrize("bad", [-2, 301, 1000])
    def test_await_timeout_bounds(self, payload_model, bad):
        with pytest.raises(ValidationError):
            payload_model.model_validate(_base(await_task_timeout=bad))

    @pytest.mark.parametrize("ok", [-1, 0, 30, 300])
    def test_await_timeout_valid(self, payload_model, ok):
        assert payload_model.model_validate(_base(await_task_timeout=ok)).await_task_timeout == ok
