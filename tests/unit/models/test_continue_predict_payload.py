"""Unit tests for ContinuePredictPayload (HITL resume / continue).

The model imports only pydantic + stdlib, so it loads without stubs.

Run from plugin root:
    python3 tests/run_tests.py unit/models/test_continue_predict_payload.py -v
"""
import ast
import pathlib
import sys
import uuid
from typing import Any, Dict, List, Optional

import pytest
from pydantic import BaseModel, Field, ValidationError, model_validator

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.cut_source import load_cut
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

    def test_toolkit_authorization_selects_its_own_resume_protocol(self, payload_model):
        m = payload_model.model_validate({
            "conversation_uuid": UUID_STR,
            "message_id": MSG_ID,
            "mcp_auth_resume": True,
            "mcp_auth_action": "authorize",
            "authorization_request_id": "mcp_auth_123",
        })
        assert m.hitl_resume is False
        assert m.mcp_auth_action == "authorize"
        assert m.authorization_request_id == "mcp_auth_123"

    def test_toolkit_authorization_requires_valid_action(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate({
                "conversation_uuid": UUID_STR,
                "message_id": MSG_ID,
                "mcp_auth_resume": True,
            })
        with pytest.raises(ValidationError):
            payload_model.model_validate({
                "conversation_uuid": UUID_STR,
                "message_id": MSG_ID,
                "mcp_auth_resume": True,
                "mcp_auth_action": "approve",
            })

    def test_resume_protocols_are_mutually_exclusive(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate({
                "conversation_uuid": UUID_STR,
                "message_id": MSG_ID,
                "hitl_resume": True,
                "hitl_action": "approve",
                "mcp_auth_resume": True,
                "mcp_auth_action": "authorize",
            })

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


class TestApproveAll:
    def test_approve_all_is_accepted_with_approve(self, payload_model):
        assert payload_model.model_validate(_base(approve_all=True)).approve_all is True

    def test_approve_all_defaults_to_false(self, payload_model):
        assert payload_model.model_validate(_base()).approve_all is False

    @pytest.mark.parametrize("action, value", [("reject", None), ("edit", "x"), ("block_with_comment", "x")])
    def test_approve_all_with_another_action_is_refused(self, payload_model, action, value):
        with pytest.raises(ValidationError):
            payload_model.model_validate(_base(hitl_action=action, hitl_value=value, approve_all=True))

    def test_approve_all_without_hitl_resume_is_refused(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate(_base(hitl_resume=False, approve_all=True))

    def test_approve_all_with_a_sign_in_resume_is_refused(self, payload_model):
        with pytest.raises(ValidationError):
            payload_model.model_validate({
                "conversation_uuid": UUID_STR, "message_id": MSG_ID, "mcp_auth_resume": True,
                "mcp_auth_action": "authorize", "approve_all": True})


@pytest.fixture(scope="module")
def sio_model(models_path):
    """SioContinuePredictModel cut out of models/pd/predict.py: the module imports the database models."""
    source = (models_path / "pd" / "predict.py").read_text()
    nodes = [node for node in ast.parse(source).body if getattr(node, "name", None) == "SioContinuePredictModel"]
    namespace = load_cut(nodes, {
        "BaseModel": BaseModel, "Field": Field, "model_validator": model_validator, "Optional": Optional,
        "Dict": Dict, "List": List, "Any": Any, "uuid": uuid, "UUID": uuid.UUID,
        "EntitySettingsLlm": dict, "AttachmentMessageItemPredict": dict}, "predict.py")
    return namespace["SioContinuePredictModel"]


def _sio(**over):
    data = {"project_id": 1, "conversation_uuid": UUID_STR, "message_id": MSG_ID,
            "hitl_resume": True, "hitl_action": "approve"}
    data.update(over)
    return data


class TestSioApproveAll:
    def test_approve_all_is_accepted_with_approve(self, sio_model):
        assert sio_model.model_validate(_sio(approve_all=True)).approve_all is True

    def test_approve_all_defaults_to_false(self, sio_model):
        assert sio_model.model_validate(_sio()).approve_all is False

    @pytest.mark.parametrize("action", ["reject", "edit", "block_with_comment", "answer"])
    def test_approve_all_with_another_action_is_refused(self, sio_model, action):
        with pytest.raises(ValidationError):
            sio_model.model_validate(_sio(hitl_action=action, approve_all=True))

    def test_approve_all_without_hitl_resume_is_refused(self, sio_model):
        with pytest.raises(ValidationError):
            sio_model.model_validate(_sio(hitl_resume=False, approve_all=True))
