"""The batch pause flag goes from the starter to the payload of the predict RPC, as an RPC argument only.

Contract under test: utils/pipeline_execution.py `execute_pipeline_via_predict_sio` (loaded from source with the
fakes of its other tests) and rpc/application.py `predict_sio` (lifted out of the module and run against mocks: the
plugin does not load in the unit env).
"""
import types
from unittest import mock

from unit.test_6947_mandate_triggers import lift
from unit.utils import test_7011_pipeline_execution_owner as owner_fixtures

world = owner_fixtures.world  # the fixture of the owner run tests, bound as an attribute
_predict = owner_fixtures._predict  # pylint: disable=W0212

FLAG = "batch_sensitive_approvals"


def test_the_predict_call_carries_the_flag_when_it_is_set(world):
    assert _predict(world, batch_sensitive_approvals=True)[FLAG] is True


def test_the_predict_call_has_no_flag_by_default_or_when_false(world):
    assert FLAG not in _predict(world)
    assert FLAG not in _predict(world, batch_sensitive_approvals=False)


def test_the_flag_does_not_change_the_other_predict_arguments(world):
    plain = _predict(world, unattended=True)
    batch = _predict(world, unattended=True, batch_sensitive_approvals=True)

    assert {key: value for key, value in batch.items() if key != FLAG} == plain


def _run_predict_sio(extra_data=None, batch_sensitive_approvals=False):
    """The payload that predict_sio hands to the task node, with every other collaborator mocked."""
    function, glob = lift("rpc/application.py", "predict_sio")
    parsed = mock.MagicMock(version_id=None, stream_id="s-1", message_id="m-1", project_id=1, version_details=None)
    glob["ApplicationChatRequest"].model_validate.return_value = parsed
    glob["take_trigger_admission"].return_value = None
    glob["generate_predict_payload"].side_effect = lambda *args, **options: {"stream_id": "s-1"}
    glob["inspect_and_unsecret"].side_effect = lambda payload, *args: payload
    module = types.SimpleNamespace(task_node=mock.MagicMock(), stream_response=mock.MagicMock(),
                                   context=mock.MagicMock())
    module.task_node.start_task.return_value = "task-1"
    data = {"project_id": 1, "message_id": "m-1", "stream_id": "s-1", **(extra_data or {})}
    function(module, sid=None, data=data, user_id=5, is_system_user=True,
             batch_sensitive_approvals=batch_sensitive_approvals)
    return module.task_node.start_task.call_args.kwargs["kwargs"], glob


def test_predict_sio_puts_the_flag_in_the_payload_when_it_is_set():
    payload, _ = _run_predict_sio(batch_sensitive_approvals=True)

    assert payload[FLAG] is True


def test_predict_sio_leaves_the_flag_out_by_default_or_when_false():
    default, _ = _run_predict_sio()
    off, _ = _run_predict_sio(batch_sensitive_approvals=False)

    assert (FLAG in default, FLAG in off) == (False, False)


def test_a_client_payload_cannot_switch_the_flag_on():
    payload, glob = _run_predict_sio(extra_data={FLAG: True})

    assert FLAG not in payload
    assert FLAG not in glob["generate_predict_payload"].call_args.kwargs
