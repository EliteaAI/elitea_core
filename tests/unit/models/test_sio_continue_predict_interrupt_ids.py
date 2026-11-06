"""SioContinuePredictModel keeps the interrupt ids of the banner and caps them like the REST payload.

The model is loaded from the real file inside a stub package: only its plugin imports (database tools,
participants, attachment and settings models) are stubbed, and the caps come from the real utils/constants.py.

Run from plugin root:
    python3 tests/run_tests.py unit/models/test_sio_continue_predict_interrupt_ids.py -v
"""
import importlib.util
import pathlib
import sys
import types

import pytest
from pydantic import BaseModel, ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'sio_continue_predict_pkg'
MSG_ID = 'a1b2c3d4-e29b-41d4-a716-446655440000'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


def _load(rel_path, name):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def loaded():
    names = [PKG, f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils']
    stubs = {name: _package(name) for name in names}
    stubs['tools'] = types.SimpleNamespace(db=None, VaultClient=None)
    stubs[f'{PKG}.models.participants'] = types.SimpleNamespace(Participant=BaseModel)
    stubs[f'{PKG}.utils.tool_call_format'] = types.SimpleNamespace(format_tool_call_as_user_input=lambda *a: '')
    stubs[f'{PKG}.models.pd.attachment'] = types.SimpleNamespace(AttachmentMessageItemPredict=BaseModel)
    stubs[f'{PKG}.models.pd.participant_settings'] = types.SimpleNamespace(EntitySettingsLlm=BaseModel)
    saved = {name: sys.modules.get(name) for name in [*stubs, f'{PKG}.utils.constants', f'{PKG}.models.pd.predict']}
    sys.modules.update(stubs)
    constants = _load('utils/constants.py', f'{PKG}.utils.constants')
    predict = _load('models/pd/predict.py', f'{PKG}.models.pd.predict')
    yield predict.SioContinuePredictModel, constants
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def _payload(interrupt_ids=None):
    return {'project_id': 1, 'conversation_uuid': 'c-1', 'message_id': MSG_ID, 'interrupt_ids': interrupt_ids}


def test_interrupt_ids_default_to_none(loaded):
    model, _ = loaded

    assert model(**_payload()).interrupt_ids is None


def test_interrupt_ids_keep_the_list_of_the_banner(loaded):
    model, _ = loaded

    assert model(**_payload(['hitl_a', 'hitl_b'])).interrupt_ids == ['hitl_a', 'hitl_b']


def test_interrupt_ids_accept_the_list_limit_and_refuse_one_more(loaded):
    model, constants = loaded
    limit = constants.PAUSE_LIST_LIMIT

    assert len(model(**_payload(['a'] * limit)).interrupt_ids) == limit
    with pytest.raises(ValidationError):
        model(**_payload(['a'] * (limit + 1)))


def test_interrupt_id_accepts_the_length_limit_and_refuses_one_more(loaded):
    model, constants = loaded
    limit = constants.INTERRUPT_ID_MAX_LENGTH

    assert model(**_payload(['a' * limit])).interrupt_ids == ['a' * limit]
    with pytest.raises(ValidationError):
        model(**_payload(['a' * (limit + 1)]))
