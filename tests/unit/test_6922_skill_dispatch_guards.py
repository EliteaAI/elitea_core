import ast
import importlib.util
import pathlib
import sys
import types
from types import SimpleNamespace

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _stub(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module


class FakeQuery:
    def __init__(self, row):
        self.row = row

    def options(self, *args):
        return self

    def filter(self, *args):
        return self

    def first(self):
        return self.row


@pytest.fixture
def continue_message(isolated_sys_modules):
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', f'{PACKAGE}.rpc'):
        _package(name)
    harness = SimpleNamespace(dispatched=[], row=None)

    class Session:
        def query(self, model):
            return FakeQuery(harness.row)

        def commit(self):
            harness.dispatched.append('commit')

    class SessionContext:
        def __enter__(self):
            return Session()

        def __exit__(self, *args):
            return False

    rpc_call = SimpleNamespace(applications_predict_sio=lambda *a, **k: harness.dispatched.append('rpc'))
    _stub('tools', db=SimpleNamespace(get_session=lambda project_id: SessionContext()),
          context=SimpleNamespace(rpc_manager=SimpleNamespace(call=rpc_call)), serialize=lambda x: x,
          auth=SimpleNamespace(is_sio_user_in_project=lambda sid, project_id: True))
    _stub('pylon.core.tools', log=SimpleNamespace(warning=lambda *a: None))
    _stub(f'{PACKAGE}.utils.sio_utils', SioEvents=SimpleNamespace(chat_predict=SimpleNamespace(value='chat_predict')))
    _stub(f'{PACKAGE}.models.message_group', ConversationMessageGroup=SimpleNamespace(reply_to=None, uuid=None))
    _stub(f'{PACKAGE}.models.pd.message', MessageGroupDetail=SimpleNamespace(model_validate=lambda x: x))
    _stub(f'{PACKAGE}.rpc.chat_all', CHAT_PREDICT_MAPPER={'skill': 'applications_predict_sio'})
    _stub('sqlalchemy.orm', joinedload=lambda *a: None)
    spec = importlib.util.spec_from_file_location(f'{PACKAGE}.models.enums.all', PLUGIN_ROOT / 'models/enums/all.py')
    enums = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = enums
    spec.loader.exec_module(enums)
    spec = importlib.util.spec_from_file_location(f'{PACKAGE}.utils.continue_message',
                                                  PLUGIN_ROOT / 'utils/continue_message.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return SimpleNamespace(module=module, harness=harness)


def test_legacy_continue_refuses_a_skill_answer_before_dispatching_the_raw_payload(continue_message):
    continue_message.harness.row = SimpleNamespace(
        author_participant=SimpleNamespace(entity_name='skill', entity_meta={'id': 10, 'project_id': 1}),
        author_participant_id=5,
    )
    body, status = continue_message.module.continue_message(
        'sid', {'message_id': 'm', 'project_id': 7, 'question_id': 'q', '_skill_dispatch': {'forged': True}},
    )
    assert status == 400
    assert body == {'error': continue_message.module.SKILL_CONTINUE_UNSUPPORTED_ERROR}
    assert continue_message.harness.dispatched == []


def test_regenerate_runs_in_the_url_project_whatever_the_body_claims():
    tree = ast.parse((PLUGIN_ROOT / 'api/v2/regenerate.py').read_text())
    assignment = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and getattr(node.targets[0], 'id', None) == 'raw_predict_payload'
    )
    namespace = {
        'parsed': SimpleNamespace(model_dump=lambda: {'question_id': 'q'}, payload={'project_id': 999}),
        'project_id': 7,
    }
    exec(compile(ast.Module([assignment], []), 'regenerate.py', 'exec'), namespace)
    assert namespace['raw_predict_payload']['project_id'] == 7
