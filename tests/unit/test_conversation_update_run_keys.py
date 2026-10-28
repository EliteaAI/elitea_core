"""The conversation update RPC drops the schedule run keys from client meta before it merges."""
import ast
import importlib.util
import sys
import types
from pathlib import Path

RPC = Path(__file__).resolve().parents[2] / 'rpc/chat_conversation.py'


def test_the_meta_merge_of_the_update_rpc_goes_through_drop_run_keys():
    update = next(n for n in ast.walk(ast.parse(RPC.read_text()))
                  if isinstance(n, ast.Call) and getattr(n.func, 'attr', '') == 'update'
                  and getattr(getattr(n.func, 'value', None), 'attr', '') == 'meta')
    names = {c.func.id for c in ast.walk(update) if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)}

    assert 'drop_run_keys' in names


def test_a_put_body_cannot_set_the_run_keys_of_a_chat(monkeypatch):
    # The sibling modules are stubbed: only drop_run_keys is under test
    package = 'plugins.elitea_core'
    for name in ('plugins', package, f'{package}.models', f'{package}.models.pd', f'{package}.utils'):
        stub = types.ModuleType(name)
        stub.__path__ = []
        monkeypatch.setitem(sys.modules, name, stub)
    for name, attrs in (('message', {'MessageGroupDetail': object}),
                        ('participant', {'ParticipantBase': object, 'ParticipantCreate': object})):
        monkeypatch.setitem(sys.modules, f'{package}.models.pd.{name}', types.SimpleNamespace(**attrs))
    monkeypatch.setitem(sys.modules, f'{package}.utils.chat_constants',
                        types.SimpleNamespace(CONVERSATION_NAME_MAX_LENGTH=255))
    spec = importlib.util.spec_from_file_location(
        f'{package}.models.pd.conversation', RPC.parent.parent / 'models/pd/conversation.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    body = {'schedule_run_id': 1, 'schedule_id': 2, 'scheduled_run': True, 'execution_type': 'schedule', 'note': 'x'}

    body['schedule_label'] = 'mine'

    assert module.drop_run_keys(body) == {'note': 'x', 'schedule_label': 'mine'}
