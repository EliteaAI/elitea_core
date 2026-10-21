"""Chat export / import (#6904): failed replies and original authors survive a round trip.

Covers:
  - build_export_payload writes is_error / error of a failed reply and the
    email of a human author
  - ImportMessage / ImportParticipantRef accept them and stay compatible with
    files exported before

Run via:
    python tests/run_tests.py unit/test_6904_chat_export_errors_authors.py -v
"""
import enum
import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timezone

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

PKG = 'elitea_core_6904_export'


def _package(name):
    pkg = types.ModuleType(name)
    pkg.__path__ = []
    sys.modules[name] = pkg
    return pkg


def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


class _ParticipantTypes(str, enum.Enum):
    user = 'user'
    application = 'application'
    dummy = 'dummy'
    toolkit = 'toolkit'


@pytest.fixture(scope='module')
def export_module():
    _package(PKG)
    _package(f'{PKG}.models')
    _package(f'{PKG}.models.message_items')
    _package(f'{PKG}.models.enums')
    _package(f'{PKG}.utils')
    _module(f'{PKG}.models.enums.all', ParticipantTypes=_ParticipantTypes)
    _module(f'{PKG}.models.folder', ConversationFolder=object)
    _module(f'{PKG}.models.message_group', ConversationMessageGroup=object)
    _module(f'{PKG}.models.message_items.base', MessageItem=object)
    _module(f'{PKG}.utils.authors', get_authors_data=lambda ids: [])

    # A private 'tools' for this import only, so the shared stub other tests use is left as it is
    shared_tools = sys.modules.get('tools')
    sys.modules['tools'] = types.SimpleNamespace(MinioClient=None, this=None)
    try:
        spec = importlib.util.spec_from_file_location(
            f'{PKG}.utils.conversation_export', PLUGIN_ROOT / 'utils' / 'conversation_export.py',
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    finally:
        if shared_tools is None:
            sys.modules.pop('tools', None)
        else:
            sys.modules['tools'] = shared_tools
    return module


@pytest.fixture(scope='module')
def import_models():
    spec = importlib.util.spec_from_file_location(
        '_ci_pd_6904', PLUGIN_ROOT / 'models' / 'pd' / 'conversation_import.py',
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)

PARTICIPANTS = [
    {'id': 1, 'entity_name': 'user', 'entity_meta': {'id': 3},
     'meta': {'user_name': 'Jane', 'user_email': 'jane@example.com'}},
    {'id': 2, 'entity_name': 'application', 'entity_meta': {'id': 5, 'project_id': 2},
     'meta': {'name': 'Helper'}},
]


def _group(uuid, author, meta=None, items=None):
    return {
        'uuid': uuid, 'author_participant_id': author, 'sent_to_id': None, 'reply_to_uuid': None,
        'created_at': NOW, 'meta': meta or {},
        'items': items or [{'id': 1, 'item_type': 'text_message', 'content': 'text'}],
    }


def _payload(export_module, groups):
    return export_module.build_export_payload(
        conversation={'id': 1, 'uuid': 'c', 'name': 'Chat', 'is_private': True, 'author_id': 3},
        participants=PARTICIPANTS, groups=groups, include_attachments=False,
        exported_by={'id': 3, 'name': 'Jane'}, project_id=2, exported_at=NOW,
    )


def test_failed_reply_keeps_its_error(export_module):
    payload = _payload(export_module, [
        _group('q', 1),
        _group('a', 2, meta={'is_error': True, 'error': 'Model is unavailable'}),
    ])
    question, answer = payload['messages']
    assert 'is_error' not in question
    assert answer['is_error'] is True
    assert answer['error'] == 'Model is unavailable'


def test_failed_reply_without_text_still_counts_as_error(export_module):
    answer = _payload(export_module, [_group('a', 2, meta={'is_error': True, 'error': ''})])['messages'][0]
    assert answer['is_error'] is True
    assert answer['error'] is None


def test_human_author_carries_email(export_module):
    question, answer = _payload(export_module, [_group('q', 1), _group('a', 2)])['messages']
    assert question['author'] == {
        'participant_id': 1, 'entity_name': 'user', 'name': 'Jane', 'email': 'jane@example.com',
    }
    assert 'email' not in answer['author']


def test_groups_without_meta_still_export(export_module):
    group = _group('q', 1)
    del group['meta']
    assert 'is_error' not in _payload(export_module, [group])['messages'][0]


def test_import_reads_error_and_email(import_models):
    message = import_models.ImportMessage.model_validate({
        'uuid': 'a', 'is_error': True, 'error': 'boom',
        'author': {'participant_id': 1, 'entity_name': 'user', 'name': 'Jane', 'email': 'jane@example.com'},
        'items': [{'type': 'text_message', 'content': 'x'}],
    })
    assert message.is_error is True
    assert message.error == 'boom'
    assert message.author.email == 'jane@example.com'


def test_import_of_older_files_has_no_error_or_email(import_models):
    message = import_models.ImportMessage.model_validate({
        'uuid': 'a', 'author': {'participant_id': 1, 'entity_name': 'user', 'name': 'Jane'},
        'items': [],
    })
    assert message.is_error is False
    assert message.error is None
    assert message.author.email is None
