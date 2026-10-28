"""The delivery object of the schedule YAML file: export, import and strict shape, on SQLite.

Run via:
    python tests/run_tests.py unit/test_schedule_yaml_delivery.py -v
"""
import uuid

import pytest
import yaml
from fixtures import schedule_sqlite as sqlite_db
from unit import test_7011_schedule_yaml_import as yaml_import_tests
from unit.test_7011_schedule_yaml_import import item

# Pytest finds a fixture by its module-level name; an assignment keeps the shared fixture without an unused import.
env = yaml_import_tests.env

PROJECT_ID = 35
CHAT = uuid.UUID(int=99)
DELIVERY_KEYS = ['mode', 'conversation_uuid', 'participant_id', 'include_history']


def seed(env, **fields):
    return sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, 42, **fields)


def exported_items(env):
    return yaml.safe_load(env.module.export_schedules_yaml(PROJECT_ID, None))['schedules']


def delivery_of(**overrides):
    return {'mode': 'conversation', 'conversation_uuid': str(CHAT), 'participant_id': 5,
            'include_history': True, **overrides}


# --- export

def test_export_writes_the_delivery_target_of_a_conversation_schedule(env):
    seed(env, delivery_mode='conversation', delivery_conversation_uuid=CHAT, delivery_participant_id=5)
    delivery = exported_items(env)[0]['delivery']
    assert list(delivery) == DELIVERY_KEYS
    assert delivery == {
        'mode': 'conversation', 'conversation_uuid': str(CHAT), 'participant_id': 5, 'include_history': False,
    }


def test_export_writes_run_history_with_empty_target_fields(env):
    seed(env)
    delivery = exported_items(env)[0]['delivery']
    assert list(delivery) == DELIVERY_KEYS
    assert delivery == {'mode': 'run_history', 'conversation_uuid': None, 'participant_id': None,
                        'include_history': False}


def test_export_writes_delivery_after_the_other_item_keys(env):
    seed(env)
    assert list(exported_items(env)[0])[-1] == 'delivery'


# --- import

def test_import_saves_a_conversation_delivery_as_run_history_without_a_target(env):
    report = env.import_items(item(delivery=delivery_of()))
    (row,) = env.rows()
    assert [entry['name'] for entry in report['created']] == ['Daily digest']
    assert (row.delivery_mode, row.delivery_conversation_uuid, row.delivery_participant_id) == (
        'run_history', None, None,
    )
    assert report['failed'] == []


def test_import_report_says_the_target_chat_is_not_imported(env):
    report = env.import_items(item(delivery=delivery_of()))
    (entry,) = report['created']
    assert entry['note'] == 'The target chat is not imported; the schedule delivers to the run history'


def test_import_report_has_no_delivery_note_for_a_run_history_item(env):
    report = env.import_items(item(delivery=delivery_of(
        mode='run_history', conversation_uuid=None, participant_id=None, include_history=False,
    )))
    (entry,) = report['created']
    assert 'note' not in entry


def test_import_keeps_the_stored_chat_delivery_of_an_updated_schedule_and_says_so(env):
    key = uuid.UUID(int=7)
    seed(env, uuid=key, delivery_mode='conversation', delivery_conversation_uuid=CHAT, delivery_participant_id=5)
    report = env.import_items(item(key=str(key), delivery=delivery_of()))
    (row,) = env.rows()
    (entry,) = report['updated']
    assert (row.delivery_mode, row.delivery_conversation_uuid, row.delivery_participant_id) == (
        'conversation', CHAT, 5,
    )
    assert entry['note'] == 'The target chat is not imported; the schedule keeps its stored delivery'


def test_import_without_a_delivery_object_creates_and_updates_as_before(env):
    key = str(uuid.UUID(int=8))
    created = env.import_items(item(key=key))
    updated = env.import_items(item(key=key, prompt='Changed'))
    (row,) = env.rows()
    assert (len(created['created']), len(updated['updated']), updated['failed']) == (1, 1, [])
    assert (row.prompt, row.delivery_mode) == ('Changed', 'run_history')


# --- strict shape

@pytest.mark.parametrize('delivery', [
    {'mode': 'run_history', 'target': 'x'},
    {'mode': 'email'},
    {'conversation_uuid': str(CHAT)},
    'conversation',
])
def test_import_rejects_a_malformed_delivery_object_with_a_reason(env, delivery):
    report = env.import_items(item(delivery=delivery), item(name='Good'))
    assert [entry['name'] for entry in report['created']] == ['Good']
    (failed,) = report['failed']
    assert failed['name'] == 'Daily digest'
    assert failed['reason'].startswith('delivery')
    assert [row.name for row in env.rows()] == ['Good']
