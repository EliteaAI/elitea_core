"""#7398 AC8: webhook rows never leave or enter through the schedules YAML file.

Reuses the `env` fixture of test_7011_schedules_import_export_api.py (real handler, YAML module and service on SQLite).
"""
import yaml
from integration.test_7011_schedules_import_export_api import env  # noqa: F401 -- `env` is a fixture
from unit.test_7011_schedule_yaml_import import file_text, item


def _seed_webhook(env, name='Hook', **fields):
    return env.seed(name, **{'trigger_kind': 'webhook', 'webhook_type': 'github', 'cron': None, 'timezone': None,
                             'secret_ref': '{{secret.webhook_secret_x}}', **fields})


def test_7398_export_skips_webhook_rows(env):
    env.grant('export')
    env.seed('Cron')
    _seed_webhook(env)
    response = env.export()
    data = yaml.safe_load(response.text)
    assert [entry['name'] for entry in data['schedules']] == ['Cron']
    assert 'secret' not in response.text


def test_7398_export_by_id_of_a_webhook_is_an_unknown_id(env):
    env.grant('export')
    hook = _seed_webhook(env)
    body, status = env.export(ids=str(hook))
    assert status == 400 and body['field'] == 'ids'


def test_7398_import_cannot_target_a_webhook_by_key(env):
    env.grant('import')
    hook = _seed_webhook(env)
    key = str(next(row.uuid for row in env.rows() if row.id == hook))
    body, status = env.import_file(file_text(item(key=key)))
    assert status == 200 and [entry['name'] for entry in body['failed']] == ['Daily digest']
    stored = next(row for row in env.rows() if row.id == hook)
    assert (stored.trigger_kind, stored.cron) == ('webhook', None)


def test_7398_import_does_not_match_a_webhook_by_name(env):
    env.grant('import')
    _seed_webhook(env, name='Daily digest')
    body, status = env.import_file(file_text(item()))
    assert status == 200 and [entry['name'] for entry in body['created']] == ['Daily digest']


def test_7398_import_item_with_webhook_fields_fails(env):
    env.grant('import')
    body, status = env.import_file(file_text(item(trigger_kind='webhook', webhook_type='github')))
    assert status == 200 and [entry['name'] for entry in body['failed']] == ['Daily digest']
    assert env.rows() == []
