"""#7398 Core A: webhook rows through the real schedule service, list query and cap on SQLite.

Reuses the `env` fixture of test_7011_schedule_service.py (real service, body models, timing and table).
Covers AC3 (list default), AC9 (cap counts cron only) and AC10 (cron unchanged; webhooks get no next run,
no cron edit).
"""
import pytest
from fixtures.helpers import load_module_with_stubs
from integration.test_7011_schedule_service import (  # noqa: F401 -- `env` is a fixture
    ADMIN_ID, OWNER_ID, PACKAGE, PLUGIN_ROOT, env,
)


def _webhook_body(env, **changes):
    values = {'name': 'On push', 'entity_type': 'agent', 'application_id': 1, 'version_id': 10,
              'prompt': 'Handle it', 'trigger_kind': 'webhook', 'webhook_type': 'github'}
    return env.pd.ScheduleCreate(**{**values, **changes})


def _seed_webhook(env, **fields):
    return env.seed(**{'trigger_kind': 'webhook', 'webhook_type': 'github', 'cron': None, 'timezone': None,
                       'next_run': None, **fields})


def test_7398_webhook_create_has_no_next_run(env):
    row = env.run('create_schedule', _webhook_body(env), env.scope(OWNER_ID))
    stored = env.row(row.id)
    assert (stored.trigger_kind, stored.webhook_type, stored.cron, stored.next_run) == ('webhook', 'github', None, None)


# AC9: webhooks do not count toward max_enabled_per_project, and creating one ignores the cap
def test_7398_cap_counts_cron_rows_only(env):
    env.config['scheduler'] = {'schedules': {'max_enabled_per_project': 1}}
    _seed_webhook(env, name='Hook A')
    _seed_webhook(env, name='Hook B')
    env.run('create_schedule', env.body(), env.scope(OWNER_ID))  # the only cron row: allowed
    env.run('create_schedule', _webhook_body(env), env.scope(OWNER_ID))  # webhook: no cap
    with pytest.raises(env.timing.ScheduleLimitError):
        env.run('create_schedule', env.body(name='Second cron'), env.scope(OWNER_ID))


def test_7398_resume_of_a_webhook_skips_the_cap(env):
    env.config['scheduler'] = {'schedules': {'max_enabled_per_project': 1}}
    env.seed(name='Cron')
    hook = _seed_webhook(env, enabled=False)
    env.edit('update_schedule', hook, env.scope(OWNER_ID), env.pd.ScheduleUpdate(enabled=True))
    stored = env.row(hook)
    assert stored.enabled and stored.next_run is None


@pytest.mark.parametrize('change', [{'cron': '0 1 * * *'}, {'timezone': 'Europe/Warsaw'}])
def test_7398_cron_fields_are_refused_on_a_webhook(env, change):
    hook = _seed_webhook(env)
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.edit('update_schedule', hook, env.scope(OWNER_ID), env.pd.ScheduleUpdate(**change))
    assert excinfo.value.response[0]['field'] == next(iter(change))


def test_7398_webhook_edit_of_other_fields_works(env):
    hook = _seed_webhook(env)
    env.edit('update_schedule', hook, env.scope(OWNER_ID), env.pd.ScheduleUpdate(name='Renamed', expires_at=None))
    assert env.row(hook).name == 'Renamed' and env.row(hook).next_run is None


@pytest.fixture
def queries(env):
    load_module_with_stubs(PLUGIN_ROOT / 'models/pd/schedule_list.py', f'{PACKAGE}.models.pd.schedule_list')
    return load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_queries.py', f'{PACKAGE}.utils.schedule_queries')


def _listed(env, queries, scope, **filters):
    import sys
    pd = sys.modules[f'{PACKAGE}.models.pd.schedule_list']
    with env.session_factory() as session:
        _total, rows = queries.list_schedules(session, scope, pd.ScheduleListFilters(**filters), 100, 0)
    return {row.name: row for row in rows}


# AC3: the default list has cron rows only; trigger_kind=webhook (and webhook_type) list webhooks
def test_7398_list_separates_the_kinds(env, queries):
    env.seed(name='Cron')
    _seed_webhook(env, name='GitHub hook')
    _seed_webhook(env, name='GitLab hook', webhook_type='gitlab', gitlab_auth_method='secret_token')
    admin = env.scope(ADMIN_ID, is_admin=True)
    assert set(_listed(env, queries, admin)) == {'Cron'}
    assert set(_listed(env, queries, admin, trigger_kind='webhook')) == {'GitHub hook', 'GitLab hook'}
    gitlab = _listed(env, queries, admin, trigger_kind='webhook', webhook_type='gitlab')
    assert set(gitlab) == {'GitLab hook'}
    assert gitlab['GitLab hook'].trigger_kind == 'webhook' and gitlab['GitLab hook'].uuid is not None


# AC4: a member sees only own webhooks, an admin all of them
def test_7398_member_sees_only_own_webhooks(env, queries):
    _seed_webhook(env, name='Mine')
    _seed_webhook(env, name='Theirs', created_by=ADMIN_ID)
    assert set(_listed(env, queries, env.scope(OWNER_ID), trigger_kind='webhook')) == {'Mine'}
    assert set(_listed(env, queries, env.scope(ADMIN_ID, is_admin=True), trigger_kind='webhook')) == {'Mine', 'Theirs'}


def test_7398_webhook_url_is_built_from_the_uuid_only_for_webhooks(env):
    hook = env.row(_seed_webhook(env))
    cron = env.row(env.seed())
    assert env.access.row_webhook_url(3, hook) == f'/api/v2/elitea_core/hooks/prompt_lib/3/{hook.uuid}'
    assert env.access.row_webhook_url(3, cron) is None
