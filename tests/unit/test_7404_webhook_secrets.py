"""#7404: per-row webhook secrets in the hidden vault store.

`utils/webhook_secrets.py` is loaded with a fake VaultClient backed by one shared dict, the way the hidden KV
behaves: every client reads the whole store and writes it back whole. The SQLite session stands in for the
caller's transaction (the advisory lock is PG-only and is covered by the PG and live tests).
"""
import sys
import threading
import time
import types
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs
from integration.test_7011_schedule_service import PACKAGE, PLUGIN_ROOT, PROJECT_ID, env  # noqa: F401

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)


class FakeVault:
    """The hidden store of one project; `delay` widens the read-modify-write window to provoke lost writes."""
    store: dict = {}
    delay = 0.0
    writes = 0

    def __init__(self, project_id):
        self.project_id = project_id

    def get_hidden_secrets(self):
        return dict(FakeVault.store)

    def set_hidden_secrets(self, secrets):
        time.sleep(FakeVault.delay)
        FakeVault.writes += 1
        FakeVault.store = dict(secrets)


@pytest.fixture
def secrets_mod(env):
    FakeVault.store, FakeVault.delay, FakeVault.writes = {'unrelated': 'keep'}, 0.0, 0
    sys.modules['tools'].VaultClient = FakeVault
    return load_module_with_stubs(PLUGIN_ROOT / 'utils/webhook_secrets.py', f'{PACKAGE}.utils.webhook_secrets')


def _hook(**fields):
    values = {'uuid': uuid.uuid4(), 'webhook_type': 'github', 'gitlab_auth_method': None,
              'secret_ref': None, 'signing_secret_ref': None, 'previous_secret_ref': None,
              'previous_secret_until': None}
    return types.SimpleNamespace(**{**values, **fields})


def _session(env):
    return env.session_factory()


def test_7404_store_writes_a_generated_secret_under_the_row_uuid(env, secrets_mod):
    row = _hook()
    with _session(env) as session:
        value = secrets_mod.store_row_secret(session, PROJECT_ID, row)
    key = f'webhook_secret_{row.uuid}'
    assert FakeVault.store == {'unrelated': 'keep', key: value}
    assert row.secret_ref == '{{secret.%s}}' % key and row.signing_secret_ref is None
    assert len(value) >= 40  # token_urlsafe(32)


def test_7404_store_of_a_gitlab_signing_row_keeps_the_pasted_token(env, secrets_mod):
    row = _hook(webhook_type='gitlab', gitlab_auth_method='signing_token')
    with _session(env) as session:
        secrets_mod.store_row_secret(session, PROJECT_ID, row, 'whsec_abc')
    key = f'webhook_signing_secret_{row.uuid}'
    assert FakeVault.store[key] == 'whsec_abc'
    assert row.signing_secret_ref == '{{secret.%s}}' % key and row.secret_ref is None


def test_7404_rotate_keeps_the_old_secret_for_24_hours(env, secrets_mod):
    row = _hook()
    with _session(env) as session:
        old = secrets_mod.store_row_secret(session, PROJECT_ID, row)
        new = secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW)
    assert new != old
    assert FakeVault.store[f'webhook_secret_{row.uuid}'] == new
    assert FakeVault.store[f'webhook_secret_{row.uuid}_prev'] == old
    assert row.previous_secret_ref == '{{secret.webhook_secret_%s_prev}}' % row.uuid
    assert row.previous_secret_until == NOW + timedelta(hours=24)


def test_7404_regenerate_replaces_at_once_and_drops_the_previous(env, secrets_mod):
    row = _hook()
    with _session(env) as session:
        secrets_mod.store_row_secret(session, PROJECT_ID, row)
        secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW)
        new = secrets_mod.regenerate_row_secret(session, PROJECT_ID, row)
    assert FakeVault.store == {'unrelated': 'keep', f'webhook_secret_{row.uuid}': new}
    assert (row.previous_secret_ref, row.previous_secret_until) == (None, None)


def test_7404_delete_removes_every_key_of_the_row_only(env, secrets_mod):
    row, other = _hook(), _hook()
    with _session(env) as session:
        secrets_mod.store_row_secret(session, PROJECT_ID, row)
        secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW)
        secrets_mod.store_row_secret(session, PROJECT_ID, other)
        secrets_mod.drop_secrets_of(session, PROJECT_ID, [row.uuid])
    assert set(FakeVault.store) == {'unrelated', f'webhook_secret_{other.uuid}'}


def test_7404_application_delete_drops_the_keys_of_its_webhooks_only(env, secrets_mod):
    """An application delete cascades its rows away; the uuids read before it drive one vault write."""
    hooks = [env.seed(name=f'Hook {n}', application_id=app, trigger_kind='webhook', webhook_type='github',
                      cron=None, timezone=None, next_run=None) for n, app in ((1, 1), (2, 1), (3, 2))]
    env.seed(name='Cron of app 1', application_id=1)
    for schedule_id in hooks:
        FakeVault.store[f'webhook_secret_{env.row(schedule_id).uuid}'] = 'v'
    with _session(env) as session:
        uuids = secrets_mod.application_webhook_uuids(session, 1)
        secrets_mod.drop_secrets_of(session, PROJECT_ID, uuids)
    assert sorted(FakeVault.store) == sorted(['unrelated', f'webhook_secret_{env.row(hooks[2]).uuid}'])
    assert FakeVault.writes == 1


def test_7404_sweep_drops_only_expired_previous_secrets(env, secrets_mod):
    keys = {}
    for name, until in (('expired', NOW - timedelta(minutes=1)), ('live', NOW + timedelta(hours=1))):
        schedule_id = env.seed(name=name, trigger_kind='webhook', webhook_type='github', cron=None, timezone=None,
                               next_run=None, previous_secret_until=until)
        row = env.row(schedule_id)
        keys[name] = (schedule_id, f'webhook_secret_{row.uuid}_prev')
        FakeVault.store[keys[name][1]] = 'old'
    with _session(env) as session:
        assert secrets_mod.sweep_expired_previous(session, PROJECT_ID, NOW) == 1
    assert keys['expired'][1] not in FakeVault.store and keys['live'][1] in FakeVault.store
    assert env.row(keys['expired'][0]).previous_secret_until is None
    assert env.row(keys['live'][0]).previous_secret_until is not None


def test_7404_sweep_with_nothing_expired_does_not_touch_the_vault(env, secrets_mod):
    with _session(env) as session:
        assert secrets_mod.sweep_expired_previous(session, PROJECT_ID, NOW) == 0
    assert FakeVault.writes == 0


# Why the lock exists: the hidden store is one document, so two unlocked writers lose one key.
# The helper serialises writers; here a plain threading lock stands in for pg_advisory_xact_lock.
def test_7404_parallel_creates_keep_both_keys_when_serialised(env, secrets_mod, monkeypatch):
    FakeVault.delay = 0.05
    gate = threading.Lock()
    real = secrets_mod._locked_store

    def locked(session, project_id):
        gate.acquire()
        session.info['released'] = gate
        return real(session, project_id)

    original_set = FakeVault.set_hidden_secrets

    def set_and_release(self, secrets):
        original_set(self, secrets)
        gate.release()  # stands in for the commit that ends the advisory lock

    monkeypatch.setattr(secrets_mod, '_locked_store', locked)
    monkeypatch.setattr(FakeVault, 'set_hidden_secrets', set_and_release)
    rows = [_hook(), _hook()]

    def create(row):
        with _session(env) as session:
            secrets_mod.store_row_secret(session, PROJECT_ID, row)

    threads = [threading.Thread(target=create, args=(row,)) for row in rows]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert {f'webhook_secret_{row.uuid}' for row in rows} <= set(FakeVault.store)


def test_7404_unserialised_writers_do_lose_a_key(env, secrets_mod):
    """Guard for the test above: without the lock the fake store really loses a write."""
    FakeVault.delay = 0.05
    rows = [_hook(), _hook()]
    clients = [FakeVault(PROJECT_ID) for _ in rows]
    reads = [client.get_hidden_secrets() for client in clients]  # both read before either writes
    threads = [threading.Thread(target=client.set_hidden_secrets, args=({**read, f'k{i}': 'v'},))
               for i, (client, read) in enumerate(zip(clients, reads))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not {'k0', 'k1'} <= set(FakeVault.store)


def test_7404_pg_session_takes_the_advisory_lock(secrets_mod):
    executed = []
    session = types.SimpleNamespace(
        get_bind=lambda: types.SimpleNamespace(dialect=types.SimpleNamespace(name='postgresql')),
        execute=lambda statement, params: executed.append((str(statement), params)),
    )
    secrets_mod.store_row_secret(session, PROJECT_ID, _hook())
    assert executed == [(secrets_mod.SECRET_LOCK, {'project_id': PROJECT_ID})]


def test_7404_engines_with_a_locked_merge_get_only_the_changed_keys(env, secrets_mod, monkeypatch):
    """The database engine merges under a row lock; the helper must not send the whole store back."""
    calls = []
    monkeypatch.setattr(FakeVault, 'update_hidden_secrets',
                        lambda self, add=None, remove=None: calls.append((add, sorted(remove))), raising=False)
    row = _hook()
    with _session(env) as session:
        value = secrets_mod.store_row_secret(session, PROJECT_ID, row)
    assert calls == [({f'webhook_secret_{row.uuid}': value}, [])]
    assert FakeVault.writes == 0


# Review fix: a second rotate inside the window would cut the oldest secret's promised 24 h short
def test_7404_second_rotate_inside_the_window_is_refused_and_keeps_the_previous(env, secrets_mod):
    row = _hook()
    with _session(env) as session:
        first = secrets_mod.store_row_secret(session, PROJECT_ID, row)
        secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW)
        with pytest.raises(secrets_mod.RotationInProgress):
            secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW + timedelta(hours=1))
    assert FakeVault.store[f'webhook_secret_{row.uuid}_prev'] == first
    assert row.previous_secret_until == NOW + timedelta(hours=24)


def test_7404_rotate_after_the_window_is_allowed(env, secrets_mod):
    row = _hook()
    with _session(env) as session:
        secrets_mod.store_row_secret(session, PROJECT_ID, row)
        middle = secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW)
        later = NOW + timedelta(hours=25)
        secrets_mod.rotate_row_secret(session, PROJECT_ID, row, later)
    assert FakeVault.store[f'webhook_secret_{row.uuid}_prev'] == middle
    assert row.previous_secret_until == later + timedelta(hours=24)


def test_7404_rotate_without_a_current_value_leaves_no_stale_previous(env, secrets_mod):
    row = _hook(previous_secret_ref='{{secret.stale}}', previous_secret_until=NOW - timedelta(hours=1))
    FakeVault.store[f'webhook_secret_{row.uuid}_prev'] = 'stale'
    with _session(env) as session:
        value = secrets_mod.rotate_row_secret(session, PROJECT_ID, row, NOW)
    assert FakeVault.store == {'unrelated': 'keep', f'webhook_secret_{row.uuid}': value}
    assert (row.previous_secret_ref, row.previous_secret_until) == (None, None)
