"""#7453: the public webhook door — ordered checks, uniform 404, ping, dedup, hand-over.

The real `api/v2/hooks.py` runs inside a Flask test request on the SQLite schedule harness. The vault is a dict,
Redis a dict with SET NX semantics, the auth RPC and the run starter are stubs the tests flip per case.
"""
import base64
import hashlib
import hmac
import io
import logging
import os
import sys
import time
import types
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fixtures.helpers import load_module_with_stubs
from integration.test_7011_schedule_service import OWNER_ID, PACKAGE, PLUGIN_ROOT, PROJECT_ID, env  # noqa: F401

SECRET = 'current-secret-value'
OLD_SECRET = 'previous-secret-value'
BODY = b'{"action": "opened", "token": "body-marker-7453"}'


class FakeRedis:
    def __init__(self):
        self.keys, self.calls = {}, []

    def set(self, key, value, nx=False, ex=None):
        self.calls.append((key, nx, ex))
        if nx and key in self.keys:
            return None
        self.keys[key] = value
        return True


class Headers(dict):
    """Case-insensitive like werkzeug's headers, which senders rely on."""

    def __init__(self, values):
        super().__init__({key.lower(): value for key, value in values.items()})

    def get(self, key, default=None):
        return super().get(key.lower(), default)


class Response:
    def __init__(self, status=200):
        self.status_code = status

    def get_data(self):
        return b''


class Request:
    @property
    def data(self):
        pytest.fail('request.data must never be read: the body cap reads the stream')


def _fake_flask(monkeypatch):
    """Other suites stub flask, jinja2 and werkzeug in turn; the route needs only `request` and `Response`."""
    flask = types.ModuleType('flask')
    flask.Response = Response
    flask.request = Request()
    monkeypatch.setitem(sys.modules, 'flask', flask)
    return flask


@pytest.fixture
def door(env, monkeypatch):
    env.flask = _fake_flask(monkeypatch)
    tools = sys.modules['tools']
    tools.VaultClient = lambda project_id: types.SimpleNamespace(get_hidden_secrets=lambda: dict(env.vault))
    env.redis = FakeRedis()
    tools.this.module.get_redis_client = lambda: env.redis
    tools.this.module_name = 'elitea_core'
    identity = lambda *args, **kwargs: (lambda function: function)  # noqa: E731
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}), APIBase=type('APIBase', (), {}),
        endpoint_metrics=lambda function: function, with_modes=lambda params: params)
    tools.register_openapi = identity

    env.access_result = None
    prerun = types.ModuleType(f'{PACKAGE}.utils.schedule_prerun')
    prerun.ACCESS_CHECK_UNAVAILABLE = 'access check unavailable'
    prerun.check_principal = lambda row: None if (row.principal or {}).get('ref') == row.created_by else \
        'principal does not match the owner'
    prerun.check_owner_access = lambda project_id, owner_id: env.access_result
    monkeypatch.setitem(sys.modules, prerun.__name__, prerun)

    env.started, env.start_ok = [], True
    dispatch = types.ModuleType(f'{PACKAGE}.utils.schedule_dispatch')
    dispatch.START_FAILED = 'the run could not start'

    def start(entity_type, project_id, schedule_id, run_id, owner_request=None, payload=None):
        env.started.append((schedule_id, run_id, payload))
        return env.start_ok
    dispatch.start_schedule_run = start
    monkeypatch.setitem(sys.modules, dispatch.__name__, dispatch)

    for name in (f'{PACKAGE}.api', f'{PACKAGE}.api.v2'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')
    exceptions = load('utils/exceptions.py', 'utils.exceptions')
    # utils/utils.py pulls the whole ORM; its GitHub check is three lines, mirrored here
    helpers = types.ModuleType(f'{PACKAGE}.utils.utils')

    def verify_signature(payload_body, secret_token, signature_header):
        expected = 'sha256=' + hmac.new(secret_token.encode(), payload_body, hashlib.sha256).hexdigest()
        if not signature_header or not hmac.compare_digest(expected, signature_header):
            raise exceptions.VerifySignatureError({'error': 'mismatch'})
    helpers.verify_signature = verify_signature
    monkeypatch.setitem(sys.modules, helpers.__name__, helpers)
    load('utils/pipeline_trigger.py', 'utils.pipeline_trigger')
    load('utils/schedule_claim.py', 'utils.schedule_claim')
    env.door = load('utils/webhook_door.py', 'utils.webhook_door')
    sys.modules[f'{PACKAGE}.utils'].webhook_door = env.door
    env.door._last_reject_log = 0.0
    env.hooks = load('api/v2/hooks.py', 'api.v2.hooks')
    return env


def _webhook(env, webhook_type='github', **fields):
    row_uuid = uuid.uuid4()
    secret = fields.pop('_secret', None) or SECRET
    schedule_id = env.seed(trigger_kind='webhook', webhook_type=webhook_type, cron=None, timezone=None,
                           next_run=None, uuid=row_uuid, **fields)
    key = 'webhook_signing_secret_%s' if fields.get('gitlab_auth_method') == 'signing_token' else 'webhook_secret_%s'
    env.vault[key % row_uuid] = secret
    return schedule_id, str(row_uuid)


def _github(body, secret=SECRET, **extra):
    signature = 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {'X-Hub-Signature-256': signature, 'X-GitHub-Event': 'issues', 'X-GitHub-Delivery': str(uuid.uuid4()),
            **extra}


def _post(env, row_uuid, headers, body=BODY, chunked=False):
    # A chunked request has no Content-Length to trust: only the stream
    request = env.flask.request
    request.stream, request.headers = io.BytesIO(body), Headers(headers)
    request.content_length = None if chunked else len(body)
    reply = env.hooks.HooksAPI().post(PROJECT_ID, row_uuid)
    if isinstance(reply, Response):
        return reply.status_code, reply.get_data(), {}
    data, status, *rest = reply
    return status, data, (rest[0] if rest else {})


def _runs(env):
    with env.session_factory() as session:
        return session.query(sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun).all()


# --- step 7: an accepted delivery

def test_7453_signed_github_delivery_starts_a_webhook_run_with_the_body_in_memory(door):
    schedule_id, row_uuid = _webhook(door)
    status, data, _ = _post(door, row_uuid, _github(BODY))
    assert status == 202
    (run,) = _runs(door)
    assert data == {'run_id': run.id} and run.trigger == 'webhook' and run.status == 'running'
    assert door.started == [(schedule_id, run.id, BODY)]
    row = door.row(schedule_id)
    assert row.last_delivery_outcome == 'accepted' and row.last_delivery_at is not None


# --- step 2: one identical empty 404, nothing written

def _rejections(env):
    _, github_uuid = _webhook(env)
    _, gitlab_uuid = _webhook(env, webhook_type='gitlab', gitlab_auth_method='secret_token')
    cron_id = env.seed()
    with env.session_factory() as session:
        cron_uuid = str(session.get(env.EntitySchedule, cron_id).uuid)
    return {
        'unknown uuid': (str(uuid.uuid4()), _github(BODY)),
        'malformed uuid': ('not-a-uuid', _github(BODY)),
        'cron row': (cron_uuid, _github(BODY)),
        'wrong type': (gitlab_uuid, _github(BODY)),
        'missing signature': (github_uuid, {'X-GitHub-Delivery': 'd1'}),
        'bad signature': (github_uuid, _github(BODY, secret='wrong')),
        'tampered body': (github_uuid, _github(b'{"other": 1}')),
    }


def test_7453_every_rejection_is_the_same_empty_404_and_writes_nothing(door):
    answers = {name: _post(door, row_uuid, headers) for name, (row_uuid, headers) in _rejections(door).items()}
    assert {(status, data, tuple(extra)) for status, data, extra in answers.values()} == {(404, b'', ())}, answers
    assert _runs(door) == [] and door.redis.calls == [] and door.started == []
    with door.session_factory() as session:
        assert all(row.last_delivery_at is None for row in session.query(door.EntitySchedule))


# --- rotation window

def test_7453_previous_secret_is_accepted_only_inside_the_rotation_window(door):
    now = datetime.now(UTC)
    _, open_uuid = _webhook(door, previous_secret_until=now + timedelta(hours=1))
    _, closed_uuid = _webhook(door, previous_secret_until=now - timedelta(seconds=1))
    for row_uuid in (open_uuid, closed_uuid):
        door.vault[f'webhook_secret_{row_uuid}_prev'] = OLD_SECRET
    assert _post(door, open_uuid, _github(BODY, secret=OLD_SECRET))[0] == 202
    assert _post(door, closed_uuid, _github(BODY, secret=OLD_SECRET))[0] == 404
    assert _post(door, closed_uuid, _github(BODY))[0] == 202


# --- step 1: body cap before the read

def test_7453_declared_length_over_the_cap_is_413_without_reading(door):
    _, row_uuid = _webhook(door)
    big = b'x' * (door.door.MAX_BODY_BYTES + 1)
    assert _post(door, row_uuid, _github(big), body=big)[0] == 413
    assert _runs(door) == []


def test_7453_chunked_body_over_the_cap_is_413_after_a_bounded_read(door):
    _, row_uuid = _webhook(door)
    big = b'x' * (door.door.MAX_BODY_BYTES * 3)
    stream = io.BytesIO(big)
    with pytest.raises(door.door.BodyTooLarge):
        door.door.read_capped_body(stream, None)
    assert stream.tell() == door.door.MAX_BODY_BYTES + 1  # never more than cap + 1 bytes read
    assert _post(door, row_uuid, _github(big), body=big, chunked=True)[0] == 413


# --- step 3 / 3a: disabled and ping

def test_7453_disabled_row_answers_409_with_its_reason_even_for_a_ping(door):
    schedule_id, row_uuid = _webhook(door, enabled=False, disabled_reason='owner_lost_access')
    for event in ('issues', 'ping'):
        status, data, _ = _post(door, row_uuid, _github(BODY, **{'X-GitHub-Event': event}))
        assert (status, data) == (409, {'code': 'webhook_disabled', 'reason': 'owner_lost_access'})
    assert _runs(door) == [] and door.row(schedule_id).last_delivery_outcome == 'disabled'


def test_7453_github_ping_records_the_delivery_and_starts_no_run(door):
    schedule_id, row_uuid = _webhook(door)
    assert _post(door, row_uuid, _github(BODY, **{'X-GitHub-Event': 'ping'}))[:2] == (200, {'ping': True})
    assert _runs(door) == [] and door.started == []
    assert door.row(schedule_id).last_delivery_outcome == 'ping'


# --- step 4: dedup

def test_7453_same_delivery_id_is_a_duplicate_for_24h_and_starts_one_run(door):
    schedule_id, row_uuid = _webhook(door)
    headers = _github(BODY)
    assert _post(door, row_uuid, headers)[0] == 202
    door.started.clear()
    with door.session_factory() as session:  # the first run ended, so only dedup can stop the second
        for run in session.query(sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun):
            run.status = 'succeeded'
        session.commit()
    assert _post(door, row_uuid, headers)[:2] == (200, {'duplicate': True})
    assert door.started == [] and len(_runs(door)) == 1
    key = f'webhook:{schedule_id}:{headers["X-GitHub-Delivery"]}'
    assert door.redis.calls[0] == (key, True, 24 * 3600)
    assert door.row(schedule_id).last_delivery_outcome == 'duplicate'


def test_7453_delivery_without_an_id_is_not_deduplicated(door):
    _, row_uuid = _webhook(door)
    headers = _github(BODY)
    headers.pop('X-GitHub-Delivery')
    assert _post(door, row_uuid, headers)[0] == 202
    assert door.redis.calls == []


# --- step 5: owner checks

def test_7453_owner_refusals_answer_403_with_a_readable_code(door):
    schedule_id, row_uuid = _webhook(door)
    door.access_result = 'owner lost access'
    assert _post(door, row_uuid, _github(BODY))[:2] == (403, {'code': 'owner_lost_access'})
    _, other_uuid = _webhook(door, principal={'kind': 'delegation', 'ref': 7})
    door.access_result = None
    assert _post(door, other_uuid, _github(BODY))[:2] == (403, {'code': 'principal_mismatch'})
    assert _runs(door) == [] and door.row(schedule_id).last_delivery_outcome == 'refused'


def test_7453_access_check_unavailable_answers_503_so_the_sender_retries(door):
    _, row_uuid = _webhook(door)
    door.access_result = 'access check unavailable'
    status, data, headers = _post(door, row_uuid, _github(BODY))
    assert (status, data, headers) == (503, {'code': 'access_check_unavailable'}, {'Retry-After': '30'})
    assert _runs(door) == []


# --- step 7: no free slot

def test_7453_no_free_slot_answers_503_and_closes_the_run_failed(door):
    _, row_uuid = _webhook(door)
    door.start_ok = False
    status, data, headers = _post(door, row_uuid, _github(BODY))
    assert (status, data, headers['Retry-After']) == (503, {'code': 'no_free_slot'}, '30')
    (run,) = _runs(door)
    assert (run.status, run.reason) == ('failed', 'the run could not start')


# --- the other sender types

def test_7453_gitlab_secret_token_and_custom_token_are_accepted(door):
    _, gitlab_uuid = _webhook(door, webhook_type='gitlab', gitlab_auth_method='secret_token')
    _, custom_uuid = _webhook(door, webhook_type='custom')
    assert _post(door, gitlab_uuid, {'X-Gitlab-Token': SECRET, 'X-Gitlab-Event-UUID': 'g1'})[0] == 202
    assert _post(door, custom_uuid, {'X-Webhook-Token': SECRET})[0] == 202
    assert _post(door, custom_uuid, {'X-Webhook-Token': 'nope'})[0] == 404


def _gitlab_signed(token_key, body, timestamp):
    webhook_id = 'msg_' + uuid.uuid4().hex
    digest = hmac.new(token_key, f'{webhook_id}.{timestamp}.'.encode() + body, hashlib.sha256).digest()
    return {'webhook-id': webhook_id, 'webhook-timestamp': str(timestamp),
            'webhook-signature': 'v1,' + base64.b64encode(digest).decode()}


def test_7453_gitlab_signing_token_is_accepted_in_the_window_and_refused_when_stale(door):
    key = os.urandom(32)
    _, row_uuid = _webhook(door, webhook_type='gitlab', gitlab_auth_method='signing_token',
                           _secret='whsec_' + base64.b64encode(key).decode())
    assert _post(door, row_uuid, _gitlab_signed(key, BODY, int(time.time())))[0] == 202
    assert _post(door, row_uuid, _gitlab_signed(key, BODY, int(time.time()) - 600))[0] == 404


# --- log hygiene

def test_7453_logs_carry_no_headers_body_or_secret(door, caplog):
    _, row_uuid = _webhook(door)
    log = sys.modules['pylon.core.tools'].log
    lines = []
    for level in ('info', 'warning', 'error', 'exception', 'debug'):
        setattr(log, level, lambda message, *args, **kwargs: lines.append(message % args if args else message))
    caplog.set_level(logging.DEBUG)
    bad = _github(BODY, secret='wrong')
    good = _github(BODY)
    assert _post(door, row_uuid, bad)[0] == 404
    assert _post(door, row_uuid, good)[0] == 202
    text = '\n'.join(lines) + caplog.text
    assert lines, 'both requests should log one line'
    for secret_or_payload in (SECRET, 'body-marker-7453', bad['X-Hub-Signature-256'], good['X-Hub-Signature-256']):
        assert secret_or_payload not in text
