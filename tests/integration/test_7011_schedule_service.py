"""EL-7011 US-07: the schedule service on SQLite (owner, scope, next run, admin write, run input).

Covers R1.1, R1.2, R2.1, R2.3, R2.4, R3.2, R4.1, R5.3 and R5.5. The service, the body models, the
timing helpers and the schedule table are real. The plugin package around them is stubbed.
The service contract these tests fix:

* `create_schedule(session, project_id, body, scope)` commits and returns the row.
* `load_schedule_in_scope(session, schedule_id, scope)` returns the row or raises `ScheduleNotFound`
  (design D3); `update_schedule(session, project_id, row, body, scope)` and
  `delete_schedule(session, project_id, row, scope)` write that row.
* `check_run_input(entity_type, run_mode, prompt, variables)` raises `ScheduleError` naming the field.

Run via:
    python tests/run_tests.py integration/test_7011_schedule_service.py -v
"""
import pathlib
import sys
import types
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
OWNER_ID, OTHER_ID, ADMIN_ID = 42, 43, 99
PAST = datetime(2020, 1, 1, tzinfo=UTC)


def _real_sqlalchemy():
    """Real sqlalchemy modules, even when an earlier suite left stubs (no `__file__`) in sys.modules."""
    stubs = {n: m for n, m in sys.modules.items()
             if (n == 'sqlalchemy' or n.startswith('sqlalchemy.')) and not hasattr(m, '__file__')}
    for name in stubs:
        del sys.modules[name]
    try:
        import sqlalchemy.dialects.postgresql
        import sqlalchemy.ext.compiler
        import sqlalchemy.orm  # noqa: F401
        return {n: m for n, m in sys.modules.items() if n == 'sqlalchemy' or n.startswith('sqlalchemy.')}
    finally:
        sys.modules.update(stubs)


REAL_SQLALCHEMY = _real_sqlalchemy()
sa = REAL_SQLALCHEMY['sqlalchemy']
orm = REAL_SQLALCHEMY['sqlalchemy.orm']


@REAL_SQLALCHEMY['sqlalchemy.ext.compiler'].compiles(REAL_SQLALCHEMY['sqlalchemy.dialects.postgresql'].JSONB, 'sqlite')
def _jsonb_as_json(type_, compiler, **kw):
    return 'JSON'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


class _LenientModule(types.ModuleType):
    """A module whose unknown names are mocks, for imports the service never exercises."""

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return MagicMock(name=name)


class Env:
    """The loaded service, the schedule model and one SQLite database."""

    def scope(self, user_id, is_admin=False):
        return self.access.ScheduleScope(user_id=user_id, is_admin=is_admin, permissions=frozenset())

    def body(self, **changes):
        values = {
            'name': 'Nightly', 'entity_type': 'agent', 'application_id': 1, 'version_id': 10,
            'prompt': 'Say hi to {{ who }}', 'variables': [{'name': 'who', 'value': 'Ann'}],
            'cron': '0 9 * * *', 'timezone': 'Etc/UTC',
        }
        return self.pd.ScheduleCreate(**{**values, **changes})

    def seed(self, created_by=OWNER_ID, **fields):
        values = {
            'entity_type': 'agent', 'application_id': 1, 'version_id': 10, 'name': 'Nightly',
            'cron': '0 9 * * *', 'timezone': 'Etc/UTC', 'prompt': 'Say hi', 'variables': [], 'enabled': True,
            'next_run': PAST, 'created_by': created_by,
            'principal': {'kind': 'delegation', 'ref': created_by},
        }
        with self.session_factory() as session:
            row = self.EntitySchedule(**{**values, **fields})
            session.add(row)
            session.commit()
            return row.id

    def row(self, schedule_id):
        with self.session_factory() as session:
            return session.get(self.EntitySchedule, schedule_id)

    def run(self, function, *args):
        with self.session_factory() as session:
            return getattr(self.service, function)(session, PROJECT_ID, *args)

    def edit(self, function, schedule_id, scope, *args):
        """Load the row in the caller's scope, then update or delete it, as the API does."""
        with self.session_factory() as session:
            row = self.service.load_schedule_in_scope(session, schedule_id, scope)
            return getattr(self.service, function)(session, PROJECT_ID, row, *args, scope)


@pytest.fixture
def env(monkeypatch):
    original = sys.modules.copy()
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    env = Env()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        _package(name)

    class Base(orm.DeclarativeBase):
        pass

    class Application(Base):
        __tablename__ = 'applications'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, default='Entity')

    class ApplicationVersion(Base):
        __tablename__ = 'application_versions'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        application_id = sa.Column(sa.Integer, sa.ForeignKey('tenant.applications.id'))
        agent_type = sa.Column(sa.String, default='openai')

    models = sys.modules[f'{PACKAGE}.models']
    models.db = types.SimpleNamespace(Base=Base)
    models.db_tools = types.SimpleNamespace(AbstractBaseMixin=type('Mixin', (), {}))
    models.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA='tenant')
    all_models = types.ModuleType(f'{PACKAGE}.models.all')
    all_models.Application, all_models.ApplicationVersion = Application, ApplicationVersion
    sys.modules[all_models.__name__] = all_models

    sys.modules['pylon.core.tools'] = types.SimpleNamespace(
        log=types.SimpleNamespace(debug=print, info=print, warning=print, error=print, exception=print),
    )
    span = types.SimpleNamespace(set_attribute=lambda key, value: None)
    otel = types.ModuleType('opentelemetry')
    otel.trace = types.SimpleNamespace(get_current_span=lambda: span)
    sys.modules['opentelemetry'] = otel
    sys.modules['opentelemetry.trace'] = otel.trace

    engine = sa.create_engine('sqlite://').execution_options(schema_translate_map={'tenant': None})
    env.session_factory = lambda: orm.Session(engine, expire_on_commit=False)
    tools = _LenientModule('tools')
    tools.auth = types.SimpleNamespace(current_user=lambda: {'id': OWNER_ID})
    tools.config = types.SimpleNamespace(
        ADMINISTRATION_MODE='administration', DEFAULT_MODE='default', POSTGRES_TENANT_SCHEMA='tenant',
    )
    tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    sys.modules['tools'] = tools
    env.config = tools.this.module.descriptor.config

    folder_access = types.ModuleType(f'{PACKAGE}.utils.folder_access')
    folder_access.entity_access_error = lambda *args, **kwargs: None
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda *args, **kwargs: True
    sys.modules[folder_access.__name__] = folder_access
    sys.modules[notifications.__name__] = notifications

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    load('models/enums/all.py', 'models.enums.all')
    env.EntitySchedule = load('models/entity_schedule.py', 'models.entity_schedule').EntitySchedule
    env.EntitySchedule.__table__.c.variables.server_default = None  # `::jsonb` is not SQLite
    Base.metadata.create_all(engine)
    load('utils/constants.py', 'utils.constants')
    env.timing = load('utils/schedule_timing.py', 'utils.schedule_timing')
    env.access = load('utils/schedule_access.py', 'utils.schedule_access')
    load('models/pd/index.py', 'models.pd.index')
    env.pd = load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
    env.service = load('utils/schedule_service.py', 'utils.schedule_service')

    with env.session_factory() as session:
        session.add_all([Application(id=1), Application(id=2)])
        session.flush()
        session.add_all([
            ApplicationVersion(id=10, application_id=1, agent_type='openai'),
            ApplicationVersion(id=11, application_id=2, agent_type='pipeline'),
        ])
        session.commit()
    yield env
    for key in set(sys.modules) - set(original):
        sys.modules.pop(key, None)


def utc(moment):
    """SQLite drops the zone of a stored datetime; the service writes UTC."""
    return moment.replace(tzinfo=UTC)


# --- R1.1, R1.2, R5.3: create

def test_7011_r1_1_create_stores_the_caller_as_creator_and_editor(env):
    row = env.run('create_schedule', env.body(), env.scope(OWNER_ID))
    stored = env.row(row.id)
    assert (stored.created_by, stored.updated_by, stored.name) == (OWNER_ID, OWNER_ID, 'Nightly')


def test_7011_r1_2_create_stores_a_delegation_principal_of_the_caller(env):
    row = env.run('create_schedule', env.body(), env.scope(OWNER_ID))
    assert env.row(row.id).principal == {'kind': 'delegation', 'ref': OWNER_ID}


def test_7011_r5_3_create_stores_variables_as_name_value_items(env):
    row = env.run('create_schedule', env.body(), env.scope(OWNER_ID))
    assert env.row(row.id).variables == [{'name': 'who', 'value': 'Ann'}]


# --- R2.1: owner fixed on every write

@pytest.mark.parametrize('changes', [{'name': 'Renamed'}, {'enabled': False}, {'cron': '30 6 * * *'}])
def test_7011_r2_1_update_keeps_created_by_and_principal(env, changes):
    schedule_id = env.seed()
    env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(**changes))
    row = env.row(schedule_id)
    assert (row.created_by, row.principal, row.updated_by) == (
        OWNER_ID, {'kind': 'delegation', 'ref': OWNER_ID}, OWNER_ID,
    )


# --- R2.3: next run

@pytest.mark.parametrize('changes', [{'cron': '30 6 * * *'}, {'timezone': 'Asia/Tokyo'}])
def test_7011_r2_3_cron_or_timezone_change_recomputes_next_run_from_now(env, changes):
    schedule_id = env.seed()
    env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(**changes))
    row = env.row(schedule_id)
    expected = env.timing.compute_next_run(row.cron, row.timezone, datetime.now(UTC))
    assert abs(utc(row.next_run) - expected) < timedelta(minutes=1)


def test_7011_r2_3_enabling_a_paused_schedule_recomputes_next_run(env):
    schedule_id = env.seed(enabled=False, next_run=None)
    env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(enabled=True))
    assert utc(env.row(schedule_id).next_run) > datetime.now(UTC)


def test_7011_r2_3_a_rename_leaves_next_run_alone(env):
    schedule_id = env.seed()
    env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(name='Renamed'))
    assert utc(env.row(schedule_id).next_run) == PAST


# --- R2.4: resume

def test_7011_r2_4_resume_clears_disabled_reason(env):
    schedule_id = env.seed(enabled=False, disabled_reason='owner_lost_access')
    env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(enabled=True))
    row = env.row(schedule_id)
    assert (row.enabled, row.disabled_reason) == (True, None)


def test_7011_r2_4_resume_over_the_cap_is_refused_and_stays_disabled(env):
    env.config['scheduler'] = {'schedules': {'max_enabled_per_project': 1}}
    env.seed(name='Active')
    schedule_id = env.seed(enabled=False, disabled_reason='expired')
    with pytest.raises(env.timing.ScheduleLimitError):
        env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(enabled=True))
    row = env.row(schedule_id)
    assert (row.enabled, row.disabled_reason) == (False, 'expired')


# --- R3.2: out of scope leaves the row alone

def test_7011_r3_2_update_outside_the_scope_is_not_found_and_keeps_the_row(env):
    schedule_id = env.seed(created_by=OTHER_ID)
    with pytest.raises(env.service.ScheduleNotFound):
        env.edit('update_schedule', schedule_id, env.scope(OWNER_ID), env.pd.ScheduleUpdate(name='Hijacked'))
    row = env.row(schedule_id)
    assert (row.name, row.updated_by) == ('Nightly', None)


def test_7011_r3_2_delete_outside_the_scope_is_not_found_and_keeps_the_row(env):
    schedule_id = env.seed(created_by=OTHER_ID)
    with pytest.raises(env.service.ScheduleNotFound):
        env.edit('delete_schedule', schedule_id, env.scope(OWNER_ID))
    assert env.row(schedule_id) is not None


# --- R4.1: an admin changes another owner's schedule

def test_7011_r4_1_admin_update_applies_and_keeps_the_owner(env):
    schedule_id = env.seed(created_by=OTHER_ID)
    env.edit('update_schedule', schedule_id, env.scope(ADMIN_ID, True), env.pd.ScheduleUpdate(name='Edited by admin'))
    row = env.row(schedule_id)
    assert (row.name, row.created_by, row.updated_by) == ('Edited by admin', OTHER_ID, ADMIN_ID)
    assert row.principal == {'kind': 'delegation', 'ref': OTHER_ID}


def test_7011_r4_1_admin_delete_removes_the_row(env):
    schedule_id = env.seed(created_by=OTHER_ID)
    env.edit('delete_schedule', schedule_id, env.scope(ADMIN_ID, True))
    assert env.row(schedule_id) is None


# --- R5.5: run input rules name the broken field

@pytest.mark.parametrize('entity_type, run_mode, prompt, variables, field', [
    ('pipeline', 'entry_point', 'Say hi', [], 'prompt'),
    ('pipeline', 'entry_point', None, [{'name': 'who', 'value': 'Ann'}], 'variables'),
    ('pipeline', 'prompt', None, [], 'prompt'),
])
def test_7011_r5_5_run_mode_rules_name_the_broken_field(env, entity_type, run_mode, prompt, variables, field):
    with pytest.raises(env.service.ScheduleError) as error:
        env.service.check_run_input(entity_type, run_mode, prompt, variables)
    assert error.value.response[0]['field'] == field


def test_7011_r5_5_a_pipeline_in_entry_point_mode_without_input_passes(env):
    assert env.service.check_run_input('pipeline', 'entry_point', None, []) is None
