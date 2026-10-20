"""EL-7011 US-14: deleting a version disables its schedules in the delete transaction (R2.1, R2.2, R2.3).

Both delete paths of `rpc/application.py` run for real (`application_delete_version` and
`batch_replace_version_references`), on SQLite with the real schedule table and the real helper.

Run via:
    python tests/run_tests.py integration/test_7011_version_delete_disables_schedules.py -v
"""
import ast
import pathlib
import sys
import types
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
FUNCTIONS = ('_notify_version_schedules_disabled', 'application_delete_version', 'batch_replace_version_references')


class Env:
    """The two real delete functions over one SQLite database; `events` lists commits and notices in order."""

    def stored(self, schedule_id):
        with self.session_factory() as session:
            row = session.get(self.EntitySchedule, schedule_id)
            return row.enabled, row.disabled_reason

    def delete_version(self, version_id=10, replacement=None):
        return self.rpc.application_delete_version(self.owner, PROJECT_ID, version_id, replacement)

    def replace_versions(self, old_id=10, new_id=11):
        return self.rpc.batch_replace_version_references(self.owner, PROJECT_ID, old_id, new_id)


@pytest.fixture
def env(monkeypatch):
    original = sys.modules.copy()
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    env = Env()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.rpc', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    Base, Application, ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    engine = sqlite_db.sqlite_engine()
    env.session_factory = lambda: sqlite_db.orm.Session(engine, expire_on_commit=False)
    env.events = []
    env.fail_commit = False

    def notify(project_id, owner_id, event_type, schedule, run=None, extra=None):
        env.events.append(('notice', project_id, owner_id, str(event_type), schedule.id, schedule.name, extra))

    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = notify
    sys.modules[notifications.__name__] = notifications

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    env.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    load('utils/schedule_lifecycle.py', 'utils.schedule_lifecycle')
    sqlite_db.add_agent_and_pipeline(env.session_factory, Application, ApplicationVersion)

    class Session:
        """The real session; tool queries answer empty, the version row looks like the full model."""

        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def query(self, model):
            if model is ApplicationVersion:
                return types.SimpleNamespace(get=self._version)
            return MagicMock(**{'filter.return_value.all.return_value': []})

        def _version(self, version_id):
            version = self._real.get(ApplicationVersion, version_id)
            if version:
                version.name, version.status, version.meta = f'v{version_id}', 'draft', {}
                version.application = types.SimpleNamespace(meta={'default_version_id': 11})
            return version

        def commit(self):
            if env.fail_commit:
                raise RuntimeError('commit failed')
            self._real.commit()
            env.events.append(('commit',))

    @contextmanager
    def get_session(project_id):
        with env.session_factory() as real:
            yield Session(real)

    tree = ast.parse((PLUGIN_ROOT / 'rpc' / 'application.py').read_text())
    functions = [
        node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS
    ]
    for function in functions:
        function.decorator_list = []
    namespace = {
        '__package__': f'{PACKAGE}.rpc', 'db': types.SimpleNamespace(get_session=get_session),
        'ApplicationVersion': ApplicationVersion, 'EliteATool': MagicMock(), 'EntityToolMapping': MagicMock(),
        'Integer': MagicMock(), 'PublishStatus': types.SimpleNamespace(published='published', embedded='embedded'),
        'ApplicationEvents': types.SimpleNamespace(application_version_deleted='version_deleted'),
        'detach_skills_for_entity_versions': lambda session, ids: None,
        'log': types.SimpleNamespace(info=lambda *a, **k: None),
    }
    exec(compile(ast.fix_missing_locations(ast.Module(body=functions, type_ignores=[])), 'rpc/application.py', 'exec'),  # noqa: S102 - runs the two real functions cut out of rpc/application.py
         namespace)
    env.owner = types.SimpleNamespace(
        context=types.SimpleNamespace(event_manager=types.SimpleNamespace(fire_event=lambda *a: None)),
    )
    env.rpc = types.SimpleNamespace(**{name: namespace[name] for name in FUNCTIONS})
    yield env
    for key in set(sys.modules) - set(original):
        sys.modules.pop(key, None)


def seed(env, created_by, **fields):
    return sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, created_by, **fields)


def notices(env):
    return [event for event in env.events if event[0] == 'notice']


DELETE_PATHS = pytest.mark.parametrize('path', ['delete_version', 'replace_versions'])


# --- R2.1: the schedules of the deleted version are disabled

@DELETE_PATHS
def test_7011_r2_1_delete_disables_active_and_paused_schedules_of_the_version(env, path):
    active, paused = seed(env, 42), seed(env, 43, name='Paused', enabled=False)
    other_version = seed(env, 42, name='Other', version_id=11, application_id=2)

    assert getattr(env, path)()['ok']

    assert env.stored(active) == (False, 'version_deleted')
    assert env.stored(paused) == (False, 'version_deleted')
    assert env.stored(other_version) == (True, None)


# --- R2.2: one notice per disabled schedule, after the commit

@DELETE_PATHS
def test_7011_r2_2_delete_sends_one_notice_per_schedule_to_its_owner_after_commit(env, path):
    first, second = seed(env, 42, name='A'), seed(env, 43, name='B')

    getattr(env, path)()

    assert env.events[0] == ('commit',)
    assert sorted((event[2], event[4], event[5]) for event in notices(env)) == [(42, first, 'A'), (43, second, 'B')]
    for event in notices(env):
        assert (event[1], event[3], event[6]) == (PROJECT_ID, 'schedule_disabled', {'reason': 'version_deleted'})


# --- R2.3: a refused or rolled back delete changes nothing and sends nothing

def test_7011_r2_3_refused_delete_keeps_the_schedules_and_sends_no_notice(env):
    schedule = seed(env, 42, version_id=11, application_id=2)

    assert 'error' in env.delete_version(version_id=11)

    assert env.stored(schedule) == (True, None)
    assert env.events == []


@DELETE_PATHS
def test_7011_r2_3_rolled_back_delete_keeps_the_schedules_and_sends_no_notice(env, path):
    schedule = seed(env, 42)
    env.fail_commit = True

    with pytest.raises(RuntimeError):
        getattr(env, path)()

    assert env.stored(schedule) == (True, None)
    assert env.events == []
