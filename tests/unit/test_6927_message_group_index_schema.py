import importlib.util
import pathlib
import sys
import types
from types import SimpleNamespace

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins_6927_index.elitea_core'


class Result:
    def __init__(self, rows=None, scalar=None):
        self.rows = rows or []
        self.scalar_value = scalar

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self.scalar_value


class Connection:
    def __init__(self, lock_granted, states):
        self.lock_granted = lock_granted
        self.states = states
        self.statements = []
        self.failing_projects = set()

    def execution_options(self, **_options):
        return self

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if 'pg_try_advisory_lock' in sql:
            return Result(scalar=self.lock_granted)
        if 'indisvalid' in sql:
            return Result(rows=self.states)
        if any(f'p_{pid}.' in sql for pid in self.failing_projects) and 'CREATE INDEX' in sql:
            raise RuntimeError('build failed')
        return Result()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _drop_stubbed(*prefixes):
    for name in [name for name in sys.modules if name.split('.')[0] in prefixes]:
        if getattr(sys.modules[name], '__file__', None) is None:
            sys.modules.pop(name, None)


@pytest.fixture
def index_schema(isolated_sys_modules):
    _drop_stubbed('sqlalchemy')
    connection_box = {}
    engine = SimpleNamespace(connect=lambda: connection_box['connection'])
    sys.modules['tools'] = types.SimpleNamespace(db=SimpleNamespace(engine=engine))
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = SimpleNamespace(exception=lambda *a, **k: None, info=lambda *a, **k: None)
    sys.modules['pylon.core.tools'] = pylon_tools
    for name in ('plugins_6927_index', PACKAGE, f'{PACKAGE}.utils'):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    models = types.ModuleType(f'{PACKAGE}.models')
    models.CONVERSATION_MESSAGE_GROUP_TABLE_NAME = 'chat_message_group'
    models.MESSAGE_GROUP_AUTHOR_INDEX_NAME = 'ix_chat_message_group_author_conversation_created'
    sys.modules[f'{PACKAGE}.models'] = models
    name = f'{PACKAGE}.utils.message_group_index_schema'
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / 'utils' / 'message_group_index_schema.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

    def run(lock_granted=True, states=(), failing_projects=()):
        connection = Connection(lock_granted, list(states))
        connection.failing_projects = set(failing_projects)
        connection_box['connection'] = connection
        return module.apply_author_index(), connection

    return run


def _ddl(connection):
    return [sql for sql in connection.statements if 'INDEX CONCURRENTLY' in sql]


class TestAuthorIndexSafetyNet:
    def test_a_replica_without_the_lock_builds_nothing(self, index_schema):
        (migrated, failed), connection = index_schema(lock_granted=False, states=[('p_2', None)])

        assert (migrated, failed) == ([], [])
        assert _ddl(connection) == []

    def test_a_missing_index_is_built_without_a_drop(self, index_schema):
        (migrated, _failed), connection = index_schema(states=[('p_2', None)])

        assert migrated == [2]
        assert len(_ddl(connection)) == 1
        assert 'CREATE INDEX CONCURRENTLY IF NOT EXISTS' in _ddl(connection)[0]
        assert 'ON p_2.chat_message_group' in _ddl(connection)[0]

    def test_an_invalid_index_is_dropped_then_rebuilt(self, index_schema):
        (migrated, _failed), connection = index_schema(states=[('p_3', False)])

        assert migrated == [3]
        assert [sql.split()[0] for sql in _ddl(connection)] == ['DROP', 'CREATE']
        assert 'p_3.ix_chat_message_group_author_conversation_created' in _ddl(connection)[0]

    def test_validity_is_read_per_schema(self, index_schema):
        _result, connection = index_schema(states=[])

        state_query = next(sql for sql in connection.statements if 'indisvalid' in sql)
        assert 'c.relnamespace = n.oid' in state_query
        assert 'n.nspname = t.table_schema' in state_query

    def test_one_failing_project_does_not_stop_the_rest_and_the_lock_is_released(self, index_schema):
        (migrated, failed), connection = index_schema(
            states=[('p_2', None), ('p_3', None)], failing_projects={2},
        )

        assert migrated == [3]
        assert failed == [{'project_id': 2}]
        assert 'pg_advisory_unlock' in connection.statements[-1]
