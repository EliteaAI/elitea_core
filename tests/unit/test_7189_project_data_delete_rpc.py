"""Bulk project data delete RPCs for agents, pipelines and counts (#7189).

The helpers and RPC methods are compiled from source with the ORM faked: rpc/application.py needs
the whole pylon runtime to import. The fakes only tell which filter a query was built with.
"""
import ast
import itertools
import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
APPLICATION_RPC = PLUGIN_ROOT / 'rpc' / 'application.py'
APPLICATION_TOOLS = PLUGIN_ROOT / 'utils' / 'application_tools.py'

HELPERS = ('_delete_application', '_application_ids_of_kind', '_blocked_application_ids')
RPC_METHODS = ('applications_delete_by_kind', 'project_data_counts', 'toolkits_delete_by_kind')


class Expr:
    """A filter expression; tag says which one it is"""

    def __init__(self, tag):
        self.tag = tag

    def __invert__(self):
        return Expr('not ' + self.tag)


class Column:
    def __init__(self, tag):
        self.tag = tag

    def __eq__(self, other):
        return Expr(self.tag)

    def in_(self, values):
        return Expr(self.tag + ' blocked')

    __hash__ = object.__hash__


class Versions:
    def any(self, expr):
        return Expr('is_pipeline')


Application = SimpleNamespace(id=Column('app.id'), versions=Versions())
ApplicationVersion = SimpleNamespace(
    agent_type=Column('agent_type'), status=Column('version'), application_id=Column('version.app_id'),
)
Skill = SimpleNamespace(id=Column('skill.id'))
SkillVersion = SimpleNamespace(skill_id=Column('skill_version.skill_id'), status=Column('skill_version'))
EliteATool = SimpleNamespace(id=Column('tool.id'), type=Column('tool.type'), meta=Column('tool.meta'))

PublishStatus = SimpleNamespace(published='published', embedded='embedded')
AgentTypes = SimpleNamespace(pipeline=SimpleNamespace(value='pipeline'))


def _app(app_id, agent_type, statuses):
    versions = [SimpleNamespace(id=app_id * 10 + i, status=s, agent_type=agent_type) for i, s in enumerate(statuses)]
    return SimpleNamespace(
        id=app_id, name=f'app {app_id}', owner_id=7, versions=versions,
        get_default_version=lambda: versions[0],
    )


# 1, 2 agents; 3, 4 pipelines; 2 and 4 cannot be deleted
APPS = {
    1: _app(1, 'react', ['draft']),
    2: _app(2, 'react', ['draft', 'published']),
    3: _app(3, 'pipeline', ['draft']),
    4: _app(4, 'pipeline', ['embedded']),
}
SKILLS = {1: 'draft', 2: 'published', 3: 'draft'}
TOOLS = [('github', {}), ('mcp', {}), ('remote', {'mcp': True}), ('application', {}), ('jira', None)]


class FakeQuery:
    def __init__(self, session, entity):
        self.session = session
        self.entity = entity
        self.tags = []

    def _is(self, *columns):
        # Columns overload ==, so compare the selected entities by identity
        return isinstance(self.entity, tuple) and len(self.entity) == len(columns) and all(
            a is b for a, b in zip(self.entity, columns)
        )

    def filter(self, *exprs):
        self.tags.extend(expr.tag for expr in exprs)
        return self

    def distinct(self):
        return self

    def get(self, item_id):
        if self.entity is EliteATool:
            return item_id if item_id in self.session.tools else None
        return self.session.apps.get(item_id)

    def all(self):
        if self.entity is Application:
            return []
        if self._is(Application.id):
            is_pipeline = lambda app: any(v.agent_type == 'pipeline' for v in app.versions)  # noqa: E731
            want = self.tags == ['is_pipeline']
            assert self.tags in (['is_pipeline'], ['not is_pipeline'])
            return [(app_id,) for app_id, app in self.session.apps.items() if is_pipeline(app) is want]
        if self._is(ApplicationVersion.application_id):
            return [
                (app_id,) for app_id, app in self.session.apps.items()
                if any(v.status in ('published', 'embedded') for v in app.versions)
            ]
        if self._is(Skill.id):
            return [(skill_id,) for skill_id in SKILLS]
        if self._is(SkillVersion.skill_id):
            return [(skill_id,) for skill_id, status in SKILLS.items() if status in ('published', 'embedded')]
        if self._is(EliteATool.type, EliteATool.meta):
            return TOOLS
        if self._is(EliteATool.id, EliteATool.type, EliteATool.meta):
            return [(tool_id, tool_type, meta) for tool_id, (tool_type, meta) in self.session.tools.items()]
        raise AssertionError(f'unexpected query {self.entity}')


class FakeSession:
    def __init__(self, apps, fail_on=()):
        self.apps = dict(apps)
        self.tools = {tool_id: tool for tool_id, tool in enumerate(TOOLS, start=1)}
        self.fail_on = set(fail_on)
        self.deleted = []
        self.pending = None
        self.rollbacks = 0

    def query(self, *entities):
        if entities[0] is Application or entities[0] is EliteATool:
            return FakeQuery(self, entities[0])
        return FakeQuery(self, entities)

    def delete(self, app):
        self.pending = app

    def commit(self):
        app, self.pending = self.pending, None
        if app.id in self.fail_on:
            raise RuntimeError('commit failed')
        self.deleted.append(app.id)
        self.apps.pop(app.id)

    def rollback(self):
        self.pending = None
        self.rollbacks += 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _toolkit_kind():
    tree = ast.parse(APPLICATION_TOOLS.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'toolkit_kind')
    namespace = {'Optional': type('Optional', (), {'__class_getitem__': classmethod(lambda cls, item: item)})}
    exec(compile(ast.Module([node], []), str(APPLICATION_TOOLS), 'exec'), namespace)  # noqa: S102
    return namespace['toolkit_kind']


def _top_level(tree):
    return {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}


def _rpc_class(tree):
    return next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'RPC')


@pytest.fixture
def harness():
    tree = ast.parse(APPLICATION_RPC.read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], 'id', '') == 'APPLICATION_KINDS']
    top = _top_level(tree)
    nodes += [top[name] for name in HELPERS]
    methods = {n.name: n for n in _rpc_class(tree).body if isinstance(n, ast.FunctionDef)}
    for name in RPC_METHODS:
        methods[name].decorator_list = []
        nodes.append(methods[name])

    state = SimpleNamespace(session=FakeSession(APPS), events=[], fail_events=set(), clock=[0.0])
    detail = MagicMock()
    detail.from_orm.side_effect = lambda app: SimpleNamespace(model_dump=lambda: {'id': app.id})
    namespace = {
        'Application': Application, 'ApplicationVersion': ApplicationVersion, 'Skill': Skill,
        'SkillVersion': SkillVersion, 'EliteATool': EliteATool, 'PublishStatus': PublishStatus,
        'AgentTypes': AgentTypes, 'ApplicationDetailModel': detail,
        'ApplicationEvents': SimpleNamespace(
            application_deleted='application_deleted', toolkit_deleted='toolkit_deleted',
        ),
        'detach_skills_for_entity_versions': lambda session, ids: None,
        'toolkit_kind': _toolkit_kind(),
        'delete_toolkit': lambda session, project_id, tool_id: {'id': tool_id},
        'time': SimpleNamespace(monotonic=lambda: state.clock[0]),
        'db': SimpleNamespace(get_session=lambda project_id: state.session),
        'log': MagicMock(),
    }
    exec(compile(ast.Module(nodes, []), str(APPLICATION_RPC), 'exec'), namespace)  # noqa: S102
    def fire_event(name, payload):
        state.events.append((name, payload['id']))
        state.payloads.append(payload)
        if payload['id'] in state.fail_events:
            raise RuntimeError('handler failed')

    state.payloads = []
    module = SimpleNamespace(context=SimpleNamespace(event_manager=SimpleNamespace(fire_event=fire_event)))
    state.ns = namespace
    state.module = module
    return state


# ---------- the helpers are reachable from the RPC methods (30a633f0) ----------

def test_helpers_are_module_functions_not_rpc_methods():
    """ RPC methods run bound to the plugin module, so a helper on the RPC class is unreachable """
    tree = ast.parse(APPLICATION_RPC.read_text())
    assert set(HELPERS) <= set(_top_level(tree))
    rpc_methods = {n.name for n in _rpc_class(tree).body if isinstance(n, ast.FunctionDef)}
    assert not set(HELPERS) & rpc_methods


def test_rpc_methods_never_call_private_helpers_on_self():
    tree = ast.parse(APPLICATION_RPC.read_text())
    methods = [n for n in _rpc_class(tree).body if isinstance(n, ast.FunctionDef) and n.name in RPC_METHODS]
    calls = [
        ast.unparse(node.func) for method in methods for node in ast.walk(method)
        if isinstance(node, ast.Call) and ast.unparse(node.func).startswith('self._')
    ]
    assert calls == []


# ---------- kind ----------

@pytest.mark.parametrize('kind, ids', [('agent', [1, 2]), ('pipeline', [3, 4])])
def test_application_ids_of_kind(harness, kind, ids):
    assert harness.ns['_application_ids_of_kind'](harness.session, kind) == ids


@pytest.mark.parametrize('kind', ['pipelines', 'agents', 'Agent', '', None, 'toolkit'])
def test_unknown_kind_is_refused_and_never_means_every_agent(harness, kind):
    with pytest.raises(ValueError):
        harness.ns['_application_ids_of_kind'](harness.session, kind)


def test_delete_by_unknown_kind_deletes_nothing(harness):
    with pytest.raises(ValueError):
        harness.ns['applications_delete_by_kind'](harness.module, 7, 'pipelines')
    assert harness.session.deleted == []


# ---------- delete ----------

def test_deletes_agents_and_skips_the_published_one(harness):
    result = harness.ns['applications_delete_by_kind'](harness.module, 7, 'agent')

    assert result == {'deleted': 1, 'failed': 0, 'skipped': 1, 'remaining': 0}
    assert harness.session.deleted == [1]
    assert harness.events == [('application_deleted', 1)]


def test_deletes_pipelines_and_skips_the_embedded_one(harness):
    result = harness.ns['applications_delete_by_kind'](harness.module, 7, 'pipeline')

    assert result == {'deleted': 1, 'failed': 0, 'skipped': 1, 'remaining': 0}
    assert harness.session.deleted == [3]


def test_a_failing_application_is_rolled_back_and_the_rest_go_on(harness):
    harness.session = FakeSession({**APPS, 5: _app(5, 'react', ['draft'])}, fail_on={1})

    result = harness.ns['applications_delete_by_kind'](harness.module, 7, 'agent')

    assert result == {'deleted': 1, 'failed': 1, 'skipped': 1, 'remaining': 0}
    assert harness.session.deleted == [5]
    assert harness.session.rollbacks == 1


# ---------- counts ----------

def test_counts_every_type_with_what_will_be_skipped(harness):
    assert harness.ns['project_data_counts'](harness.module, 7) == {
        'agents': {'count': 2, 'blocked': 1},
        'pipelines': {'count': 2, 'blocked': 1},
        'skills': {'count': 3, 'blocked': 1},
        'toolkits': {'count': 2, 'blocked': 0},
        'mcps': {'count': 2, 'blocked': 0},
    }


# ---------- review of #604: time budget, event handlers, payload ----------

def test_bulk_delete_builds_the_event_payload_without_serializing_versions(harness):
    harness.ns['applications_delete_by_kind'](harness.module, 7, 'agent')

    harness.ns['ApplicationDetailModel'].from_orm.assert_not_called()
    assert harness.payloads == [{
        'id': 1, 'name': 'app 1', 'owner_id': 7, 'project_id': 7, 'version_details': {'agent_type': 'react'},
    }]


def test_single_delete_keeps_the_full_event_payload(harness):
    assert harness.ns['_delete_application'](harness.module, harness.session, 7, APPS[1]) is True

    harness.ns['ApplicationDetailModel'].from_orm.assert_called_once()


def test_a_failing_application_handler_counts_the_delete_and_the_rest_go_on(harness):
    harness.session = FakeSession({**APPS, 5: _app(5, 'react', ['draft'])})
    harness.fail_events = {1}

    result = harness.ns['applications_delete_by_kind'](harness.module, 7, 'agent')

    assert result == {'deleted': 2, 'failed': 0, 'skipped': 1, 'remaining': 0}
    assert harness.session.deleted == [1, 5]


def test_a_failing_toolkit_handler_counts_the_delete_and_the_rest_go_on(harness):
    harness.fail_events = {1}

    result = harness.ns['toolkits_delete_by_kind'](harness.module, 7, 'toolkit')

    assert result == {'deleted': 2, 'failed': 0, 'skipped': 0, 'remaining': 0}
    assert [event[1] for event in harness.events] == [1, 5]


def test_the_time_budget_stops_the_run_and_reports_what_is_left(harness):
    ticks = itertools.count()
    harness.ns['time'] = SimpleNamespace(monotonic=lambda: float(next(ticks)))
    harness.session = FakeSession({**APPS, 5: _app(5, 'react', ['draft']), 6: _app(6, 'react', ['draft'])})

    # budget 2.5: start at 0, items checked at 1 and 2, stop at 3
    result = harness.ns['applications_delete_by_kind'](harness.module, 7, 'agent', time_budget=2.5)

    assert result == {'deleted': 1, 'failed': 0, 'skipped': 1, 'remaining': 2}
    assert harness.session.deleted == [1]


def test_no_time_budget_runs_to_the_end(harness):
    result = harness.ns['toolkits_delete_by_kind'](harness.module, 7, 'mcp')

    assert result == {'deleted': 2, 'failed': 0, 'skipped': 0, 'remaining': 0}
