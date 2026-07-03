"""Issue #5680 — sub-agent tree walk: cycle detection + leaf-only rule.

Exercises the pure decision logic of ``collect_sub_agent_tree`` against a fake in-memory
session, without a real DB. Heavy model/dep imports are stubbed so publish_utils loads
standalone.

Run from the elitea_core plugin root:

    python3 -m pytest --rootdir=utils/tests --import-mode=importlib \
        utils/tests/test_5680_subagent_tree_walk.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------- #
# Fake ORM column: ApplicationVersion.id == X -> a Criterion capturing X, so the
# fake session can resolve the requested version id.
# --------------------------------------------------------------------------- #

class _Criterion:
    def __init__(self, value):
        self.value = value


class _IdColumn:
    def __eq__(self, other):
        return _Criterion(other)


class _AgentTypes:
    class pipeline:
        value = 'pipeline'


@pytest.fixture(scope='module')
def pu():
    """Import publish_utils with model/dep imports stubbed to lightweight fakes."""
    fake_av = type('ApplicationVersion', (), {'id': _IdColumn(), 'tools': None})

    models_all = types.ModuleType('plugins.elitea_core.models.all')
    models_all.Application = type('Application', (), {})
    models_all.ApplicationVersion = fake_av

    elitea_tools = types.ModuleType('plugins.elitea_core.models.elitea_tools')
    elitea_tools.EliteATool = type('EliteATool', (), {})
    elitea_tools.EntityToolMapping = type('EntityToolMapping', (), {})

    enums = types.ModuleType('plugins.elitea_core.models.enums.all')
    enums.AgentTypes = _AgentTypes
    enums.NotificationEventTypes = type('N', (), {})
    enums.PublishStatus = type('P', (), {})
    enums.ToolEntityTypes = type('T', (), {})

    for modname, attrs in {
        'plugins.elitea_core.models.pd.application': {'ApplicationImportModel': object},
        'plugins.elitea_core.models.pd.version': {'ApplicationVersionForkCreateModel': object},
        'plugins.elitea_core.models.pd.publish': {'PublishAIResult': object},
        'plugins.elitea_core.utils.create_utils': {'create_application': None, 'create_version': None},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': None},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': None, 'is_valid_category': None},
    }.items():
        mod = types.ModuleType(modname)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[modname] = mod

    sys.modules['plugins.elitea_core.models.all'] = models_all
    sys.modules['plugins.elitea_core.models.elitea_tools'] = elitea_tools
    sys.modules['plugins.elitea_core.models.enums.all'] = enums

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.utils.publish_utils',
        PLUGIN_ROOT / 'utils' / 'publish_utils.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    # Neutralize selectinload — the fake ORM attrs aren't real mapped columns, and the fake
    # session ignores .options() anyway.
    module.selectinload = lambda *_a, **_k: None
    return module


# --------------------------------------------------------------------------- #
# Fakes mirroring the ORM shape the walk reads
# --------------------------------------------------------------------------- #

class FakeTool:
    def __init__(self, app_id, ver_id, name=None):
        self.type = 'application'
        self.name = name or f'app_{app_id}'
        self.settings = {'application_id': app_id, 'application_version_id': ver_id}


class FakeVersion:
    def __init__(self, ver_id, agent_type='openai', tools=None):
        self.id = ver_id
        self.agent_type = agent_type
        self.tools = tools or []


class _Query:
    def __init__(self, registry):
        self._registry = registry
        self._vid = None

    def filter(self, *criteria):
        for c in criteria:
            if hasattr(c, 'value'):
                self._vid = c.value
        return self

    def options(self, *_a, **_k):
        return self

    def first(self):
        return self._registry.get(self._vid)


class FakeSession:
    """registry maps version_id -> FakeVersion."""

    def __init__(self, registry):
        self._registry = registry

    def query(self, _model):
        return _Query(self._registry)


def _walk(pu, registry, root_id, **flags):
    return pu.collect_sub_agent_tree(
        project_id=1, version_id=root_id, session=FakeSession(registry), **flags,
    )


# --------------------------------------------------------------------------- #
# is_container_version — pure predicate
# --------------------------------------------------------------------------- #

def test_is_container_version(pu):
    leaf = FakeVersion(1, tools=[])
    container = FakeVersion(2, tools=[FakeTool(9, 9)])
    assert pu.is_container_version(leaf) is False
    assert pu.is_container_version(container) is True


# --------------------------------------------------------------------------- #
# Cycle detection (always on)
# --------------------------------------------------------------------------- #

def test_pure_pipeline_cycle_detected(pu):
    # P1(v1)->P2(v2)->P1(v1), all pipelines. Pipelines are exempt from the leaf rule, so the
    # walk recurses far enough to hit the back-edge and reports a cycle.
    registry = {
        1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(1, 1)]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert ei.value.error_code == 'cycle_detected'


def test_cycle_detected_without_leaf_rule(pu):
    # With the leaf rule OFF (pure cycle-detection mode), an agent 2-hop cycle is a cycle.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[FakeTool(1, 1)]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=False)
    assert ei.value.error_code == 'cycle_detected'


def test_self_referencing_agent_rejected_as_container(pu):
    # A(v1)->A(v1): a self-referencing NON-pipeline agent is a container, so the (more
    # informative) leaf rule fires before the cycle would be re-entered. Either way: rejected.
    registry = {1: FakeVersion(1, tools=[FakeTool(1, 1)])}
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert ei.value.error_code == 'container_child_forbidden'


def test_agent_two_hop_cycle_rejected_as_container(pu):
    # A(v1)->B(v2)->A(v1): B is a non-pipeline container, so with the leaf rule on it is
    # rejected as container_child_forbidden (B "uses other agents") before the back-edge to A.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[FakeTool(1, 1)]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert ei.value.error_code == 'container_child_forbidden'


# --------------------------------------------------------------------------- #
# Leaf-only rule
# --------------------------------------------------------------------------- #

def test_leaf_child_allowed(pu):
    # A(v1)->leaf(v2). No violation.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[]),
    }
    tree = _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert [n.app_id for n in tree] == [2]


def test_container_agent_child_rejected(pu):
    # A(v1)->B(v2), where B is itself a container (B->C). B is a non-pipeline agent → reject.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert ei.value.error_code == 'container_child_forbidden'


def test_pipeline_child_with_subagents_allowed(pu):
    # A(v1)->P(v2, pipeline)->leaf(v3). Pipeline is exempt from the leaf rule → allowed.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[]),
    }
    tree = _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert [n.app_id for n in tree] == [2]
    assert [c.app_id for c in tree[0].children] == [3]


def test_shared_leaf_across_branches_is_not_a_cycle(pu):
    # Root pipeline P(v1) -> two pipeline branches both using the SAME leaf L(v4).
    # Path-based cycle detection must NOT flag this diamond.
    registry = {
        1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2), FakeTool(3, 3)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(4, 4)]),
        3: FakeVersion(3, agent_type='pipeline', tools=[FakeTool(4, 4)]),
        4: FakeVersion(4, tools=[]),
    }
    tree = _walk(pu, registry, 1, recurse_pipelines=True, enforce_leaf_rule=True)
    assert sorted(n.app_id for n in tree) == [2, 3]


# --------------------------------------------------------------------------- #
# Publish path (defaults) unchanged: pipelines skipped, not walked
# --------------------------------------------------------------------------- #

def test_publish_path_skips_pipeline_children(pu):
    # A(v1)->P(v2, pipeline)->A(v1). In PUBLISH mode (defaults) the pipeline is skipped, so
    # the back-reference is NOT walked and no cycle is raised — matches prior behavior.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(1, 1)]),
    }
    tree = _walk(pu, registry, 1)  # defaults: recurse_pipelines=False, enforce_leaf_rule=False
    assert tree == []  # pipeline child skipped, nothing collected
