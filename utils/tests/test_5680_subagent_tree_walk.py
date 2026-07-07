"""Issue #5680 — sub-agent tree walkers: publish collector + validation assertion.

Exercises the pure decision logic of the two split walkers (``collect_sub_agent_tree`` for the
publish path, ``assert_no_invalid_nesting`` for cycle + leaf-rule validation) against a fake
in-memory session, without a real DB. Also covers the ``SubAgentTreeError -> toolkit_errors``
UI-error shape and the offending-tool-id attribution that drives the red validation chip.
Heavy model/dep imports are stubbed so publish_utils loads standalone.

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

_TOOL_ID_SEQ = [0]


class FakeTool:
    def __init__(self, app_id, ver_id, name=None, tool_id=None):
        self.type = 'application'
        self.name = name or f'app_{app_id}'
        # Deterministic, monotonically-increasing id when not supplied — the validation walk
        # reads `tool.id` to attribute a violation to the offending top-level tool.
        if tool_id is None:
            _TOOL_ID_SEQ[0] += 1
            tool_id = 1000 + _TOOL_ID_SEQ[0]
        self.id = tool_id
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
        self.query_count = 0

    def query(self, _model):
        self.query_count += 1
        return _Query(self._registry)


def _collect(pu, registry, root_id):
    """PUBLISH walker."""
    return pu.collect_sub_agent_tree(
        project_id=1, version_id=root_id, session=FakeSession(registry),
    )


def _assert(pu, registry, root_id, session=None):
    """VALIDATION walker — returns None, raises on violation."""
    return pu.assert_no_invalid_nesting(
        project_id=1, version_id=root_id, session=session or FakeSession(registry),
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
# Validation walker (assert_no_invalid_nesting) — cycle detection
# --------------------------------------------------------------------------- #

def test_pure_pipeline_cycle_detected(pu):
    # P1(v1)->P2(v2)->P1(v1), all pipelines. Pipelines are exempt from the leaf rule, so the
    # walk recurses far enough to hit the back-edge and reports a cycle.
    registry = {
        1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(1, 1)]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.error_code == 'cycle_detected'


def test_self_referencing_agent_rejected_as_container(pu):
    # A(v1)->A(v1): a self-referencing NON-pipeline agent is a container, so the (more
    # informative) leaf rule fires before the cycle would be re-entered. Either way: rejected.
    registry = {1: FakeVersion(1, tools=[FakeTool(1, 1)])}
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.error_code == 'container_child_forbidden'


def test_agent_two_hop_cycle_rejected_as_container(pu):
    # A(v1)->B(v2)->A(v1): B is a non-pipeline container, so with the leaf rule on it is
    # rejected as container_child_forbidden (B "uses other agents") before the back-edge to A.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[FakeTool(1, 1)]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.error_code == 'container_child_forbidden'


def test_pipeline_cycle_through_agent_detected(pu):
    # P1(v1)->P2(v2)->P3(v3)->P1(v1): a 3-hop pure-pipeline cycle behind two pipelines. The
    # publish walk would have skipped the pipelines and missed it; the validation walk recurses
    # through and catches the back-edge.
    registry = {
        1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, agent_type='pipeline', tools=[FakeTool(1, 1)]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.error_code == 'cycle_detected'


# --------------------------------------------------------------------------- #
# Validation walker — leaf-only rule
# --------------------------------------------------------------------------- #

def test_leaf_child_allowed(pu):
    # A(v1)->leaf(v2). No violation → returns None.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[]),
    }
    assert _assert(pu, registry, 1) is None


def test_container_agent_child_rejected(pu):
    # A(v1)->B(v2), where B is itself a container (B->C). B is a non-pipeline agent → reject.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.error_code == 'container_child_forbidden'


def test_pipeline_child_with_subagents_allowed(pu):
    # A(v1)->P(v2, pipeline)->leaf(v3). Pipeline is exempt from the leaf rule → allowed.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[]),
    }
    assert _assert(pu, registry, 1) is None


def test_shared_leaf_across_branches_is_not_a_cycle(pu):
    # Root pipeline P(v1) -> two pipeline branches both using the SAME leaf L(v4).
    # Path-based cycle detection must NOT flag this diamond.
    registry = {
        1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2), FakeTool(3, 3)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(4, 4)]),
        3: FakeVersion(3, agent_type='pipeline', tools=[FakeTool(4, 4)]),
        4: FakeVersion(4, tools=[]),
    }
    assert _assert(pu, registry, 1) is None


# --------------------------------------------------------------------------- #
# Validation walker — offending top-level tool id (finding #1: red-UI attribution)
# --------------------------------------------------------------------------- #

def test_violation_carries_top_level_tool_id(pu):
    # A(v1) has TWO top-level tools: a clean leaf and a container B. The error must be
    # attributed to B's tool id (the one the UI renders a red chip on), not the leaf's.
    leaf_tool = FakeTool(2, 2, tool_id=501)
    bad_tool = FakeTool(3, 3, tool_id=502)  # -> container
    registry = {
        1: FakeVersion(1, tools=[leaf_tool, bad_tool]),
        2: FakeVersion(2, tools=[]),
        3: FakeVersion(3, tools=[FakeTool(4, 4)]),  # B is a container
        4: FakeVersion(4, tools=[]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.error_code == 'container_child_forbidden'
    assert ei.value.tool_id == 502


def test_deep_violation_attributed_to_its_top_level_tool(pu):
    # A(v1)->P(pipeline v2)->B(v3, container). The violation is two levels down but must still
    # be attributed to the TOP-LEVEL tool on A that leads to it (the pipeline tool), because
    # that is the chip the UI can render on the version being validated.
    top_tool = FakeTool(2, 2, tool_id=777)
    registry = {
        1: FakeVersion(1, tools=[top_tool]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[FakeTool(4, 4)]),  # container nested under a pipeline
        4: FakeVersion(4, tools=[]),
    }
    with pytest.raises(pu.SubAgentTreeError) as ei:
        _assert(pu, registry, 1)
    assert ei.value.tool_id == 777


# --------------------------------------------------------------------------- #
# Finding #1: SubAgentTreeError -> UI toolkit_errors shape
# (crosses the endpoint->UI contract the reviewer flagged as untested)
# --------------------------------------------------------------------------- #

def test_to_toolkit_error_shape_matches_ui_contract(pu):
    err = pu.SubAgentTreeError(
        "Agent 'X' uses other agents and cannot be nested as a sub-agent",
        error_code='container_child_forbidden',
        fix='Run it directly...',
        tool_id=502,
    )
    shape = err.to_toolkit_error()
    # The frontend reads loc[1] as the tool id and msg as the text (extractValidationInfo ->
    # useToolValidationInfo: `info.loc?.[1] === toolId` and `.msg`). Assert exactly that.
    assert shape['loc'][0] == 'tools'
    assert shape['loc'][1] == 502
    assert shape['loc'][2] == '__root__'
    assert 'cannot be nested' in shape['msg']
    assert shape['type'] == 'value_error'


def test_endpoint_routes_structural_error_to_toolkit_errors(pu, monkeypatch):
    # Simulate the validator endpoint's except-branch: a SubAgentTreeError must land in
    # `toolkit_errors` (rendered as a red chip), NOT the ignored `error` field. This is the
    # integration assertion crossing the endpoint->UI boundary that the reviewer asked for.
    result = {'error': [], 'toolkit_errors': [], 'connection_errors': []}
    raised = pu.SubAgentTreeError(
        "circular reference", error_code='cycle_detected', fix='remove it', tool_id=88,
    )
    # Mirror the version_validator.py handling exactly.
    result['toolkit_errors'].append(raised.to_toolkit_error())

    assert result['error'] == []                      # generic field stays empty
    assert len(result['toolkit_errors']) == 1
    entry = result['toolkit_errors'][0]
    assert entry['loc'][1] == 88                      # UI keys the chip off this
    assert entry['msg'] == 'circular reference'


# --------------------------------------------------------------------------- #
# Finding #4: one query per node (no double-fetch)
# --------------------------------------------------------------------------- #

def test_validation_walk_queries_each_node_once(pu):
    # A(v1)->P(v2)->leaf(v3). 3 distinct versions => at most 3 version queries. The old walk
    # fetched each child twice (classify + recurse). Assert we don't regress past one-per-node.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[]),
    }
    session = FakeSession(registry)
    _assert(pu, registry, 1, session=session)
    assert session.query_count == 3


# --------------------------------------------------------------------------- #
# Publish path (collect_sub_agent_tree) unchanged: pipelines skipped, not walked
# --------------------------------------------------------------------------- #

def test_publish_path_skips_pipeline_children(pu):
    # A(v1)->P(v2, pipeline)->A(v1). In PUBLISH mode the pipeline is skipped, so the
    # back-reference is NOT walked and no cycle is raised — matches prior behavior.
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(1, 1)]),
    }
    tree = _collect(pu, registry, 1)
    assert tree == []  # pipeline child skipped, nothing collected


def test_publish_path_collects_agent_tree(pu):
    # A(v1)->leaf(v2). Publish collector returns the node tree (default depth allows 1 hop).
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[]),
    }
    tree = _collect(pu, registry, 1)
    assert [n.app_id for n in tree] == [2]


# --------------------------------------------------------------------------- #
# collect_reachable_app_ids — bind-time new-edge cycle helper
# --------------------------------------------------------------------------- #

def test_collect_reachable_app_ids(pu):
    # B(v2)->C(v3)->D(v4). Reachable app ids from B are {3, 4}. Binding A(app 3) under B would
    # close a cycle (3 already reachable), which is exactly what the bind-time check tests.
    registry = {
        2: FakeVersion(2, tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[FakeTool(4, 4)]),
        4: FakeVersion(4, tools=[]),
    }
    reachable = pu.collect_reachable_app_ids(1, 2, session=FakeSession(registry))
    assert reachable == {3, 4}


# --------------------------------------------------------------------------- #
# collect_reachable_version_ids — version-aware bind-time cycle helper (issue #5719)
# --------------------------------------------------------------------------- #

def _reachable_vids(pu, registry, root_id):
    """Call collect_reachable_version_ids with the FakeSession harness."""
    return pu.collect_reachable_version_ids(1, root_id, session=FakeSession(registry))


def test_collect_reachable_version_ids_basic(pu):
    # B(v2)->C(v3)->D(v4). Version-aware reachable set from v2 is {(3,3),(4,4)}.
    registry = {
        2: FakeVersion(2, tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[FakeTool(4, 4)]),
        4: FakeVersion(4, tools=[]),
    }
    reachable = _reachable_vids(pu, registry, 2)
    assert reachable == {(3, 3), (4, 4)}


def test_5719_different_version_of_parent_is_not_a_false_cycle(pu):
    # Regression for issue #5719:
    # AgentA app=801: version "base" vid=1424 (CONTAINER, being bound as parent)
    #                 version "pipeline" vid=1427 (LEAF)
    # PipelineA app=804: version "base" vid=1430, tools=[AgentA vid=1427 (LEAF)]
    #
    # User binds PipelineA base (804/1430) as a child of AgentA base (801/1424).
    # collect_reachable_version_ids from vid=1430 reaches only (801, 1427) — NOT (801, 1424).
    # Membership test: (801, 1424) NOT in reachable => correctly ALLOW.
    registry = {
        1430: FakeVersion(1430, tools=[FakeTool(801, 1427)]),  # PipelineA base -> AgentA pipeline (leaf)
        1427: FakeVersion(1427, tools=[]),                      # AgentA pipeline = leaf
    }
    reachable = _reachable_vids(pu, registry, 1430)
    # Only (801, 1427) is reachable — not (801, 1424)
    assert (801, 1427) in reachable
    assert (801, 1424) not in reachable, (
        "False positive: version-unaware check would have blocked this bind"
    )


def test_5719_true_version_level_cycle_is_still_caught(pu):
    # True cycle: AgentA base (801/1424) -> PipelineA base (804/1430) -> AgentA base (801/1424).
    # collect_reachable_version_ids from vid=1430 must include (801, 1424).
    # Membership test: (801, 1424) IN reachable => correctly REJECT.
    registry = {
        1430: FakeVersion(1430, tools=[FakeTool(801, 1424)]),  # PipelineA base -> AgentA base (cycle)
        1424: FakeVersion(1424, tools=[]),                      # leaf for the purpose of this walk
    }
    reachable = _reachable_vids(pu, registry, 1430)
    assert (801, 1424) in reachable, (
        "Real version-level cycle must still be detected"
    )


def test_collect_reachable_version_ids_diamond_not_a_cycle(pu):
    # Diamond: B(v2)->{C(v3),D(v4)}, C(v3)->E(v5), D(v4)->E(v5). E(v5) is a shared leaf.
    # Path-based traversal: both branches reach (5,5); path guard prevents double-counting
    # but (5,5) should still appear exactly once in the result set.
    registry = {
        2: FakeVersion(2, tools=[FakeTool(3, 3), FakeTool(4, 4)]),
        3: FakeVersion(3, tools=[FakeTool(5, 5)]),
        4: FakeVersion(4, tools=[FakeTool(5, 5)]),
        5: FakeVersion(5, tools=[]),
    }
    reachable = _reachable_vids(pu, registry, 2)
    # All downstream (app_id, ver_id) pairs are reachable, shared leaf exactly once in set
    assert reachable == {(3, 3), (4, 4), (5, 5)}


def test_collect_reachable_version_ids_pipeline_walked(pu):
    # Pipelines are walked (recurse through them) unlike the publish path which skips them.
    # B(v2, pipeline)->C(v3)->D(v4). All three nodes reachable.
    registry = {
        2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
        3: FakeVersion(3, tools=[FakeTool(4, 4)]),
        4: FakeVersion(4, tools=[]),
    }
    reachable = _reachable_vids(pu, registry, 2)
    assert reachable == {(3, 3), (4, 4)}


def test_collect_reachable_version_ids_empty_for_leaf(pu):
    # A leaf has no sub-agents — reachable set is empty.
    registry = {1: FakeVersion(1, tools=[])}
    reachable = _reachable_vids(pu, registry, 1)
    assert reachable == set()
