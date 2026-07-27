"""Issues #5680/#5778 — canonical sub-agent traversal contracts.

Exercises publishing, validation, and tier calculation against a fake in-memory session, without
a real DB. The three public operations share one cached path-local traversal. Also covers the
``SubAgentTreeError -> toolkit_errors`` UI-error shape and offending-tool-id attribution.
Heavy model/dep imports are stubbed so publish_utils loads standalone.

Run via:
    python tests/run_tests.py integration/test_5680_subagent_tree_walk.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


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
    enums.SkillEntityTypes = type('S', (), {'agent': 'agent'})
    enums.ToolEntityTypes = type('T', (), {})

    for modname, attrs in {
        'plugins.elitea_core.models.pd.application': {'ApplicationImportModel': object},
        'plugins.elitea_core.models.pd.version': {'ApplicationVersionForkCreateModel': object},
        'plugins.elitea_core.models.pd.publish': {'PublishAIResult': object},
        'plugins.elitea_core.models.skill': {
            'EntitySkillMapping': type('EntitySkillMapping', (), {}),
            'Skill': type('Skill', (), {}),
            'SkillVersion': type('SkillVersion', (), {}),
        },
        'plugins.elitea_core.utils.create_utils': {'create_application': None, 'create_version': None},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': None},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': None, 'is_valid_category': None},
        'plugins.elitea_core.utils.application_utils': {'build_skill_mappings_list': None},
        'plugins.elitea_core.utils.skill_export_import': {'build_skill_fork_payload': None},
        'plugins.elitea_core.utils.skill_utils': {'attach_skill_to_agent': None},
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
    module.selectinload = lambda *_a, **_k: None
    return module


_TOOL_ID_SEQ = [0]


class FakeTool:
    def __init__(self, app_id, ver_id, name=None, tool_id=None):
        self.type = 'application'
        self.name = name or f'app_{app_id}'
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


def _assert(pu, registry, root_id, session=None, start_depth=1):
    """VALIDATION walker — returns None, raises on violation."""
    return pu.assert_no_invalid_nesting(
        project_id=1, version_id=root_id, session=session or FakeSession(registry),
        start_depth=start_depth,
    )


class TestIsContainerVersion:
    """is_container_version — pure predicate."""

    def test_is_container_version(self, pu):
        leaf = FakeVersion(1, tools=[])
        container = FakeVersion(2, tools=[FakeTool(9, 9)])
        assert pu.is_container_version(leaf) is False
        assert pu.is_container_version(container) is True


class TestCycleDetection:
    """Validation walker (assert_no_invalid_nesting) — cycle detection."""

    def test_pure_pipeline_cycle_detected(self, pu):
        registry = {
            1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(1, 1)]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.error_code == 'cycle_detected'

    def test_self_referencing_agent_rejected_as_cycle(self, pu):
        registry = {1: FakeVersion(1, tools=[FakeTool(1, 1)])}
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.error_code == 'cycle_detected'

    def test_agent_two_hop_cycle_rejected_as_container(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[FakeTool(1, 1)]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.error_code == 'container_child_forbidden'

    def test_pipeline_cycle_through_agent_detected(self, pu):
        registry = {
            1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, agent_type='pipeline', tools=[FakeTool(1, 1)]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.error_code == 'cycle_detected'


class TestLeafOnlyRule:
    """Validation walker — leaf-only rule."""

    def test_leaf_child_allowed(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[]),
        }
        assert _assert(pu, registry, 1) is None

    def test_container_agent_child_at_tier2_allowed(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        assert _assert(pu, registry, 1) is None

    def test_container_agent_child_at_tier3_rejected(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.error_code == 'container_child_forbidden'

    def test_pipeline_child_with_subagents_allowed(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        assert _assert(pu, registry, 1) is None

    def test_shared_leaf_across_branches_is_not_a_cycle(self, pu):
        registry = {
            1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2), FakeTool(3, 3)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(4, 4)]),
            3: FakeVersion(3, agent_type='pipeline', tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        assert _assert(pu, registry, 1) is None

    def test_pipeline_is_transparent_to_agent_tier_count(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        assert _assert(pu, registry, 1) is None


class TestBindPathDepth:
    """Binding a subtree starts the child at agent tier two."""

    def test_rejects_container_at_tier3(self, pu):
        registry = {
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 2, start_depth=2)
        assert ei.value.error_code == 'container_child_forbidden'

    def test_allows_leaf_at_tier3(self, pu):
        registry = {
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        assert _assert(pu, registry, 2, start_depth=2) is None

    def test_pipeline_remains_tier_transparent(self, pu):
        registry = {
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        assert _assert(pu, registry, 2, start_depth=2) is None

    def test_pipeline_does_not_hide_a_fourth_agent_tier(self, pu):
        registry = {
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[FakeTool(5, 5)]),
            5: FakeVersion(5, tools=[]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 2, start_depth=2)
        assert ei.value.error_code == 'container_child_forbidden'


class TestToolIdAttribution:
    """Validation walker — offending top-level tool id (finding #1: red-UI attribution)."""

    def test_violation_carries_top_level_tool_id(self, pu):
        leaf_tool = FakeTool(2, 2, tool_id=501)
        bad_tool = FakeTool(3, 3, tool_id=502)
        registry = {
            1: FakeVersion(1, tools=[leaf_tool, bad_tool]),
            2: FakeVersion(2, tools=[]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[FakeTool(5, 5)]),
            5: FakeVersion(5, tools=[]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.error_code == 'container_child_forbidden'
        assert ei.value.tool_id == 502

    def test_deep_violation_attributed_to_its_top_level_tool(self, pu):
        top_tool = FakeTool(2, 2, tool_id=777)
        registry = {
            1: FakeVersion(1, tools=[top_tool]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[FakeTool(5, 5)]),
            5: FakeVersion(5, tools=[]),
        }
        with pytest.raises(pu.SubAgentTreeError) as ei:
            _assert(pu, registry, 1)
        assert ei.value.tool_id == 777


class TestUIErrorShape:
    """Finding #1: SubAgentTreeError -> UI toolkit_errors shape."""

    def test_to_toolkit_error_shape_matches_ui_contract(self, pu):
        err = pu.SubAgentTreeError(
            "Agent 'X' uses other agents and cannot be nested as a sub-agent",
            error_code='container_child_forbidden',
            fix='Run it directly...',
            tool_id=502,
        )
        shape = err.to_toolkit_error()
        assert shape['loc'][0] == 'tools'
        assert shape['loc'][1] == 502
        assert shape['loc'][2] == '__root__'
        assert 'cannot be nested' in shape['msg']
        assert shape['type'] == 'value_error'

    def test_endpoint_routes_structural_error_to_toolkit_errors(self, pu):
        result = {'error': [], 'toolkit_errors': [], 'connection_errors': []}
        raised = pu.SubAgentTreeError(
            "circular reference", error_code='cycle_detected', fix='remove it', tool_id=88,
        )
        result['toolkit_errors'].append(raised.to_toolkit_error())

        assert result['error'] == []
        assert len(result['toolkit_errors']) == 1
        entry = result['toolkit_errors'][0]
        assert entry['loc'][1] == 88
        assert entry['msg'] == 'circular reference'


class TestQueryEfficiency:
    """One cached traversal: no per-node or validation/collection double-fetch."""

    def test_validation_walk_queries_each_node_once(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        session = FakeSession(registry)
        _assert(pu, registry, 1, session=session)
        assert session.query_count == 3

    def test_publish_validation_and_collection_share_one_walk(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        session = FakeSession(registry)

        tree = pu.collect_sub_agent_tree(1, 1, session=session)

        assert tree == []
        assert session.query_count == 3

    def test_tier_calculation_loads_each_version_once(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        session = FakeSession(registry)

        tiers = pu.compute_agent_subtree_tiers(1, 1, session=session)

        assert tiers == 2
        assert session.query_count == 3

    def test_tier_calculation_reuses_preloaded_root(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[]),
        }
        session = FakeSession(registry)

        metadata = pu.get_agent_nesting_metadata(
            1,
            1,
            session=session,
            root_version=registry[1],
        )

        assert metadata == {
            'agent_subtree_tiers': 2,
            'max_agent_nesting_tiers': 3,
        }
        assert session.query_count == 1


class TestAgentSubtreeTiers:
    """Depth helper used by the UI add guard."""

    @staticmethod
    def _tiers(pu, registry, root_id):
        return pu.compute_agent_subtree_tiers(1, root_id, session=FakeSession(registry))

    def test_leaf_is_one_tier(self, pu):
        assert self._tiers(pu, {1: FakeVersion(1, tools=[])}, 1) == 1

    def test_container_of_leaves_is_two_tiers(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[]),
        }
        assert self._tiers(pu, registry, 1) == 2

    def test_two_hop_agent_tree_is_three_tiers(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        assert self._tiers(pu, registry, 1) == 3

    def test_pipeline_is_transparent(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        assert self._tiers(pu, registry, 1) == 2

    def test_pipeline_root_consumes_no_tier(self, pu):
        registry = {
            1: FakeVersion(1, agent_type='pipeline', tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        assert self._tiers(pu, registry, 1) == 2

    def test_empty_pipeline_has_zero_tiers(self, pu):
        assert self._tiers(
            pu, {1: FakeVersion(1, agent_type='pipeline', tools=[])}, 1,
        ) == 0


class TestPublishPath:
    """Publish materialization excludes pipelines after the canonical validation walk."""

    def test_publish_path_skips_pipeline_children(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[]),
        }
        tree = _collect(pu, registry, 1)
        assert tree == []

    def test_publish_path_collects_agent_tree(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2)]),
            2: FakeVersion(2, tools=[]),
        }
        tree = _collect(pu, registry, 1)
        assert [n.app_id for n in tree] == [2]

    def test_publish_path_accepts_shared_leaf_and_reuses_its_load(self, pu):
        registry = {
            1: FakeVersion(1, tools=[FakeTool(2, 2), FakeTool(3, 3)]),
            2: FakeVersion(2, tools=[FakeTool(4, 4)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        session = FakeSession(registry)

        tree = pu.collect_sub_agent_tree(1, 1, session=session)

        assert [[child.app_id for child in node.children] for node in tree] == [[4], [4]]
        assert session.query_count == 4


class TestCollectReachableVersionIds:
    """collect_reachable_version_ids — version-aware bind-time cycle helper (issue #5719)."""

    def _reachable_vids(self, pu, registry, root_id):
        return pu.collect_reachable_version_ids(1, root_id, session=FakeSession(registry))

    def test_collect_reachable_version_ids_basic(self, pu):
        registry = {
            2: FakeVersion(2, tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        reachable = self._reachable_vids(pu, registry, 2)
        assert reachable == {(3, 3), (4, 4)}

    def test_5719_different_version_of_parent_is_not_a_false_cycle(self, pu):
        registry = {
            1430: FakeVersion(1430, tools=[FakeTool(801, 1427)]),
            1427: FakeVersion(1427, tools=[]),
        }
        reachable = self._reachable_vids(pu, registry, 1430)
        assert (801, 1427) in reachable
        assert (801, 1424) not in reachable

    def test_5719_true_version_level_cycle_is_still_caught(self, pu):
        registry = {
            1430: FakeVersion(1430, tools=[FakeTool(801, 1424)]),
            1424: FakeVersion(1424, tools=[]),
        }
        reachable = self._reachable_vids(pu, registry, 1430)
        assert (801, 1424) in reachable

    def test_collect_reachable_version_ids_diamond_not_a_cycle(self, pu):
        registry = {
            2: FakeVersion(2, tools=[FakeTool(3, 3), FakeTool(4, 4)]),
            3: FakeVersion(3, tools=[FakeTool(5, 5)]),
            4: FakeVersion(4, tools=[FakeTool(5, 5)]),
            5: FakeVersion(5, tools=[]),
        }
        reachable = self._reachable_vids(pu, registry, 2)
        assert reachable == {(3, 3), (4, 4), (5, 5)}

    def test_collect_reachable_version_ids_pipeline_walked(self, pu):
        registry = {
            2: FakeVersion(2, agent_type='pipeline', tools=[FakeTool(3, 3)]),
            3: FakeVersion(3, tools=[FakeTool(4, 4)]),
            4: FakeVersion(4, tools=[]),
        }
        reachable = self._reachable_vids(pu, registry, 2)
        assert reachable == {(3, 3), (4, 4)}

    def test_collect_reachable_version_ids_empty_for_leaf(self, pu):
        registry = {1: FakeVersion(1, tools=[])}
        reachable = self._reachable_vids(pu, registry, 1)
        assert reachable == set()
