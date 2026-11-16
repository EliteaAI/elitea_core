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


class _InCriterion:
    def __init__(self, values):
        self.values = set(values)


class _IdColumn:
    def __eq__(self, other):
        return _Criterion(other)

    def in_(self, values):
        return _InCriterion(values)


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
        'plugins.elitea_core.utils.llm_judge': {'run_llm_judge': None},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': None},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': None, 'is_valid_category': None},
        'plugins.elitea_core.utils.application_utils': {'build_skill_mappings_list': None},
        'plugins.elitea_core.utils.skill_export_import': {'build_skill_fork_payload': None},
        'plugins.elitea_core.utils.skill_frontmatter': {'FRONTMATTER_META_KEY': 'frontmatter'},
        'plugins.elitea_core.utils.skill_utils': {'attach_skill_to_public_copy': None},
        'plugins.elitea_core.utils.export_import': {
            'PUBLISHED_TOOLKITS_META_KEY': 'fork_toolkits', 'export_version_toolkit': None,
        },
        'plugins.elitea_core.utils.toolkit_meta': {'drop_index_schedules': None},
        'plugins.elitea_core.utils.conversation_starters_utils': {'get_max_conversation_starters': lambda: 4},
    }.items():
        mod = types.ModuleType(modname)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[modname] = mod

    sys.modules['plugins.elitea_core.models.all'] = models_all
    sys.modules['plugins.elitea_core.models.elitea_tools'] = elitea_tools
    sys.modules['plugins.elitea_core.models.enums.all'] = enums
    if not hasattr(sys.modules['tools'], 'serialize'):
        sys.modules['tools'].serialize = lambda value: value

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
    def __init__(self, registry, session=None):
        self._registry = registry
        self._session = session
        self._vid = None
        self._vids = None

    def filter(self, *criteria):
        for c in criteria:
            if hasattr(c, 'value'):
                self._vid = c.value
            elif hasattr(c, 'values'):
                self._vids = c.values
        return self

    def options(self, *_a, **_k):
        return self

    def first(self):
        if self._session is not None and self._vid in self._session._fail_on:
            self._session.broken = True
            raise RuntimeError('connection dropped')
        return self._registry.get(self._vid)

    def all(self):
        if self._session is not None and self._session._fail_bulk:
            self._session.broken = True
            raise RuntimeError('connection dropped')
        return [self._registry[v] for v in sorted(self._vids or ()) if v in self._registry]


class FakeSession:
    """registry maps version_id -> FakeVersion.

    ``fail_on`` is a set of version ids whose single-version load raises like a dropped
    connection; once one does, the session stays broken (every query raises) until
    ``rollback()``, the way a failed transaction behaves.
    """

    def __init__(self, registry, fail_on=(), fail_bulk=False):
        self._registry = registry
        self._fail_on = set(fail_on)
        self._fail_bulk = fail_bulk
        self.query_count = 0
        self.rollback_count = 0
        self.broken = False

    def query(self, _model):
        self.query_count += 1
        if self.broken:
            raise RuntimeError('current transaction is aborted')
        return _Query(self._registry, self)

    def rollback(self):
        self.rollback_count += 1
        self.broken = False


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


class _FakeListApp:
    """The slice of a listed application the nesting-tier fill reads and writes."""

    def __init__(self, app_id, versions, meta=None):
        self.id = app_id
        self.versions = versions
        self.meta = meta or {}
        self.agent_subtree_tiers = None
        self.max_agent_nesting_tiers = None


class _ListVersion:
    def __init__(self, ver_id, name):
        self.id = ver_id
        self.name = name


class TestFillListAgentNestingTiers:
    """List rows carry the default version's depth so the picker can disable too-deep agents."""

    @staticmethod
    def _registry():
        return {
            # 10: leaf, 20: container of leaves, 30: three-tier chain (30 -> 20 -> 10)
            10: FakeVersion(10, tools=[]),
            20: FakeVersion(20, tools=[FakeTool(1, 10)]),
            30: FakeVersion(30, tools=[FakeTool(2, 20)]),
        }

    def test_measures_the_default_version(self, pu):
        apps = [
            _FakeListApp(
                2, [_ListVersion(20, 'base'), _ListVersion(30, 'v2')], meta={'default_version_id': 30}
            ),
        ]
        pu.fill_list_agent_nesting_tiers(1, apps, session=FakeSession(self._registry()))
        assert apps[0].agent_subtree_tiers == 3
        assert apps[0].max_agent_nesting_tiers == pu.MAX_AGENT_NESTING_TIERS

    def test_falls_back_to_base_without_a_default(self, pu):
        apps = [_FakeListApp(2, [_ListVersion(30, 'v2'), _ListVersion(20, 'base')])]
        pu.fill_list_agent_nesting_tiers(1, apps, session=FakeSession(self._registry()))
        assert apps[0].agent_subtree_tiers == 2

    def test_leaves_the_fields_empty_when_there_is_no_version_to_measure(self, pu):
        apps = [_FakeListApp(2, [])]
        pu.fill_list_agent_nesting_tiers(1, apps, session=FakeSession(self._registry()))
        assert apps[0].agent_subtree_tiers is None
        assert apps[0].max_agent_nesting_tiers is None

    def test_one_bad_row_does_not_fail_the_list(self, pu):
        apps = [
            _FakeListApp(1, [_ListVersion(999, 'base')]),  # version missing from the registry
            _FakeListApp(2, [_ListVersion(10, 'base')]),
        ]
        pu.fill_list_agent_nesting_tiers(1, apps, session=FakeSession(self._registry()))
        assert apps[0].agent_subtree_tiers is None
        assert apps[1].agent_subtree_tiers == 1

    def test_query_count_does_not_grow_with_the_number_of_rows(self, pu):
        def run(rows):
            # every row's default version uses the same sub-agent (version 10)
            registry = {10: FakeVersion(10, tools=[])}
            apps = []
            for i in range(rows):
                registry[100 + i] = FakeVersion(100 + i, tools=[FakeTool(1, 10)])
                apps.append(_FakeListApp(i, [_ListVersion(100 + i, 'base')]))
            session = FakeSession(registry)
            pu.fill_list_agent_nesting_tiers(1, apps, session=session)
            assert all(app.agent_subtree_tiers == 2 for app in apps)
            return session.query_count

        # one bulk load of the default versions + the shared sub-agent loaded once
        assert run(3) == run(50) == 2

    def test_a_database_error_rolls_back_so_later_rows_still_get_their_tiers(self, pu):
        registry = {
            10: FakeVersion(10, tools=[]),
            40: FakeVersion(40, tools=[FakeTool(7, 777)]),  # its sub-agent load will fail
            777: FakeVersion(777, tools=[]),
        }
        apps = [
            _FakeListApp(4, [_ListVersion(40, 'base')]),
            _FakeListApp(1, [_ListVersion(10, 'base')]),
        ]
        session = FakeSession(registry, fail_on={777})
        pu.fill_list_agent_nesting_tiers(1, apps, session=session)

        assert apps[0].agent_subtree_tiers is None
        assert apps[1].agent_subtree_tiers == 1
        assert session.rollback_count == 1

    def test_a_failed_bulk_load_leaves_every_row_empty_without_raising(self, pu):
        apps = [_FakeListApp(1, [_ListVersion(10, 'base')])]
        session = FakeSession(self._registry(), fail_bulk=True)
        pu.fill_list_agent_nesting_tiers(1, apps, session=session)

        assert apps[0].agent_subtree_tiers is None
        assert session.rollback_count == 1


class TestCapNestingTiersPageSize:
    """The page size of a list that measures nesting depth can never be unbounded."""

    @pytest.mark.parametrize('limit, expected', [
        (20, 20),    # the pickers' page size is kept
        (50, 50),
        (51, 50),    # above the cap
        (100000, 50),
        (1, 1),
        (0, 50),     # falsy limit means "no limit" downstream, so it lands on the cap
        (-1, 50),    # not a page size
        (-100, 50),
        (None, 50),  # nothing given
    ])
    def test_limit_is_bounded(self, pu, limit, expected):
        assert pu.cap_nesting_tiers_page_size(limit) == expected

    def test_the_cap_is_fifty(self, pu):
        assert pu.MAX_NESTING_TIERS_PAGE_SIZE == 50
