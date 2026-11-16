"""Issue #7085 - the nesting tiers of a listed agent are measured on the version the add flow binds.

Review question: do list rows really carry every version in `versions`, and is
`meta['default_version_id']` the canonical source? If rows held only a subset of versions, the
'base' fallback of `fill_list_agent_nesting_tiers` could measure the wrong version.

The rows here are built by the REAL `ApplicationListModel` (the model the list API serializes), and
the REAL `_default_version_id` / `fill_list_agent_nesting_tiers` are run on them. Only the tree walk
(`compute_agent_subtree_tiers`) is replaced by a recorder, so the test shows WHICH version is
measured, not how deep it is.

Run via:
    python tests/run_tests.py integration/test_7085_list_default_version.py -v
"""
import ast
import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta
from typing import Dict, Optional

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
T0 = datetime(2026, 10, 1, 9, 0)
MAX_TIERS = 3


def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _package(name):
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


def _load(dotted_name, relative_path):
    spec = importlib.util.spec_from_file_location(dotted_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[dotted_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def list_model():
    """The real ApplicationListModel, loaded with only its storage/RPC neighbours stubbed."""
    saved = {}

    def install(name, module):
        if name not in saved:
            saved[name] = sys.modules.get(name)
        sys.modules[name] = module

    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(info=noop, error=noop, warning=noop, debug=noop, exception=noop)

    for pkg in (
        'plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
        'plugins.elitea_core.models.pd', 'plugins.elitea_core.models.enums',
        'plugins.elitea_core.utils',
    ):
        install(pkg, _package(pkg))

    install('pylon', _package('pylon'))
    install('pylon.core', _package('pylon.core'))
    install('pylon.core.tools', _module('pylon.core.tools', log=log))
    install('tools', _module(
        'tools',
        auth=types.SimpleNamespace(current_user=lambda: {'id': 3}),
        this=types.SimpleNamespace(module=None, descriptor=None),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
        serialize=lambda value: value,
        db=types.SimpleNamespace(get_session=noop),
        SecretString=str,
    ))

    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': type('Application', (), {}),
            'ApplicationVersion': type('ApplicationVersion', (), {}),
        },
        'plugins.elitea_core.utils.authors': {'get_authors_data': lambda *a, **k: []},
        'plugins.elitea_core.utils.toolkits_utils': {'get_mcp_schemas': lambda *a, **k: {}},
        'plugins.elitea_core.utils.pipeline_utils': {'validate_yaml_from_str': noop},
        'plugins.elitea_core.utils.conversation_starters_utils': {'ABSOLUTE_MAX_CONVERSATION_STARTERS': 10},
        'plugins.elitea_core.utils.application_utils': {
            'apply_selected_tools_intersection': noop,
            'check_if_usable_attachment_toolkit': lambda *a, **k: True,
        },
        'plugins.elitea_core.utils.application_tools': {
            'expand_toolkit_settings': lambda type_, settings, project_id, user_id: settings,
            'ValidatorNotSupportedError': type('ValidatorNotSupportedError', (Exception,), {}),
            'ConfigurationExpandError': type('ConfigurationExpandError', (Exception,), {}),
            'raise_validation_error_if_any': noop,
            'find_suggested_toolkit_name_field': lambda *a, **k: None,
            'find_suggested_toolkit_max_length': lambda *a, **k: None,
        },
    }
    for name, attrs in stubs.items():
        install(name, _module(name, **attrs))

    _load('plugins.elitea_core.utils.constants', 'utils/constants.py')
    _load('plugins.elitea_core.models.pd.collection_base', 'models/pd/collection_base.py')
    _load('plugins.elitea_core.models.pd.llm', 'models/pd/llm.py')
    _load('plugins.elitea_core.models.pd.tag', 'models/pd/tag.py')
    _load('plugins.elitea_core.utils.trigger_write_rule', 'utils/trigger_write_rule.py')
    _load('plugins.elitea_core.models.enums.all', 'models/enums/all.py')
    _load('plugins.elitea_core.models.pd.tool', 'models/pd/tool.py')
    _load('plugins.elitea_core.models.pd.version', 'models/pd/version.py')
    module = _load('plugins.elitea_core.models.pd.application', 'models/pd/application.py')

    yield module.ApplicationListModel

    for name, previous in saved.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


class _Recorder:
    """Stands in for the tree walk: records which version was asked for, answers a depth."""

    def __init__(self):
        self.measured = []

    def compute_agent_subtree_tiers(self, project_id, version_id, session=None, cache=None):
        self.measured.append(version_id)
        return 2


class _FakeQuery:
    """The bulk preload query: `session.query(...).filter(...).options(...).all()`."""

    def filter(self, *_a, **_k):
        return self

    def options(self, *_a, **_k):
        return self

    def all(self):
        return []


class _FakeSession:
    def query(self, _model):
        return _FakeQuery()

    def rollback(self):
        pass


class _IdColumn:
    def in_(self, values):
        return values


@pytest.fixture
def fill(list_model):
    """The real `fill_list_agent_nesting_tiers` (and `_default_version_id`) from publish_utils.py."""
    source = (PLUGIN_ROOT / 'utils' / 'publish_utils.py').read_text()
    wanted = {'_default_version_id', 'fill_list_agent_nesting_tiers'}
    functions = [
        node for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    assert {f.name for f in functions} == wanted

    recorder = _Recorder()
    noop = lambda *a, **k: None  # noqa: E731
    namespace = {
        'Optional': Optional,
        'Dict': Dict,
        'log': types.SimpleNamespace(info=noop, warning=noop, error=noop, debug=noop),
        'ApplicationVersion': type('ApplicationVersion', (), {'id': _IdColumn(), 'tools': None}),
        'selectinload': noop,
        'compute_agent_subtree_tiers': recorder.compute_agent_subtree_tiers,
        'MAX_AGENT_NESTING_TIERS': MAX_TIERS,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), 'publish_utils.py', 'exec'), namespace)

    def run(rows):
        namespace['fill_list_agent_nesting_tiers'](1, rows, session=_FakeSession())
        return recorder.measured

    run.default_version_id = namespace['_default_version_id']
    return run


def _version(version_id, name, age_days, meta=None):
    return {
        'id': version_id,
        'name': name,
        'status': 'draft',
        'created_at': T0 + timedelta(days=age_days),
        'author_id': 3,
        'tags': [],
        'agent_type': 'openai',
        'instructions': 'Help.',
        'meta': meta or {},
    }


def _row(list_model, versions, meta=None, app_id=1):
    """A row as the list API validates it: versions newest first, as `list_applications` orders them."""
    return list_model.model_validate({
        'id': app_id,
        'name': f'Agent {app_id}',
        'owner_id': 1,
        'created_at': T0,
        'meta': meta or {},
        'versions': sorted(versions, key=lambda v: v['created_at'], reverse=True),
    })


def test_a_row_keeps_every_version_it_was_given(list_model):
    row = _row(list_model, [_version(10, 'base', 0), _version(11, 'v2', 1), _version(12, 'v3', 2)])

    assert sorted(v.id for v in row.versions) == [10, 11, 12]


def test_the_default_version_id_in_meta_is_read_from_the_row(list_model, fill):
    row = _row(
        list_model,
        [_version(10, 'base', 0), _version(11, 'v2', 1), _version(12, 'v3', 2)],
        meta={'default_version_id': 11},
    )

    assert fill.default_version_id(row) == 11


def test_without_a_default_the_base_version_is_measured_even_though_it_is_listed_last(list_model, fill):
    # the list orders versions newest first, so 'base' (the oldest) is the LAST entry
    row = _row(list_model, [_version(10, 'base', 0), _version(11, 'v2', 1), _version(12, 'v3', 2)])
    assert row.versions[-1].name == 'base'

    assert fill.default_version_id(row) == 10


def test_a_stale_default_version_id_falls_back_to_base(list_model, fill):
    row = _row(
        list_model,
        [_version(10, 'base', 0), _version(11, 'v2', 1)],
        meta={'default_version_id': 999},
    )

    assert fill.default_version_id(row) == 10


def test_the_default_survives_the_forked_lineage_merge_into_meta(list_model, fill):
    forked = _version(11, 'v2', 1, meta={'parent_entity_id': 5, 'parent_project_id': 2})
    row = _row(
        list_model,
        [_version(10, 'base', 0), forked],
        meta={'default_version_id': 11},
    )

    assert row.meta['parent_entity_id'] == 5
    assert fill.default_version_id(row) == 11


def test_fill_measures_the_default_version_of_each_row_and_serializes_the_result(list_model, fill):
    with_default = _row(
        list_model,
        [_version(10, 'base', 0), _version(11, 'v2', 1)],
        meta={'default_version_id': 11},
        app_id=1,
    )
    base_only = _row(list_model, [_version(20, 'base', 0), _version(21, 'v2', 1)], app_id=2)
    rows = [with_default, base_only]

    measured = fill(rows)

    assert measured == [11, 20]
    dumped = [row.model_dump(mode='json') for row in rows]
    assert [(d['agent_subtree_tiers'], d['max_agent_nesting_tiers']) for d in dumped] == [(2, 3), (2, 3)]


def test_a_row_without_versions_serializes_with_empty_tier_fields(list_model, fill):
    row = _row(list_model, [], app_id=3)

    assert fill([row]) == []
    dumped = row.model_dump(mode='json')
    assert dumped['agent_subtree_tiers'] is None
    assert dumped['max_agent_nesting_tiers'] is None
