"""Issue #5803 — skill twins materialized by agent publishing must never
surface as standalone public-catalog entries.

The catalog lists only skill versions with ``status='published'``; twins stay
invisible because (a) the fork/import payload models carry no ``status`` field
at all, so imported twin versions can only ever be drafts, and (b) the twin
resolver stamps lineage under ``agent_publish_*``-namespaced meta keys — bare
``parent_*`` keys would make the catalog lineage queries (which filter them
without project scoping) mistake twins for user forks. This suite pins both
constructions plus the dedup/no-op guards around them, against fakes — no DB.

Heavy runtime imports (``tools``, ``pylon``) are stubbed with ``setdefault`` so
the module composes with the harness-installed stubs.

Run via:
    python tests/run_tests.py integration/test_5803_skill_twin_catalog_invisibility.py -v
"""

import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------- #
# Fake ORM column: supports the filter/JSON-path expressions the twin queries
# build (``SkillVersion.meta['k'].astext == v``, ``EntitySkillMapping.x == v``,
# ``order_by(created_at.asc())``) without evaluating them — the fake session
# ignores criteria and returns canned rows.
# --------------------------------------------------------------------------- #

class _Col:
    def __eq__(self, other):  # noqa: PLW3201 — criterion object, not bool
        return object()

    def __getitem__(self, key):
        return _Col()

    @property
    def astext(self):
        return _Col()

    def asc(self):
        return self

    def in_(self, values):
        return object()

    def __hash__(self):
        return id(self)


class _Query:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *a, **k):
        return self

    def options(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class FakeSession:
    """Returns queued row-lists, one list per successive .query() call."""

    def __init__(self, results_per_query):
        self._results = list(results_per_query)

    def query(self, *entities):
        rows = self._results.pop(0) if self._results else []
        return _Query(rows)


def _fake_db(sessions):
    queue = list(sessions)

    @contextmanager
    def get_session(project_id):
        yield queue.pop(0) if queue else FakeSession([])

    return types.SimpleNamespace(get_session=get_session)


# --------------------------------------------------------------------------- #
# Module fixture: import publish_utils with runtime/model deps stubbed
# --------------------------------------------------------------------------- #

def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


@pytest.fixture(scope='module')
def pu():
    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(info=noop, error=noop, warning=noop, debug=noop)

    sys.modules.setdefault('pylon', _module('pylon'))
    sys.modules.setdefault('pylon.core', _module('pylon.core'))
    sys.modules.setdefault('pylon.core.tools', _module('pylon.core.tools', log=log))
    if not hasattr(sys.modules['pylon.core.tools'], 'log'):
        sys.modules['pylon.core.tools'].log = log
    sys.modules.setdefault('tools', _module(
        'tools',
        db=types.SimpleNamespace(get_session=None),
        this=types.SimpleNamespace(module=None, descriptor=None),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
    ))

    col_model = lambda cls_name: type(cls_name, (), {  # noqa: E731
        'id': _Col(), 'skill_id': _Col(), 'name': _Col(), 'meta': _Col(),
        'created_at': _Col(), 'entity_version_id': _Col(), 'entity_type': _Col(),
        'skill_version_id': _Col(),
    })

    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': col_model('Application'),
            'ApplicationVersion': col_model('ApplicationVersion'),
        },
        'plugins.elitea_core.models.elitea_tools': {
            'EliteATool': col_model('EliteATool'),
            'EntityToolMapping': col_model('EntityToolMapping'),
        },
        'plugins.elitea_core.models.enums.all': {
            'AgentTypes': types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
            'NotificationEventTypes': object,
            'PublishStatus': types.SimpleNamespace(
                draft=types.SimpleNamespace(value='draft'),
                published=types.SimpleNamespace(value='published'),
                embedded=types.SimpleNamespace(value='embedded'),
            ),
            'SkillEntityTypes': types.SimpleNamespace(agent='agent'),
            'ToolEntityTypes': types.SimpleNamespace(agent='agent'),
        },
        'plugins.elitea_core.models.pd.application': {'ApplicationImportModel': object},
        'plugins.elitea_core.models.pd.version': {'ApplicationVersionForkCreateModel': object},
        'plugins.elitea_core.models.pd.publish': {'PublishAIResult': object},
        'plugins.elitea_core.models.skill': {
            'EntitySkillMapping': col_model('EntitySkillMapping'),
            'Skill': col_model('Skill'),
            'SkillVersion': col_model('SkillVersion'),
        },
        'plugins.elitea_core.utils.create_utils': {'create_application': noop, 'create_version': noop},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': lambda: 1},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': lambda tags, cat: tags,
            'is_valid_category': lambda name: True,
        },
        'plugins.elitea_core.utils.application_utils': {'build_skill_mappings_list': lambda ms: list(ms)},
        'plugins.elitea_core.utils.skill_export_import': {'build_skill_fork_payload': noop},
        'plugins.elitea_core.utils.skill_utils': {'attach_skill_to_agent': noop},
    }
    for modname, attrs in stubs.items():
        sys.modules[modname] = _module(modname, **attrs)

    # Earlier tests in the session may have replaced sqlalchemy.orm / tools
    # with stubs missing names publish_utils imports at load time; the fixture
    # only needs importability — behavior is monkeypatched per test.
    sqla_orm = sys.modules.get('sqlalchemy.orm')
    if sqla_orm is not None and not hasattr(sqla_orm, 'selectinload'):
        sqla_orm.selectinload = lambda *a, **k: None
    tools_mod = sys.modules.get('tools')
    if tools_mod is not None:
        for attr, default in (
            ('db', types.SimpleNamespace(get_session=None)),
            ('this', types.SimpleNamespace(module=None, descriptor=None)),
            ('rpc_tools', types.SimpleNamespace(RpcMixin=object)),
        ):
            if not hasattr(tools_mod, attr):
                setattr(tools_mod, attr, default)

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.utils.publish_utils',
        PLUGIN_ROOT / 'utils' / 'publish_utils.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.selectinload = lambda *a, **k: None
    return module


def _fork_payload():
    return {
        'name': 'sk-under-test',
        'entity': 'skills',
        'owner_id': 2,
        'versions': [{
            'name': 'base',
            'instructions': 'x' * 120,
            'author_id': 9,
            'meta': {},
        }],
    }


def _skill_info(instructions='x' * 120):
    return {
        'skill_id': 10,
        'skill_version_id': 20,
        'name': 'sk-under-test',
        'instructions': instructions,
    }


# --------------------------------------------------------------------------- #
# Twin fork: draft-only import payload + namespaced lineage meta
# --------------------------------------------------------------------------- #

def test_forked_twin_payload_has_no_status_and_namespaced_lineage(pu, monkeypatch):
    captured = {}

    def import_wizard(entities, project_id, user_id):
        captured['entities'] = entities
        captured['project_id'] = project_id
        return {'skills': [{'id': 77}]}, {}

    monkeypatch.setattr(pu, 'build_skill_fork_payload', lambda *a, **k: _fork_payload())
    monkeypatch.setattr(pu, 'this', types.SimpleNamespace(
        module=types.SimpleNamespace(import_wizard=import_wizard)))
    # Session 1: dedup lookup (miss); session 2: post-import version lookup.
    monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[]]), FakeSession([[(501,)]])]))

    twin_skill_id, twin_version_id = pu._resolve_or_fork_skill_twin(2, 1, _skill_info(), 3)

    assert (twin_skill_id, twin_version_id) == (77, 501)
    assert captured['project_id'] == 1
    payload = captured['entities'][0]
    version = payload['versions'][0]

    # No status anywhere in the import payload: imported versions default to
    # draft, which is what keeps twins out of the published-only catalog.
    assert 'status' not in payload
    assert 'status' not in version
    assert 'shared_owner_id' not in payload
    assert 'shared_id' not in payload

    meta = version['meta']
    assert meta['agent_publish_twin'] is True
    assert meta['agent_publish_parent_project_id'] == 2
    assert meta['agent_publish_parent_entity_id'] == 10
    assert meta['agent_publish_parent_version_id'] == 20
    assert meta['agent_publish_parent_author_id'] == 9
    assert meta['agent_publish_content_sha'] == pu._skill_content_sha('x' * 120)
    # Bare parent_* keys would collide with the catalog fork-lineage queries.
    assert not any(key.startswith('parent_') for key in meta)


def test_twin_import_uuid_tracks_content(pu, monkeypatch):
    seen = []

    def import_wizard(entities, project_id, user_id):
        seen.append(entities[0]['import_uuid'])
        return {'skills': [{'id': 77}]}, {}

    monkeypatch.setattr(pu, 'build_skill_fork_payload', lambda *a, **k: _fork_payload())
    monkeypatch.setattr(pu, 'this', types.SimpleNamespace(
        module=types.SimpleNamespace(import_wizard=import_wizard)))

    def run(instructions):
        monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[]]), FakeSession([[(501,)]])]))
        pu._resolve_or_fork_skill_twin(2, 1, _skill_info(instructions), 3)

    run('x' * 120)
    run('x' * 120)
    run('y' * 120)
    assert seen[0] == seen[1]
    assert seen[2] != seen[0]


def test_existing_twin_dedups_without_forking(pu, monkeypatch):
    def explode(*a, **k):
        raise AssertionError('must not fork when a matching twin exists')

    monkeypatch.setattr(pu, 'build_skill_fork_payload', explode)
    monkeypatch.setattr(pu, 'this', types.SimpleNamespace(
        module=types.SimpleNamespace(import_wizard=explode)))
    monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[(55, 66)]])]))

    assert pu._resolve_or_fork_skill_twin(2, 1, _skill_info(), 3) == (55, 66)


# --------------------------------------------------------------------------- #
# Zero attachments: publishing an agent without skills must not touch the
# skill machinery at all (snapshot/hash identity is guarded elsewhere).
# --------------------------------------------------------------------------- #

def test_publish_attached_skills_is_noop_without_mappings(pu, monkeypatch):
    def explode(*a, **k):
        raise AssertionError('skill machinery must not run for zero attachments')

    monkeypatch.setattr(pu, 'build_skill_fork_payload', explode)
    monkeypatch.setattr(pu, 'attach_skill_to_agent', explode)
    monkeypatch.setattr(pu, 'this', types.SimpleNamespace(
        module=types.SimpleNamespace(import_wizard=explode)))
    monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[]])]))

    assert pu.publish_attached_skills(
        source_project_id=2, public_project_id=1,
        source_version_id=5, public_version_id=6, user_id=3,
    ) is None


# --------------------------------------------------------------------------- #
# Export/import payload models: no status field — the schema-level guarantee
# that a fork/import can never produce a published (catalog-visible) version.
# --------------------------------------------------------------------------- #

def test_skill_version_transfer_models_omit_status():
    sys.modules.setdefault('tools', _module(
        'tools',
        db=types.SimpleNamespace(get_session=None),
        this=types.SimpleNamespace(module=None, descriptor=None),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
    ))
    pkg_names = [
        'plugins.elitea_core.models.pd.collection_base',
        'plugins.elitea_core.models.pd.tag',
    ]
    for pkg_name in pkg_names:
        path = PLUGIN_ROOT / 'models' / 'pd' / (pkg_name.rsplit('.', 1)[1] + '.py')
        spec = importlib.util.spec_from_file_location(pkg_name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[pkg_name] = mod
        spec.loader.exec_module(mod)
    sys.modules['plugins.elitea_core.utils.authors'] = _module(
        'plugins.elitea_core.utils.authors', get_authors_data=lambda ids: [])

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.models.pd.skill_version',
        PLUGIN_ROOT / 'models' / 'pd' / 'skill_version.py',
    )
    sv = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = sv
    spec.loader.exec_module(sv)

    assert 'status' not in sv.SkillVersionExportModel.model_fields
    assert 'status' not in sv.SkillVersionImportModel.model_fields
