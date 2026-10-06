import importlib.util
import json
import pathlib
import sys
import types
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

SOURCE_PROJECT_ID = 2
PUBLIC_PROJECT_ID = 1
AUTHOR_ID = 3

GITHUB_TOOL = {
    'id': 7,
    'type': 'github',
    'name': 'Team GitHub',
    'description': 'repo access',
    'author_id': AUTHOR_ID,
    'created_at': datetime(2026, 10, 1, 9, 30),
    'settings': {
        'repository': 'EliteaAI/elitea_core',
        'active_branch': 'main',
        'github_configuration': {'private': True, 'elitea_title': 'gh-creds'},
        'access_token': 'ghp_must_not_leak',
        'selected_tools': ['get_issue', 'create_pr'],
    },
    'meta': {
        'indexes_meta': {'docs': {'schedules': {'1': {'cron': '0 * * * *'}}}},
        'parent_entity_id': 5,
        'parent_project_id': 9,
        'parent_author_id': 11,
        'kept': 'yes',
    },
}

TOOLKIT_AUTHOR = {'id': AUTHOR_ID, 'name': 'Alice', 'email': 'alice@example.com', 'avatar': None}

SUB_AGENT_TOOL = {
    'id': 8,
    'type': 'application',
    'name': 'helper',
    'author_id': AUTHOR_ID,
    'created_at': datetime(2026, 10, 1, 9, 30),
    'settings': {'application_id': 40, 'application_version_id': 41},
    'meta': {},
}


class _AuthorLookupSpy:
    def __init__(self):
        self.calls = 0

    def __call__(self, *a, **k):
        self.calls += 1
        return [dict(TOOLKIT_AUTHOR)]


AUTHOR_LOOKUP = _AuthorLookupSpy()


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


class _Col:
    def __eq__(self, other):  # noqa: PLW3201
        return object()

    def in_(self, values):
        return object()

    def __hash__(self):
        return id(self)


class _Loader:
    def selectinload(self, *a, **k):
        return self


class _Query:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *a, **k):
        return self

    def options(self, *a, **k):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


def _sessions(*rows):
    @contextmanager
    def get_session(project_id):
        yield types.SimpleNamespace(query=lambda *a, **k: _Query(list(rows)))
    return types.SimpleNamespace(get_session=get_session)


class _FakeApplicationExportPayloadModel:
    def __init__(self, data):
        self._data = data

    @classmethod
    def model_validate(cls, data):
        return cls(data)

    def model_dump(self, mode=None):
        data = deepcopy(self._data)
        data['import_uuid'] = f"app-{data['id']}"
        for version in data['versions']:
            version['import_version_uuid'] = f"ver-{version['id']}"
            version['tools'] = [{'import_uuid': f"tool-{t['id']}"} for t in version.get('tools', [])]
        return data


def _json_roundtrip(value):
    return json.loads(json.dumps(value, default=str))


@pytest.fixture(scope='module')
def modules():
    saved = {}

    def install(name, module):
        if name not in saved:
            saved[name] = sys.modules.get(name)
        sys.modules[name] = module

    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(
        info=noop, error=noop, warning=noop, debug=noop, exception=noop,
    )
    col_model = lambda cls_name: type(cls_name, (), {  # noqa: E731
        'id': _Col(), 'versions': _Col(), 'tools': _Col(), 'tool_mappings': _Col(),
        'variables': _Col(), 'tags': _Col(), 'skill_mappings': _Col(),
    })

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
        auth=types.SimpleNamespace(current_user=lambda: {'id': AUTHOR_ID}),
        this=types.SimpleNamespace(module=None, descriptor=None),
        db=types.SimpleNamespace(get_session=noop),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
        serialize=_json_roundtrip,
    ))

    from pydantic import BaseModel, ConfigDict

    class _Permissive(BaseModel):
        model_config = ConfigDict(extra='allow', from_attributes=True)

    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': col_model('Application'),
            'ApplicationVersion': col_model('ApplicationVersion'),
        },
        'plugins.elitea_core.models.elitea_tools': {
            'EliteATool': col_model('EliteATool'),
            'EntityToolMapping': col_model('EntityToolMapping'),
        },
        'plugins.elitea_core.models.pd.collection_base': {'AuthorBaseModel': _Permissive},
        'plugins.elitea_core.models.pd.application': {
            'ApplicationExportModel': _FakeApplicationExportPayloadModel,
            'ApplicationImportModel': object,
        },
        'plugins.elitea_core.models.pd.export_import': {
            'ApplicationForkModel': _FakeApplicationExportPayloadModel,
        },
        'plugins.elitea_core.models.pd.version': {'ApplicationVersionForkCreateModel': object},
        'plugins.elitea_core.models.pd.publish': {'PublishAIResult': object},
        'plugins.elitea_core.models.pd.skill': {'SkillExportModel': object},
        'plugins.elitea_core.models.skill': {
            'EntitySkillMapping': col_model('EntitySkillMapping'),
            'Skill': col_model('Skill'),
            'SkillVersion': col_model('SkillVersion'),
        },
        'plugins.elitea_core.utils.authors': {'get_authors_data': AUTHOR_LOOKUP},
        'plugins.elitea_core.utils.toolkits_utils': {'get_mcp_schemas': lambda *a, **k: {}},
        'plugins.elitea_core.utils.application_tools': {
            'expand_toolkit_settings': lambda type_, settings, project_id, user_id: settings,
            'ValidatorNotSupportedError': type('ValidatorNotSupportedError', (Exception,), {}),
            'ConfigurationExpandError': type('ConfigurationExpandError', (Exception,), {}),
            'raise_validation_error_if_any': noop,
            'find_suggested_toolkit_name_field': lambda *a, **k: None,
            'find_suggested_toolkit_max_length': lambda *a, **k: None,
        },
        'plugins.elitea_core.utils.create_utils': {'create_application': noop, 'create_version': noop},
        'plugins.elitea_core.utils.llm_judge': {'run_llm_judge': noop},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': lambda: PUBLIC_PROJECT_ID},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': lambda tags, cat: tags,
            'is_valid_category': lambda name: True,
        },
        'plugins.elitea_core.utils.application_utils': {'build_skill_mappings_list': lambda ms: list(ms)},
        'plugins.elitea_core.utils.skill_export_import': {'build_skill_fork_payload': noop},
        'plugins.elitea_core.utils.skill_utils': {'attach_skill_to_public_copy': noop},
    }
    for name, attrs in stubs.items():
        install(name, _module(name, **attrs))

    _load('plugins.elitea_core.models.enums.all', 'models/enums/all.py')
    _load('plugins.elitea_core.models.pd.tool', 'models/pd/tool.py')
    _load('plugins.elitea_core.utils.export_import_utils', 'utils/export_import_utils.py')
    _load('plugins.elitea_core.utils.toolkit_meta', 'utils/toolkit_meta.py')
    export_import = _load('plugins.elitea_core.utils.export_import', 'utils/export_import.py')
    publish_utils = _load('plugins.elitea_core.utils.publish_utils', 'utils/publish_utils.py')
    export_import.selectinload = lambda *a, **k: _Loader()
    publish_utils.selectinload = lambda *a, **k: _Loader()

    yield types.SimpleNamespace(export_import=export_import, publish_utils=publish_utils)

    for name, original in saved.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


def _orm_version(version_dict, application):
    return types.SimpleNamespace(to_dict=lambda: deepcopy(version_dict), application=application)


def _orm_application(app_id, versions=()):
    app = types.SimpleNamespace(
        id=app_id,
        to_json=lambda: {
            'id': app_id, 'name': 'Release Helper', 'description': 'helps',
            'owner_id': PUBLIC_PROJECT_ID,
        },
    )
    app.versions = [_orm_version(v, app) for v in versions]
    for orm_version, raw in zip(app.versions, versions):
        orm_version.id = raw['id']
    return app


def _published_version_meta(modules, monkeypatch, tools):
    source_version = {
        'id': 79,
        'name': 'base',
        'instructions': 'Help with releases.',
        'meta': {'step_limit': 25, 'internal_tools': ['attachments']},
        'tools': deepcopy(tools),
    }
    pu = modules.publish_utils
    monkeypatch.setattr(pu, 'db', _sessions(_orm_version(source_version, _orm_application(10))))
    snapshot = pu.create_publish_snapshot(SOURCE_PROJECT_ID, 79, AUTHOR_ID)
    published = pu._build_published_version_dict(
        snapshot['version'], 'v1', AUTHOR_ID, PUBLIC_PROJECT_ID, snapshot['source'],
    )
    assert 'tools' not in published, 'the public copy itself must stay toolless'
    return published['meta']


def _export_public_version(
    modules, monkeypatch, meta, forked, status='published', project_id=PUBLIC_PROJECT_ID,
):
    public_version = {'id': 112, 'name': 'v1', 'status': status, 'meta': deepcopy(meta), 'tools': []}
    ei = modules.export_import
    monkeypatch.setattr(ei, 'db', _sessions(_orm_application(60, [public_version])))
    result = ei.export_application(
        project_id, AUTHOR_ID, [60], forked=forked, follow_version_ids=[112],
    )
    assert result['ok'], result
    return result


def test_forking_a_published_version_carries_its_toolkit(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL, SUB_AGENT_TOOL])

    payload = _export_public_version(modules, monkeypatch, meta, forked=True)

    [toolkit] = payload['toolkits']
    [version] = payload['applications'][0]['versions']
    assert toolkit['type'] == 'github'
    assert toolkit['name'] == 'Team GitHub'
    assert {'import_uuid': toolkit['import_uuid']} in version['tools']
    assert toolkit['settings']['repository'] == 'EliteaAI/elitea_core'
    assert toolkit['selected_tools'] == ['get_issue', 'create_pr']


def test_forked_toolkit_keeps_the_credential_placeholder_but_not_the_secret(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    [toolkit] = _export_public_version(modules, monkeypatch, meta, forked=True)['toolkits']

    assert 'access_token' not in toolkit['settings']
    assert toolkit['settings']['github_configuration'] == {'private': True, 'elitea_title': 'gh-creds'}
    assert 'ghp_must_not_leak' not in json.dumps(meta)


def test_published_snapshot_drops_index_schedules_and_source_lineage(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    [toolkit] = meta[modules.export_import.PUBLISHED_TOOLKITS_META_KEY]

    assert toolkit['meta'] == {'kept': 'yes'}


def test_sub_agents_are_not_snapshotted_as_toolkits(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [SUB_AGENT_TOOL])

    assert modules.export_import.PUBLISHED_TOOLKITS_META_KEY not in meta


def test_snapshot_is_stored_in_json_shape(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    assert _json_roundtrip(meta) == meta


def test_fork_export_does_not_copy_the_snapshot_into_the_forked_meta(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    payload = _export_public_version(modules, monkeypatch, meta, forked=True)

    [version] = payload['applications'][0]['versions']
    assert modules.export_import.PUBLISHED_TOOLKITS_META_KEY not in version['meta']
    assert version['meta']['internal_tools'] == ['attachments']


def test_plain_export_neither_restores_toolkits_nor_leaks_the_snapshot(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    payload = _export_public_version(modules, monkeypatch, meta, forked=False)

    [version] = payload['applications'][0]['versions']
    assert payload['toolkits'] == []
    assert version['tools'] == []
    assert modules.export_import.PUBLISHED_TOOLKITS_META_KEY not in version['meta']


def test_version_published_before_the_snapshot_existed_forks_unchanged(modules, monkeypatch):
    payload = _export_public_version(modules, monkeypatch, {'step_limit': 25}, forked=True)

    [version] = payload['applications'][0]['versions']
    assert payload['toolkits'] == []
    assert version['tools'] == []


def test_catalog_readable_snapshot_holds_only_what_the_fork_wizard_reads(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    [toolkit] = meta[modules.export_import.PUBLISHED_TOOLKITS_META_KEY]

    assert set(toolkit) <= {'type', 'name', 'settings', 'selected_tools', 'meta', 'import_uuid'}
    assert 'alice@example.com' not in json.dumps(meta)


def test_publish_snapshot_does_not_look_up_toolkit_authors(modules, monkeypatch):
    AUTHOR_LOOKUP.calls = 0

    _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    assert AUTHOR_LOOKUP.calls == 0


def test_fork_of_an_embedded_sub_agent_copy_carries_its_toolkit(modules, monkeypatch):
    meta = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    payload = _export_public_version(modules, monkeypatch, meta, forked=True, status='embedded')

    assert [t['name'] for t in payload['toolkits']] == ['Team GitHub']


@pytest.mark.parametrize('status, project_id', [
    pytest.param('draft', PUBLIC_PROJECT_ID, id='draft-in-public-project'),
    pytest.param('published', SOURCE_PROJECT_ID, id='team-project-version'),
])
def test_toolkits_planted_in_editable_meta_are_not_forked(modules, monkeypatch, status, project_id):
    planted = _published_version_meta(modules, monkeypatch, [GITHUB_TOOL])

    payload = _export_public_version(
        modules, monkeypatch, planted, forked=True, status=status, project_id=project_id,
    )

    [version] = payload['applications'][0]['versions']
    assert payload['toolkits'] == []
    assert version['tools'] == []
    assert modules.export_import.PUBLISHED_TOOLKITS_META_KEY not in version['meta']
