"""#7183: skill frontmatter follows the open Agent Skills spec.

Import keeps every key ELITEA does not own, export writes them back, and ELITEA's own
fields travel as ``metadata.elitea-*`` strings so the export passes spec validators.
"""

import contextlib
import importlib.util
import pathlib
import sys
import types
from unittest import mock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'
SPEC_TOP_LEVEL_KEYS = {'name', 'description', 'license', 'compatibility', 'metadata', 'allowed-tools'}
AUTO_SELECTION = {
    'mode': 'auto',
    'profile_ref': {'id': 'balanced', 'revision': 1},
    'scope_mode': 'run_locked',
    'reasoning': {'mode': 'auto'},
}
DOCX_SKILL = """---
name: docx
description: Create and edit Word documents.
license: Proprietary. LICENSE.txt has complete terms
---

Use python-docx.
"""
CLAUDE_CODE_SKILL = """---
name: release-notes
description: Draft release notes from merged PRs.
allowed-tools: Bash(git:*) Read
argument-hint: '[version]'
disable-model-invocation: true
compatibility: Requires GitHub access
metadata:
  author: platform-team
  version: '2.0'
---

Collect the merged PRs.
"""


class AnyModule(types.ModuleType):
    """Answers every `from x import name` so a module imports without its collaborators."""

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=f'{self.__name__}.{name}')


def _install_packages(monkeypatch):
    for name in ('plugins', PKG, f'{PKG}.utils', f'{PKG}.models', f'{PKG}.models.enums', f'{PKG}.models.pd'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)


def _load(monkeypatch, relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module


class ImportRecorder:
    def __init__(self):
        self.calls = []

    @contextlib.contextmanager
    def session(self, session, project_id):
        yield object()

    def import_skill(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(id=7)

    def get_skill_details(self, **kwargs):
        return {'data': {'id': 7}}


@pytest.fixture
def md(monkeypatch):
    _install_packages(monkeypatch)
    recorder = ImportRecorder()
    for name in ('sqlalchemy', 'sqlalchemy.orm', f'{PKG}.models.skill', f'{PKG}.models.pd.skill',
                 f'{PKG}.utils.export_import_utils'):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    skill_utils._skill_session = recorder.session
    skill_utils.import_skill = recorder.import_skill
    skill_utils.get_skill_details = recorder.get_skill_details
    monkeypatch.setitem(sys.modules, skill_utils.__name__, skill_utils)
    _load(monkeypatch, 'utils/skill_frontmatter.py', f'{PKG}.utils.skill_frontmatter')
    module = _load(monkeypatch, 'utils/skill_export_import.py', f'{PKG}.utils.skill_export_import')
    module.recorder = recorder
    return module


def imported_version(md, content):
    result = md.import_skill_md(project_id=1, content=content, author_id=3)
    call = md.recorder.calls[-1]
    return result, call, call['versions'][0]


def exported(md, *, name='release-notes', description='Draft release notes.', version_name='base',
             tags=None, run_settings=None, meta=None, instructions='Body.'):
    skill = {
        'name': name,
        'description': description,
        'versions': [{'name': version_name}],
        'version_details': {
            'name': version_name,
            'instructions': instructions,
            'tags': [{'name': tag} for tag in (tags or [])],
            'run_settings': run_settings,
            'meta': meta or {},
        },
    }
    markdown = md.skill_to_md(skill)
    return markdown, md.parse_skill_md(markdown)['frontmatter']


class TestImportKeepsForeignKeys:
    def test_license_is_kept_on_the_version(self, md):
        result, _, version = imported_version(md, DOCX_SKILL)
        assert version['meta'] == {'frontmatter': {'license': 'Proprietary. LICENSE.txt has complete terms'}}
        assert result['unused_frontmatter_keys'] == []

    def test_claude_code_keys_are_kept_and_reported_as_unused(self, md):
        result, _, version = imported_version(md, CLAUDE_CODE_SKILL)
        assert version['meta']['frontmatter'] == {
            'allowed-tools': 'Bash(git:*) Read',
            'argument-hint': '[version]',
            'disable-model-invocation': True,
            'compatibility': 'Requires GitHub access',
            'metadata': {'author': 'platform-team', 'version': '2.0'},
        }
        assert result['unused_frontmatter_keys'] == [
            'allowed-tools', 'argument-hint', 'compatibility', 'disable-model-invocation', 'metadata',
        ]

    def test_a_skill_with_only_elitea_keys_stores_no_frontmatter(self, md):
        _, _, version = imported_version(md, '---\nname: plain\ndescription: d\ntags: [a]\n---\nBody')
        assert 'meta' not in version

    def test_yaml_dates_are_stored_as_strings(self, md):
        _, _, version = imported_version(md, '---\nname: dated\ndescription: d\nreviewed: 2026-10-23\n---\nBody')
        assert version['meta']['frontmatter'] == {'reviewed': '2026-10-23'}


class TestExportFollowsTheSpec:
    def test_license_and_foreign_keys_are_written_back_unchanged(self, md):
        _, _, version = imported_version(md, CLAUDE_CODE_SKILL)
        _, frontmatter = exported(md, meta=version['meta'])
        assert frontmatter == {
            'name': 'release-notes',
            'description': 'Draft release notes.',
            'compatibility': 'Requires GitHub access',
            'allowed-tools': 'Bash(git:*) Read',
            'metadata': {'author': 'platform-team', 'version': '2.0'},
            'argument-hint': '[version]',
            'disable-model-invocation': True,
        }

    def test_spec_keys_follow_the_description_whatever_the_stored_order(self, md):
        meta = {'frontmatter': {'argument-hint': '[v]', 'metadata': {'author': 'me'}, 'license': 'MIT'}}
        _, frontmatter = exported(md, meta=meta)
        assert list(frontmatter) == ['name', 'description', 'license', 'metadata', 'argument-hint']

    def test_elitea_fields_go_under_metadata_as_strings(self, md):
        run_settings = {
            'llm_settings': {'model_name': 'gpt-4.1', 'temperature': 0.2, 'max_tokens': 4000,
                             'selection': AUTO_SELECTION},
            'ignore_project_context': True,
        }
        _, frontmatter = exported(md, version_name='v2', tags=['python', 'a, b'], run_settings=run_settings,
                                  meta={'frontmatter': {'license': 'MIT'}})

        assert set(frontmatter) <= SPEC_TOP_LEVEL_KEYS
        assert all(isinstance(value, str) for value in frontmatter['metadata'].values())
        assert frontmatter['license'] == 'MIT'
        assert frontmatter['metadata']['elitea-tags'] == '["python", "a, b"]'
        assert frontmatter['metadata']['elitea-version'] == 'v2'
        assert frontmatter['metadata']['elitea-temperature'] == '0.2'

    def test_stale_elitea_metadata_in_the_stored_frontmatter_is_not_exported(self, md):
        meta = {'frontmatter': {'metadata': {'author': 'me', 'elitea-tags': '["old"]'}}}
        _, frontmatter = exported(md, meta=meta)
        assert frontmatter['metadata'] == {'author': 'me'}

    def test_a_plain_skill_exports_only_name_and_description(self, md):
        markdown, frontmatter = exported(md)
        assert frontmatter == {'name': 'release-notes', 'description': 'Draft release notes.'}
        assert 'elitea' not in markdown


class TestReimportRestoresEliteaFields:
    def test_tags_and_run_settings_survive_a_round_trip(self, md):
        run_settings = {
            'llm_settings': {'model_name': 'gpt-4.1', 'temperature': 0.2, 'max_tokens': 4000,
                             'selection': AUTO_SELECTION},
            'ignore_project_context': True,
        }
        markdown, _ = exported(md, version_name='v2', tags=['python', 'a, b'], run_settings=run_settings,
                               meta={'frontmatter': {'metadata': {'author': 'me'}}})

        _, call, version = imported_version(md, markdown)

        assert version['name'] == 'base'
        assert version['tags'] == [{'name': 'python'}, {'name': 'a, b'}]
        assert version['run_settings'] == {
            'llm_settings': {'model_name': 'gpt-4.1', 'temperature': 0.2, 'max_tokens': 4000,
                             'selection': AUTO_SELECTION},
            'ignore_project_context': True,
        }
        assert version['meta'] == {'frontmatter': {'metadata': {'author': 'me'}}}

    def test_a_legacy_export_still_imports(self, md):
        legacy = (
            '---\nname: legacy\ndescription: d\nelitea_version: v3\ntags:\n- python\n'
            'elitea_run_settings:\n  temperature: 0.5\n---\nBody'
        )
        _, _, version = imported_version(md, legacy)
        assert version['tags'] == [{'name': 'python'}]
        assert version['run_settings'] == {'ignore_project_context': False, 'llm_settings': {'temperature': 0.5}}
        assert 'meta' not in version

    def test_metadata_wins_over_legacy_top_level_keys(self, md):
        both = (
            '---\nname: both\ndescription: d\ntags:\n- legacy\n'
            'metadata:\n  elitea-tags: \'["current"]\'\n---\nBody'
        )
        _, _, version = imported_version(md, both)
        assert version['tags'] == [{'name': 'current'}]

    def test_hand_written_yaml_values_are_accepted(self, md):
        typed = (
            '---\nname: typed\ndescription: d\nmetadata:\n  elitea-tags:\n  - python\n'
            '  elitea-temperature: 0.4\n  elitea-ignore-project-context: false\n---\nBody'
        )
        _, _, version = imported_version(md, typed)
        assert version['tags'] == [{'name': 'python'}]
        assert version['run_settings'] == {'ignore_project_context': False, 'llm_settings': {'temperature': 0.4}}

    def test_broken_json_is_rejected_with_the_key_named(self, md):
        broken = "---\nname: broken\ndescription: d\nmetadata:\n  elitea-tags: '[python'\n---\nBody"
        with pytest.raises(ValueError, match='metadata.elitea-tags'):
            imported_version(md, broken)


class TestImportRejectsSpecViolations:
    @pytest.mark.parametrize('frontmatter, message', [
        ('name: my--skill\ndescription: d', 'consecutive hyphens'),
        ('name: ok\ndescription: Use <tag> here', 'angle brackets'),
        ('name: ok\ndescription: d\ncompatibility: ' + 'x' * 501, '"compatibility" is 501 characters'),
        ('name: ok\ndescription: d\nlicense: [MIT]', '"license" must be a string'),
        ('name: ok\ndescription: d\nmetadata: author', '"metadata" must be a mapping'),
    ])
    def test_violation_is_rejected(self, md, frontmatter, message):
        with pytest.raises(ValueError, match=message):
            imported_version(md, f'---\n{frontmatter}\n---\nBody')
        assert md.recorder.calls == []

    def test_a_long_description_is_still_accepted(self, md):
        _, call, _ = imported_version(md, f'---\nname: long\ndescription: {"x" * 1500}\n---\nBody')
        assert len(call['description']) == 1500


class FakeQuery:
    def __init__(self, skill):
        self.skill = skill

    def filter(self, *args):
        return self

    def options(self, *args):
        return self

    def first(self):
        return self.skill


@pytest.fixture
def skill_utils(monkeypatch):
    _install_packages(monkeypatch)
    for name in (
        'tools', 'sqlalchemy', 'sqlalchemy.orm', 'pylon', 'pylon.core', 'pylon.core.tools',
        f'{PKG}.utils.authors', f'{PKG}.utils.utils', f'{PKG}.utils.like_utils',
        f'{PKG}.utils.folder_access', f'{PKG}.utils.skill_run_settings', f'{PKG}.models.skill',
        f'{PKG}.models.all', f'{PKG}.models.enums.all', f'{PKG}.models.enums.events',
        f'{PKG}.models.pd.skill', f'{PKG}.models.pd.skill_version', f'{PKG}.models.pd.skill_run_settings',
    ):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    _load(monkeypatch, 'utils/skill_frontmatter.py', f'{PKG}.utils.skill_frontmatter')
    module = _load(monkeypatch, 'utils/skill_utils.py', f'{PKG}.utils.skill_utils')
    monkeypatch.setattr(module, 'participant_chip', lambda skill: {})
    monkeypatch.setattr(module, '_build_authors_map', lambda skill: {})
    monkeypatch.setattr(module, 'serialize', lambda value: value)
    return module


def update_skill(skill_utils, skill, **fields):
    @contextlib.contextmanager
    def session(session, project_id):
        yield types.SimpleNamespace(query=lambda model: FakeQuery(skill))

    skill_utils._skill_session = session
    update = types.SimpleNamespace(version=None, meta=None, name=None, description=None)
    update.__dict__.update(fields)
    skill_utils.update_skill(project_id=1, skill_id=skill.id, update_data=update)


def stored_skill(name='my--skill', description='Use <b> carefully'):
    return types.SimpleNamespace(id=7, name=name, description=description, meta={}, versions=[])


class TestExistingSkillsStaySavable:
    def test_unchanged_grandfathered_name_and_description_save(self, skill_utils):
        skill = stored_skill()
        update_skill(skill_utils, skill, name='my--skill', description='Use <b> carefully')
        assert (skill.name, skill.description) == ('my--skill', 'Use <b> carefully')

    def test_renaming_to_a_double_hyphen_is_rejected(self, skill_utils):
        skill = stored_skill(name='ok')
        with pytest.raises(skill_utils.SkillSpecViolationError, match='consecutive hyphens'):
            update_skill(skill_utils, skill, name='still--bad')
        assert skill.name == 'ok'

    def test_changing_the_description_to_angle_brackets_is_rejected(self, skill_utils):
        skill = stored_skill(description='plain')
        with pytest.raises(skill_utils.SkillSpecViolationError, match='angle brackets'):
            update_skill(skill_utils, skill, description='a <b>')
        assert skill.description == 'plain'


def update_version(skill_utils, version, meta):
    update = types.SimpleNamespace(name=None, instructions=None, meta=meta, tags=None, model_fields_set=set())
    skill_utils._update_version_fields(session=None, version=version, update_data=update)


class TestEditorFrontmatterIsSaved:
    def test_new_frontmatter_is_normalized_and_merged_into_meta(self, skill_utils):
        version = types.SimpleNamespace(meta={'icon_meta': {'url': 'x'}})
        frontmatter = {'license': 'MIT', 'name': 'echoed', 'metadata': {'author': 'me', 'elitea-tags': '[]'}}
        update_version(skill_utils, version, {'frontmatter': frontmatter})
        assert version.meta == {
            'icon_meta': {'url': 'x'},
            'frontmatter': {'license': 'MIT', 'metadata': {'author': 'me'}},
        }

    def test_an_invalid_new_value_is_rejected(self, skill_utils):
        version = types.SimpleNamespace(meta={})
        with pytest.raises(skill_utils.SkillSpecViolationError, match='compatibility'):
            update_version(skill_utils, version, {'frontmatter': {'compatibility': 'x' * 501}})
        assert version.meta == {}

    def test_an_unchanged_stored_value_is_not_revalidated(self, skill_utils):
        stored = {'compatibility': 'x' * 501}
        version = types.SimpleNamespace(meta={'frontmatter': stored})
        update_version(skill_utils, version, {'frontmatter': dict(stored), 'icon_meta': {}})
        assert version.meta['frontmatter'] == stored

    def test_a_non_mapping_frontmatter_is_rejected(self, skill_utils):
        version = types.SimpleNamespace(meta={})
        with pytest.raises(skill_utils.SkillSpecViolationError, match='must be a mapping'):
            update_version(skill_utils, version, {'frontmatter': 'license: MIT'})



def new_skill(name='release-notes', description='Draft notes.', meta=None):
    return types.SimpleNamespace(name=name, description=description, versions=[types.SimpleNamespace(meta=meta)])


class TestCreatingASkill:
    @pytest.mark.parametrize('skill, message', [
        (new_skill(name='my--skill'), 'consecutive hyphens'),
        (new_skill(description='a <b>'), 'angle brackets'),
        (new_skill(meta={'frontmatter': {'license': 1}}), '"license" must be a string'),
    ])
    def test_spec_violations_are_rejected(self, skill_utils, skill, message):
        with pytest.raises(skill_utils.SkillSpecViolationError, match=message):
            skill_utils.enforce_new_skill_spec(skill)

    def test_editor_frontmatter_is_normalized_before_create(self, skill_utils):
        skill = new_skill(meta={'frontmatter': {'license': 'MIT', 'description': 'echoed'}})
        skill_utils.enforce_new_skill_spec(skill)
        assert skill.versions[0].meta == {'frontmatter': {'license': 'MIT'}}
