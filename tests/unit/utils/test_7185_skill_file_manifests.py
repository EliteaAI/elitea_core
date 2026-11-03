"""#7185: the skills a chat turn can reach - project skills, skill participants and an agent's attached skills - announce their files.

Reuses the #7126 catalog harness and the #6922 participant harness, which load the real ``utils/project_skills.py``,
``utils/skill_run_utils.py`` and ``utils/skill_participant_utils.py``; only the manifest lookup is the test's own.
"""
import ast
import importlib.util
import pathlib
import sys
import types
from typing import Optional

import pytest
from pydantic import BaseModel, ConfigDict


def _load_sibling(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).with_name(file_name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_catalog = _load_sibling('test_7126_project_skills_in_chat.py', 'project_skills_harness_7185')
_participants = _load_sibling('test_6922_skill_participant.py', 'skill_participant_harness_7185')
env = _catalog.env
project_skills = _catalog.project_skills
platform = _catalog.platform
PACKAGE = _participants.PACKAGE
FILE = {'path': 'references/guide.md', 'size': 7, 'content_type': 'text/markdown', 'is_text': True}


@pytest.fixture
def manifests(project_skills, monkeypatch):
    state = types.SimpleNamespace(requested=[], result={}, error=None)

    def lookup(session, version_ids):
        state.requested.append(list(version_ids))
        if state.error:
            raise state.error
        return state.result

    monkeypatch.setattr(project_skills, 'list_runtime_manifests', lookup)
    return state


class TestProjectSkillCatalog:
    def test_an_entry_carries_its_versions_manifest_and_a_file_less_one_carries_none(self, project_skills, platform,
                                                                                     manifests):
        manifests.result = {101: [FILE]}

        catalog = _catalog._private_catalog(project_skills, platform, [
            _catalog._row(1, 'forms', 101), _catalog._row(2, 'plain', 102),
        ])

        assert manifests.requested == [[101, 102]]
        assert [entry.get('files') for entry in catalog] == [[FILE], None]

    def test_an_unreadable_manifest_keeps_the_catalog_without_files(self, project_skills, platform, manifests):
        manifests.error = RuntimeError('table missing')

        catalog = _catalog._private_catalog(project_skills, platform, [_catalog._row(1, 'forms', 101)])

        assert [entry['name'] for entry in catalog] == ['forms'] and 'files' not in catalog[0]

    def test_an_empty_catalog_asks_for_no_manifest(self, project_skills, platform, manifests):
        assert _catalog._private_catalog(project_skills, platform, []) == []
        assert manifests.requested == []


class TestSkillParticipantCandidate:
    def _target(self, env, files):
        run_utils = sys.modules[f'{PACKAGE}.utils.skill_run_utils']
        version = _participants.FakeVersion(100, instructions='Fill the form.')
        version.files = files
        return run_utils.build_skill_run_target(_participants.FakeSkill(10, 'forms', [version]), 100, False)

    def test_the_candidate_lists_the_pinned_versions_files_without_hash(self, env):
        participant_utils = sys.modules[f'{PACKAGE}.utils.skill_participant_utils']
        version_file = types.SimpleNamespace(sha256='a' * 64, **FILE)

        candidate = participant_utils.mention_candidate(self._target(env, [version_file]), 1)

        assert candidate['files'] == [FILE] and candidate['skill_project_id'] == 1

    def test_a_file_less_version_adds_no_files_key(self, env):
        participant_utils = sys.modules[f'{PACKAGE}.utils.skill_participant_utils']

        assert 'files' not in participant_utils.mention_candidate(self._target(env, []), 1)


def _attached_skill_model():
    """The real ``AttachedSkillModel`` from models/pd/version.py, compiled alone so it loads without the plugin."""
    source = pathlib.Path(__file__).parents[3].joinpath('models', 'pd', 'version.py').read_text()
    node = next(node for node in ast.parse(source).body
                if isinstance(node, ast.ClassDef) and node.name == 'AttachedSkillModel')
    namespace = {'BaseModel': BaseModel, 'ConfigDict': ConfigDict, 'Optional': Optional}
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'version.py', 'exec'), namespace)
    return namespace['AttachedSkillModel']


class TestAgentAttachedSkillPayload:
    def test_an_attached_skill_keeps_its_files_through_the_agent_details_model(self):
        dumped = _attached_skill_model().model_validate({'skill_id': 3, 'files': [FILE]}).model_dump(mode='json')

        assert dumped['files'] == [FILE]

    def test_a_file_less_attached_skill_dumps_no_files_key(self):
        assert 'files' not in _attached_skill_model().model_validate({'skill_id': 4}).model_dump(mode='json')
