"""#7185 option A: a skill version's files travel through the fork payload.

Runs the real ``utils/skill_export_import.py`` over the in-memory session and bucket fakes of ``skill_file_env``:
the fork payload lists the manifest and names the source project.
"""
import hashlib
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import FakeSkill, FakeSkillVersion, install  # noqa: E402  pylint: disable=C0413

PROJECT = 7
SKILL = 3


@pytest.fixture
def limits():
    return {}


@pytest.fixture
def env(monkeypatch, limits, isolated_sys_modules):
    return install(monkeypatch, limits)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _seed_skill(env, files=((b'# Guide', 'references/guide.md'), (b'\x89PNG\x00', 'assets/logo.png'))):
    session = env.session_of(PROJECT)
    session.add(FakeSkill())
    session.add(FakeSkillVersion(id=10))
    for data, path in files:
        env.files.write_version_file(PROJECT, SKILL, 10, path, data, session=session)
    return session


class ExportModel:
    @classmethod
    def model_validate(cls, data):
        return types.SimpleNamespace(model_dump=lambda mode=None: {
            'name': data['name'], 'versions': [{'name': v['name']} for v in data['versions']],
        })


class TestForkPayload:
    def _payload(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'SkillExportModel', ExportModel)
        FakeSkill.to_json = lambda self: {'name': self.name}
        FakeSkill.author_id = 1
        FakeSkill.get_default_version = lambda self: self.versions[0]
        FakeSkillVersion.to_json = lambda self: {'name': self.name}
        try:
            return env.export.build_skill_fork_payload(PROJECT, SKILL, 10, session=env.session_of(PROJECT))
        finally:
            for attribute in ('to_json', 'author_id', 'get_default_version'):
                delattr(FakeSkill, attribute)
            del FakeSkillVersion.to_json

    def test_the_payload_lists_the_manifest_and_names_the_source_project(self, env, monkeypatch):
        _seed_skill(env)

        payload = self._payload(env, monkeypatch)

        assert payload['owner_id'] == PROJECT and payload['versions'][0]['id'] == 10
        assert payload['versions'][0]['files'] == [
            {'path': 'assets/logo.png', 'size': 5, 'sha256': _sha(b'\x89PNG\x00'), 'content_type': 'image/png',
             'is_text': False},
            {'path': 'references/guide.md', 'size': 7, 'sha256': _sha(b'# Guide'), 'content_type': 'text/markdown',
             'is_text': True},
        ]

    def test_a_version_without_files_gets_no_files_key(self, env, monkeypatch):
        _seed_skill(env, files=())

        assert 'files' not in self._payload(env, monkeypatch)['versions'][0]
