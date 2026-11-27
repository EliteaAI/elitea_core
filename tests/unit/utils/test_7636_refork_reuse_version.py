"""Issue #7636 - an existing fork is reused only when it holds every source version the import links.

Forking agent A (skill S at v1.2) and then agent B (S at v1.3) into one project reused A's copy of S, which has no
v1.3; the attach fell back to 'base' and B ran on v1.2's instructions and files without anyone being told.

Runs the real ``import_skill`` over the in-memory session of the #7185 harness, so the project, lineage and source
version filters of the reuse lookup are evaluated against seeded rows.
"""
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import FakeSkill, FakeSkillVersion, install  # noqa: E402  pylint: disable=C0413

PROJECT = 7
SOURCE_PROJECT = 1
SOURCE_SKILL = 5
NEW_SKILL = 999


@pytest.fixture
def env(monkeypatch, isolated_sys_modules):
    env = install(monkeypatch, {})
    monkeypatch.setattr(env.utils, 'SkillImportResultModel', types.SimpleNamespace)
    monkeypatch.setattr(
        env.utils, 'create_skill',
        lambda model, session, project_id, **kwargs: types.SimpleNamespace(id=NEW_SKILL, versions=[]),
    )
    return env


@pytest.fixture
def session(env):
    return env.session_of(PROJECT)


def make_lineage(parent_version_id=None, source_skill=SOURCE_SKILL):
    meta = {'parent_project_id': SOURCE_PROJECT, 'parent_entity_id': source_skill}
    if parent_version_id is not None:
        meta['parent_version_id'] = parent_version_id
    return meta


def seed_fork(session, skill_id, versions, owner_id=PROJECT, source_skill=SOURCE_SKILL):
    session.add(FakeSkill(id=skill_id, owner_id=owner_id))
    for version_id, name, parent_version_id in versions:
        session.add(FakeSkillVersion(
            id=version_id, skill_id=skill_id, name=name, meta=make_lineage(parent_version_id, source_skill),
        ))


def import_versions(env, session, *versions):
    payload = [{'name': name, 'meta': make_lineage(parent_version_id)} for name, parent_version_id in versions]
    return env.utils.import_skill(
        project_id=PROJECT, name='pdf', description='PDF.', versions=payload, author_id=1, session=session,
    )


class TestForkReuse:
    def test_a_fork_lacking_the_linked_source_version_gets_a_new_copy(self, env, session):
        seed_fork(session, 213, [(900, 'base', 818)])

        result = import_versions(env, session, ('base', 819))

        assert (result.id, result.reused) == (NEW_SKILL, False)

    def test_a_fork_holding_the_linked_source_version_is_reused_under_the_imported_name(self, env, session):
        seed_fork(session, 213, [(900, 'base', 818)])

        result = import_versions(env, session, ('base', 818))

        assert (result.id, result.versions, result.reused) == (213, {'base': 900}, True)

    def test_only_a_fork_holding_all_linked_versions_is_reused(self, env, session):
        seed_fork(session, 213, [(900, 'base', 818)])
        seed_fork(session, 220, [(950, 'base', 817), (951, 'v1.2', 818)])

        result = import_versions(env, session, ('base', 817), ('v1.2', 818))

        assert (result.id, result.versions) == (220, {'base': 950, 'v1.2': 951})

    @pytest.mark.parametrize('elsewhere', [{'owner_id': 8}, {'source_skill': 6}], ids=['other project', 'other source'])
    def test_a_copy_of_the_version_outside_this_projects_forks_of_the_source_is_not_reused(
        self, env, session, elsewhere,
    ):
        seed_fork(session, 213, [(900, 'base', 818)])
        seed_fork(session, 230, [(960, 'base', 819)], **elsewhere)

        result = import_versions(env, session, ('base', 819))

        assert (result.id, result.reused) == (NEW_SKILL, False)

    def test_versions_without_a_source_version_id_reuse_on_lineage_alone(self, env, session):
        seed_fork(session, 213, [(900, 'base', 818)])

        result = import_versions(env, session, ('base', None))

        assert (result.id, result.versions, result.reused) == (213, {'base': 900}, True)
