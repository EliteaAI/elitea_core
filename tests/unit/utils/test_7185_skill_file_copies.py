"""#7185 option A: every server path that copies a skill version carries its files into the right project's bucket.

Runs the real publish (``utils/skill_publish_utils.py``), the import RPC (``rpc/skill.py``), the agent fork export
(``utils/export_import.py``) and the agent publish twin (``utils/publish_utils.py``) over the ``skill_file_env``
fakes: one session per project schema and one bucket per project, so each test can say which bucket holds a file
after the copy.
"""
import base64
import contextlib
import hashlib
import pathlib
import sys
import types
from unittest import mock

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import (  # noqa: E402  pylint: disable=C0413
    PKG, AnyModule, FakeSkill, FakeSkillVersion, FakeSkillVersionFile, install, load, stub_everything_else,
)

SOURCE = 7
PUBLIC = 1
CONSUMER = 9
SKILL = 3
FILES = ((b'# Guide', 'references/guide.md'), (b'\x89PNG\x00', 'assets/logo.png'))


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _paths(session, version_id):
    return sorted(f.path for f in session.rows[FakeSkillVersionFile] if f.skill_version_id == version_id)


@pytest.fixture
def env(monkeypatch, isolated_sys_modules):
    loaded = install(monkeypatch, {})
    stub_everything_else(monkeypatch)
    return loaded


def _seed(env, project_id=SOURCE, version_id=10, files=FILES, **version):
    session = env.session_of(project_id)
    if not session.rows[FakeSkill]:
        session.add(FakeSkill(owner_id=project_id))
    session.add(FakeSkillVersion(id=version_id, **version))
    for data, path in files:
        env.files.write_version_file(project_id, SKILL, version_id, path, data, session=session)
    return session


@pytest.fixture
def publish(env, monkeypatch):
    publish_utils = AnyModule(f'{PKG}.utils.publish_utils')
    publish_utils.BaseChecker = object
    publish_utils.generate_validation_token = lambda version_id, content_hash, secret: f'{version_id}:{content_hash}'
    publish_utils.verify_validation_token = lambda token, version_id, content_hash, secret, ttl: (
        token == f'{version_id}:{content_hash}', 'token does not match')
    publish_utils.DEFAULT_VALIDATION_TOKEN_TTL = 60
    monkeypatch.setitem(sys.modules, publish_utils.__name__, publish_utils)
    publish_models = types.ModuleType(f'{PKG}.models.pd.publish')
    publish_models.VERSION_NAME_PATTERN = r'.+'
    monkeypatch.setitem(sys.modules, publish_models.__name__, publish_models)
    env.tools.this.module = types.SimpleNamespace(_publish_validation_secret='secret')
    module = load(monkeypatch, 'utils/skill_publish_utils.py', f'{PKG}.utils.skill_publish_utils')
    monkeypatch.setattr(module, 'SkillVersionCreateModel', lambda **fields: types.SimpleNamespace(**fields))
    return module


def _published_twin(env, publish):
    source = _seed(env)
    clone_id = publish._clone_source_version_for_publish(SOURCE, SKILL, 10, 'v1', 1)
    snapshot = publish.create_skill_publish_snapshot(SOURCE, SKILL, clone_id, 1)
    result = publish.publish_skill_first_version(PUBLIC, snapshot, 'v1', 1, snapshot['source'])
    return source, clone_id, snapshot, result


class TestPublish:
    def test_the_source_clone_gets_the_rows_and_no_new_object(self, env, publish):
        source = _seed(env)
        objects_before = dict(env.storage.objects)

        clone_id = publish._clone_source_version_for_publish(SOURCE, SKILL, 10, 'v1', 1)

        assert clone_id != 10 and _paths(source, clone_id) == _paths(source, 10)
        assert env.storage.objects == objects_before

    def test_the_snapshot_carries_the_manifest(self, env, publish):
        _seed(env)

        snapshot = publish.create_skill_publish_snapshot(SOURCE, SKILL, 10, 1)

        assert [(f['path'], f['sha256']) for f in snapshot['version']['files']] == [
            ('assets/logo.png', _sha(b'\x89PNG\x00')), ('references/guide.md', _sha(b'# Guide'))]

    def test_the_public_twin_lists_the_files_and_its_project_holds_the_bytes(self, env, publish):
        _, _, _, result = _published_twin(env, publish)

        public = env.session_of(PUBLIC)
        assert _paths(public, result['version_id']) == ['assets/logo.png', 'references/guide.md']
        assert env.storage.keys(PUBLIC) == sorted([_sha(b'# Guide'), _sha(b'\x89PNG\x00')])
        assert env.storage.objects[(f'p--{PUBLIC}.skill-files', _sha(b'# Guide'))] == b'# Guide'

    def test_an_additional_published_version_copies_into_the_existing_twin(self, env, publish):
        _, _, snapshot, result = _published_twin(env, publish)
        env.storage.objects = {key: value for key, value in env.storage.objects.items() if key[0] != f'p--{PUBLIC}.skill-files'}

        second = publish.publish_skill_additional_version(PUBLIC, result['skill_id'], snapshot, 'v2', 1, snapshot['source'])

        assert _paths(env.session_of(PUBLIC), second['version_id']) == ['assets/logo.png', 'references/guide.md']
        assert len(env.storage.keys(PUBLIC)) == 2

    def test_the_twin_keeps_its_files_when_the_source_project_loses_them(self, env, publish):
        source, clone_id, _, result = _published_twin(env, publish)
        for version_id in (10, clone_id):
            for path in _paths(source, version_id):
                source.rows[FakeSkillVersionFile].remove(
                    next(f for f in source.rows[FakeSkillVersionFile] if f.skill_version_id == version_id and f.path == path))
        env.files.sweep_skill_files(SOURCE)

        assert env.storage.keys(SOURCE) == []
        _, content = env.files.read_version_file(PUBLIC, result['skill_id'], result['version_id'], 'references/guide.md',
                                                 session=env.session_of(PUBLIC))
        assert content == b'# Guide'

    def test_unpublishing_releases_the_twins_objects_and_leaves_the_source_alone(self, env, publish):
        _, _, _, result = _published_twin(env, publish)
        public_twin = next(s for s in env.session_of(PUBLIC).rows[FakeSkill] if s.id == result['skill_id'])
        public_twin.shared_owner_id = SOURCE
        next(v for v in env.session_of(PUBLIC).rows[FakeSkillVersion] if v.id == result['version_id']).status = 'published'

        publish.delete_public_skill_version(PUBLIC, result['version_id'])

        assert env.storage.keys(PUBLIC) == []
        assert len(env.storage.keys(SOURCE)) == 2


    def test_deleting_the_twin_shell_releases_the_objects_of_its_remaining_drafts(self, env, publish):
        _, _, _, result = _published_twin(env, publish)
        public = env.session_of(PUBLIC)
        next(s for s in public.rows[FakeSkill] if s.id == result['skill_id']).shared_owner_id = SOURCE
        next(v for v in public.rows[FakeSkillVersion] if v.id == result['version_id']).status = 'published'
        public.add(FakeSkillVersion(id=77, skill_id=result['skill_id'], name='draft'))
        env.files.write_version_file(PUBLIC, result['skill_id'], 77, 'notes.md', b'draft only', session=public)
        assert _sha(b'draft only') in env.storage.keys(PUBLIC)

        publish.delete_public_skill_version(PUBLIC, result['version_id'])

        assert public.rows[FakeSkill] == [] and env.storage.keys(PUBLIC) == []


class TestPublishToken:
    def test_a_skill_without_files_keeps_its_existing_hash(self, publish):
        legacy = hashlib.sha256(b'{"description":"d","instructions":"i","name":"n"}').hexdigest()

        assert publish.compute_skill_content_hash('i', 'n', 'd') == legacy
        assert publish.compute_skill_content_hash('i', 'n', 'd', []) == legacy

    def test_the_hash_pins_each_path_and_its_content(self, publish):
        base = publish.compute_skill_content_hash('i', 'n', 'd', [{'path': 'a.md', 'sha256': 'x'}])

        assert base != publish.compute_skill_content_hash('i', 'n', 'd', [{'path': 'a.md', 'sha256': 'y'}])
        assert base != publish.compute_skill_content_hash('i', 'n', 'd', [{'path': 'b.md', 'sha256': 'x'}])
        assert base != publish.compute_skill_content_hash('i', 'n', 'd')

    def test_a_file_swapped_after_validation_invalidates_the_token(self, env, publish):
        session = _seed(env, instructions='Body.')
        manifest = env.files.list_version_manifest(session, 10)
        token = f"10:{publish.compute_skill_content_hash('Body.', 'forms', 'Fill forms.', manifest)}"
        assert publish.verify_skill_token_for_publish(SOURCE, 10, 1, token)[0] is True

        env.files.write_version_file(SOURCE, SKILL, 10, 'references/guide.md', b'# Swapped', session=session)

        assert publish.verify_skill_token_for_publish(SOURCE, 10, 1, token)[0] is False

    def test_the_token_validation_issues_for_a_version_with_files_passes_verification(self, env, publish,
                                                                                       monkeypatch):
        _seed(env, instructions='Body.')
        monkeypatch.setattr(publish, 'run_skill_deterministic_checks', lambda *args, **kwargs: {})
        monkeypatch.setattr(publish, 'run_skill_ai_validation', lambda *args, **kwargs: {})
        monkeypatch.setattr(publish, 'merge_skill_validation_results', lambda det, ai: {'status': 'PASS', 'counts': {}})

        token = publish.validate_skill_for_publish(SOURCE, SKILL, 10, 'base')['validation_token']

        assert publish.verify_skill_token_for_publish(SOURCE, 10, 1, token)[0] is True


@pytest.fixture
def skill_rpc(env, monkeypatch):
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.web = types.SimpleNamespace(rpc=lambda *names: (lambda function: function))
    pylon_tools.log = types.SimpleNamespace(error=lambda *args, **kwargs: None)
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', pylon_tools)
    rpc_package = types.ModuleType(f'{PKG}.rpc')
    rpc_package.__path__ = []
    monkeypatch.setitem(sys.modules, rpc_package.__name__, rpc_package)
    module = load(monkeypatch, 'rpc/skill.py', f'{PKG}.rpc.skill')
    module.created = []

    def import_skill(project_id, versions, session, **kwargs):
        reused = kwargs['name'] == 'reused'
        version_ids = {}
        for index, version in enumerate(versions):
            created = FakeSkillVersion(id=500 + index, skill_id=50, name=version.get('name', 'base'))
            session.add(created)
            version_ids[created.name] = created.id
        module.created.append(project_id)
        return types.SimpleNamespace(id=50, versions=version_ids, reused=reused)

    monkeypatch.setattr(module, 'import_skill', import_skill)
    return module


def _fork_payload(manifest, name='forms', owner_id=SOURCE):
    return {'name': name, 'owner_id': owner_id, 'versions': [
        {'name': 'base', 'id': 10, 'instructions': 'Body.', 'files': manifest},
    ]}


class TestImportRpc:
    def test_a_fork_copies_the_source_bucket_into_the_target_and_lists_the_rows(self, env, skill_rpc):
        manifest = env.files.list_version_manifest(_seed(env), 10)

        result, errors = skill_rpc.RPC.applications_import_skill(None, _fork_payload(manifest), CONSUMER, 1)

        target = env.session_of(CONSUMER)
        assert errors == [] and result['versions'] == {'base': 500}
        assert _paths(target, 500) == ['assets/logo.png', 'references/guide.md']
        assert env.storage.keys(CONSUMER) == sorted([_sha(b'# Guide'), _sha(b'\x89PNG\x00')])
        assert target.commits == 1

    def test_a_reused_fork_gets_no_second_copy(self, env, skill_rpc):
        manifest = env.files.list_version_manifest(_seed(env), 10)

        skill_rpc.RPC.applications_import_skill(None, _fork_payload(manifest, name='reused'), CONSUMER, 1)

        assert env.session_of(CONSUMER).rows[FakeSkillVersionFile] == [] and env.storage.keys(CONSUMER) == []

    def test_a_payload_naming_content_the_source_version_lacks_imports_nothing(self, env, skill_rpc):
        _seed(env)
        forged = [{'path': 'secret.md', 'sha256': _sha(b'secret')}]

        result, errors = skill_rpc.RPC.applications_import_skill(None, _fork_payload(forged), CONSUMER, 1)

        assert result is None and "'secret.md' is not a file of skill version 10" in errors[0]
        assert env.session_of(CONSUMER).commits == 0 and env.storage.keys(CONSUMER) == []

    def test_an_exported_file_with_its_bytes_needs_no_source(self, env, skill_rpc):
        payload = _fork_payload([{'path': 'n.md', 'content_base64': base64.b64encode(b'n').decode()}], owner_id=404)

        result, errors = skill_rpc.RPC.applications_import_skill(None, payload, CONSUMER, 1)

        assert errors == [] and _paths(env.session_of(CONSUMER), 500) == ['n.md']
        assert env.storage.keys(CONSUMER) == [_sha(b'n')]


class ExportModel:
    @classmethod
    def model_validate(cls, data):
        return types.SimpleNamespace(model_dump=lambda mode=None: {
            'name': data['name'], 'import_uuid': f"uuid-{data['id']}-0000",
            'versions': [{'name': v['name'], 'instructions': 'Body.'} for v in data['versions']],
        })


@pytest.fixture
def agent_export(env, monkeypatch):
    module = load(monkeypatch, 'utils/export_import.py', f'{PKG}.utils.export_import')
    monkeypatch.setattr(module, 'SkillExportModel', ExportModel)
    monkeypatch.setattr(FakeSkill, 'to_json', lambda self: {'id': self.id, 'name': self.name}, raising=False)
    monkeypatch.setattr(FakeSkillVersion, 'to_json', lambda self: {'name': self.name}, raising=False)
    return module


def _export(agent_export, forked=False, skill_id=SKILL):
    applications = [{'versions': [{'name': 'base', 'skills': [{'skill_id': skill_id, 'version_name': 'base'}]}]}]
    result = [{'versions': [{'name': 'base', 'import_version_uuid': 'agent-v'}]}]
    return agent_export._export_skills_main(SOURCE, 1, applications, result, forked=forked)


class TestAgentExport:
    def test_a_fork_payload_lists_the_files_without_reading_their_bytes(self, env, agent_export):
        _seed(env)
        env.storage.broken_reads = True

        [skill] = _export(agent_export, forked=True)

        assert [set(f) for f in skill['versions'][0]['files']] == [
            {'path', 'size', 'sha256', 'content_type', 'is_text'}] * 2
        assert skill['owner_id'] == SOURCE and skill['versions'][0]['id'] == 10

    @pytest.mark.parametrize('forked', [True, False])
    def test_an_agent_export_tells_the_skill_export_whether_it_is_a_fork(self, agent_export, monkeypatch, forked):
        asked = []
        applications = types.SimpleNamespace(options=lambda *a: types.SimpleNamespace(all=lambda: []))
        session = types.SimpleNamespace(query=lambda model: types.SimpleNamespace(filter=lambda *a: applications))
        monkeypatch.setattr(agent_export, 'db', types.SimpleNamespace(
            get_session=lambda project_id: contextlib.nullcontext(session)))
        monkeypatch.setattr(agent_export, '_export_skills_main', lambda **kwargs: asked.append(kwargs['forked']) or [])
        monkeypatch.setattr(agent_export, '_export_compound_application_tools', lambda res, **kwargs: res)
        monkeypatch.setattr(agent_export, 'serialize', lambda res: res)

        agent_export._export_application_main(SOURCE, 1, [], forked, {})

        assert asked == [forked]


@pytest.fixture
def agent_publish(env, monkeypatch):
    return load(monkeypatch, 'utils/publish_utils.py', f'{PKG}.utils.publish_utils')


class TestAgentPublishTwin:
    def test_a_skill_without_files_keeps_its_existing_fingerprint(self, agent_publish):
        assert agent_publish._skill_content_sha('Do it.') == hashlib.sha256(b'Do it.').hexdigest()
        assert agent_publish._skill_content_sha('Do it.', None, None, []) == hashlib.sha256(b'Do it.').hexdigest()

    def test_the_fingerprint_pins_each_file_path_and_content_in_any_order(self, agent_publish):
        first = agent_publish._skill_content_sha('Do it.', None, None, [['a.md', 'x'], ['b.md', 'y']])

        assert first == agent_publish._skill_content_sha('Do it.', None, None, [['b.md', 'y'], ['a.md', 'x']])
        assert first != agent_publish._skill_content_sha('Do it.', None, None, [['a.md', 'x'], ['b.md', 'z']])
        assert first != agent_publish._skill_content_sha('Do it.')

    def test_the_twin_lookup_fingerprints_the_file_hashes(self, agent_publish, monkeypatch):
        fingerprinted = []
        twin = types.SimpleNamespace(order_by=lambda *a: types.SimpleNamespace(first=lambda: (8, 80)))
        session = types.SimpleNamespace(query=lambda *columns: types.SimpleNamespace(filter=lambda *a: twin))

        @contextlib.contextmanager
        def get_session(project_id):
            yield session

        monkeypatch.setattr(agent_publish, 'db', types.SimpleNamespace(get_session=get_session))
        monkeypatch.setattr(agent_publish, 'SkillVersion', mock.MagicMock())
        monkeypatch.setattr(agent_publish, '_skill_content_sha', lambda *args: fingerprinted.append(args) or 'sha')
        skill = {'skill_id': 7, 'skill_version_id': 70, 'instructions': 'Do it.', 'run_settings': None,
                 'frontmatter': None, 'file_hashes': [['a.md', 'x']]}

        assert agent_publish._resolve_or_fork_skill_twin(SOURCE, PUBLIC, skill, 1) == (8, 80)
        assert fingerprinted == [('Do it.', None, None, [['a.md', 'x']])]

    def test_publishing_reads_each_attached_versions_file_hashes(self, env, agent_publish, monkeypatch):
        version_file = types.SimpleNamespace(path='a.md', sha256='x' * 64)
        mapping = types.SimpleNamespace(skill_version=types.SimpleNamespace(run_settings=None, meta={}, files=[version_file]))
        session = types.SimpleNamespace(query=lambda model: types.SimpleNamespace(
            filter=lambda *a: types.SimpleNamespace(all=lambda: [mapping])))

        @contextlib.contextmanager
        def get_session(project_id):
            yield session

        resolved = []
        monkeypatch.setattr(agent_publish, 'db', types.SimpleNamespace(get_session=get_session))
        monkeypatch.setattr(agent_publish, 'build_skill_mappings_list',
                            lambda mappings: [{'skill_id': 7, 'skill_version_id': 70, 'name': 'forms'}])
        monkeypatch.setattr(agent_publish, '_resolve_or_fork_skill_twin',
                            lambda source, public, skill, user: resolved.append(skill) or (8, 80))
        monkeypatch.setattr(agent_publish, 'attach_skill_to_public_copy', lambda **kwargs: None)

        assert agent_publish.publish_attached_skills(SOURCE, PUBLIC, 20, 10, 1) is None
        assert resolved[0]['file_hashes'] == [['a.md', 'x' * 64]]
