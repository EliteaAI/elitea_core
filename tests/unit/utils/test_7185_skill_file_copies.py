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
from werkzeug.datastructures import MultiDict

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
    @pytest.mark.parametrize('additional', [False, True])
    def test_a_publish_moves_every_byte_before_it_opens_the_public_transaction(self, env, publish, monkeypatch,
                                                                               additional):
        _, _, snapshot, result = _published_twin(env, publish)
        env.storage.objects = {key: value for key, value in env.storage.objects.items()
                               if key[0] != f'p--{PUBLIC}.skill-files'}
        events = []
        upload = env.store.MinioClient.upload_file
        monkeypatch.setattr(env.store.MinioClient, 'upload_file', lambda client, bucket, data, name: events.append(
            'upload') or upload(client, bucket, data, name))
        get_session = publish.db.get_session

        @contextlib.contextmanager
        def recording_session(project_id):
            events.append(('transaction', project_id))
            with get_session(project_id) as session:
                yield session

        monkeypatch.setattr(publish.db, 'get_session', recording_session)

        if additional:
            publish.publish_skill_additional_version(PUBLIC, result['skill_id'], snapshot, 'v2', 1, snapshot['source'])
        else:
            publish.publish_skill_first_version(PUBLIC, snapshot, 'v1', 1, snapshot['source'])

        assert events[:3] == ['upload', 'upload', ('transaction', PUBLIC)] and events.count('upload') == 2

    @pytest.mark.parametrize('additional', [False, True])
    def test_a_publish_restores_an_object_lost_after_staging_from_the_staged_bytes_not_the_source(
            self, env, publish, monkeypatch, additional):
        _, _, snapshot, result = _published_twin(env, publish)
        env.storage.objects = {key: value for key, value in env.storage.objects.items()
                               if key[0] != f'p--{PUBLIC}.skill-files'}
        rows_phase_reads = []
        download = env.store.MinioClient.download_file
        add_version_files = publish.add_version_files

        def lose_an_object_then_add(session, project_id, *args):
            del env.storage.objects[(f'p--{PUBLIC}.skill-files', _sha(b'# Guide'))]
            monkeypatch.setattr(env.store.MinioClient, 'download_file', lambda client, bucket, name, project_id=None: (
                rows_phase_reads.append((client.project_id, name)) or download(client, bucket, name)))
            return add_version_files(session, project_id, *args)

        monkeypatch.setattr(publish, 'add_version_files', lose_an_object_then_add)

        if additional:
            publish.publish_skill_additional_version(PUBLIC, result['skill_id'], snapshot, 'v2', 1, snapshot['source'])
        else:
            publish.publish_skill_first_version(PUBLIC, snapshot, 'v1', 1, snapshot['source'])

        assert env.storage.objects[(f'p--{PUBLIC}.skill-files', _sha(b'# Guide'))] == b'# Guide'
        assert rows_phase_reads == []

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

    def test_a_reused_fork_gets_no_rows_and_no_bytes_its_project_already_holds(self, env, skill_rpc, monkeypatch):
        manifest = env.files.list_version_manifest(_seed(env), 10)
        env.store.put_contents(CONSUMER, {_sha(b'# Guide'): b'# Guide', _sha(b'\x89PNG\x00'): b'\x89PNG\x00'})
        uploads = []
        monkeypatch.setattr(env.store.MinioClient, 'upload_file', lambda *args: uploads.append(args))

        skill_rpc.RPC.applications_import_skill(None, _fork_payload(manifest, name='reused'), CONSUMER, 1)

        assert env.session_of(CONSUMER).rows[FakeSkillVersionFile] == [] and uploads == []

    def test_a_fork_moves_every_byte_before_it_opens_the_targets_transaction(self, env, skill_rpc, monkeypatch):
        manifest = env.files.list_version_manifest(_seed(env), 10)
        events = []
        upload = env.store.MinioClient.upload_file
        monkeypatch.setattr(env.store.MinioClient, 'upload_file', lambda client, bucket, data, name: events.append(
            ('upload', client.project_id)) or upload(client, bucket, data, name))
        get_session = skill_rpc.db.get_session

        @contextlib.contextmanager
        def recording_session(project_id):
            events.append(('transaction', project_id))
            with get_session(project_id) as session:
                yield session

        monkeypatch.setattr(skill_rpc.db, 'get_session', recording_session)

        skill_rpc.RPC.applications_import_skill(None, _fork_payload(manifest), CONSUMER, 1)

        target_events = [event for event in events if event != ('transaction', SOURCE)]
        assert target_events == [('upload', CONSUMER), ('upload', CONSUMER), ('transaction', CONSUMER)]

    def test_a_source_object_gone_meanwhile_fails_the_import_naming_the_path(self, env, skill_rpc):
        manifest = env.files.list_version_manifest(_seed(env), 10)
        del env.storage.objects[(f'p--{SOURCE}.skill-files', _sha(b'# Guide'))]

        result, errors = skill_rpc.RPC.applications_import_skill(None, _fork_payload(manifest), CONSUMER, 1)

        assert result is None and "'references/guide.md' could not be copied" in errors[0]
        assert env.session_of(CONSUMER).commits == 0

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


OWNER = 1
OUTSIDER = 66


class TestCopyAuthorization:
    """A payload names its source project, version and hashes itself, so the import reads the source rows only for an
    acting user who could read them through the file endpoint; anything else is refused exactly like a pair the
    source does not hold."""

    def _fork(self, env, skill_rpc, actor, version_id=10, owner_id=SOURCE, target=CONSUMER):
        manifest = env.files.list_version_manifest(env.session_of(owner_id), 10)
        payload = _fork_payload(manifest, owner_id=owner_id)
        payload['versions'][0]['id'] = version_id
        env.storage.clients.clear()
        return skill_rpc.RPC.applications_import_skill(None, payload, target, actor)

    def _refused(self, env, result, errors):
        assert result is None
        assert errors == ["Skill import failed: 'assets/logo.png' is not a file of skill version 10 in project 7"]
        assert env.session_of(CONSUMER).commits == 0 and env.storage.keys(CONSUMER) == []
        assert SOURCE not in env.storage.clients

    def test_an_actor_outside_the_source_project_gets_nothing(self, env, skill_rpc):
        _seed(env)
        env.access.non_members.add((OUTSIDER, SOURCE))

        self._refused(env, *self._fork(env, skill_rpc, OUTSIDER))
        assert env.access.permission_lookups == [(OUTSIDER, 'default', SOURCE)]

    def test_a_member_removed_from_the_source_project_gets_nothing(self, env, skill_rpc):
        _seed(env)
        env.access.non_members.add((OWNER, SOURCE))

        self._refused(env, *self._fork(env, skill_rpc, OWNER))

    def test_an_actor_without_access_to_the_skills_folder_gets_nothing(self, env, skill_rpc):
        _seed(env)
        env.access.restricted.add((OWNER, SOURCE, SKILL))

        self._refused(env, *self._fork(env, skill_rpc, OWNER))

    def test_a_forbidden_source_answers_like_a_missing_version_or_a_hash_the_source_lacks(self, env, skill_rpc):
        manifest = env.files.list_version_manifest(_seed(env), 10)
        env.access.non_members.add((OUTSIDER, SOURCE))
        forged = [{**manifest[0], 'sha256': _sha(b'guess')}]

        _, forbidden = self._fork(env, skill_rpc, OUTSIDER)
        _, missing = self._fork(env, skill_rpc, OWNER, version_id=999)
        _, lacking = skill_rpc.RPC.applications_import_skill(None, _fork_payload(forged), CONSUMER, OWNER)

        assert forbidden == lacking == [m.replace('999', '10') for m in missing]

    def test_the_owner_copies(self, env, skill_rpc):
        _seed(env)

        result, errors = self._fork(env, skill_rpc, OWNER)

        assert errors == [] and _paths(env.session_of(CONSUMER), 500) == ['assets/logo.png', 'references/guide.md']

    def test_a_catalog_attach_copies_from_the_public_project_for_anyone(self, env, skill_rpc):
        _seed(env, project_id=PUBLIC)
        env.access.non_members.add((OUTSIDER, PUBLIC))

        result, errors = self._fork(env, skill_rpc, OUTSIDER, owner_id=PUBLIC)

        assert errors == [] and len(_paths(env.session_of(CONSUMER), 500)) == 2

    def test_an_agent_publish_twin_copies_from_the_publishers_project_into_the_public_one(self, env, skill_rpc):
        _seed(env)

        result, errors = self._fork(env, skill_rpc, OWNER, target=PUBLIC)

        assert errors == [] and len(_paths(env.session_of(PUBLIC), 500)) == 2
        assert len(env.storage.keys(PUBLIC)) == 2

    def test_a_skill_publish_copies_from_its_server_built_snapshot_without_an_actor_check(self, env, publish):
        env.access.non_members.add((OWNER, SOURCE))

        _, _, _, result = _published_twin(env, publish)

        assert len(_paths(env.session_of(PUBLIC), result['version_id'])) == 2
        assert env.access.permission_lookups == []


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

    def test_an_export_file_carries_each_files_bytes(self, env, agent_export):
        _seed(env)

        [skill] = _export(agent_export, forked=False)

        files = skill['versions'][0]['files']
        assert [(f['path'], base64.b64decode(f['content_base64'])) for f in files] == [
            ('assets/logo.png', b'\x89PNG\x00'), ('references/guide.md', b'# Guide')]
        assert files[0]['sha256'] == _sha(b'\x89PNG\x00')

    def test_a_skill_without_files_exports_exactly_as_before(self, env, agent_export):
        _seed(env, files=())

        [skill] = _export(agent_export, forked=False)

        assert skill['versions'][0] == {'name': 'base', 'instructions': 'Body.', 'id': 10, 'author_id': 1}

    def test_an_export_reads_every_file_through_one_storage_client(self, env, agent_export):
        _seed(env)
        env.storage.clients.clear()

        _export(agent_export, forked=False)

        assert env.storage.clients == [SOURCE]

    def test_an_unreadable_file_fails_the_export_naming_the_skill_version_and_path(self, env, agent_export):
        _seed(env)
        env.storage.broken_reads = True

        with pytest.raises(env.files.SkillVersionExportUnavailableError,
                           match="Skill 'forms' version 'base' could not be exported: the stored content of "
                                 "'assets/logo.png' could not be read") as info:
            _export(agent_export, forked=False)
        assert info.value.http_status == 503

    def test_bytes_that_do_not_match_their_hash_fail_the_export_too(self, env, agent_export):
        _seed(env)
        env.storage.objects[(f'p--{SOURCE}.skill-files', _sha(b'# Guide'))] = b'# Tampered'

        with pytest.raises(env.files.SkillVersionExportUnavailableError, match="'references/guide.md'"):
            _export(agent_export, forked=False)

    def test_the_markdown_zip_puts_the_bytes_under_the_skill_folder_and_the_block_lists_them(self, env, agent_export):
        _seed(env)
        skills = _export(agent_export)

        members = agent_export.assign_skill_file_folders(skills)
        [block] = agent_export._extract_skills_for_md({'skills': [{'import_uuid': skills[0]['import_uuid'],
                                                                   'version_name': 'base'}]}, skills)

        assert [(m['filename'], m['content']) for m in members] == [
            ('skills/forms/assets/logo.png', b'\x89PNG\x00'), ('skills/forms/references/guide.md', b'# Guide')]
        assert block['files_folder'] == 'skills/forms'
        assert [set(f) for f in block['files']] == [{'path', 'size', 'sha256', 'content_type', 'is_text'}] * 2

    def test_two_exported_skills_of_one_name_get_separate_folders(self, agent_export):
        version = {'name': 'base', 'files': [{'path': 'a.md', 'content_base64': base64.b64encode(b'a').decode()}]}
        skills = [{'name': 'forms', 'import_uuid': 'aaaaaaaa-1', 'versions': [dict(version)]},
                  {'name': 'forms', 'import_uuid': 'bbbbbbbb-2', 'versions': [dict(version)]},
                  {'name': 'forms', 'import_uuid': 'cccccccc-3', 'versions': [{**version, 'name': 'v2'}]}]

        members = agent_export.assign_skill_file_folders(skills)

        assert [m['filename'] for m in members] == [
            'skills/forms/a.md', 'skills/forms-bbbbbbbb/a.md', 'skills/forms.v2/a.md']

    def test_an_agent_whose_skills_have_no_files_adds_no_member(self, agent_export):
        skills = [{'name': 'forms', 'import_uuid': 'u', 'versions': [{'name': 'base', 'instructions': 'x'}]}]

        assert agent_export.assign_skill_file_folders(skills) == []
        assert 'files_folder' not in skills[0]['versions'][0]

    def _agent_export_with_skills(self, agent_export, monkeypatch, skills_export):
        applications = types.SimpleNamespace(options=lambda *a: types.SimpleNamespace(all=lambda: []))
        session = types.SimpleNamespace(query=lambda model: types.SimpleNamespace(filter=lambda *a: applications))
        monkeypatch.setattr(agent_export, 'db', types.SimpleNamespace(
            get_session=lambda project_id: contextlib.nullcontext(session)))
        monkeypatch.setattr(agent_export, '_export_skills_main', skills_export)
        monkeypatch.setattr(agent_export, '_export_compound_application_tools', lambda res, **kwargs: res)
        monkeypatch.setattr(agent_export, 'serialize', lambda res: res)

    def test_an_unreadable_file_fails_the_agent_export_instead_of_dropping_every_skill(self, env, agent_export,
                                                                                     monkeypatch):
        def unreadable(**kwargs):
            raise env.files.SkillVersionExportUnavailableError('forms', 'base', 'assets/logo.png')

        self._agent_export_with_skills(agent_export, monkeypatch, unreadable)

        with pytest.raises(env.files.SkillVersionExportUnavailableError):
            agent_export.export_application(SOURCE, 1, [], forked=False)

    def test_an_unreadable_file_of_a_nested_agents_skill_fails_the_export_too(self, env, agent_export, monkeypatch):
        def unreadable(res, **kwargs):
            raise env.files.SkillVersionExportUnavailableError('forms', 'base', 'assets/logo.png')

        self._agent_export_with_skills(agent_export, monkeypatch, lambda **kwargs: [])
        monkeypatch.setattr(agent_export, '_export_compound_application_tools', unreadable)

        with pytest.raises(env.files.SkillVersionExportUnavailableError):
            agent_export.export_application(SOURCE, 1, [], forked=False)

    def test_any_other_skill_export_error_still_becomes_a_warning(self, agent_export, monkeypatch):
        def broken(**kwargs):
            raise ValueError('boom')

        self._agent_export_with_skills(agent_export, monkeypatch, broken)

        result = agent_export._export_application_main(SOURCE, 1, [], False, {})

        assert result['warnings'] == ['Skill export failed: boom'] and 'skills' not in result

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


def _agent_json_export(skill_files=()):
    skill_version = {'name': 'base', 'instructions': 'Read references/guide.md first.'}
    if skill_files:
        skill_version['files'] = [
            {'path': path, 'size': len(data), 'sha256': _sha(data), 'content_type': 'text/markdown', 'is_text': True,
             'content_base64': base64.b64encode(data).decode()}
            for data, path in skill_files
        ]
    return {
        'ok': True,
        'applications': [{'id': 5, 'name': 'Helper', 'description': 'Helps.', 'versions': [{
            'name': 'base', 'agent_type': 'react', 'instructions': 'Help.', 'llm_settings': {'model_name': 'gpt-x'},
            'meta': {}, 'tools': [], 'skills': [{'import_uuid': 'uuid-3-0000', 'version_name': 'base'}],
        }]}],
        'toolkits': [],
        'skills': [{'name': 'forms', 'description': 'Fill forms.', 'import_uuid': 'uuid-3-0000',
                    'versions': [skill_version]}],
    }


class TestAgentMarkdownExport:
    def test_an_agent_whose_skill_has_files_exports_a_zip_with_the_bytes_under_the_skill_folder(
            self, agent_export, monkeypatch):
        monkeypatch.setattr(agent_export, 'export_application',
                            lambda **kwargs: _agent_json_export(skill_files=[(b'# Guide', 'references/guide.md')]))

        result = agent_export.export_application_md(SOURCE, 1, [5])

        assert result['has_dependencies'] is True
        assert {'filename': 'skills/forms/references/guide.md', 'content': b'# Guide'} in result['files']
        [agent_md] = [f for f in result['files'] if f['filename'] == 'helper.agent.md']
        assert '  files_folder: skills/forms\n' in agent_md['content']
        assert 'content_base64' not in agent_md['content']

    def test_an_agent_whose_skills_have_no_files_exports_the_markdown_it_exported_before(
            self, agent_export, monkeypatch):
        monkeypatch.setattr(agent_export, 'export_application', lambda **kwargs: _agent_json_export())

        result = agent_export.export_application_md(SOURCE, 1, [5])

        assert result == {
            'ok': True,
            'files': [{
                'filename': 'helper.agent.md',
                'content': '---\nname: Helper\ndescription: Helps.\nmodel: gpt-x\nagent_type: react\nskills:\n'
                           '- name: forms\n  description: Fill forms.\n  version: base\n'
                           '  instructions: Read references/guide.md first.\n---\n\nHelp.',
                'is_original': True,
            }],
            'has_dependencies': False,
        }



def _skill_entry(version_name, data, path):
    return {'name': 'pdf', 'description': 'PDF.', 'import_uuid': 'uuid-3-0000', 'versions': [{
        'name': version_name, 'instructions': 'Read the files.',
        'files': [{'path': path, 'size': len(data), 'sha256': _sha(data), 'content_type': 'text/markdown',
                   'is_text': True, 'content_base64': base64.b64encode(data).decode()}],
    }]}


def _agent(app_id, name, skill_version_name):
    return {'id': app_id, 'name': name, 'description': f'{name}.', 'versions': [{
        'name': 'base', 'agent_type': 'react', 'instructions': 'Help.', 'llm_settings': {'model_name': 'gpt-x'},
        'meta': {}, 'tools': [], 'skills': [{'import_uuid': 'uuid-3-0000', 'version_name': skill_version_name}],
    }]}


class TestNestedAgentSkillVersions:
    def test_entries_of_one_skill_merge_their_versions_instead_of_dropping_the_later_one(self, agent_export):
        result = {'ok': True, 'applications': [], 'toolkits': [], 'skills': [
            {'import_uuid': 'u', 'name': 'pdf', 'versions': [{'name': 'base'}]},
            {'import_uuid': 'u', 'name': 'pdf', 'versions': [{'name': 'v2'}]},
            {'import_uuid': 'u', 'name': 'pdf', 'versions': [{'name': 'base'}]},
            {'import_uuid': 'w', 'name': 'forms', 'versions': [{'name': 'base'}]},
        ]}

        skills = agent_export._post_export(SOURCE, result, {})['skills']

        assert [(s['import_uuid'], [v['name'] for v in s['versions']]) for s in skills] == [
            ('u', ['base', 'v2']), ('w', ['base'])]

    def test_a_nested_agent_using_another_version_of_its_parents_skill_exports_that_versions_files(
            self, agent_export, monkeypatch):
        monkeypatch.setattr(agent_export, '_export_application_main', lambda *args: {
            'ok': True,
            'applications': [_agent(5, 'Parent', 'base'), _agent(6, 'Child', 'v2')],
            'toolkits': [],
            'skills': [_skill_entry('base', b'# Base', 'guide.md'), _skill_entry('v2', b'# V2', 'guide.md')],
        })

        result = agent_export.export_application_md(SOURCE, 1, [5])

        members = {f['filename']: f['content'] for f in result['files']}
        assert members['skills/pdf/guide.md'] == b'# Base'
        assert members['skills/pdf.v2/guide.md'] == b'# V2'
        assert '  files_folder: skills/pdf.v2\n' in members['child.agent.md']


@pytest.fixture
def export_api(env, monkeypatch):
    request = types.SimpleNamespace(args=MultiDict())
    flask = types.ModuleType('flask')
    flask.request, flask.send_file, flask.Response = request, mock.MagicMock(), mock.MagicMock()
    monkeypatch.setitem(sys.modules, 'flask', flask)
    passthrough = lambda *args, **kwargs: (lambda func: func)
    monkeypatch.setattr(env.tools, 'api_tools', types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func, with_modes=lambda params: params),
        raising=False)
    monkeypatch.setattr(env.tools, 'register_openapi', passthrough, raising=False)
    monkeypatch.setattr(env.tools.auth, 'decorators', types.SimpleNamespace(check_api=passthrough), raising=False)
    monkeypatch.setattr(env.tools.config, 'ADMINISTRATION_MODE', 'administration', raising=False)
    api_package = types.ModuleType(f'{PKG}.api')
    api_package.__path__ = []
    monkeypatch.setitem(sys.modules, api_package.__name__, api_package)
    v2_package = types.ModuleType(f'{PKG}.api.v2')
    v2_package.__path__ = []
    monkeypatch.setitem(sys.modules, v2_package.__name__, v2_package)
    monkeypatch.setattr(sys.modules[f'{PKG}.utils.export_import_utils'], 'content_disposition_attachment', None,
                        raising=False)
    module = load(monkeypatch, 'api/v2/export_import.py', f'{PKG}.api.v2.export_import')
    monkeypatch.setattr(module, 'entities_access_error', lambda *args: None)
    return module


class TestExportEndpoint:
    def test_an_unreadable_skill_file_is_a_503_naming_it_with_no_partial_payload(self, env, export_api, monkeypatch):
        def unreadable(**kwargs):
            raise env.files.SkillVersionExportUnavailableError('forms', 'base', 'assets/logo.png')

        monkeypatch.setattr(export_api, 'export_application', unreadable)

        response = export_api.PromptLibAPI().get(project_id=SOURCE, application_ids='5')

        assert response == ({'error': "Skill 'forms' version 'base' could not be exported: the stored content of "
                                      "'assets/logo.png' could not be read; try again later"}, 503)

    def test_an_unreadable_skill_file_fails_the_markdown_export_with_the_same_503(self, env, export_api, monkeypatch):
        def unreadable(**kwargs):
            raise env.files.SkillVersionExportUnavailableError('forms', 'base', 'assets/logo.png')

        monkeypatch.setattr(export_api, 'export_application_md', unreadable)
        export_api.request.args = MultiDict({'format': 'md'})

        response = export_api.PromptLibAPI().get(project_id=SOURCE, application_ids='5')

        assert response == ({'error': "Skill 'forms' version 'base' could not be exported: the stored content of "
                                      "'assets/logo.png' could not be read; try again later"}, 503)


class TestRuntimeManifests:
    def test_the_registry_entry_announces_files_without_hash_or_bytes(self, env):
        version_details = {'skills': [
            {'skill_id': 3, 'skill_version_id': 10, 'name': 'forms', 'instructions': 'Body.',
             'files': [{'path': 'a.md', 'size': 1, 'content_type': 'text/markdown', 'is_text': True}]},
            {'skill_id': 4, 'skill_version_id': 11, 'name': 'plain', 'instructions': 'Body.'},
        ]}

        registry = env.utils.resolve_runtime_skills(version_details)

        assert registry[0]['files'] == [{'path': 'a.md', 'size': 1, 'content_type': 'text/markdown', 'is_text': True}]
        assert 'files' not in registry[1]

    def test_manifests_for_many_versions_come_from_one_query_and_skip_file_less_versions(self, env):
        session = _seed(env)
        _seed(env, version_id=11, files=())

        manifests = env.files.list_runtime_manifests(session, [10, 11])

        assert manifests == {10: [
            {'path': 'assets/logo.png', 'size': 5, 'content_type': 'image/png', 'is_text': False},
            {'path': 'references/guide.md', 'size': 7, 'content_type': 'text/markdown', 'is_text': True},
        ]}


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


@pytest.fixture
def application_utils(env, monkeypatch):
    env.tools.store_secrets = None
    return load(monkeypatch, 'utils/application_utils.py', f'{PKG}.utils.application_utils')


class TestAgentVersionSkillList:
    def _mapping(self, files):
        version = types.SimpleNamespace(name='base', meta={}, instructions='Body.', files=files)
        return types.SimpleNamespace(skill_id=3, skill_version_id=10, skill=types.SimpleNamespace(
            name='forms', description='Fill forms.'), skill_version=version)

    def test_an_attached_skill_with_files_lists_them_without_hash(self, application_utils):
        version_file = types.SimpleNamespace(path='a.md', size=1, sha256='x' * 64, content_type='text/markdown',
                                             is_text=True)

        [entry] = application_utils.build_skill_mappings_list([self._mapping([version_file])])

        assert entry['files'] == [{'path': 'a.md', 'size': 1, 'content_type': 'text/markdown', 'is_text': True}]

    def test_an_attached_skill_without_files_is_listed_as_before(self, application_utils):
        [entry] = application_utils.build_skill_mappings_list([self._mapping([])])

        assert entry == {'skill_id': 3, 'skill_version_id': 10, 'name': 'forms', 'description': 'Fill forms.',
                         'version_name': 'base', 'icon_meta': None, 'instructions': 'Body.'}

    def test_a_public_agents_attached_skills_name_the_public_project(self, application_utils):
        [entry] = application_utils.build_skill_mappings_list([self._mapping([])], skill_project_id=PUBLIC)

        assert entry['skill_project_id'] == PUBLIC


@pytest.fixture
def public_application(env, application_utils, monkeypatch):
    env.tools.api_tools = types.SimpleNamespace(APIModeHandler=object, APIBase=object,
                                                endpoint_metrics=lambda handler: handler, with_modes=lambda urls: urls)
    env.tools.auth.decorators = types.SimpleNamespace(check_api=lambda spec: lambda handler: handler)
    env.tools.config.ADMINISTRATION_MODE = 'administration'
    sys.modules[f'{PKG}.utils.utils'].add_public_project_id = lambda handler: handler
    monkeypatch.setitem(sys.modules, 'flask', types.SimpleNamespace(request=None))
    module = load(monkeypatch, 'api/v2/public_application.py', f'{PKG}.api.v2.public_application')
    monkeypatch.setattr(module, 'build_skill_mappings_list', application_utils.build_skill_mappings_list)
    monkeypatch.setattr(module, 'apply_runtime_skills', env.utils.apply_runtime_skills)
    monkeypatch.setattr(module, 'request', types.SimpleNamespace(args={'runtime': 'true'}))
    monkeypatch.setattr(module, 'joinedload', mock.MagicMock())
    monkeypatch.setattr(module, 'selectinload', mock.MagicMock())
    return module


class TestPublicAgentRuntimeSkills:
    def _serve(self, public_application, monkeypatch, files):
        version = types.SimpleNamespace(name='base', meta={}, instructions='Body.', files=files)
        mapping = types.SimpleNamespace(skill_id=3, skill_version_id=10, skill=types.SimpleNamespace(
            name='forms', description='Fill forms.'), skill_version=version)
        application_version = types.SimpleNamespace(application=None, skill_mappings=[mapping])
        query = mock.MagicMock()
        query.filter.return_value.options.return_value.order_by.return_value.first.return_value = application_version

        @contextlib.contextmanager
        def with_project_schema_session(project_id):
            yield types.SimpleNamespace(query=lambda model: query)

        result = types.SimpleNamespace(
            get_likes=lambda project_id: None, check_is_liked=lambda project_id: None,
            model_dump=lambda mode=None: {'version_details': {'instructions': 'Help.', 'agent_type': 'react'}},
        )
        monkeypatch.setattr(public_application, 'db', types.SimpleNamespace(
            with_project_schema_session=with_project_schema_session))
        monkeypatch.setattr(public_application, 'PublishedApplicationDetailModel',
                            types.SimpleNamespace(from_orm=lambda application: result))
        monkeypatch.setattr(public_application, 'ApplicationVersionDetailModel',
                            types.SimpleNamespace(from_orm=lambda version: types.SimpleNamespace(tools=[])))
        payload, status = public_application.PromptLibAPI().get(PUBLIC, 5)
        assert status == 200
        return payload['version_details']['attached_skills']

    def test_a_skill_with_files_is_read_from_the_public_project(self, public_application, monkeypatch):
        version_file = types.SimpleNamespace(path='a.md', size=1, sha256='x' * 64, content_type='text/markdown',
                                             is_text=True)

        [entry] = self._serve(public_application, monkeypatch, [version_file])

        assert entry['skill_project_id'] == PUBLIC and [f['path'] for f in entry['files']] == ['a.md']
