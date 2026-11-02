"""#7185 option A: bundled files of a skill version - path rules, caps, manifest rows over a per-project bucket.

Runs the real ``utils/skill_file_store.py``, ``utils/skill_utils.py`` and ``utils/skill_files.py`` over an in-memory
session per project schema and a fake MinioClient whose objects are keyed by ``(bucket, sha256)``, so writes, reads,
copies across projects, release after deletes and the sweep are checked against what each bucket really holds.
"""
import base64
import datetime
import hashlib
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import (  # noqa: E402  pylint: disable=C0413
    FakeEntitySkillMapping, FakeSession, FakeSkill, FakeSkillVersion, FakeSkillVersionFile, install,
)

PROJECT = 7
OTHER = 9
PUBLIC = 1
ACTOR = 1
SKILL = 3
MB = 1024 * 1024


@pytest.fixture
def limits():
    return {}


@pytest.fixture
def env(monkeypatch, limits, isolated_sys_modules):
    return install(monkeypatch, limits)


@pytest.fixture
def session(env):
    s = env.session_of(PROJECT)
    s.add(FakeSkill())
    return s


def _version(session, id=10, **kwargs):
    version = FakeSkillVersion(id=id, **kwargs)
    session.add(version)
    return version


def _write(env, session, path, data, version_id=10, project_id=PROJECT, skill_id=SKILL):
    return env.files.write_version_file(project_id, skill_id, version_id, path, data, session=session)


def _paths(session, version_id):
    return sorted(f.path for f in session.rows[FakeSkillVersionFile] if f.skill_version_id == version_id)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _row(session, version_id, path):
    return next(f for f in session.rows[FakeSkillVersionFile] if f.skill_version_id == version_id and f.path == path)


class TestStore:
    def test_the_first_write_creates_a_system_bucket_without_expiry_in_the_projects_namespace(self, env):
        env.store.put_content(PROJECT, 'a' * 64, b'x')

        assert env.storage.buckets == {f'p--{PROJECT}.skill-files': {'type': 'system', 'retention_days': None}}
        assert env.storage.keys(PROJECT) == ['a' * 64]

    def test_putting_a_held_hash_again_writes_nothing(self, env, monkeypatch):
        env.store.put_content(PROJECT, 'a' * 64, b'x')
        uploads = []
        monkeypatch.setattr(env.store.MinioClient, 'upload_file', lambda *args: uploads.append(args))

        env.store.put_content(PROJECT, 'a' * 64, b'x')

        assert uploads == []

    def test_an_object_cut_short_under_the_hash_is_stored_again(self, env):
        data = b'# Forms\n' * 100
        env.storage.buckets[f'p--{PROJECT}.skill-files'] = {'type': 'system', 'retention_days': None}
        env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(data))] = data[:10]

        env.store.put_content(PROJECT, _sha(data), data)

        assert env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(data))] == data

    def test_on_s3_the_size_comes_from_one_head_of_the_object(self, env, monkeypatch):
        data = b'# Forms\n' * 100
        env.storage.buckets[f'p--{PROJECT}.skill-files'] = {'type': 'system', 'retention_days': None}
        env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(data))] = data[:10]
        heads = []

        def head_object(Bucket, Key):
            heads.append((Bucket, Key))
            return {'ContentLength': len(env.storage.objects[(Bucket, Key)])}

        monkeypatch.setattr(env.store.MinioClient, 's3_client', types.SimpleNamespace(head_object=head_object), raising=False)
        monkeypatch.setattr(env.store.MinioClient, 'get_file_size', None)

        env.store.put_content(PROJECT, _sha(data), data)
        env.store.put_content(PROJECT, _sha(data), data)

        assert heads == [(f'p--{PROJECT}.skill-files', _sha(data))] * 2
        assert env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(data))] == data

    def test_each_project_has_its_own_bucket(self, env):
        env.store.put_content(PROJECT, 'a' * 64, b'x')

        assert env.store.content_exists(PROJECT, 'a' * 64)
        assert not env.store.content_exists(OTHER, 'a' * 64)
        assert env.store.list_contents(OTHER) == []

    def test_get_and_delete_address_the_projects_bucket(self, env):
        env.store.put_content(PROJECT, 'a' * 64, b'x')

        assert env.store.get_content(PROJECT, 'a' * 64) == b'x'
        env.store.delete_content(PROJECT, 'a' * 64)
        assert env.storage.keys(PROJECT) == []

    def test_ensure_bucket_is_idempotent(self, env):
        env.store.ensure_bucket(PROJECT)
        env.store.ensure_bucket(PROJECT)

        assert list(env.storage.buckets) == [f'p--{PROJECT}.skill-files']


class TestBucket:
    BUCKET = f'p--{PROJECT}.skill-files'

    def test_the_bucket_is_looked_up_once_per_process_not_on_every_write(self, env):
        for data in (b'a', b'b', b'c'):
            env.store.put_content(PROJECT, _sha(data), data)
        env.store.content_exists(PROJECT, _sha(b'a'))

        assert env.storage.bucket_listings == 1

    def test_an_expiry_rule_on_a_bucket_made_elsewhere_is_removed_once(self, env):
        env.storage.buckets[self.BUCKET] = {'type': 'local', 'retention_days': 30}

        env.store.put_content(PROJECT, _sha(b'a'), b'a')
        env.store.put_content(PROJECT, _sha(b'b'), b'b')

        assert env.storage.buckets[self.BUCKET]['retention_days'] is None
        assert env.storage.lifecycle_reads == 1

    def test_on_s3_the_expiry_rule_is_deleted_and_a_bucket_without_one_is_left_alone(self, env, monkeypatch):
        env.storage.buckets[self.BUCKET] = {'type': 'local', 'retention_days': 30}
        env.storage.buckets[f'p--{OTHER}.skill-files'] = {'type': 'system', 'retention_days': None}
        deleted = []

        def get_bucket_lifecycle(client, bucket):
            if client.project_id == OTHER:
                raise _missing_lifecycle()
            return {'Rules': [{'Expiration': {'Days': 30}}]}

        monkeypatch.setattr(env.store.MinioClient, 'get_bucket_lifecycle', get_bucket_lifecycle)
        monkeypatch.setattr(env.store.MinioClient, 's3_client',
                            types.SimpleNamespace(delete_bucket_lifecycle=lambda Bucket: deleted.append(Bucket)),
                            raising=False)

        env.store.ensure_bucket(PROJECT)
        env.store.ensure_bucket(OTHER)

        assert deleted == [self.BUCKET]

    def test_a_bucket_that_could_not_be_created_is_looked_up_again(self, env, monkeypatch):
        monkeypatch.setattr(env.store.MinioClient, 'create_bucket', lambda *args, **kwargs: {'error': 'busy'})

        env.store.ensure_bucket(PROJECT)
        env.store.ensure_bucket(PROJECT)

        assert env.storage.bucket_listings == 2


class TestClients:
    def test_store_calls_given_a_client_build_no_other(self, env):
        client = env.store.project_client(PROJECT)

        env.store.put_contents(PROJECT, {_sha(b'a'): b'a'}, client)
        env.store.put_content(PROJECT, _sha(b'b'), b'b', client)
        env.store.get_content(PROJECT, _sha(b'a'), client)
        env.store.content_exists(PROJECT, _sha(b'b'), client)

        assert env.storage.clients == [PROJECT]

    def test_a_write_and_a_read_build_one_client_each(self, env, session):
        _version(session)

        _write(env, session, 'forms.md', b'x')
        env.files.read_version_file(PROJECT, SKILL, 10, 'forms.md', session=session)

        assert env.storage.clients == [PROJECT, PROJECT]


def _missing_lifecycle():
    error = Exception('NoSuchLifecycleConfiguration')
    error.response = {'Error': {'Code': 'NoSuchLifecycleConfiguration'}}
    return error


class TestPathRules:
    @pytest.mark.parametrize('given, stored', [
        ('references/guide.md', 'references/guide.md'),
        ('./forms.md', 'forms.md'),
        ('  scripts/run.py ', 'scripts/run.py'),
        ('LICENSE.txt', 'LICENSE.txt'),
        ('references/SKILL.md', 'references/SKILL.md'),
    ])
    def test_a_relative_path_under_the_root_is_kept(self, env, given, stored):
        assert env.files.normalize_skill_file_path(given) == stored

    @pytest.mark.parametrize('bad', [
        '', '   ', 'dir/', '/etc/passwd', '../SKILL.md', 'a/../b', 'a/./b', 'a//b', 'a\\b', 'a\x00b', 'C:x',
        'SKILL.md', 'skill.md', './SKILL.md', 'x/' * 17 + 'f', 'a' * 1025,
    ])
    def test_a_path_that_leaves_or_aliases_the_root_is_refused(self, env, bad):
        with pytest.raises(env.utils.SkillFileError):
            env.files.normalize_skill_file_path(bad)

    @pytest.mark.parametrize('path, is_text, expected', [
        ('forms.md', True, 'text/markdown'),
        ('scripts/x.py', True, 'text/x-python'),
        ('notes', True, 'text/plain'),
        ('font.bin', False, 'application/octet-stream'),
        ('img.png', False, 'image/png'),
    ])
    def test_content_type_comes_from_the_extension_then_the_text_split(self, env, path, is_text, expected):
        assert env.files.detect_content_type(path, is_text) == expected

    def test_text_is_utf8_without_nul(self, env):
        assert env.files.is_text_content('héllo'.encode('utf-8'))
        assert not env.files.is_text_content(b'PK\x03\x04\x00')
        assert not env.files.is_text_content(b'\xff\xfe')


class TestPackageLimits:
    def _check(self, env, current, path, size, **caps):
        caps = {'max_file_bytes': 10 * MB, 'max_package_bytes': 30 * MB, 'max_package_files': 500, **caps}
        env.files.validate_package_limits(current, path, size, **caps)

    def test_a_file_over_the_per_file_cap_is_refused_with_both_sizes(self, env):
        with pytest.raises(env.utils.SkillFileError, match=r"'big.bin' is 10.0 MB; a skill file can be at most 1.0 MB"):
            self._check(env, {}, 'big.bin', 10 * MB, max_file_bytes=MB)

    def test_the_file_count_cap_counts_the_new_file_and_a_replace_counts_once(self, env):
        current = {f'f{i}': 1 for i in range(3)}
        with pytest.raises(env.utils.SkillFileError, match='at most 3 files'):
            self._check(env, current, 'f3', 1, max_package_files=3)
        self._check(env, current, 'f1', 1, max_package_files=3)

    def test_the_total_cap_counts_the_other_files_plus_the_new_size(self, env):
        current = {'a': 2 * MB, 'b': 2 * MB}
        with pytest.raises(env.utils.SkillFileError, match=r"would be 5.0 MB; a skill version can hold at most 4.0 MB"):
            self._check(env, current, 'c', MB, max_package_bytes=4 * MB)
        self._check(env, current, 'b', MB, max_package_bytes=4 * MB)

    def test_a_whole_set_refuses_one_path_twice_whatever_the_case(self, env):
        entries = [env.files.build_file_entry('Forms.md', b'a'), env.files.build_file_entry('forms.md', b'b')]

        with pytest.raises(env.utils.SkillFileError, match="'forms.md' is listed more than once"):
            env.files.validate_package(entries)

    def test_a_whole_set_is_held_to_the_admin_caps(self, env, limits):
        limits['max_package_files'] = 1
        entries = [env.files.build_file_entry('a.md', b'a'), env.files.build_file_entry('b.md', b'b')]

        with pytest.raises(env.utils.SkillFileError, match='at most 1 files'):
            env.files.validate_package(entries)


class TestWrite:
    def test_a_write_stores_the_object_under_its_hash_and_one_row(self, env, session):
        _version(session)

        entry = _write(env, session, './forms.md', b'# Forms\n')

        assert entry == {
            'path': 'forms.md', 'size': 8, 'sha256': _sha(b'# Forms\n'), 'content_type': 'text/markdown',
            'is_text': True, 'created_at': '2026-02-02T00:00:00',
        }
        assert _paths(session, 10) == ['forms.md']
        assert env.storage.keys(PROJECT) == [_sha(b'# Forms\n')]
        assert env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(b'# Forms\n'))] == b'# Forms\n'

    def test_the_same_bytes_at_two_paths_are_one_object(self, env, session):
        _version(session)
        _write(env, session, 'a.txt', b'same')
        _write(env, session, 'b/a.txt', b'same')

        assert _paths(session, 10) == ['a.txt', 'b/a.txt'] and env.storage.keys(PROJECT) == [_sha(b'same')]

    def test_replacing_a_path_releases_the_old_object(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'old')

        entry = _write(env, session, 'FORMS.md', b'new')

        assert entry['sha256'] == _sha(b'new') and _paths(session, 10) == ['FORMS.md']
        assert env.storage.keys(PROJECT) == [_sha(b'new')]

    def test_an_object_another_version_uses_survives_a_replace(self, env, session):
        _version(session, id=10)
        _version(session, id=11, name='v2')
        _write(env, session, 'forms.md', b'shared', version_id=10)
        _write(env, session, 'forms.md', b'shared', version_id=11)

        _write(env, session, 'forms.md', b'changed', version_id=10)

        assert env.storage.keys(PROJECT) == sorted([_sha(b'shared'), _sha(b'changed')])

    def test_a_binary_file_is_stored_as_binary(self, env, session):
        _version(session)

        entry = _write(env, session, 'assets/logo.png', b'\x89PNG\x00')

        assert (entry['is_text'], entry['content_type']) == (False, 'image/png')

    @pytest.mark.parametrize('status', ['published', 'embedded'])
    def test_a_published_or_embedded_version_refuses_writes_before_storage(self, env, session, status):
        _version(session, status=status)

        with pytest.raises(env.utils.SkillVersionNotUpdatableError):
            _write(env, session, 'forms.md', b'x')
        assert _paths(session, 10) == [] and env.storage.objects == {}

    def test_a_bad_path_or_a_cap_stores_nothing(self, env, session, limits):
        _version(session)
        limits['max_file_size_mb'] = 1

        with pytest.raises(env.utils.SkillFileError):
            _write(env, session, '../forms.md', b'x')
        with pytest.raises(env.utils.SkillFileError, match='at most 1.0 MB'):
            _write(env, session, 'big.bin', b'x' * (MB + 1))
        assert env.storage.objects == {}

    def test_an_unknown_version_is_a_404(self, env, session):
        with pytest.raises(env.utils.SkillVersionNotFoundError) as info:
            _write(env, session, 'forms.md', b'x', version_id=99)
        assert info.value.http_status == 404

    def test_the_admin_caps_are_read_on_every_write(self, env, session, limits):
        _version(session)
        _write(env, session, 'ok.bin', b'x' * MB)

        limits['max_package_files'] = 1
        with pytest.raises(env.utils.SkillFileError, match='at most 1 files'):
            _write(env, session, 'second.bin', b'y')
        limits['max_package_files'] = 500
        limits['max_package_size_mb'] = 1
        with pytest.raises(env.utils.SkillFileError, match='at most 1.0 MB'):
            _write(env, session, 'second.bin', b'y')


    def test_replacing_a_file_in_another_case_counts_it_once_against_the_caps(self, env, session, limits):
        _version(session)
        limits['max_package_files'] = 1
        _write(env, session, 'forms.md', b'old')

        entry = _write(env, session, 'FORMS.md', b'new')

        assert entry['sha256'] == _sha(b'new') and _paths(session, 10) == ['FORMS.md']


class TestConcurrentPaths:
    def _take_the_path_first(self, env, session, monkeypatch):
        def flush():
            raise env.files.IntegrityError('uq_skill_version_files_path_ci')
        monkeypatch.setattr(session, 'flush', flush)

    def test_a_write_racing_another_for_the_same_path_in_any_case_is_a_409(self, env, session, monkeypatch):
        _version(session)
        self._take_the_path_first(env, session, monkeypatch)

        with pytest.raises(env.files.SkillFilePathTakenError, match="'forms.md' already exists in skill version 10") as info:
            _write(env, session, 'forms.md', b'x')
        assert info.value.http_status == 409

    def test_a_rename_racing_a_write_onto_the_new_path_is_a_409(self, env, session, monkeypatch):
        _version(session)
        _write(env, session, 'a.md', b'a')
        self._take_the_path_first(env, session, monkeypatch)

        with pytest.raises(env.files.SkillFilePathTakenError, match="'b.md' already exists") as info:
            env.files.rename_version_file(PROJECT, SKILL, 10, 'a.md', 'b.md', session=session)
        assert info.value.http_status == 409


class TestConcurrentWrites:
    def test_a_write_reads_the_files_it_checks_the_caps_against_only_once_it_holds_the_version_row(self, env, session):
        _version(session)

        _write(env, session, 'forms.md', b'x')

        assert session.file_reads == [(10, True)]


class TestRead:
    def test_read_returns_the_entry_and_the_bytes_from_the_bucket(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'# Forms\n')

        entry, content = env.files.read_version_file(PROJECT, SKILL, 10, 'FORMS.md', session=session)

        assert (entry['path'], content) == ('forms.md', b'# Forms\n')

    def test_an_exact_match_wins_over_a_case_insensitive_one(self, env, session):
        _version(session)
        env.store.put_contents(PROJECT, {_sha(b'A'): b'A', _sha(b'B'): b'B'})
        session.add(FakeSkillVersionFile(10, 'Forms.md', 1, _sha(b'A'), 'text/plain', True))
        session.add(FakeSkillVersionFile(10, 'forms.md', 1, _sha(b'B'), 'text/plain', True))

        _, content = env.files.read_version_file(PROJECT, SKILL, 10, 'forms.md', session=session)

        assert content == b'B'

    def test_a_missing_file_is_a_404_naming_the_path(self, env, session):
        _version(session)

        with pytest.raises(env.utils.SkillFileNotFoundError, match="'nope.md' not found in skill version 10"):
            env.files.read_version_file(PROJECT, SKILL, 10, 'nope.md', session=session)

    def test_unreadable_storage_is_a_503_naming_the_path(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')
        env.storage.broken_reads = True

        with pytest.raises(env.files.SkillFileContentUnavailableError, match="'forms.md'") as info:
            env.files.read_version_file(PROJECT, SKILL, 10, 'forms.md', session=session)
        assert info.value.http_status == 503

    def test_bytes_that_do_not_match_the_rows_hash_are_a_503_naming_the_path(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'# Forms\n')
        env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(b'# Forms\n'))] = b'# Forms'

        with pytest.raises(env.files.SkillFileContentUnavailableError, match="'forms.md'") as info:
            env.files.read_version_file(PROJECT, SKILL, 10, 'forms.md', session=session)
        assert info.value.http_status == 503

    def test_the_manifest_lists_every_file_sorted_without_bytes(self, env, session):
        _version(session)
        _write(env, session, 'scripts/b.py', b'print()')
        _write(env, session, 'a.md', b'# A')

        manifest = env.files.list_version_files(PROJECT, SKILL, 10, session=session)

        assert [(f['path'], f['size']) for f in manifest] == [('a.md', 3), ('scripts/b.py', 7)]
        assert 'content' not in manifest[0]


class TestRenameAndDelete:
    def test_rename_moves_the_row_and_keeps_the_object(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')

        entry = env.files.rename_version_file(PROJECT, SKILL, 10, 'forms.md', './references/forms.md', session=session)

        assert entry['path'] == 'references/forms.md' and _paths(session, 10) == ['references/forms.md']
        assert env.storage.keys(PROJECT) == [_sha(b'x')]

    def test_rename_onto_another_file_is_refused(self, env, session):
        _version(session)
        _write(env, session, 'a.md', b'a')
        _write(env, session, 'b.md', b'b')

        with pytest.raises(env.utils.SkillFileError, match="'b.md' already exists") as info:
            env.files.rename_version_file(PROJECT, SKILL, 10, 'a.md', 'B.md', session=session)
        assert info.value.http_status == 409

    def test_rename_may_change_only_the_case(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')

        env.files.rename_version_file(PROJECT, SKILL, 10, 'forms.md', 'Forms.md', session=session)

        assert _paths(session, 10) == ['Forms.md']

    def test_rename_to_a_bad_path_or_of_a_missing_file_is_refused(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')

        with pytest.raises(env.utils.SkillFileError, match="'a//b' is not a valid skill file path"):
            env.files.rename_version_file(PROJECT, SKILL, 10, 'forms.md', 'a//b', session=session)
        with pytest.raises(env.utils.SkillFileNotFoundError) as info:
            env.files.rename_version_file(PROJECT, SKILL, 10, 'missing.md', 'other.md', session=session)
        assert info.value.http_status == 404 and _paths(session, 10) == ['forms.md']

    def test_delete_removes_the_row_and_releases_the_object(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')

        env.files.delete_version_file(PROJECT, SKILL, 10, 'FORMS.md', session=session)

        assert _paths(session, 10) == [] and env.storage.keys(PROJECT) == []

    def test_delete_keeps_an_object_another_file_uses(self, env, session):
        _version(session)
        _write(env, session, 'a.md', b'same')
        _write(env, session, 'b.md', b'same')

        env.files.delete_version_file(PROJECT, SKILL, 10, 'a.md', session=session)

        assert _paths(session, 10) == ['b.md'] and env.storage.keys(PROJECT) == [_sha(b'same')]

    def test_deleting_a_missing_file_is_a_404(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')

        with pytest.raises(env.utils.SkillFileNotFoundError) as info:
            env.files.delete_version_file(PROJECT, SKILL, 10, 'missing.md', session=session)
        assert info.value.http_status == 404 and _paths(session, 10) == ['forms.md']

    def test_a_published_version_refuses_renames_and_deletes(self, env, session):
        _version(session, status='published')
        session.add(FakeSkillVersionFile(10, 'a.md', 1, 'a' * 64, 'text/plain', True))

        with pytest.raises(env.utils.SkillVersionNotUpdatableError):
            env.files.rename_version_file(PROJECT, SKILL, 10, 'a.md', 'b.md', session=session)
        with pytest.raises(env.utils.SkillVersionNotUpdatableError):
            env.files.delete_version_file(PROJECT, SKILL, 10, 'a.md', session=session)
        assert _paths(session, 10) == ['a.md']

    def test_a_storage_failure_after_the_delete_never_fails_the_delete(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')
        env.storage.broken_deletes = True

        env.files.delete_version_file(PROJECT, SKILL, 10, 'forms.md', session=session)

        assert _paths(session, 10) == [] and env.storage.keys(PROJECT) == [_sha(b'x')]


class TestSameProjectCopies:
    def _version_data(self, files_from_version_id):
        return types.SimpleNamespace(
            name='v2', instructions='Body.', author_id=1, tags=None, meta=None, run_settings=None,
            files_from_version_id=files_from_version_id,
        )

    def test_copy_version_files_adds_rows_and_writes_no_object(self, env, session):
        _version(session, id=10)
        _version(session, id=11, name='v2')
        _write(env, session, 'forms.md', b'x')
        _write(env, session, 'scripts/a.py', b'y')
        writes_before = dict(env.storage.objects)

        copied = env.utils.copy_version_files(session, 10, 11)

        assert copied == 2 and _paths(session, 11) == ['forms.md', 'scripts/a.py']
        assert env.storage.objects == writes_before
        target = _row(session, 11, 'forms.md')
        assert (target.sha256, target.size, target.content_type, target.is_text) == (_sha(b'x'), 1, 'text/markdown', True)

    def test_save_as_version_starts_with_the_source_versions_files(self, env, session):
        _version(session, id=10)
        _write(env, session, 'forms.md', b'x')

        env.utils.create_skill_version(PROJECT, SKILL, self._version_data(10), session=session,
                                       instructions_already_saved=True)

        new_version = next(v for v in session.rows[FakeSkillVersion] if v.name == 'v2')
        assert _paths(session, new_version.id) == ['forms.md']

    def test_save_as_version_without_a_source_copies_nothing(self, env, session):
        _version(session, id=10)
        _write(env, session, 'forms.md', b'x')

        env.utils.create_skill_version(PROJECT, SKILL, self._version_data(None), session=session,
                                       instructions_already_saved=True)

        new_version = next(v for v in session.rows[FakeSkillVersion] if v.name == 'v2')
        assert _paths(session, new_version.id) == []

    def test_a_source_version_of_another_skill_is_refused(self, env, session):
        _version(session, id=10)
        _version(session, id=50, skill_id=SKILL + 1)

        with pytest.raises(env.utils.SkillVersionNotFoundError):
            env.utils.create_skill_version(PROJECT, SKILL, self._version_data(50), session=session,
                                           instructions_already_saved=True)

    def test_deleting_a_version_releases_only_what_no_other_version_uses(self, env, session):
        _version(session, id=10)
        _version(session, id=11, name='v2')
        _write(env, session, 'shared.md', b'shared', version_id=10)
        _write(env, session, 'shared.md', b'shared', version_id=11)
        _write(env, session, 'only.md', b'only', version_id=11)

        env.utils.delete_skill_version(PROJECT, SKILL, 11, session=session)

        assert env.storage.keys(PROJECT) == [_sha(b'shared')] and _paths(session, 11) == []

    def test_deleting_a_skill_releases_its_objects_and_keeps_other_skills(self, env, session):
        session.add(FakeSkill(id=SKILL + 1))
        _version(session, id=10)
        _version(session, id=20, skill_id=SKILL + 1)
        _write(env, session, 'mine.md', b'mine', version_id=10)
        _write(env, session, 'shared.md', b'shared', version_id=10)
        _write(env, session, 'shared.md', b'shared', version_id=20, skill_id=SKILL + 1)

        env.utils.delete_skill(PROJECT, SKILL, session=session)

        assert env.storage.keys(PROJECT) == [_sha(b'shared')]

    def test_a_version_an_agent_uses_keeps_its_files(self, env, session):
        _version(session, id=10)
        _version(session, id=11, name='v2')
        _write(env, session, 'forms.md', b'x', version_id=11)
        session.add(FakeEntitySkillMapping(11))

        with pytest.raises(env.utils.SkillVersionInUseError):
            env.utils.delete_skill_version(PROJECT, SKILL, 11, session=session)
        assert env.storage.keys(PROJECT) == [_sha(b'x')]


def _seed_source(env, project_id=PROJECT, version_id=10, files=((b'# Forms', 'forms.md'), (b'\x89PNG\x00', 'a.png'))):
    """A source version in ``project_id`` holding ``files``; returns its manifest as a payload lists it."""
    session = env.session_of(project_id)
    session.add(FakeSkill(owner_id=project_id))
    _version(session, id=version_id)
    for data, path in files:
        _write(env, session, path, data, version_id=version_id, project_id=project_id)
    return env.files.list_version_manifest(session, version_id)


def _copy(env, target, entries, source_project_id=PROJECT, project_id=OTHER, version_id=40):
    """What every copy does: the objects first, with no transaction open, then the rows."""
    staged = env.files.stage_version_files(project_id, entries, source_project_id)
    env.files.add_version_files(target, project_id, version_id, entries, staged, source_project_id)
    return staged


class TestCrossProjectCopies:
    def test_entries_from_another_project_are_copied_into_the_targets_bucket_then_listed(self, env):
        manifest = _seed_source(env)
        target = env.session_of(OTHER)
        _version(target, id=40)

        _copy(env, target, manifest)

        assert _paths(target, 40) == ['a.png', 'forms.md']
        assert env.storage.keys(OTHER) == sorted([_sha(b'# Forms'), _sha(b'\x89PNG\x00')])
        assert _row(target, 40, 'a.png').content_type == 'image/png'

    def test_a_target_that_already_holds_the_hash_gets_rows_only(self, env):
        manifest = _seed_source(env)
        env.store.put_contents(OTHER, {entry['sha256']: b'kept' for entry in manifest})
        target = env.session_of(OTHER)

        _copy(env, target, manifest)

        assert set(env.storage.objects[(f'p--{OTHER}.skill-files', e['sha256'])] for e in manifest) == {b'kept'}

    def test_entries_with_bytes_are_stored_without_reading_any_source(self, env):
        target = env.session_of(OTHER)
        entries = [env.files.build_file_entry('notes.md', b'n')]

        _copy(env, target, entries, source_project_id=None)

        assert env.storage.keys(OTHER) == [_sha(b'n')] and set(env.storage.clients) == {OTHER}

    def test_bytes_the_source_holds_under_another_hash_are_refused_naming_the_path(self, env):
        manifest = _seed_source(env)
        env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(b'# Forms'))] = b'# Tampered'

        with pytest.raises(env.files.SkillFileSourceUnavailableError, match="'forms.md' could not be copied"):
            env.files.stage_version_files(OTHER, manifest, PROJECT)
        assert _sha(b'# Forms') not in env.storage.keys(OTHER)

    def test_a_source_object_deleted_meanwhile_is_refused_naming_the_path(self, env):
        manifest = _seed_source(env)
        del env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(b'\x89PNG\x00'))]

        with pytest.raises(env.files.SkillFileSourceUnavailableError, match="'a.png' could not be copied") as info:
            env.files.stage_version_files(OTHER, manifest, PROJECT)
        assert isinstance(info.value, env.utils.SkillFileError) and info.value.http_status == 400

    def test_an_object_deleted_between_staging_and_the_rows_is_stored_again_from_the_staged_bytes(self, env):
        manifest = _seed_source(env)
        target = env.session_of(OTHER)
        staged = env.files.stage_version_files(OTHER, manifest, PROJECT)
        del env.storage.objects[(f'p--{OTHER}.skill-files', _sha(b'# Forms'))]
        del env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(b'# Forms'))]

        env.files.add_version_files(target, OTHER, 40, manifest, staged, PROJECT)

        assert env.storage.objects[(f'p--{OTHER}.skill-files', _sha(b'# Forms'))] == b'# Forms'
        assert _paths(target, 40) == ['a.png', 'forms.md']

    def test_an_object_the_target_held_at_staging_and_lost_is_read_again_from_the_source(self, env):
        manifest = _seed_source(env)
        env.store.put_contents(OTHER, {_sha(b'# Forms'): b'# Forms'})
        target = env.session_of(OTHER)
        staged = env.files.stage_version_files(OTHER, manifest, PROJECT)
        del env.storage.objects[(f'p--{OTHER}.skill-files', _sha(b'# Forms'))]

        env.files.add_version_files(target, OTHER, 40, manifest, staged, PROJECT)

        assert _sha(b'# Forms') not in staged
        assert env.storage.objects[(f'p--{OTHER}.skill-files', _sha(b'# Forms'))] == b'# Forms'

    def test_a_same_project_entry_whose_object_is_gone_is_refused_naming_the_path(self, env):
        manifest = _seed_source(env)
        del env.storage.objects[(f'p--{PROJECT}.skill-files', _sha(b'# Forms'))]

        with pytest.raises(env.files.SkillFileSourceUnavailableError, match="'forms.md'"):
            env.files.stage_version_files(PROJECT, manifest, PROJECT)


class TestPayloadFiles:
    def test_carried_base64_content_becomes_entries_with_hashes_computed_here(self, env):
        payload = [{'path': './notes.md', 'sha256': 'forged', 'size': 1,
                    'content_base64': base64.b64encode(b'hello').decode()}]

        [entry] = env.files.resolve_payload_files(payload)

        assert (entry['path'], entry['sha256'], entry['size'], entry['data']) == (
            'notes.md', _sha(b'hello'), 5, b'hello')

    def test_invalid_base64_is_refused_naming_the_path(self, env):
        with pytest.raises(env.utils.SkillFileError, match="'notes.md' does not carry valid base64"):
            env.files.resolve_payload_files([{'path': 'notes.md', 'content_base64': '***'}])

    def test_listed_files_resolve_to_the_source_versions_own_rows(self, env):
        manifest = _seed_source(env)
        payload = [{'path': entry['path'], 'sha256': entry['sha256'], 'size': 0, 'content_type': 'x'}
                   for entry in manifest]

        entries = env.files.resolve_payload_files(payload, PROJECT, 10, ACTOR)

        assert entries == manifest

    def test_files_the_source_version_holds_copy_even_after_an_admin_lowers_the_caps(self, env, limits):
        manifest = _seed_source(env)
        limits['max_package_files'] = 1

        entries = env.files.resolve_payload_files([dict(entry) for entry in manifest], PROJECT, 10, ACTOR)

        assert [entry['path'] for entry in entries] == ['a.png', 'forms.md']

    def test_carried_content_is_held_to_the_admin_caps(self, env, limits):
        limits['max_package_files'] = 1
        payload = [{'path': path, 'content_base64': base64.b64encode(path.encode()).decode()}
                   for path in ('a.md', 'b.md')]

        with pytest.raises(env.utils.SkillFileError, match='at most 1 files'):
            env.files.resolve_payload_files(payload)

    def test_a_pair_the_source_version_does_not_hold_is_refused(self, env):
        _seed_source(env)
        foreign = _seed_source(env, project_id=OTHER, version_id=60, files=((b'secret', 'secret.md'),))

        with pytest.raises(env.utils.SkillFileError, match="'secret.md' is not a file of skill version 10 in project 7"):
            env.files.resolve_payload_files([dict(foreign[0])], PROJECT, 10, ACTOR)

    def test_listed_files_without_a_source_are_refused(self, env):
        with pytest.raises(env.utils.SkillFileError, match='without their content'):
            env.files.resolve_payload_files([{'path': 'a.md', 'sha256': 'a' * 64}])

    def test_import_gives_each_created_version_its_listed_files(self, env):
        manifest = _seed_source(env)
        target = env.session_of(OTHER)
        versions = [
            {'name': 'base', 'id': 10, 'files': manifest},
            {'name': 'v2', 'id': 10},
            {'name': 'notes', 'files': [{'path': 'n.md', 'content_base64': base64.b64encode(b'n').decode()}]},
        ]

        staged = env.files.stage_import_files(OTHER, versions, PROJECT, ACTOR)
        added = env.files.import_version_files(target, OTHER, {'base': 41, 'v2': 42, 'notes': 43}, staged, PROJECT)

        assert added == 3
        assert (_paths(target, 41), _paths(target, 42), _paths(target, 43)) == (['a.png', 'forms.md'], [], ['n.md'])
        assert env.storage.keys(OTHER) == sorted([_sha(b'# Forms'), _sha(b'\x89PNG\x00'), _sha(b'n')])


class TestSweep:
    NOW = datetime.datetime(2026, 1, 1, 2, 0, tzinfo=datetime.timezone.utc)

    def _seed(self, env, session):
        _version(session)
        _write(env, session, 'kept.md', b'kept')
        env.store.put_contents(PROJECT, {'old' * 21 + 'o': b'orphan', 'new' * 21 + 'n': b'recent'})
        env.storage.modified[(f'p--{PROJECT}.skill-files', 'new' * 21 + 'n')] = '2026-01-01T01:30:00+00:00'

    def test_the_sweep_deletes_unreferenced_objects_older_than_an_hour(self, env, session):
        self._seed(env, session)

        result = env.files.sweep_skill_files(PROJECT, now=self.NOW)

        assert result == {'project_id': PROJECT, 'deleted': ['old' * 21 + 'o'], 'kept_recent': 1, 'kept_undated': 0,
                          'dry_run': False}
        assert env.storage.keys(PROJECT) == sorted([_sha(b'kept'), 'new' * 21 + 'n'])

    def test_a_dry_run_deletes_nothing(self, env, session):
        self._seed(env, session)

        result = env.files.sweep_skill_files(PROJECT, dry_run=True, now=self.NOW)

        assert result['deleted'] == ['old' * 21 + 'o'] and len(env.storage.keys(PROJECT)) == 3

    def test_an_object_storage_gives_no_time_for_is_kept_and_counted(self, env, session):
        self._seed(env, session)
        env.storage.modified[(f'p--{PROJECT}.skill-files', 'old' * 21 + 'o')] = None

        result = env.files.sweep_skill_files(PROJECT, now=self.NOW)

        assert (result['deleted'], result['kept_undated'], result['kept_recent']) == ([], 1, 1)
        assert len(env.storage.keys(PROJECT)) == 3

    def test_a_project_without_a_bucket_sweeps_nothing(self, env):
        assert env.files.sweep_skill_files(OTHER, now=self.NOW)['deleted'] == []


class TestDeletesWaitForOpenWrites:
    """A transaction that reuses an object or adds rows naming it keeps the project's deletes off until it ends, so
    neither the sweep nor a release deletes the object between the reuse and the row becoming visible."""

    def _open_transaction(self):
        """Another session of the project: what it adds stays invisible to ``session_of`` until the test moves it."""
        other = FakeSession(PROJECT)
        other.add(FakeSkill())
        other.add(FakeSkillVersion(id=20))
        return other

    def test_the_sweep_keeps_an_old_unreferenced_object_an_open_write_reused(self, env, session):
        env.store.put_content(PROJECT, _sha(b'reused'), b'reused')
        writer = self._open_transaction()
        _write(env, writer, 'forms.md', b'reused', version_id=20)

        with pytest.raises(env.utils.SkillFileError, match='being written'):
            env.files.sweep_skill_files(PROJECT, now=TestSweep.NOW)
        writer.commit()
        session.rows[FakeSkillVersionFile].extend(writer.rows[FakeSkillVersionFile])

        assert env.files.sweep_skill_files(PROJECT, now=TestSweep.NOW)['deleted'] == []
        assert env.storage.keys(PROJECT) == [_sha(b'reused')]

    def test_a_release_leaves_an_object_an_open_import_reuses_to_the_sweep(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'shared')
        session.commit()
        importer = self._open_transaction()
        env.files.add_version_files(importer, PROJECT, 20, [env.files.build_file_entry('forms.md', b'shared')])

        env.files.delete_version_file(PROJECT, SKILL, 10, 'forms.md', session=session)

        assert _paths(session, 10) == [] and env.storage.keys(PROJECT) == [_sha(b'shared')]

    def test_a_release_leaves_an_object_an_open_save_as_version_copies_to_the_sweep(self, env, session):
        env.store.put_content(PROJECT, _sha(b'x'), b'x')
        saver = self._open_transaction()
        saver.add(FakeSkillVersionFile(20, 'forms.md', 1, _sha(b'x'), 'text/markdown', True))
        version_data = types.SimpleNamespace(
            name='v2', instructions='Body.', author_id=1, tags=None, meta=None, run_settings=None,
            files_from_version_id=20,
        )
        env.utils.create_skill_version(PROJECT, SKILL, version_data, session=saver, instructions_already_saved=True)

        assert env.utils.release_unreferenced_contents(PROJECT, {_sha(b'x')}) == []
        assert env.storage.keys(PROJECT) == [_sha(b'x')]


class TestDeletesAreHeldOffAroundTheirCriticalSections:
    def test_a_write_holds_deletes_off_before_it_looks_for_a_reusable_object(self, env, session, monkeypatch):
        _version(session)
        held = []
        monkeypatch.setattr(env.files, 'put_content',
                            lambda project_id, sha256, data, client=None: held.append(
                                session in FakeSession.locks.get(project_id, ())))

        _write(env, session, 'forms.md', b'x')

        assert held == [True]

    def test_an_add_holds_deletes_off_before_it_checks_the_objects_and_adds_the_rows(self, env, session, monkeypatch):
        _version(session)
        held = []
        content_exists = env.files.content_exists
        add = session.add
        monkeypatch.setattr(env.files, 'content_exists', lambda project_id, sha256, client=None: held.append(
            ('check', session in FakeSession.locks.get(project_id, ()))) or content_exists(project_id, sha256, client))
        monkeypatch.setattr(session, 'add', lambda row: held.append(
            ('row', session in FakeSession.locks.get(PROJECT, ()))) or add(row))
        entries = [env.files.build_file_entry('a.md', b'a')]
        staged = env.files.stage_version_files(PROJECT, entries)
        held.clear()

        env.files.add_version_files(session, PROJECT, 10, entries, staged)

        assert held == [('check', True), ('row', True)]

    def test_staging_moves_the_bytes_with_no_transaction_and_no_lock(self, env, monkeypatch):
        manifest = _seed_source(env)
        env.session_of(PROJECT).commit()
        uploads = []
        upload = env.store.MinioClient.upload_file
        monkeypatch.setattr(env.store.MinioClient, 'upload_file', lambda client, bucket, data, name: uploads.append(
            any(FakeSession.locks.values())) or upload(client, bucket, data, name))

        env.files.stage_version_files(OTHER, manifest, PROJECT)

        assert uploads == [False, False]

    def test_a_save_as_version_holds_deletes_off_before_it_reads_the_source_rows(self, env, session, monkeypatch):
        session.add(FakeSkill())
        _version(session, id=20)
        held = []
        monkeypatch.setattr(env.utils, 'copy_version_files',
                            lambda s, source_id, target_id: held.append(s in FakeSession.locks.get(PROJECT, ())))
        version_data = types.SimpleNamespace(
            name='v2', instructions='Body.', author_id=1, tags=None, meta=None, run_settings=None,
            files_from_version_id=20,
        )

        env.utils.create_skill_version(PROJECT, SKILL, version_data, session=session, instructions_already_saved=True)

        assert held == [True]

    def test_the_sweep_deletes_only_while_it_holds_writes_off(self, env, monkeypatch):
        env.store.put_content(PROJECT, _sha(b'old'), b'old')
        claimed = []
        monkeypatch.setattr(env.files, 'delete_contents',
                            lambda project_id, hashes: claimed.append(bool(FakeSession.locks.get(project_id))))

        env.files.sweep_skill_files(PROJECT, now=TestSweep.NOW)

        assert claimed == [True]

    def test_a_release_deletes_only_while_it_holds_writes_off(self, env, monkeypatch):
        claimed = []
        monkeypatch.setattr(env.utils, 'delete_contents',
                            lambda project_id, hashes: claimed.append(bool(FakeSession.locks.get(project_id))))

        env.utils.release_unreferenced_contents(PROJECT, {_sha(b'old')})

        assert claimed == [True]
