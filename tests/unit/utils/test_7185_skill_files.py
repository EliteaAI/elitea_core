"""#7185 option A: bundled files of a skill version - path rules, caps, manifest rows over a per-project bucket.

Runs the real ``utils/skill_file_store.py``, ``utils/skill_utils.py`` and ``utils/skill_files.py`` over an in-memory
session per project schema and a fake MinioClient whose objects are keyed by ``(bucket, sha256)``, so writes, reads,
renames and deletes are checked against what each bucket really holds.
"""
import hashlib
import pathlib
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import (  # noqa: E402  pylint: disable=C0413
    FakeSkill, FakeSkillVersion, FakeSkillVersionFile, install,
)

PROJECT = 7
OTHER = 9
PUBLIC = 1
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

        monkeypatch.setattr(env.store.MinioClient, 's3_client', SimpleNamespace(head_object=head_object), raising=False)
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

    def test_replacing_a_path_replaces_its_row(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'old')

        entry = _write(env, session, 'FORMS.md', b'new')

        assert entry['sha256'] == _sha(b'new') and _paths(session, 10) == ['FORMS.md']

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
        env.store.put_contents(PROJECT, {'a' * 64: b'A', 'b' * 64: b'B'})
        session.add(FakeSkillVersionFile(10, 'Forms.md', 1, 'a' * 64, 'text/plain', True))
        session.add(FakeSkillVersionFile(10, 'forms.md', 1, 'b' * 64, 'text/plain', True))

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

    def test_delete_removes_the_row(self, env, session):
        _version(session)
        _write(env, session, 'forms.md', b'x')

        env.files.delete_version_file(PROJECT, SKILL, 10, 'FORMS.md', session=session)

        assert _paths(session, 10) == []

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
