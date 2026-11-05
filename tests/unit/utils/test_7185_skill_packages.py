"""#7185 option A: a skill version's files travel through its ZIP export and import and through the fork payload.

Runs the real ``utils/skill_export_import.py`` over the in-memory session and bucket fakes of ``skill_file_env``:
the ZIP reader's refusals, the deterministic archive, the export that stays ``.md`` without files, the import that
writes files through the store, and the fork payload that lists the manifest.
"""
import hashlib
import io
import pathlib
import sys
import types
import zipfile

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import FakeSkill, FakeSkillVersion, FakeSkillVersionFile, install  # noqa: E402  pylint: disable=C0413

PROJECT = 7
SKILL = 3
MB = 1024 * 1024
SKILL_MD = '---\nname: forms\ndescription: Fill forms.\n---\n\nRead references/guide.md first.\n'


@pytest.fixture
def limits():
    return {}


@pytest.fixture
def env(monkeypatch, limits, isolated_sys_modules):
    return install(monkeypatch, limits)


def _zip(members, modify=None):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            info = zipfile.ZipInfo(name)
            if modify:
                modify(name, info)
            archive.writestr(info, data)
    return buffer.getvalue()


def _flag_encrypted(data, name):
    """Set the encryption bit of one member in the central directory, which is where ``infolist`` reads it."""
    patched = bytearray(data)
    position = patched.find(b'PK\x01\x02')
    while position != -1:
        name_length = int.from_bytes(patched[position + 28:position + 30], 'little')
        if patched[position + 46:position + 46 + name_length].decode() == name:
            patched[position + 8] |= 0x1
        position = patched.find(b'PK\x01\x02', position + 46)
    return bytes(patched)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


class TestReadSkillZip:
    def test_the_folder_layout_maps_members_to_paths_under_the_skill_root(self, env):
        content, entries = env.export.read_skill_zip(_zip({
            'forms/SKILL.md': SKILL_MD, 'forms/references/guide.md': '# Guide', 'forms/assets/logo.png': b'\x89PNG\x00',
        }))

        assert content == SKILL_MD
        assert [(e['path'], e['sha256'], e['is_text']) for e in entries] == [
            ('references/guide.md', _sha(b'# Guide'), True), ('assets/logo.png', _sha(b'\x89PNG\x00'), False),
        ]

    def test_the_root_layout_keeps_a_nested_skill_md_as_an_ordinary_file(self, env):
        content, entries = env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'examples/SKILL.md': 'x'}))

        assert content == SKILL_MD and [e['path'] for e in entries] == ['examples/SKILL.md']

    def test_operating_system_litter_is_ignored(self, env):
        _, entries = env.export.read_skill_zip(_zip({
            'forms/SKILL.md': SKILL_MD, '__MACOSX/forms/._SKILL.md': 'x', 'forms/.DS_Store': 'x', 'forms/a.md': 'a',
        }))

        assert [e['path'] for e in entries] == ['a.md']

    @pytest.mark.parametrize('members, message', [
        ({'a/SKILL.md': SKILL_MD, 'b/SKILL.md': SKILL_MD}, 'exactly one SKILL.md.*found 2'),
        ({'a/notes.md': 'x'}, 'exactly one SKILL.md.*found 0'),
        ({'a/b/SKILL.md': SKILL_MD}, 'exactly one SKILL.md.*found 0'),
        ({'forms/SKILL.md': SKILL_MD, 'other/x.md': 'x'}, "'other/x.md' is outside the skill folder 'forms'"),
        ({'forms/SKILL.md': SKILL_MD, 'forms/../evil.md': 'x'}, 'cannot be unpacked safely'),
        ({'SKILL.md': SKILL_MD, '/etc/passwd': 'x'}, 'cannot be unpacked safely'),
        ({'SKILL.md': SKILL_MD, 'a//b.md': 'x'}, 'not a valid skill file path'),
    ])
    def test_an_archive_that_could_escape_or_is_ambiguous_is_refused(self, env, members, message):
        with pytest.raises(env.utils.SkillFileError, match=message):
            env.export.read_skill_zip(_zip(members))

    def test_a_symlink_member_is_refused(self, env):
        def as_symlink(name, info):
            if name == 'forms/link':
                info.external_attr = 0o120777 << 16

        with pytest.raises(env.utils.SkillFileError, match="'forms/link' cannot be unpacked safely"):
            env.export.read_skill_zip(_zip({'forms/SKILL.md': SKILL_MD, 'forms/link': '/etc/passwd'}, as_symlink))

    def test_an_encrypted_member_is_refused(self, env):
        archive = _flag_encrypted(_zip({'forms/SKILL.md': SKILL_MD, 'forms/a.md': 'x'}), 'forms/a.md')

        with pytest.raises(env.utils.SkillFileError, match="'forms/a.md' is encrypted"):
            env.export.read_skill_zip(archive)

    def test_the_admin_caps_bound_the_archive_before_any_member_is_read(self, env, limits, monkeypatch):
        reads = []
        monkeypatch.setattr(env.export, '_read_zip_member', lambda archive, info: reads.append(info.filename) or b'x')
        limits['max_package_size_mb'] = 1
        with pytest.raises(env.utils.SkillFileError, match='a skill version can hold at most 1.0 MB'):
            env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'a.bin': b'a' * MB, 'b.bin': b'b'}))

        limits['max_package_files'] = 1
        with pytest.raises(env.utils.SkillFileError, match='at most 1 files'):
            env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'a.md': 'a', 'b.md': 'b'}))

        limits['max_file_size_mb'] = 1
        with pytest.raises(env.utils.SkillFileError, match="'big.bin' is 1.0 MB; a skill file can be at most 1.0 MB"):
            env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'big.bin': b'x' * (MB + 1)}))
        with pytest.raises(env.utils.SkillFileError, match='SKILL.md is 1.0 MB; a skill file can be at most 1.0 MB'):
            env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD + 'x' * MB}))

        with pytest.raises(env.utils.SkillFileError, match="'A.md' is listed more than once"):
            env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'a.md': 'a', 'A.md': 'b'}))
        assert reads == []

    def test_an_empty_skill_md_is_refused(self, env):
        with pytest.raises(env.utils.SkillFileError, match='SKILL.md is empty'):
            env.export.read_skill_zip(_zip({'forms/SKILL.md': ' \n', 'forms/a.md': 'a'}))

    def test_bytes_that_are_not_a_zip_are_refused(self, env):
        with pytest.raises(env.utils.SkillFileError, match='not a valid ZIP'):
            env.export.read_skill_zip(b'---\nname: x\n---\nbody')

    def test_a_member_that_expands_far_beyond_its_stored_size_is_refused(self, env):
        def deflated(name, info):
            info.compress_type = zipfile.ZIP_DEFLATED

        archive = _zip({'forms/SKILL.md': SKILL_MD, 'forms/zeros.bin': b'\0' * (2 * MB)}, deflated)

        with pytest.raises(env.utils.SkillFileError, match="'forms/zeros.bin' cannot be unpacked safely"):
            env.export.read_skill_zip(archive)

    def test_a_member_whose_bytes_do_not_match_its_checksum_is_refused(self, env):
        archive = _zip({'forms/SKILL.md': SKILL_MD, 'forms/a.md': 'hello world'})

        with pytest.raises(env.utils.SkillFileError, match="'forms/a.md' could not be read from the ZIP"):
            env.export.read_skill_zip(archive.replace(b'hello world', b'jello world'))

    def test_a_skill_md_that_is_not_utf8_is_refused(self, env):
        with pytest.raises(env.utils.SkillFileError, match='SKILL.md is not UTF-8 text'):
            env.export.read_skill_zip(_zip({'SKILL.md': b'\xff\xfe\x00'}))


def _seed_skill(env, files=((b'# Guide', 'references/guide.md'), (b'\x89PNG\x00', 'assets/logo.png'))):
    session = env.session_of(PROJECT)
    session.add(FakeSkill())
    session.add(FakeSkillVersion(id=10))
    for data, path in files:
        env.files.write_version_file(PROJECT, SKILL, 10, path, data, session=session)
    return session


def _skill_details(monkeypatch, env, versions=({'id': 10, 'name': 'base'},)):
    data = {
        'id': SKILL, 'name': 'forms', 'description': 'Fill forms.', 'versions': list(versions),
        'version_details': {'name': 'base', 'instructions': 'Read references/guide.md first.', 'tags': [], 'meta': {}},
    }
    monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': data})


class TestExport:
    def test_a_version_with_files_exports_a_claude_layout_zip(self, env, monkeypatch):
        _seed_skill(env)
        _skill_details(monkeypatch, env)

        result = env.export.export_skill_md(PROJECT, SKILL, session=env.session_of(PROJECT))

        assert (result['filename'], result['mimetype']) == ('forms.zip', 'application/zip')
        archive = zipfile.ZipFile(io.BytesIO(result['content']))
        assert archive.namelist() == ['forms/SKILL.md', 'forms/assets/logo.png', 'forms/references/guide.md']
        assert archive.read('forms/assets/logo.png') == b'\x89PNG\x00'
        assert {info.date_time for info in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}

    def test_the_same_version_exports_the_same_bytes(self, env, monkeypatch):
        _seed_skill(env)
        _skill_details(monkeypatch, env)

        first = env.export.export_skill_md(PROJECT, SKILL, session=env.session_of(PROJECT))['content']
        second = env.export.export_skill_md(PROJECT, SKILL, session=env.session_of(PROJECT))['content']

        assert first == second

    def test_a_version_without_files_exports_the_markdown_it_exported_before(self, env, monkeypatch):
        _seed_skill(env, files=())
        _skill_details(monkeypatch, env)

        result = env.export.export_skill_md(PROJECT, SKILL, session=env.session_of(PROJECT))

        assert result == {
            'ok': True,
            'filename': 'forms.md',
            'content': '---\nname: forms\ndescription: Fill forms.\n---\n\nRead references/guide.md first.',
            'mimetype': 'text/markdown; charset=utf-8',
        }

    def test_reimporting_the_export_gives_the_same_manifest_hashes(self, env, monkeypatch):
        session = _seed_skill(env)
        _skill_details(monkeypatch, env)
        exported = env.export.export_skill_md(PROJECT, SKILL, session=session)['content']

        _, entries = env.export.read_skill_zip(exported)

        source = env.files.list_version_manifest(session, 10)
        assert sorted((e['path'], e['sha256'], e['size']) for e in entries) == sorted(
            (e['path'], e['sha256'], e['size']) for e in source)


class TestImport:
    def test_the_zip_files_are_stored_before_the_import_transaction_and_land_on_its_base_version(self, env, monkeypatch):
        target = env.session_of(PROJECT)
        target.add(FakeSkillVersion(id=55, skill_id=SKILL))
        imported = []
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: imported.append(
            (kwargs['name'], env.storage.keys(PROJECT))) or types.SimpleNamespace(id=SKILL, versions={'base': 55}))
        monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': {'id': SKILL}})
        content, entries = env.export.read_skill_zip(_zip({'forms/SKILL.md': SKILL_MD, 'forms/guide.md': '# G'}))

        env.export.import_skill_md(PROJECT, content, author_id=1, session=target, files=entries)

        assert imported == [('forms', [_sha(b'# G')])]
        assert [(f.path, f.sha256) for f in target.rows[FakeSkillVersionFile]] == [('guide.md', _sha(b'# G'))]
        assert env.storage.keys(PROJECT) == [_sha(b'# G')]

    def test_an_over_long_skill_md_is_refused_before_any_file_is_stored(self, env, limits, monkeypatch):
        limits['max_instructions_length'] = 10
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: pytest.fail('imported'))
        content, entries = env.export.read_skill_zip(_zip({'forms/SKILL.md': SKILL_MD, 'forms/guide.md': '# G'}))

        with pytest.raises(env.utils.SkillInstructionsTooLongError):
            env.export.import_skill_md(PROJECT, content, author_id=1, session=env.session_of(PROJECT), files=entries)

        assert env.storage.objects == {}

    def test_a_markdown_import_writes_no_file(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: types.SimpleNamespace(id=SKILL, versions={'base': 55}))
        monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': {'id': SKILL}})

        env.export.import_skill_md(PROJECT, SKILL_MD, author_id=1, session=env.session_of(PROJECT))

        assert env.session_of(PROJECT).rows[FakeSkillVersionFile] == [] and env.storage.objects == {}

    @pytest.mark.parametrize('filename, accepted', [('forms.md', True), ('forms.ZIP', True), ('forms.txt', False)])
    def test_the_import_accepts_markdown_and_zip(self, env, filename, accepted):
        if accepted:
            env.export.validate_skill_import_filename(filename)
        else:
            with pytest.raises(ValueError, match='.md or .zip'):
                env.export.validate_skill_import_filename(filename)


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
