"""#7589: a Claude plugin ZIP imports every skill and command it bundles, each as its own skill, in one transaction.

Runs the real ``utils/skill_export_import.py`` over the in-memory session and bucket fakes of ``skill_file_env``.
"""
import io
import pathlib
import sys
import types
import zipfile

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import install  # noqa: E402  pylint: disable=C0413

PROJECT = 7
MANIFEST = '{"name": "pdf-viewer", "version": "0.2.0"}'
DEBUG_MD = '---\nname: debug\ndescription: Structured debugging.\n---\n\n# /debug\n\nReproduce first.\n'
OPEN_MD = (
    '---\ndescription: Open a PDF in the viewer\nargument-hint: "[path-or-url]"\n---\n\n'
    '# Open PDF\n\nCall display_pdf.\n'
)


@pytest.fixture
def env(monkeypatch, isolated_sys_modules):
    return install(monkeypatch, {})


def _zip(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _plugin(prefix='', **extra):
    return _zip({
        f'{prefix}.claude-plugin/plugin.json': MANIFEST,
        f'{prefix}.mcp.json': '{}',
        f'{prefix}README.md': '# PDF Viewer',
        f'{prefix}CONNECTORS.md': '# Connectors',
        f'{prefix}skills/debug/SKILL.md': DEBUG_MD,
        f'{prefix}skills/debug/references/steps.md': '# Steps',
        f'{prefix}commands/open.md': OPEN_MD,
        **{f'{prefix}{name}': data for name, data in extra.items()},
    })


class TestReadPluginZip:
    def test_every_skill_folder_and_command_of_the_plugin_is_one_package_and_the_rest_is_left_out(self, env):
        packages = env.export.read_plugin_zip(_plugin())

        assert [(source, [entry['path'] for entry in files]) for source, _, files in packages] == [
            ('skills/debug/SKILL.md', ['references/steps.md']),
            ('commands/open.md', []),
        ]
        assert packages[0][1] == env.export.parse_skill_md(DEBUG_MD)

    def test_a_plugin_zipped_inside_its_folder_is_read_the_same(self, env):
        packages = env.export.read_plugin_zip(_plugin('pdf-viewer/'))

        assert [source for source, _, _ in packages] == [
            'pdf-viewer/skills/debug/SKILL.md', 'pdf-viewer/commands/open.md']

    def test_a_command_becomes_a_skill_named_after_its_file_that_keeps_its_frontmatter(self, env):
        _, parsed, _ = env.export.read_plugin_zip(_plugin())[1]

        assert parsed['frontmatter'] == {
            'description': 'Open a PDF in the viewer', 'argument-hint': '[path-or-url]', 'name': 'open'}
        assert parsed['body'] == '# Open PDF\n\nCall display_pdf.'

    @pytest.mark.parametrize('command', [
        '# Sign PDF\n\nPlace a signature.\n',
        '---\nargument-hint: x\n---\n\n# Sign PDF\n\nPlace a signature.\n',
    ])
    def test_an_undescribed_command_is_described_by_its_first_line_the_way_claude_code_does(self, env, command):
        _, document, _ = env.export.read_plugin_zip(_plugin(**{'commands/sign.md': command}))[2]

        assert document['frontmatter']['description'] == 'Sign PDF'

    def test_a_command_body_holding_three_dashes_keeps_its_description_and_instructions_whole(self, env):
        command = '# Deploy --- staging then prod\nRun it.\n'

        _, document, _ = env.export.read_plugin_zip(_plugin(**{'commands/deploy.md': command}))[2]

        assert document == {
            'frontmatter': {'name': 'deploy', 'description': 'Deploy --- staging then prod'},
            'body': '# Deploy --- staging then prod\nRun it.',
        }

    def test_a_command_without_frontmatter_has_its_line_endings_normalised_as_a_skill_file_does(self, env):
        crlf_command = 'Deploy it\r\nStep 2\rStep 3\r\n'
        _, document, _ = env.export.read_plugin_zip(_plugin(**{'commands/deploy.md': crlf_command}))[2]

        assert document['body'] == 'Deploy it\nStep 2\nStep 3'

    @pytest.mark.parametrize('command, description', [
        ('#\nTake notes about $ARGUMENTS\n', 'Take notes about $ARGUMENTS'),
        ('#\n##\n', 'notes'),
    ])
    def test_a_bare_heading_is_skipped_when_describing_a_command(self, env, command, description):
        _, document, _ = env.export.read_plugin_zip(_plugin(**{'commands/notes.md': command}))[2]

        assert document['frontmatter']['description'] == description

    def test_a_zip_without_a_plugin_manifest_is_not_a_plugin(self, env):
        assert env.export.read_plugin_zip(_zip({'forms/SKILL.md': DEBUG_MD})) is None

    def test_a_manifest_nested_deeper_than_one_folder_is_not_a_plugin(self, env):
        assert env.export.read_plugin_zip(_plugin('a/b/')) is None

    def test_a_plugin_without_skills_or_commands_is_read_as_a_single_skill(self, env):
        archive = _zip({'.claude-plugin/plugin.json': MANIFEST, 'SKILL.md': DEBUG_MD})

        assert env.export.read_plugin_zip(archive) is None
        assert env.export.read_skill_zip(archive)[0] == DEBUG_MD

    @pytest.mark.parametrize('file_name, skill_name', [
        ('review_pr.md', 'review-pr'),
        ('revise-claude-md.md', 'revise-md'),
        ('Fill Form.md', 'fill-form'),
        (f'{"a" * 70}.md', 'a' * 64),
    ])
    def test_a_command_file_name_becomes_a_valid_skill_name(self, env, file_name, skill_name):
        _, document, _ = env.export.read_plugin_zip(_plugin(**{f'commands/{file_name}': OPEN_MD}))[2]

        assert document['frontmatter']['name'] == skill_name

    def test_a_command_whose_file_name_leaves_no_skill_name_is_refused(self, env):
        with pytest.raises(env.utils.SkillFileError, match="'commands/claude.md': .*no valid skill name"):
            env.export.read_plugin_zip(_plugin(**{'commands/claude.md': OPEN_MD}))

    def test_the_plugin_as_a_whole_is_bounded_by_the_package_caps_before_reading(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'get_max_package_bytes', lambda: 2 * 1024)
        monkeypatch.setattr(env.export, '_read_zip_member', lambda *args: pytest.fail('a member was read'))
        skills = {f'skills/s{index}/SKILL.md': DEBUG_MD + 'x' * 900 for index in range(3)}

        with pytest.raises(env.utils.SkillFileError, match='a plugin can hold at most 2 KB'):
            env.export.read_plugin_zip(_plugin(**skills))

    def test_files_under_commands_that_are_not_commands_do_not_count_toward_the_caps(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'get_max_package_bytes', lambda: 2 * 1024)

        packages = env.export.read_plugin_zip(_plugin(**{'commands/assets/logo.png': b'x' * 4096}))

        assert [source for source, _, _ in packages] == ['skills/debug/SKILL.md', 'commands/open.md']

    def test_the_plugin_file_count_is_bounded_by_the_package_cap(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'get_max_package_files', lambda: 2)

        with pytest.raises(env.utils.SkillFileError, match='at most 2 skill and command files'):
            env.export.read_plugin_zip(_plugin())

    def test_an_unsafe_member_of_a_skill_is_refused(self, env):
        with pytest.raises(env.utils.SkillFileError, match='cannot be unpacked safely'):
            env.export.read_plugin_zip(_plugin(**{'skills/debug/../../escape.md': 'x'}))

    def test_an_unsafe_member_the_import_never_reads_is_left_alone(self, env):
        packages = env.export.read_plugin_zip(_plugin(**{'../hooks/escape.sh': 'x'}))

        assert [source for source, _, _ in packages] == ['skills/debug/SKILL.md', 'commands/open.md']

    def test_a_zip_holding_more_than_one_plugin_is_refused(self, env):
        archive = _zip({'design/.claude-plugin/plugin.json': MANIFEST, 'design/skills/a/SKILL.md': DEBUG_MD,
                        'engineering/.claude-plugin/plugin.json': MANIFEST, 'engineering/skills/b/SKILL.md': DEBUG_MD})

        with pytest.raises(env.utils.SkillFileError, match='more than one plugin'):
            env.export.read_plugin_zip(archive)

    def test_commands_of_different_folders_keep_different_names(self, env):
        packages = env.export.read_plugin_zip(_plugin(**{'commands/frontend/test.md': OPEN_MD,
                                                         'commands/backend/test.md': OPEN_MD}))

        names = [document['frontmatter']['name'] for _, document, _ in packages[1:]]
        assert sorted(names) == ['backend-test', 'frontend-test', 'open']

    def test_an_empty_command_names_its_file(self, env):
        with pytest.raises(env.utils.SkillFileError, match="commands/blank.md' is empty"):
            env.export.read_plugin_zip(_plugin(**{'commands/blank.md': ' \n'}))


class TestImportPluginSkills:
    def test_every_package_is_imported_as_its_own_skill_in_one_committed_transaction(self, env, monkeypatch):
        imported = []
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: imported.append(kwargs['name']) or
                            types.SimpleNamespace(id=len(imported), versions={'base': 50 + len(imported)}))
        monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': {'id': kwargs['skill_id']}})

        result = env.export.import_plugin_skills(PROJECT, env.export.read_plugin_zip(_plugin()), author_id=1)

        assert imported == ['debug', 'open']
        assert [skill['id'] for skill in result] == [1, 2]
        assert env.session_of(PROJECT).commits == 1

    def test_one_invalid_skill_rolls_back_the_whole_plugin_and_names_its_file(self, env, monkeypatch):
        imported = []
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: imported.append(kwargs['name']) or
                            types.SimpleNamespace(id=1, versions={'base': 51}))
        monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': {'id': 1}})
        packages = env.export.read_plugin_zip(_plugin(**{'skills/broken/SKILL.md': '---\nname: broken\n---\n\nBody\n'}))

        with pytest.raises(env.utils.SkillFileError, match="'skills/broken/SKILL.md': .*description") as raised:
            env.export.import_plugin_skills(PROJECT, packages, author_id=1)

        assert raised.value.http_status == 400
        assert imported == [] and env.session_of(PROJECT).commits == 0

    def test_an_invalid_skill_name_stores_no_file_of_any_skill(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: pytest.fail('imported'))
        bad_name = DEBUG_MD.replace('name: debug', 'name: Bad_Name')
        packages = env.export.read_plugin_zip(_plugin(**{'skills/z/SKILL.md': bad_name}))

        with pytest.raises(env.utils.SkillFileError, match="'skills/z/SKILL.md': "):
            env.export.import_plugin_skills(PROJECT, packages, author_id=1)

        assert env.storage.objects == {}

    def test_a_description_the_skill_cannot_hold_stores_no_file_and_names_the_field(self, env, monkeypatch):
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: pytest.fail('imported'))
        long_description = f"---\ndescription: {'x' * 2305}\n---\n\nBody\n"
        packages = env.export.read_plugin_zip(_plugin(**{'commands/long.md': long_description}))

        with pytest.raises(env.utils.SkillFileError, match="'commands/long.md': .*for description"):
            env.export.import_plugin_skills(PROJECT, packages, author_id=1)

        assert env.storage.objects == {}

    def test_a_description_taken_from_a_long_first_line_is_cut_to_what_the_skill_holds(self, env):
        long_line = '# ' + 'x' * 3000 + '\n\nBody\n'
        _, document, _ = env.export.read_plugin_zip(_plugin(**{'commands/long.md': long_line}))[2]

        assert document['frontmatter']['description'] == 'x' * 2304

    def test_a_falsy_description_falls_back_to_the_name_as_a_single_skill_import_does(self, env, monkeypatch):
        created = []
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: created.append(kwargs['description']) or
                            types.SimpleNamespace(id=1, versions={'base': 51}))
        monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': {'id': 1}})
        packages = env.export.read_plugin_zip(_zip({
            '.claude-plugin/plugin.json': MANIFEST,
            'skills/zeta/SKILL.md': '---\nname: zeta\ndescription: []\n---\n\nBody\n',
        }))

        env.export.import_plugin_skills(PROJECT, packages, author_id=1)

        assert created == [[]]

    def test_the_files_of_every_skill_are_stored_once_before_the_transaction_starts(self, env, monkeypatch):
        stage_calls = []
        stage = env.export.stage_version_files
        monkeypatch.setattr(env.export, 'stage_version_files',
                            lambda *args: stage_calls.append(args[1]) or stage(*args))
        stored_when_imported = []
        monkeypatch.setattr(env.export, 'import_skill', lambda **kwargs: stored_when_imported.append(
            len(env.storage.keys(PROJECT))) or types.SimpleNamespace(id=1, versions={'base': 51}))
        monkeypatch.setattr(env.export, 'get_skill_details', lambda **kwargs: {'data': {'id': 1}})
        packages = env.export.read_plugin_zip(_plugin(**{'skills/z/SKILL.md': DEBUG_MD.replace('debug', 'zeta'),
                                                         'skills/z/notes.md': '# Notes'}))

        env.export.import_plugin_skills(PROJECT, packages, author_id=1)

        staged_paths = [[entry['path'] for entry in entries] for entries in stage_calls]
        assert staged_paths == [['references/steps.md', 'notes.md']]
        assert stored_when_imported == [2, 2, 2]
