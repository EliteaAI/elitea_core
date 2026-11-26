"""#7764: skills_limits hands the UI the package caps a skill ZIP import enforces, so it can refuse before uploading.

Loads the real endpoint over the ``skill_file_env`` fakes, whose ``tools.this`` carries the admin ``skills`` config,
and checks the endpoint answers with the very caps ``read_skill_zip`` refuses by.
"""
import io
import pathlib
import sys
import types
import zipfile

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fixtures.skill_file_env import PKG, install, load  # noqa: E402  pylint: disable=C0413

MB = 1024 * 1024
SKILL_MD = '---\nname: forms\ndescription: Fill forms.\n---\n\nBody.\n'


def _passthrough(*args, **kwargs):  # pylint: disable=W0613
    return lambda func: func


@pytest.fixture
def limits():
    return {}


@pytest.fixture
def env(monkeypatch, limits, isolated_sys_modules):  # pylint: disable=W0613
    env = install(monkeypatch, limits)
    env.tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func, with_modes=lambda params: params,
    )
    env.tools.auth.decorators = types.SimpleNamespace(check_api=_passthrough)
    env.tools.config.ADMINISTRATION_MODE = 'administration'
    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'
    monkeypatch.setitem(sys.modules, constants.__name__, constants)
    for name in (f'{PKG}.api', f'{PKG}.api.v2'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    env.api = load(monkeypatch, 'api/v2/skills_limits.py', f'{PKG}.api.v2.skills_limits')
    return env


def _get(env):
    body, status = env.api.PromptLibAPI().get(project_id=2)
    assert status == 200
    return body


def _zip(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_STORED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def test_the_defaults_are_returned_in_bytes(env):
    assert _get(env) == {
        'max_skills_per_agent': 50, 'max_file_bytes': 10 * MB, 'max_package_bytes': 30 * MB, 'max_package_files': 500,
    }


def test_raised_admin_caps_are_returned(env, limits):
    limits.update(max_file_size_mb=12, max_package_size_mb=50, max_package_files=900)
    body = _get(env)
    assert (body['max_file_bytes'], body['max_package_bytes'], body['max_package_files']) == (12 * MB, 50 * MB, 900)


def test_a_file_one_byte_over_the_returned_cap_is_what_the_import_refuses(env, limits):
    limits['max_file_size_mb'] = 1
    cap = _get(env)['max_file_bytes']

    _, entries = env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'a.bin': b'a' * cap}))
    assert [entry['size'] for entry in entries] == [cap]
    with pytest.raises(env.utils.SkillFileError, match="'a.bin' is 1.0 MB; a skill file can be at most 1.0 MB"):
        env.export.read_skill_zip(_zip({'SKILL.md': SKILL_MD, 'a.bin': b'a' * (cap + 1)}))
