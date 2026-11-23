"""#7626: a refused skill import names the problem in the skill editor's words, without pydantic's technical text.

Runs the real ``utils/skill_export_import.py`` and ``models/pd/skill.py`` over the fakes of ``skill_file_env``.
"""
import pathlib
import sys
import types

import pytest
from pydantic import BaseModel

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import PKG, install, load  # noqa: E402  pylint: disable=C0413

PROJECT = 7
EDITOR_NAME_RULE = (
    'Name must be lowercase letters, digits and hyphens only (no spaces), '
    'and cannot start or end with a hyphen or contain consecutive hyphens'
)


class AnyModelModule(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return type(name, (BaseModel,), {})


@pytest.fixture
def env(monkeypatch, isolated_sys_modules):
    environment = install(monkeypatch, {})
    for name in (f'{PKG}.models.pd.collection_base', f'{PKG}.models.pd.tag', f'{PKG}.models.pd.skill_version'):
        monkeypatch.setitem(sys.modules, name, AnyModelModule(name))
    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API = 128
    monkeypatch.setitem(sys.modules, constants.__name__, constants)
    load(monkeypatch, 'models/enums/all.py', f'{PKG}.models.enums.all')
    skill_models = load(monkeypatch, 'models/pd/skill.py', f'{PKG}.models.pd.skill')
    monkeypatch.setattr(environment.utils, 'SkillCreateModel', skill_models.SkillCreateModel)
    monkeypatch.setattr(environment.export, 'import_skill', lambda **kwargs: pytest.fail('imported'))
    return environment


def _skill_md(name, description='Reads PDFs.'):
    return f'---\nname: {name}\ndescription: {description}\n---\n\nBody\n'


@pytest.mark.parametrize(('name', 'description', 'message'), [
    ('pdf-hel--per5', 'Reads PDFs.', EDITOR_NAME_RULE),
    ('-pdf', 'Reads PDFs.', EDITOR_NAME_RULE),
    ('a' * 65, 'Reads PDFs.', 'Name must be at most 64 characters'),
    ('claude-pdf', 'Reads PDFs.', 'Name cannot contain "claude" or "anthropic"'),
    ('pdf', 'x' * 2305, 'Description must be at most 2304 characters'),
    ('2024', 'Reads PDFs.', 'Name: Input should be a valid string'),
    ('pdf', '123', 'Description: Input should be a valid string'),
])
def test_a_refused_import_raises_the_editor_message_alone(env, name, description, message):
    with pytest.raises(ValueError) as refused:
        env.export.import_skill_md(project_id=PROJECT, content=_skill_md(name, description), author_id=1)

    assert str(refused.value) == message
