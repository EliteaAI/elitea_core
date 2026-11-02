"""#7185: one path per skill version whatever its case - ``FORMS.md`` and ``forms.md`` cannot both be stored.

Loads the real ``models/skill.py`` on real SQLAlchemy, compiles the index for PostgreSQL and creates the table in an
in-memory SQLite database, where the same functional unique index refuses a second path that differs only in case.
"""
import enum
import pathlib
import sys
import types

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import CreateIndex

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[1].parent
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413

PKG = 'elitea_core_7185_model'
INDEX = 'uq_skill_version_files_path_ci'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


@pytest.fixture
def skill_models(isolated_sys_modules):
    class Base(DeclarativeBase):
        pass

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(Base=Base)
    tools.db_tools = types.SimpleNamespace(AbstractBaseMixin=type('AbstractBaseMixin', (), {}))
    tools.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA='tenant')
    enums = types.ModuleType(f'{PKG}.models.enums.all')
    enums.PublishStatus = enum.Enum('PublishStatus', {'draft': 'draft'})
    mixins = types.ModuleType(f'{PKG}.models.mixins')
    mixins.AbstractLikesMixin = type('AbstractLikesMixin', (), {})
    stubs = {'tools': tools, enums.__name__: enums, mixins.__name__: mixins}
    for name in (PKG, f'{PKG}.models', f'{PKG}.models.enums'):
        stubs[name] = _package(name)
    return load_module_with_stubs(PLUGIN_ROOT / 'models' / 'skill.py', f'{PKG}.models.skill', stubs)


def _index(model):
    return next(index for index in model.__table__.indexes if index.name == INDEX)


def test_the_path_is_unique_per_version_ignoring_case(skill_models):
    ddl = ' '.join(str(CreateIndex(_index(skill_models.SkillVersionFile)).compile(dialect=postgresql.dialect())).split())

    assert ddl == f'CREATE UNIQUE INDEX {INDEX} ON tenant.skill_version_files (skill_version_id, lower(path))'


def test_a_second_path_differing_only_in_case_is_refused_by_the_database(skill_models):
    table = skill_models.SkillVersionFile.__table__
    engine = create_engine('sqlite://').execution_options(schema_translate_map={'tenant': None})
    row = {'skill_version_id': 10, 'size': 1, 'sha256': 'a' * 64, 'content_type': 'text/plain', 'is_text': True}
    with engine.begin() as connection:
        table.create(connection)
        connection.execute(table.insert(), {**row, 'path': 'FORMS.md'})
        connection.execute(table.insert(), {**row, 'path': 'forms.md', 'skill_version_id': 11})

        with pytest.raises(IntegrityError):
            connection.execute(table.insert(), {**row, 'path': 'forms.md'})
