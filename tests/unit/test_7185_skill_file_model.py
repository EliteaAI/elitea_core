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
from fixtures.identity import fresh_sqlalchemy  # noqa: E402  pylint: disable=C0413

PKG = 'elitea_core_7185_model'
INDEX = 'uq_skill_version_files_path_ci'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


@pytest.fixture
def skill_models(isolated_sys_modules):
    fresh_sqlalchemy()

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


def _release_text():
    return ' '.join((PLUGIN_ROOT / 'db_migrations.txt').read_text().split())


def _release_table_definitions():
    """Column definitions of the CREATE TABLE statement for skill_version_files, keyed by column name."""
    text = _release_text()
    start = text.index('CREATE TABLE IF NOT EXISTS {pid}.skill_version_files (')
    body = text[start:text.index(');', start)]
    parts = [part.strip() for part in body.split('(', 1)[1].split(', ')]
    return {part.split()[0]: part for part in parts}


def _model_type(column):
    compiled = column.type.compile(dialect=postgresql.dialect())
    return compiled.replace(' WITHOUT TIME ZONE', '')


def test_the_release_script_creates_the_table_before_the_index_with_the_model_columns(skill_models):
    text = _release_text()
    start = text.index('CREATE TABLE IF NOT EXISTS {pid}.skill_version_files (')

    assert list(_release_table_definitions()) == [column.name for column in skill_models.SkillVersionFile.__table__.columns]
    assert start < text.index(f'CREATE UNIQUE INDEX IF NOT EXISTS {INDEX} ON {{pid}}.skill_version_files')


def test_the_release_script_columns_match_the_model_types_and_nullability(skill_models):
    definitions = _release_table_definitions()

    for column in skill_models.SkillVersionFile.__table__.columns:
        if column.primary_key:
            expected = f'{column.name} SERIAL PRIMARY KEY'
        else:
            expected = f'{column.name} {_model_type(column)}{"" if column.nullable else " NOT NULL"}'
        assert definitions[column.name].startswith(expected), f'{column.name}: {definitions[column.name]!r}'


def test_the_release_script_column_defaults_match_the_model(skill_models):
    definitions = _release_table_definitions()

    assert definitions['created_at'].endswith('NOT NULL DEFAULT NOW()')
    assert str(skill_models.SkillVersionFile.__table__.c.created_at.server_default.arg).lower() == 'now()'


def test_the_release_script_foreign_key_points_to_the_version_and_cascades(skill_models):
    column = skill_models.SkillVersionFile.__table__.c.skill_version_id
    (foreign_key,) = column.foreign_keys

    assert foreign_key.ondelete == 'CASCADE'
    assert foreign_key.target_fullname == 'tenant.skill_versions.id'
    assert _release_table_definitions()['skill_version_id'].endswith(
        'REFERENCES {pid}.skill_versions (id) ON DELETE CASCADE'
    )


def test_the_release_script_creates_every_model_index_if_missing(skill_models):
    text = _release_text()
    indexes = skill_models.SkillVersionFile.__table__.indexes
    plain = sorted(tuple(column.name for column in index.columns) for index in indexes if not index.unique)

    assert plain == [('sha256',), ('skill_version_id',)]
    for index in indexes:
        if not index.unique:
            (column,) = index.columns
            assert f'CREATE INDEX IF NOT EXISTS {index.name} ON {{pid}}.skill_version_files ({column.name});' in text
    assert 'INDEX IF NOT EXISTS ix_skill_version_files_' not in text
    assert (
        f'CREATE UNIQUE INDEX IF NOT EXISTS {INDEX} ON {{pid}}.skill_version_files (skill_version_id, lower(path));' in text
    )
