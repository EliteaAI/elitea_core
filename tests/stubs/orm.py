"""Reusable SQLAlchemy ORM stubs."""
import sys
import types


def install_orm_stubs():
    """Install minimal SQLAlchemy stubs.

    Returns:
        The sqlalchemy.orm module stub
    """
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy_orm = types.ModuleType("sqlalchemy.orm")
    sqlalchemy_attrs = types.ModuleType("sqlalchemy.orm.attributes")

    sqlalchemy_attrs.flag_modified = lambda *args, **kwargs: None
    sqlalchemy_orm.selectinload = lambda *args, **kwargs: None
    sqlalchemy_orm.joinedload = lambda *args, **kwargs: None

    sys.modules["sqlalchemy"] = sqlalchemy
    sys.modules["sqlalchemy.orm"] = sqlalchemy_orm
    sys.modules["sqlalchemy.orm.attributes"] = sqlalchemy_attrs

    return sqlalchemy_orm


def mock_selectinload(*args, **kwargs):
    """No-op selectinload for testing."""
    return None
