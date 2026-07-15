"""Reusable fake ORM models for testing.

These fakes mirror the ORM shape that the walkers and utils read,
allowing tests to run without a real database.

Usage:
    from fixtures.models import FakeTool, FakeVersion, FakeSession
"""

_TOOL_ID_SEQ = [0]


class _Criterion:
    """Fake SQLAlchemy filter criterion."""
    def __init__(self, value):
        self.value = value


class _IdColumn:
    """Fake ORM column that supports == filtering."""
    def __eq__(self, other):
        return _Criterion(other)


class FakeTool:
    """Fake EliteATool for testing.

    Mirrors the ORM shape: type, name, id, settings with application_id/version_id.
    """
    def __init__(self, app_id: int, ver_id: int, name: str = None, tool_id: int = None):
        self.type = 'application'
        self.name = name or f'app_{app_id}'
        if tool_id is None:
            _TOOL_ID_SEQ[0] += 1
            tool_id = 1000 + _TOOL_ID_SEQ[0]
        self.id = tool_id
        self.settings = {
            'application_id': app_id,
            'application_version_id': ver_id
        }


class FakeVersion:
    """Fake ApplicationVersion for testing.

    Mirrors the ORM shape: id, agent_type, tools list.
    """
    def __init__(self, ver_id: int, agent_type: str = 'openai', tools: list = None):
        self.id = ver_id
        self.agent_type = agent_type
        self.tools = tools or []


class _Query:
    """Fake SQLAlchemy query object."""
    def __init__(self, registry: dict):
        self._registry = registry
        self._vid = None

    def filter(self, *criteria):
        for c in criteria:
            if hasattr(c, 'value'):
                self._vid = c.value
        return self

    def options(self, *args, **kwargs):
        return self

    def first(self):
        return self._registry.get(self._vid)

    def all(self):
        return list(self._registry.values())


class FakeSession:
    """Fake SQLAlchemy session for testing.

    Args:
        registry: Dict mapping version_id -> FakeVersion
    """
    def __init__(self, registry: dict):
        self._registry = registry
        self.query_count = 0

    def query(self, model):
        self.query_count += 1
        return _Query(self._registry)

    def add(self, obj):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


def reset_tool_id_seq():
    """Reset the tool ID sequence for tests that need deterministic IDs."""
    _TOOL_ID_SEQ[0] = 0
