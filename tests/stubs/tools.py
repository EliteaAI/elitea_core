"""Reusable tools package stubs."""
import sys
import types


class _FakeDB:
    """Stub for tools.db."""
    @staticmethod
    def get_session(project_id=None):
        from tests.fixtures.models import FakeSession
        return FakeSession({})

    @staticmethod
    def with_project_schema(project_id):
        def decorator(func):
            return func
        return decorator


class _FakeAuth:
    """Stub for tools.auth."""
    class decorators:
        @staticmethod
        def check_api(*args, **kwargs):
            def decorator(func):
                return func
            return decorator

        @staticmethod
        def check_slot(*args, **kwargs):
            def decorator(func):
                return func
            return decorator


class _FakeConfig:
    """Stub for tools.config."""
    @staticmethod
    def get(key, default=None):
        return default


class _FakeVaultClient:
    """Stub for tools.VaultClient."""
    def __init__(self, *args, **kwargs):
        pass

    def get_all_secrets(self):
        return {}

    @classmethod
    def from_project(cls, project_id):
        return cls()


def install_tools_stubs():
    """Install minimal tools package stubs.

    Returns:
        The tools module
    """
    tools = types.ModuleType("tools")
    tools.db = _FakeDB()
    tools.auth = _FakeAuth()
    tools.config = _FakeConfig()
    tools.VaultClient = _FakeVaultClient
    tools.this = types.SimpleNamespace()

    sys.modules["tools"] = tools
    return tools
