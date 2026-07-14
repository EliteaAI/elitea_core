#!/usr/bin/env python3
"""Test runner that isolates tests from Pylon runtime dependencies.

This runner works by:
1. Installing minimal stubs for pylon/tools/etc BEFORE pytest loads
2. Running pytest from the tests/ directory with --noconftest for parent
3. Using importlib mode to avoid __init__.py chain loading

Run from plugin root:
    python tests/run_tests.py [pytest args...]

Examples:
    python tests/run_tests.py -v
    python tests/run_tests.py unit/utils/test_tool_call_dedup.py -v
    python tests/run_tests.py -m unit -v
    python tests/run_tests.py --collect-only
"""
import sys
import types
import pathlib
import os


def install_minimal_stubs():
    """Install minimal stubs - just enough for conftest.py to load."""

    # --- Pylon stubs ---
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")

    class _Log:
        @staticmethod
        def info(*a, **k): pass
        @staticmethod
        def warning(*a, **k): pass
        @staticmethod
        def error(*a, **k): pass
        @staticmethod
        def debug(*a, **k): pass
        @staticmethod
        def exception(*a, **k): pass
        @staticmethod
        def critical(*a, **k): pass

    class _Web:
        def __getattr__(self, name):
            def decorator_factory(*a, **k):
                def decorator(func): return func
                return decorator
            return decorator_factory

    class _Module:
        @staticmethod
        def require(*a, **k):
            def decorator(func): return func
            return decorator

    tools_mod.log = _Log()
    tools_mod.web = _Web()
    tools_mod.module = _Module()

    sys.modules["pylon"] = pylon
    sys.modules["pylon.core"] = core
    sys.modules["pylon.core.tools"] = tools_mod

    # --- tools package stubs ---
    tools = types.ModuleType("tools")

    class _FakeDB:
        @staticmethod
        def get_session(project_id=None):
            class FakeSess:
                def query(self, m): return self
                def filter(self, *a): return self
                def first(self): return None
                def all(self): return []
            return FakeSess()

        @staticmethod
        def with_project_schema(project_id):
            def decorator(func): return func
            return decorator

    class _FakeAuthDecorators:
        @staticmethod
        def check_api(*a, **k):
            def decorator(func): return func
            return decorator
        @staticmethod
        def check_slot(*a, **k):
            def decorator(func): return func
            return decorator

    class _FakeAuth:
        decorators = _FakeAuthDecorators()

    class _FakeConfig:
        @staticmethod
        def get(key, default=None):
            return default

    class _FakeVaultClient:
        def __init__(self, *a, **k): pass
        def get_all_secrets(self): return {}
        @classmethod
        def from_project(cls, project_id): return cls()

    class _FakeRpcTools:
        @staticmethod
        def wrap_exceptions(*a, **k):
            def decorator(func): return func
            return decorator

    tools.db = _FakeDB()
    tools.auth = _FakeAuth()
    tools.config = _FakeConfig()
    tools.context = types.SimpleNamespace()
    tools.this = types.SimpleNamespace()
    tools.VaultClient = _FakeVaultClient
    tools.c = _FakeConfig()
    tools.rpc_tools = _FakeRpcTools()

    sys.modules["tools"] = tools


if __name__ == "__main__":
    # Install minimal stubs first
    install_minimal_stubs()

    import pytest

    # Determine tests directory
    script_dir = pathlib.Path(__file__).resolve().parent
    tests_dir = script_dir
    plugin_root = tests_dir.parent

    # Change to tests directory
    os.chdir(tests_dir)

    # Remove parent from sys.path to prevent accidental imports
    parent_str = str(plugin_root)
    if parent_str in sys.path:
        sys.path.remove(parent_str)

    # Build pytest args
    args = sys.argv[1:] if len(sys.argv) > 1 else ["-v"]

    # Critical: Use importlib mode and set rootdir to tests/ to prevent
    # pytest from discovering/loading the parent plugin's __init__.py
    default_args = [
        "--import-mode=importlib",
        f"--rootdir={tests_dir}",
        f"--ignore={plugin_root}",  # Ignore parent directory
        "-p", "no:cacheprovider",
    ]

    sys.exit(pytest.main(default_args + args))
