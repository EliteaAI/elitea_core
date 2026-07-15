"""Root conftest.py - shared fixtures available to all tests.

This conftest is designed to work WITHOUT the Pylon runtime by using
stubs for pylon/tools imports. Run tests via:

    python tests/run_tests.py [pytest args...]
"""
import pathlib
import pytest

# PLUGIN_ROOT is computed relative to this file, no imports needed
PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def plugin_root() -> pathlib.Path:
    """Absolute path to the elitea_core plugin root."""
    return PLUGIN_ROOT


@pytest.fixture(scope="session")
def utils_path(plugin_root: pathlib.Path) -> pathlib.Path:
    """Path to the utils/ directory."""
    return plugin_root / "utils"


@pytest.fixture(scope="session")
def models_path(plugin_root: pathlib.Path) -> pathlib.Path:
    """Path to the models/ directory."""
    return plugin_root / "models"


def pytest_collection_modifyitems(config, items):
    """Auto-mark tests by directory."""
    for item in items:
        path_str = str(item.fspath)
        if "/unit/" in path_str:
            item.add_marker(pytest.mark.unit)
        elif "/integration/" in path_str:
            item.add_marker(pytest.mark.integration)

        if "/utils/" in path_str:
            item.add_marker(pytest.mark.utils)
        elif "/models/" in path_str:
            item.add_marker(pytest.mark.models)
