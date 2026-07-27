"""Unit test fixtures - pure functions, no I/O."""
import sys
import pytest
from typing import Generator


@pytest.fixture(scope="function")
def isolated_sys_modules() -> Generator[dict, None, None]:
    """Temporarily isolate sys.modules for module stubbing.

    Yields the original sys.modules dict. Restores state after test.
    """
    original_modules = sys.modules.copy()
    try:
        yield sys.modules
    finally:
        added_keys = set(sys.modules.keys()) - set(original_modules.keys())
        for key in added_keys:
            sys.modules.pop(key, None)
        for key, value in original_modules.items():
            if sys.modules.get(key) is not value:
                sys.modules[key] = value


@pytest.fixture
def pylon_stubs(isolated_sys_modules):
    """Install minimal Pylon runtime stubs."""
    from tests.stubs.pylon_runtime import install_pylon_stubs
    return install_pylon_stubs()


@pytest.fixture
def tools_stubs(isolated_sys_modules):
    """Install minimal tools package stubs."""
    from tests.stubs.tools import install_tools_stubs
    return install_tools_stubs()


@pytest.fixture
def orm_stubs(isolated_sys_modules):
    """Install SQLAlchemy ORM stubs."""
    from tests.stubs.orm import install_orm_stubs
    return install_orm_stubs()
