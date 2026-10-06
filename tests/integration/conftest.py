"""Integration test fixtures - database, sessions, etc."""
import pathlib
import sys

import pytest

# Add fixtures directory to path for imports
TESTS_DIR = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.models import FakeSession


@pytest.fixture
def fake_db_session():
    """Lightweight fake session for tests that don't need real DB."""
    return FakeSession(registry={})


@pytest.fixture
def sio_all():
    """`sio/all.py` loaded with the chat surface stubbed; see fixtures/sio_harness.py."""
    from fixtures.sio_harness import load_sio_module, unload_sio_module
    module = load_sio_module()
    yield module
    unload_sio_module()
