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
