"""The draft rate limit forgets a user once the user's drafts leave the window."""
import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

SOURCE = Path(__file__).resolve().parents[2] / 'utils' / 'schedule_draft_handler.py'


@pytest.fixture
def handler():
    names = ['flask', 'pydantic', 'pylon', 'pylon.core', 'pylon.core.tools', 'tools']
    pkg = 'plugins.elitea_core.utils'
    rels = ['models.pd.generate_schedule_draft', 'utils.draft_llm_utils', 'utils.exceptions', 'utils.predict_utils',
            'utils.schedule_access', 'utils.schedule_draft_utils', 'utils.service_prompt_utils', 'utils.utils']
    stubs = {n: mock.MagicMock() for n in names}
    stubs.update({f'plugins.elitea_core.{r}': mock.MagicMock() for r in rels})
    stubs['plugins.elitea_core.utils'] = types.ModuleType(pkg)
    stubs['plugins.elitea_core.utils'].__path__ = []
    stubs['plugins.elitea_core'] = types.ModuleType('plugins.elitea_core')
    stubs['plugins.elitea_core'].__path__ = []
    stubs['plugins'] = types.ModuleType('plugins')
    stubs['plugins'].__path__ = []
    with mock.patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location(f'{pkg}.schedule_draft_handler', SOURCE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module


def test_idle_user_is_dropped_after_the_window(handler):
    with mock.patch.object(handler.time, 'monotonic', side_effect=[0, 1000]):
        assert handler._wait_seconds(1, 1) == 0
        assert handler._wait_seconds(1, 2) == 0
    assert list(handler._recent_drafts) == [(1, 2)]


def test_user_over_the_limit_waits(handler):
    with mock.patch.object(handler.time, 'monotonic', return_value=5):
        waits = [handler._wait_seconds(1, 1) for _ in range(handler._DRAFTS_PER_WINDOW + 1)]
    assert waits[:-1] == [0] * handler._DRAFTS_PER_WINDOW
    assert waits[-1] > 0
