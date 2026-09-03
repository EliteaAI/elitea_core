"""A batch run's agent call must carry the acting user (#6486 follow-up).

``get_predict_token_and_session`` stopped falling back to a project system-user PAT so that LLM
spend lands on the real member's budget. A batch run executes on the ``eval_runs`` pool, where
``predict_sio`` has neither a sid nor a live HTTP request to recover the user from, so the acting
user has to be passed down explicitly — otherwise every case of every offline-batch run fails with
"User token not found. Please create user_token".

The run row already knows who it belongs to (``EvalRun.owner_id``), and ``_make_agent_runner``
already receives it for the version-details load; these tests pin the two hops that carry it the
rest of the way to ``predict_sio``.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def runner(utils_path):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def orch(utils_path):
    load_utils_module(utils_path, 'evaluation_scoring')
    load_utils_module(utils_path, 'evaluation_ai_judge')
    load_utils_module(utils_path, 'evaluation_agent_runner')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


def _ok_predict(recorder):
    def predict(**kwargs):
        recorder.append(kwargs)
        return {'result': {'chat_history': [{'role': 'assistant', 'content': 'a'}]}}
    return predict


# --- hop 1: run_agent -> predict_sio ----------------------------------------

def test_run_agent_forwards_the_acting_user(runner):
    calls = []

    runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'},
                     user_id=42, predict=_ok_predict(calls))

    assert calls[0]['user_id'] == 42


def test_run_agent_always_names_user_id(runner):
    """Passed even when unknown: relying on predict_sio's request-context fallback is what broke,
    so the argument must be present rather than left to a default."""
    calls = []

    runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=_ok_predict(calls))

    assert calls[0]['user_id'] is None


def test_run_agent_does_not_claim_to_be_a_system_user(runner):
    """The system-user path no longer resolves a token, and the run has a real owner anyway."""
    calls = []

    runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'},
                     user_id=42, predict=_ok_predict(calls))

    assert calls[0].get('is_system_user') in (None, False)
    assert calls[0]['skip_expansion'] is True


# --- hop 2: _make_agent_runner -> run_agent ---------------------------------

@pytest.fixture
def agent_calls(orch, utils_path, monkeypatch):
    """Record what the orchestrator hands ``run_agent``, and satisfy its two lazy imports."""
    calls = []
    sibling = sys.modules['plugins.elitea_core.utils.evaluation_agent_runner']
    monkeypatch.setattr(
        sibling, 'run_agent',
        lambda *args, **kwargs: calls.append(kwargs) or {'status': 'ok', 'output': 'a'},
    )
    app_utils = types.ModuleType('plugins.elitea_core.utils.application_utils')
    app_utils.get_application_version_details_expanded = (
        lambda project_id, application_id, version_id, user_id: {'agent_type': 'openai'}
    )
    monkeypatch.setitem(
        sys.modules, 'plugins.elitea_core.utils.application_utils', app_utils)
    return calls


def test_orchestrator_passes_the_run_owner_down(orch, agent_calls):
    snapshot = {'application_id': 3, 'application_version_id': 7, 'dataset_id': 5}

    orch._make_agent_runner(1, snapshot, user_id=42)({'input': 'q'})

    assert agent_calls[0]['user_id'] == 42
