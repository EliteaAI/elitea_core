"""EL-7011 US-12 R1.1, R1.5: the pipeline starter sits in the run starter map."""

from unit._tool_spend_loader import load

DISPATCH = "utils/schedule_dispatch.py"
STARTER = "plugins.elitea_core.utils.schedule_run_starter"


def test_7011_r1_1_pipeline_starter_is_registered():
    starters = load(DISPATCH, fresh=True).RUN_STARTERS
    assert callable(starters["pipeline"])


def test_7011_r1_5_start_schedule_run_starts_a_pipeline_run(monkeypatch):
    dispatch = load(DISPATCH, fresh=True)
    started = []

    class _Thread:
        def __init__(self, target, args, daemon):
            started.append((target, args, daemon))

        def start(self):
            pass

    monkeypatch.setattr(dispatch.threading, "Thread", _Thread)
    assert dispatch.start_schedule_run("pipeline", 5, 6, 7) is True
    assert started[0][1] == (dispatch.RUN_STARTERS["pipeline"], 5, 6, 7)
