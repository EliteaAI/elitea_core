"""Issue #7048 - a scheduled re-index must index with the project's default LLM.

The scheduler dispatched no model at all, so the SDK fell back to a hardcoded `gpt-4o-mini`.
Where that model is not configured, every image description failed and was swallowed, and
the run still finished as a success. Manual indexing has no model picker and always sends the
project default, so the schedule must send the same one, and must fail visibly when there is
none.

Run via:
    python tests/run_tests.py integration/test_7048_scheduled_index_project_model.py -v
"""

import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6583c_tick_behaviour import Recorder, _due_schedule, tick  # noqa: E402,F401

UNTOUCHED_CURSOR = "2026-01-01T00:00:00+00:00"
NO_MODEL = (None, "no LLM model is available in this project to index with", False)
LOOKUP_FAILED = (None, "default LLM model lookup failed: TimeoutError()", True)
SHARED_DEFAULT = {"model_name": "shared-default", "model_project_id": 1, "openai_compatible": True}


def _models_rpc(monkeypatch, sched_utils, get_models):
    monkeypatch.setattr(sched_utils, "rpc_tools", types.SimpleNamespace(
        RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
            timeout=lambda t: types.SimpleNamespace(configurations_get_models=get_models)))))


class TestDispatchCarriesTheProjectModel:
    def test_the_dispatched_run_names_the_resolved_model(self, tick, monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       llm_resolution=(dict(SHARED_DEFAULT), None, False)).run()

        assert rec.dispatched == 1
        payload = rec.dispatched_payloads[0]
        assert payload["llm_model"] == "shared-default"
        assert payload["llm_settings"] == SHARED_DEFAULT


class TestNoUsableModel:
    def test_a_project_without_models_fails_visibly_once(self, tick, monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       llm_resolution=NO_MODEL).run(ticks=3)

        assert rec.dispatched == 0
        assert len(rec.notifications) == 1, rec.notifications
        assert rec.notifications[0]["state"] == "failed"
        assert "no LLM model is available" in rec.notifications[0]["error"]
        assert rec.entry["last_run"] != UNTOUCHED_CURSOR

    def test_a_failed_lookup_retries_without_reporting(self, tick, monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       llm_resolution=LOOKUP_FAILED).run(ticks=3)

        assert rec.credential_checks == 3, "the tick never reached the model lookup"
        assert rec.dispatched == 0
        assert rec.notifications == []
        assert rec.entry["last_run"] == UNTOUCHED_CURSOR
        assert rec.entry.get("retry_since"), "the outage must be recorded for a later tick"

    def test_an_ongoing_lookup_outage_keeps_its_stamp(self, tick, monkeypatch):
        """Healthy credentials must not clear the stamp of a model lookup that still fails,
        or the outage never outlives the grace and is never reported."""
        started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(retry_since=started),
                       llm_resolution=LOOKUP_FAILED).run()

        assert rec.entry.get("retry_since") == started

    def test_a_lookup_outage_past_the_grace_is_reported_once(self, tick, monkeypatch):
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(retry_since=old),
                       llm_resolution=LOOKUP_FAILED).run(ticks=3)

        assert rec.dispatched == 0
        assert len(rec.notifications) == 1, rec.notifications
        assert "still failing for over" in rec.notifications[0]["error"]

    def test_a_recovered_model_lookup_clears_the_outage_stamp(self, tick, monkeypatch):
        old = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(retry_since=old)).run()

        assert rec.dispatched == 1
        assert not rec.entry.get("retry_since")


class TestResolveIndexLlmSettings:
    def test_the_flagged_default_wins_over_the_first_item(self, tick, monkeypatch):
        sched_utils = tick._sched_utils
        _models_rpc(monkeypatch, sched_utils, lambda **kw: {"items": [
            {"name": "alphabetically-first", "project_id": 2, "default": False},
            {"name": "project-default", "project_id": 1, "default": True,
             "openai_compatible": True},
        ]})

        settings, issue, retryable = sched_utils.resolve_index_llm_settings(2)

        assert (issue, retryable) == (None, False)
        assert settings == {"model_name": "project-default", "model_project_id": 1,
                            "openai_compatible": True}

    def test_the_lookup_asks_for_shared_models(self, tick, monkeypatch):
        calls = []
        _models_rpc(monkeypatch, tick._sched_utils,
                    lambda **kw: calls.append(kw) or {"items": []})

        tick._sched_utils.resolve_index_llm_settings(2)

        assert calls == [{"project_id": 2, "section": "llm", "include_shared": True}]

    def test_no_models_is_a_terminal_issue(self, tick, monkeypatch):
        _models_rpc(monkeypatch, tick._sched_utils, lambda **kw: {"items": []})

        settings, issue, retryable = tick._sched_utils.resolve_index_llm_settings(2)

        assert settings is None
        assert "no LLM model is available" in issue
        assert retryable is False

    def test_a_raising_lookup_is_retryable(self, tick, monkeypatch):
        def _raise(**kw):
            raise TimeoutError()
        _models_rpc(monkeypatch, tick._sched_utils, _raise)

        settings, issue, retryable = tick._sched_utils.resolve_index_llm_settings(2)

        assert settings is None
        assert "lookup failed" in issue
        assert retryable is True
