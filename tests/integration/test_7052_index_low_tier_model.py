"""Issue #7052 - a scheduled re-index must index with the project's Low-tier model, then its
Default, then the first available model, the same order the index pages preselect.

Re-breaks if `resolve_index_llm_settings` goes back to reading only the `default` flag.
"""

import pathlib
import sys
import types

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6583c_tick_behaviour import tick  # noqa: E402,F401

LOW_TIER = {"name": "project-low-tier", "project_id": 2, "default": False, "openai_compatible": True}
PROJECT_DEFAULT = {"name": "project-default", "project_id": 1, "default": True}
ALPHABETICALLY_FIRST = {"name": "alphabetically-first", "project_id": 1, "default": False}


def _resolve(tick, monkeypatch, models):
    monkeypatch.setattr(tick._sched_utils, "rpc_tools", types.SimpleNamespace(
        RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
            timeout=lambda t: types.SimpleNamespace(configurations_get_models=lambda **kw: models)))))
    return tick._sched_utils.resolve_index_llm_settings(2)


class TestScheduledIndexModelOrder:
    def test_the_project_low_tier_wins_over_the_default(self, tick, monkeypatch):
        settings, issue, retryable = _resolve(tick, monkeypatch, {
            "items": [ALPHABETICALLY_FIRST, PROJECT_DEFAULT, LOW_TIER],
            "low_tier_default_model_name": "project-low-tier",
            "low_tier_default_model_project_id": "2",
        })

        assert (issue, retryable) == (None, False)
        assert settings == {"model_name": "project-low-tier", "model_project_id": 2, "openai_compatible": True}

    def test_a_low_tier_name_from_another_project_is_not_matched(self, tick, monkeypatch):
        settings, _, _ = _resolve(tick, monkeypatch, {
            "items": [ALPHABETICALLY_FIRST, PROJECT_DEFAULT, LOW_TIER],
            "low_tier_default_model_name": "project-low-tier",
            "low_tier_default_model_project_id": 1,
        })

        assert settings["model_name"] == "project-default"

    def test_without_a_low_tier_the_project_default_is_used(self, tick, monkeypatch):
        settings, _, _ = _resolve(tick, monkeypatch, {
            "items": [ALPHABETICALLY_FIRST, PROJECT_DEFAULT, LOW_TIER],
            "low_tier_default_model_name": "",
            "low_tier_default_model_project_id": "",
        })

        assert settings["model_name"] == "project-default"

    def test_without_low_tier_or_default_the_first_model_is_used(self, tick, monkeypatch):
        settings, issue, _ = _resolve(tick, monkeypatch, {"items": [ALPHABETICALLY_FIRST, LOW_TIER]})

        assert issue is None
        assert settings["model_name"] == "alphabetically-first"
