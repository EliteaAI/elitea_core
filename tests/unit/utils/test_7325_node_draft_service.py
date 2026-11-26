"""EL-7325: node draft orchestration (model settings, budget, one repair, error mapping).

Covers AC-31 (no default-model fallback), AC-33 (one repair with errors sent back, then 422),
AC-34 (60 s budget, repair only with >= 20 s left, 504), AC-35 (cut-off output), AC-36 (temperature,
max_tokens, reasoning models), AC-32 (response shape).

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_service.py -v
"""
import json
import pathlib
import sys

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

_service, _models = load('utils.node_draft.service'), load('models.pd.node_draft')
draft_llm_settings, run_draft = _service.draft_llm_settings, _service.run_draft
ModelReply, DraftTarget = _models.ModelReply, _models.DraftTarget
BUDGET_SECONDS, MIN_REPAIR_SECONDS = _models.LIMITS.budget_seconds, _models.LIMITS.min_repair_seconds

VALID = {
    "summary": "Summarizes issues.",
    "node": {"input": ["issues"], "output": ["summary"], "transition": "END",
             "input_mapping": {"task": {"type": "fstring", "value": "Summarize {issues}"}}},
    "new_state": [{"name": "summary", "type": "str"}],
    "warnings": [],
}
INVALID = {"summary": "x", "node": {"input_mapping": {"task": {"type": "python", "value": "x"}}},
           "new_state": [], "warnings": []}


def _target():
    return DraftTarget(
        node_id="step", node_type="llm", mode="build", current={"id": "step", "type": "llm"}, base=None,
        state={"input": "str", "messages": "list", "issues": "list"},
        node_ids={"step", "END"}, resources={"toolkit": {}, "mcp": {}, "agent": {}},
    )


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class ScriptedModel:
    """Returns the scripted replies in order; each reply may advance the fake clock."""

    def __init__(self, clock, replies):
        self.clock = clock
        self.replies = list(replies)
        self.calls = []

    def __call__(self, system, messages, llm_settings, timeout):
        self.calls.append({"system": system, "messages": list(messages), "llm_settings": dict(llm_settings),
                           "timeout": timeout})
        reply, elapsed = self.replies.pop(0)
        self.clock.now += elapsed
        return reply


def _text(obj):
    return ModelReply(text=json.dumps(obj))


def _extract(text):
    """Stand-in for utils.utils.extract_json_from_text (that module needs the DB layer)."""
    start, end = text.find("{"), text.rfind("}") + 1
    return text[start:end] if start != -1 and end > start else text


def _run(replies, clock=None):
    clock = clock or FakeClock()
    model = ScriptedModel(clock, replies)
    body, status = run_draft(
        system="SYSTEM", user="USER", target=_target(), llm_settings={"model_name": "m", "temperature": 0.2},
        call_model=model, clock=clock, extract_json=_extract,
    )
    return body, status, model


class TestLlmSettings:
    """AC-36, AC-31."""

    @pytest.mark.parametrize("node_type,tokens", [
        ("llm", 2048), ("code", 2048), ("state_modifier", 2048),
        ("router", 1024), ("toolkit", 1024), ("printer", 1024), ("hitl", 1024),
    ])
    def test_max_tokens_per_type(self, node_type, tokens):
        settings = draft_llm_settings({"model_name": "m", "max_tokens": 9999, "temperature": 0.9},
                                      node_type, supports_reasoning=False)
        assert settings["max_tokens"] == tokens
        assert settings["temperature"] == 0.2
        assert "reasoning_effort" not in settings

    def test_reasoning_model(self):
        settings = draft_llm_settings({"model_name": "m"}, "router", supports_reasoning=True)
        assert settings["max_tokens"] == 8192
        assert settings["reasoning_effort"] == "low"
        assert "temperature" not in settings

    def test_model_name_required(self):
        with pytest.raises(ValueError):
            draft_llm_settings({"model_name": ""}, "llm", supports_reasoning=False)
        with pytest.raises(ValueError):
            draft_llm_settings(None, "llm", supports_reasoning=False)

    def test_other_settings_kept(self):
        settings = draft_llm_settings({"model_name": "m", "model_project_id": 1}, "llm", supports_reasoning=False)
        assert settings["model_name"] == "m" and settings["model_project_id"] == 1


class TestRunDraft:
    def test_success_shape(self):
        """AC-32."""
        body, status, model = _run([(_text(VALID), 3)])
        assert status == 200
        assert set(body) == {"summary", "node", "changed_paths", "attachments", "new_state", "warnings"}
        assert body["node"]["id"] == "step" and body["node"]["type"] == "llm"
        assert len(model.calls) == 1
        assert model.calls[0]["system"] == "SYSTEM"
        assert model.calls[0]["messages"] == [{"role": "user", "content": "USER"}]

    def test_first_timeout_uses_whole_budget(self):
        _, _, model = _run([(_text(VALID), 1)])
        assert model.calls[0]["timeout"] == BUDGET_SECONDS

    def test_repair_once_with_errors_sent_back(self):
        """AC-33."""
        body, status, model = _run([(_text(INVALID), 5), (_text(VALID), 5)])
        assert status == 200
        assert len(model.calls) == 2
        repair = model.calls[1]["messages"]
        assert repair[0] == {"role": "user", "content": "USER"}
        assert repair[1]["role"] == "assistant"
        assert repair[2]["role"] == "user"
        assert repair[2]["content"].startswith("Your previous answer was invalid:")
        assert repair[2]["content"].endswith("Return the corrected JSON only.")
        assert "input_mapping.task.type" in repair[2]["content"]
        assert model.calls[1]["timeout"] == pytest.approx(BUDGET_SECONDS - 5)

    def test_not_json_triggers_repair(self):
        body, status, model = _run([(ModelReply(text="Sure! Here is the node."), 2), (_text(VALID), 2)])
        assert status == 200 and len(model.calls) == 2

    def test_two_invalid_answers_give_422(self):
        """AC-33."""
        body, status, model = _run([(_text(INVALID), 2), (_text(INVALID), 2)])
        assert status == 422
        assert body["error"] == ("The AI couldn't produce a valid node. "
                                 "Try a more specific description or another model.")
        assert 1 <= len(body["details"]) <= 10
        assert len(model.calls) == 2

    def test_repair_details_trimmed(self):
        bad = {"summary": "x", "new_state": [], "warnings": [],
               "node": {"input_mapping": {f"p{i}" * 20: {"type": "bad", "value": 1} for i in range(40)}}}
        _, _, model = _run([(_text(bad), 1), (_text(VALID), 1)])
        content = model.calls[1]["messages"][2]["content"]
        details = content.removeprefix("Your previous answer was invalid: ").removesuffix(
            " Return the corrected JSON only.")
        assert len(details) <= 1500

    def test_no_repair_when_budget_low(self):
        """AC-34: the retry runs only with >= 20 s left."""
        body, status, model = _run([(_text(INVALID), BUDGET_SECONDS - MIN_REPAIR_SECONDS + 1)])
        assert status == 422
        assert len(model.calls) == 1

    def test_timeout_gives_504(self):
        """AC-34."""
        body, status, model = _run([(ModelReply(timed_out=True), BUDGET_SECONDS)])
        assert status == 504
        assert body["error"] == "Generating took too long. Try again or simplify the request."
        assert len(model.calls) == 1

    def test_timeout_on_repair_gives_504(self):
        body, status, _ = _run([(_text(INVALID), 10), (ModelReply(timed_out=True), 50)])
        assert status == 504

    def test_cut_short_gives_422_without_repair(self):
        """AC-35."""
        body, status, model = _run([(ModelReply(text='{"summary": "x", "node": {', cut_short=True), 5)])
        assert status == 422
        assert body["error"] == "The draft was too long for the model's output limit. Simplify the request."
        assert len(model.calls) == 1

    def test_model_error_passed_through(self):
        body, status, model = _run([(ModelReply(error="Provider rejected the request."), 1)])
        assert status == 500
        assert "Provider rejected the request." in body["error"]
        assert len(model.calls) == 1

    def test_fenced_json_accepted(self):
        reply = ModelReply(text="```json\n" + json.dumps(VALID) + "\n```")
        _, status, _ = _run([(reply, 1)])
        assert status == 200
