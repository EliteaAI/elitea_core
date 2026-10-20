"""EL-7011 US-09 R2.1-R2.4, R3.2: the owner token and credential check before a run."""
import json
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

PRERUN = "utils/schedule_prerun.py"
UTILS = "plugins.elitea_core.utils"
OWNER = 12


class PredictPayloadError(Exception):
    pass


class FakeHelpers:
    """Stands in for predict_utils and subagent_prefetch; records each call in order."""

    def __init__(self, token_error=None, expanded=None):
        self.token_error = token_error
        self.expanded = {"id": 7, "tools": []} if expanded is None else expanded
        self.calls = []

    def get_predict_token_and_session(self, project_id, user_id, sid="unset"):
        self.calls.append(("token", project_id, user_id, sid))
        if self.token_error:
            raise self.token_error
        return "owner-token", None

    def expand_version_for_runtime(self, project_id, application_id, version_id, user_id):
        self.calls.append(("expand", project_id, application_id, version_id, user_id))
        return self.expanded


@pytest.fixture
def check(monkeypatch):
    prerun = load(PRERUN, fresh=True)

    def run(helpers):
        monkeypatch.setitem(sys.modules, f"{UTILS}.predict_utils", SimpleNamespace(
            PredictPayloadError=PredictPayloadError,
            get_predict_token_and_session=helpers.get_predict_token_and_session))
        monkeypatch.setitem(sys.modules, f"{UTILS}.subagent_prefetch", SimpleNamespace(
            expand_version_for_runtime=helpers.expand_version_for_runtime))
        schedule = SimpleNamespace(created_by=OWNER, application_id=5, version_id=7)
        return prerun.check_owner_credentials(34, schedule)
    return run


def test_7011_r2_1_r2_3_token_then_expansion_as_the_owner_passes(check):
    helpers = FakeHelpers()
    assert check(helpers) is None
    assert helpers.calls == [("token", 34, OWNER, None), ("expand", 34, 5, 7, OWNER)]


def test_7011_r2_2_r3_2_no_token_fails_before_any_expansion(check):
    helpers = FakeHelpers(token_error=PredictPayloadError("User token not found. Please create user_token"))
    assert check(helpers) == "no token for the owner"
    assert [call[0] for call in helpers.calls] == ["token"]


def _expansion_error(error_type):
    # The credential error reaches the caller as text inside the pydantic message
    body = json.dumps({"error_type": error_type, "credential_id": "jira-main"})
    return {"error": f"1 validation error for ToolValidatedDetails\nsettings\n  Value error, {body}"}


@pytest.mark.parametrize("error_type, reason", [
    ("private_credential_not_found", "private credential jira-main not found"),
    ("credential_not_found", "credential jira-main not found"),
    ("credential_type_mismatch", "credential jira-main has the wrong type"),
])
def test_7011_r2_4_credential_error_fails_with_the_credential_name(check, error_type, reason):
    assert check(FakeHelpers(expanded=_expansion_error(error_type))) == reason


@pytest.mark.parametrize("expanded", [
    {"error": "Can not get application details for version_id=7"},
    _expansion_error("configuration_model_not_found"),
], ids=["generic", "other-type"])
def test_7011_r3_2_any_other_expansion_error_still_fails_the_run(check, expanded):
    assert check(FakeHelpers(expanded=expanded)) == "version could not be expanded for the owner"
