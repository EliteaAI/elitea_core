"""EL-7011 US-23: the version save applies the trigger write rule.

Contract under test: applications_update_version in utils/application_utils.py.
R2.2: a body schedule trigger is stored as chat_message when no schedule is stored.
R2.3: a stored legacy schedule trigger is kept as stored.
R2.4: a body without a trigger keeps the stored trigger.
The function is compiled from its source, so no Pylon import or database is needed.
"""
import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fixtures.cut_source import load_cut

UTILS = Path(__file__).resolve().parents[2] / "utils"
STORED_SCHEDULE = {"type": "schedule", "cron": "0 9 * * *", "timezone": "UTC", "created_by": 7}
BODY_SCHEDULE = {"type": "schedule", "cron": "0 1 * * *", "timezone": "UTC", "created_by": 9}
CHAT = {"type": "chat_message"}
WEBHOOK = {"type": "webhook", "webhook_type": "custom", "created_by": 7}


def _function(name):
    path = UTILS / ("trigger_write_rule.py" if name == "sanitize_trigger_for_write" else "application_utils.py")
    return next(n for n in ast.parse(path.read_text()).body
                if isinstance(n, ast.FunctionDef) and n.name == name)


def _save(stored_settings, body_settings):
    """Run applications_update_version on a fake version, return the stored pipeline_settings."""
    namespace = {
        "ApplicationVersion": MagicMock(), "ApplicationVariable": MagicMock(), "Tag": MagicMock(),
        "ApplicationVersionDetailModel": MagicMock(), "VersionNotUpdatableError": Exception,
        "validate_model_selection_surface": lambda *a, **k: None,
        "loads": lambda _: {}, "IntegrityError": ValueError, "log": MagicMock(),
    }
    names = ("sanitize_trigger_for_write", "applications_update_version")
    namespace = load_cut([_function(n) for n in names], namespace, "utils/application_utils.py")  # the real functions cut out of the source files
    version = SimpleNamespace(id=1, name="v1", status="draft", agent_type="pipeline",
                              llm_settings={}, pipeline_settings=stored_settings, meta={})
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = version
    body = SimpleNamespace(
        id=1, model_fields={"id": object()}, model_fields_set={"pipeline_settings"}, name=None,
        variables=None, tags=None, project_id=1,
        model_dump=lambda **kw: {"pipeline_settings": body_settings})
    namespace["applications_update_version"](body, session, commit=False)
    return version.pipeline_settings


@pytest.mark.unit
class TestEL7011UpdateVersionTrigger:
    def test_r2_2_body_schedule_without_stored_schedule_is_stored_as_chat_message(self):
        # TC-US23-05
        stored = _save({"trigger": WEBHOOK}, {"trigger": BODY_SCHEDULE, "other": 1})
        assert stored == {"trigger": CHAT, "other": 1}

    def test_r2_3_stored_schedule_is_kept_on_save(self):
        # TC-US23-06
        stored = _save({"trigger": STORED_SCHEDULE}, {"trigger": BODY_SCHEDULE})
        assert stored == {"trigger": STORED_SCHEDULE}

    def test_r2_4_save_without_trigger_keeps_stored_trigger(self):
        # TC-US23-07
        stored = _save({"trigger": STORED_SCHEDULE}, {"other": 1})
        assert stored == {"other": 1, "trigger": STORED_SCHEDULE}

    def test_r2_4_save_with_other_trigger_type_is_stored_as_sent(self):
        # TC-US23-08
        stored = _save({"trigger": CHAT}, {"trigger": WEBHOOK})
        assert stored == {"trigger": WEBHOOK}
