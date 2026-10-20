"""EL-7011 US-23: the write rule for a pipeline version trigger.

Contract under test: utils/pipeline_trigger.py (sanitize_trigger_for_write).
R2.1, R2.2: a body trigger of type schedule is stored as chat_message.
R2.3: a stored legacy schedule trigger is kept as stored.
R2.4: a body without a trigger is returned as is (the save keeps the stored trigger).
"""
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module

CHAT = {"type": "chat_message"}
SCHEDULE = {"type": "schedule", "cron": "0 9 * * *", "timezone": "UTC", "created_by": 7}
OTHER_SCHEDULE = {"type": "schedule", "cron": "0 1 * * *", "timezone": "UTC", "created_by": 9}
WEBHOOK = {"type": "webhook", "webhook_type": "custom", "created_by": 7}


@pytest.fixture(scope="module")
def rule():
    """sanitize_trigger_for_write from utils/pipeline_trigger.py, plugin imports stubbed."""
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.utils"):
        if name not in sys.modules:
            pkg = types.ModuleType(name)
            pkg.__path__ = []
            sys.modules[name] = pkg
    constants = sys.modules.setdefault(
        "plugins.elitea_core.utils.constants", types.ModuleType("plugins.elitea_core.utils.constants"))
    if not hasattr(constants, "PROMPT_LIB_MODE"):
        constants.PROMPT_LIB_MODE = "prompt_lib"
    tools = sys.modules.setdefault("tools", types.ModuleType("tools"))
    if not hasattr(tools, "this"):
        tools.this = types.SimpleNamespace(module_name="elitea_core")
    if not hasattr(tools, "VaultClient"):
        tools.VaultClient = object
    module = load_utils_module(PLUGIN_ROOT / "utils", "pipeline_trigger")
    return module.sanitize_trigger_for_write


@pytest.mark.unit
@pytest.mark.utils
class TestEL7011LegacyTriggerWriteRules:
    def test_r2_1_create_with_schedule_stores_chat_message(self, rule):
        # TC-US23-05: create, clone, publish and import pass stored_trigger=None
        settings = {"trigger": dict(SCHEDULE), "other": 1}
        assert rule(settings, None) == {"trigger": CHAT, "other": 1}
        assert settings["trigger"] == SCHEDULE

    def test_r2_2_save_with_schedule_and_no_stored_schedule_stores_chat_message(self, rule):
        # TC-US23-05
        assert rule({"trigger": dict(SCHEDULE)}, WEBHOOK) == {"trigger": CHAT}

    @pytest.mark.parametrize("trigger", [CHAT, WEBHOOK])
    def test_r2_1_other_trigger_passes_through(self, rule, trigger):
        # TC-US23-06
        assert rule({"trigger": dict(trigger)}, None) == {"trigger": trigger}

    def test_r2_3_stored_schedule_is_kept_on_save(self, rule):
        # TC-US23-07: the body schedule does not replace the stored one
        assert rule({"trigger": dict(OTHER_SCHEDULE)}, SCHEDULE) == {"trigger": SCHEDULE}

    @pytest.mark.parametrize("trigger", [CHAT, WEBHOOK])
    def test_r2_3_stored_schedule_is_kept_against_another_trigger(self, rule, trigger):
        # TC-US23-08
        assert rule({"trigger": dict(trigger)}, SCHEDULE) == {"trigger": SCHEDULE}

    def test_r2_4_no_trigger_in_body_returns_settings_as_is(self, rule):
        # TC-US23-07: the save path then keeps the stored trigger
        assert rule({"other": 1}, SCHEDULE) == {"other": 1}
        assert rule({}, None) == {}
