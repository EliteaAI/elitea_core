"""EL-7011 US-23: every version create applies the trigger write rule.

Contract under test: utils/create_utils.py (create_version).
R2.1: a create, clone, publish or import that carries a schedule trigger stores chat_message.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from unit.test_7011_legacy_trigger_write_rules import load_trigger_rule_module

PKG = "pkg_7011_create"
CHAT = {"type": "chat_message"}
SCHEDULE = {"type": "schedule", "cron": "0 9 * * *", "timezone": "UTC", "created_by": 7}
WEBHOOK = {"type": "webhook", "webhook_type": "custom", "created_by": 7}


class FakeVersion:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.tags = []
        self.id = 1


class FakeSession:
    def add(self, obj):
        pass

    def flush(self):
        pass


class FakeData:
    """Stands in for the create model: .dict() returns the given fields."""
    tags = None
    variables = None
    tools = None

    def __init__(self, **fields):
        self.fields = fields

    def dict(self, **_):
        return dict(self.fields)


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


@pytest.fixture
def create_version(isolated_sys_modules):
    rule_module = load_trigger_rule_module()
    for name in (PKG, f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        _package(name)
    tools = types.ModuleType("tools")
    tools.serialize = tools.store_secrets = lambda *a, **k: None
    sys.modules["tools"] = tools
    stubs = {
        f"{PKG}.models.all": {
            "ApplicationVersion": FakeVersion, "Application": object, "ApplicationVariable": object,
            "EliteATool": object, "EntityToolMapping": object, "Tag": object},
        f"{PKG}.models.pd.application": {"ApplicationCreateModel": object, "ApplicationImportModel": object},
        f"{PKG}.models.pd.version": {
            "ApplicationVersionBaseCreateModel": object, "ApplicationVersionCloneModel": object,
            "ApplicationVersionCreateModel": object, "TagBaseModel": object},
        f"{PKG}.utils.skill_utils": {"copy_skill_mappings": lambda **k: None},
    }
    for name, attrs in stubs.items():
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        sys.modules[name] = module
    sys.modules[f"{PKG}.utils.pipeline_trigger"] = rule_module
    spec = importlib.util.spec_from_file_location(
        f"{PKG}.utils.create_utils", PLUGIN_ROOT / "utils" / "create_utils.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.create_version


def _stored(create_version, **fields):
    return create_version(FakeData(**fields), session=FakeSession()).kwargs


@pytest.mark.unit
@pytest.mark.utils
class TestEL7011CreateVersionTrigger:
    def test_r2_1_create_with_schedule_stores_chat_message(self, create_version):
        # TC-US23-05
        stored = _stored(create_version, pipeline_settings={"trigger": SCHEDULE, "x": 1})
        assert stored["pipeline_settings"] == {"trigger": CHAT, "x": 1}

    def test_r2_1_create_with_other_trigger_is_unchanged(self, create_version):
        # TC-US23-06
        stored = _stored(create_version, pipeline_settings={"trigger": WEBHOOK})
        assert stored["pipeline_settings"] == {"trigger": WEBHOOK}

    def test_r2_1_create_without_pipeline_settings_adds_none(self, create_version):
        assert "pipeline_settings" not in _stored(create_version, name="v1")
