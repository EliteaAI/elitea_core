"""Integration test fixtures - database, sessions, etc."""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
TESTS_DIR = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.models import FakeSession


def _register(name, module):
    sys.modules[name] = module
    return module


@pytest.fixture
def fake_db_session():
    """Lightweight fake session for tests that don't need real DB."""
    return FakeSession(registry={})


@pytest.fixture(scope='module')
def skill_utils_module():
    """Load skill_utils.py standalone with minimal stubs."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.models.pd",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    utils_mod = types.ModuleType("plugins.elitea_core.utils.utils")
    utils_mod.set_columns_as_attrs = lambda *a, **k: None
    utils_mod.get_public_project_id = lambda: 1
    _register("plugins.elitea_core.utils.utils", utils_mod)

    like_utils = types.ModuleType("plugins.elitea_core.utils.like_utils")
    like_utils.add_likes = lambda *a, **k: None
    like_utils.add_my_liked = lambda *a, **k: None
    like_utils.add_trending_likes = lambda *a, **k: None
    like_utils.get_like_model = lambda *a, **k: None
    _register("plugins.elitea_core.utils.like_utils", like_utils)

    models_skill = types.ModuleType("plugins.elitea_core.models.skill")
    models_skill.Skill = type("Skill", (), {"id": 1})
    models_skill.SkillVersion = type("SkillVersion", (), {"id": 1, "skill_id": 1})
    models_skill.EntitySkillMapping = type(
        "EntitySkillMapping",
        (),
        {
            "id": 1,
            "entity_version_id": 1,
            "entity_type": 1,
            "skill_id": 1,
            "skill_version_id": 1,
        },
    )
    _register("plugins.elitea_core.models.skill", models_skill)

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.Tag = type("Tag", (), {})
    models_all.ApplicationVersion = type(
        "ApplicationVersion", (), {"id": 1, "application_id": 1, "status": "draft"}
    )
    models_all.Application = type("Application", (), {"id": 1})
    _register("plugins.elitea_core.models.all", models_all)

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.SkillEntityTypes = type("SkillEntityTypes", (), {"agent": "agent"})

    class _PublishStatus:
        draft = "draft"
        on_moderation = "on_moderation"
        published = "published"
        rejected = "rejected"
        user_approval = "user_approval"
        unpublished = "unpublished"
        embedded = "embedded"

    enums.PublishStatus = _PublishStatus
    enums.AgentTypes = type(
        "AgentTypes", (), {"pipeline": types.SimpleNamespace(value="pipeline")}
    )
    _register("plugins.elitea_core.models.enums.all", enums)

    from pydantic import BaseModel, ConfigDict

    class _PdBase(BaseModel):
        model_config = ConfigDict(extra="allow")

    models_pd_skill = types.ModuleType("plugins.elitea_core.models.pd.skill")
    for cls_name in (
        "SkillCreateModel",
        "SkillDetailModel",
        "SkillUpdateModel",
        "SkillImportResultModel",
        "AgentsWithSkillItemModel",
    ):
        setattr(models_pd_skill, cls_name, type(cls_name, (_PdBase,), {}))
    _register("plugins.elitea_core.models.pd.skill", models_pd_skill)

    models_pd_skill_version = types.ModuleType("plugins.elitea_core.models.pd.skill_version")
    for cls_name in (
        "SkillVersionCreateModel",
        "SkillVersionUpdateModel",
        "SkillVersionDetailModel",
    ):
        setattr(models_pd_skill_version, cls_name, type(cls_name, (_PdBase,), {}))
    _register("plugins.elitea_core.models.pd.skill_version", models_pd_skill_version)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.skill_utils",
        PLUGIN_ROOT / "utils" / "skill_utils.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module

