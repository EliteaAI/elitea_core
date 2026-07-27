"""Issue #5955 - block toolkit/MCP/skill mapping changes on published/embedded agent versions.

Once an agent version is published or embedded, its tool/skill graph is
duplicated into a decoupled public copy. Mutating the toolkit or skill
mappings on the original version afterwards silently diverges from what
was published, without any error. These tests confirm the new guards in
`toolkit_change_relation`, `attach_skill_to_agent` and
`detach_skill_from_agent` reject such mutations.

Run via:
    python tests/run_tests.py integration/test_5955_published_mapping_guards.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _FakeQuery:
    def __init__(self, result):
        self._result = result

    def filter(self, *args, **kwargs):
        return self

    def filter_by(self, *args, **kwargs):
        return self

    def options(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self._result

    def all(self):
        if self._result is None:
            return []
        return self._result if isinstance(self._result, list) else [self._result]

    def delete(self):
        return None


class FakeSession:
    """Routes session.query(Model) to a preconfigured result for that model."""

    def __init__(self, results_by_model):
        self._results_by_model = results_by_model
        self.added = []
        self.flushed = False
        self.committed = False
        self.closed = False

    def query(self, model):
        return _FakeQuery(self._results_by_model.get(model))

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        self.flushed = True

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def _register(name, module):
    sys.modules[name] = module
    return module


@pytest.fixture(scope='module')
def application_tools_module():
    """Load application_tools.py standalone with minimal stubs."""
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
    tools_pkg.db = types.SimpleNamespace(get_project_schema_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    def _init_with_kwargs(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    models_all.EliteATool = type("EliteATool", (), {"id": 1})
    models_all.EntityToolMapping = type(
        "EntityToolMapping",
        (),
        {
            "tool_id": 1,
            "entity_version_id": 1,
            "entity_id": 1,
            "entity_type": 1,
            "id": 1,
            "__init__": _init_with_kwargs,
        },
    )
    models_all.ApplicationVersion = type(
        "ApplicationVersion", (), {"id": 1, "application_id": 1, "status": "draft"}
    )
    _register("plugins.elitea_core.models.all", models_all)

    models_indexer = types.ModuleType("plugins.elitea_core.models.indexer")
    models_indexer.EmbeddingStore = type("EmbeddingStore", (), {})
    _register("plugins.elitea_core.models.indexer", models_indexer)

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.ToolEntityTypes = type("ToolEntityTypes", (), {})
    enums.AgentTypes = type("AgentTypes", (), {})
    enums.InitiatorType = type("InitiatorType", (), {"user": "user"})
    enums.IndexDataStatus = type(
        "IndexDataStatus",
        (),
        {
            "in_progress": types.SimpleNamespace(value="in_progress"),
            "cancelled": types.SimpleNamespace(value="cancelled"),
        },
    )
    _register("plugins.elitea_core.models.enums.all", enums)

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    _register("plugins.elitea_core.utils.exceptions", exceptions)

    # Lightweight stand-in for ToolUpdateRelationModel - avoids pulling in
    # the real model's heavy toolkit-schema/author dependencies.
    from pydantic import BaseModel
    from typing import Optional, List

    class ToolUpdateRelationModel(BaseModel):
        entity_id: int
        entity_version_id: int
        entity_type: str
        has_relation: bool = False
        selected_tools: Optional[List[str]] = None

    models_pd_tool = types.ModuleType("plugins.elitea_core.models.pd.tool")
    models_pd_tool.ToolUpdateRelationModel = ToolUpdateRelationModel
    _register("plugins.elitea_core.models.pd.tool", models_pd_tool)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestToolkitChangeRelationPublishedGuard:
    """toolkit_change_relation must reject mutation once the version is public."""

    @pytest.mark.parametrize("status", ["published", "embedded"])
    def test_rejects_mapping_change_on_public_version(self, application_tools_module, status):
        ApplicationVersion = application_tools_module.ApplicationVersion
        EliteATool = application_tools_module.EliteATool
        EntityToolMapping = application_tools_module.EntityToolMapping

        parent_version = ApplicationVersion()
        parent_version.id = 5
        parent_version.status = status

        session = FakeSession({
            ApplicationVersion: parent_version,
            EliteATool: EliteATool(),
            EntityToolMapping: None,
        })

        with pytest.raises(application_tools_module.ToolkitChangeRelationError) as exc_info:
            application_tools_module.toolkit_change_relation(
                project_id=1,
                toolkit_id=42,
                relation_data={
                    "entity_id": 9,
                    "entity_version_id": 5,
                    "entity_type": "agent",
                    "has_relation": True,
                },
                session=session,
            )

        assert status in str(exc_info.value)
        assert "can not be updated" in str(exc_info.value)
        # Guard must short-circuit before any mapping is written.
        assert session.added == []
        assert not session.flushed

    @pytest.mark.parametrize("status", ["draft", "on_moderation", "rejected", "unpublished"])
    def test_allows_mapping_change_on_non_public_version(self, application_tools_module, status):
        ApplicationVersion = application_tools_module.ApplicationVersion
        EliteATool = application_tools_module.EliteATool
        EntityToolMapping = application_tools_module.EntityToolMapping

        parent_version = ApplicationVersion()
        parent_version.id = 5
        parent_version.status = status

        elitea_toolkit = EliteATool()
        elitea_toolkit.id = 42

        session = FakeSession({
            ApplicationVersion: parent_version,
            EliteATool: elitea_toolkit,
            EntityToolMapping: None,
        })

        result = application_tools_module.toolkit_change_relation(
            project_id=1,
            toolkit_id=42,
            relation_data={
                "entity_id": 9,
                "entity_version_id": 5,
                "entity_type": "agent",
                "has_relation": True,
            },
            session=session,
        )

        assert result == {"has_relation": True, "tool_id": 42}
        assert len(session.added) == 1
        assert session.flushed


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
    enums.AgentTypes = type("AgentTypes", (), {})
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


class _FakeSkillSession:
    """Stands in for the `with _skill_session(...) as s:` contextmanager body."""

    def __init__(self, results_by_model):
        self._session = FakeSession(results_by_model)

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class TestSkillAttachDetachPublishedGuard:
    """attach/detach_skill_to_agent must reject mutation once the version is public."""

    @pytest.mark.parametrize("status", ["published", "embedded"])
    def test_attach_rejects_on_public_version(self, skill_utils_module, monkeypatch, status):
        ApplicationVersion = skill_utils_module.ApplicationVersion
        agent_version = ApplicationVersion()
        agent_version.id = 5
        agent_version.status = status

        monkeypatch.setattr(
            skill_utils_module,
            "_skill_session",
            lambda session, project_id: _FakeSkillSession({ApplicationVersion: agent_version}),
        )

        with pytest.raises(skill_utils_module.AgentVersionNotUpdatableError) as exc_info:
            skill_utils_module.attach_skill_to_agent(
                session=None,
                project_id=1,
                entity_version_id=5,
                entity_type="agent",
                skill_id=7,
                skill_version_id=1,
            )

        assert str(5) in str(exc_info.value)
        assert status in str(exc_info.value)
        assert exc_info.value.http_status == 409

    @pytest.mark.parametrize("status", ["published", "embedded"])
    def test_detach_rejects_on_public_version(self, skill_utils_module, monkeypatch, status):
        ApplicationVersion = skill_utils_module.ApplicationVersion
        agent_version = ApplicationVersion()
        agent_version.id = 5
        agent_version.status = status

        monkeypatch.setattr(
            skill_utils_module,
            "_skill_session",
            lambda session, project_id: _FakeSkillSession({ApplicationVersion: agent_version}),
        )

        with pytest.raises(skill_utils_module.AgentVersionNotUpdatableError) as exc_info:
            skill_utils_module.detach_skill_from_agent(
                session=None,
                project_id=1,
                entity_version_id=5,
                entity_type="agent",
                skill_id=7,
            )

        assert str(5) in str(exc_info.value)
        assert status in str(exc_info.value)
        assert exc_info.value.http_status == 409
