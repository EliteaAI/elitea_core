"""Unit tests for EL-6724 chat configuration templates.

Covers:
  1. Pydantic model validation — ChatTemplateCreate / Update / Read
  2. API handler business logic — template limits, name uniqueness, default guards
  3. Migration task — participant field remapping, idempotency, dry-run
  4. chat_template_utils — delete and update helpers
  5. Event handlers — _affected_projects scope, delete/rename propagation

Run standalone: python3 tests/unit/test_6724_chat_templates.py
"""

import contextlib
import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# Path bootstrap (needed for standalone execution)
# ──────────────────────────────────────────────────────────────────────────────

PLUGIN_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TESTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (PLUGIN_ROOT, TESTS_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ──────────────────────────────────────────────────────────────────────────────
# Core helpers
# ──────────────────────────────────────────────────────────────────────────────

def _load_pd_module():
    """Load models/pd/chat_template.py — pure pydantic, no stubs needed."""
    spec = importlib.util.spec_from_file_location(
        "_ct_pd_under_test",
        os.path.join(PLUGIN_ROOT, "models", "pd", "chat_template.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pylon_stubs():
    """Return a dict of minimal pylon stubs."""
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        debug=lambda *a, **kw: None, info=lambda *a, **kw: None,
        warning=lambda *a, **kw: None, error=lambda *a, **kw: None,
        exception=lambda *a, **kw: None,
    )

    class _Web:
        def __getattr__(self, name):
            return lambda *a, **kw: (lambda func: func)

    pylon_tools.web = _Web()
    return {"pylon": pylon, "pylon.core": core, "pylon.core.tools": pylon_tools}


@contextlib.contextmanager
def _with_stubs(extra: dict):
    """Temporarily install stubs into sys.modules; restore on exit."""
    stubs = {**_pylon_stubs(), **extra}
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)
    try:
        yield stubs
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


# ──────────────────────────────────────────────────────────────────────────────
# Fake ORM column descriptor (supports .desc(), .asc(), .ilike(), ==, !=)
# ──────────────────────────────────────────────────────────────────────────────

class _Col:
    """Fake SQLAlchemy column descriptor for use as class attributes.

    Python's attribute lookup always returns the instance attribute for data
    reads (row.is_default) while class-level access (ChatTemplate.is_default)
    returns this descriptor so .desc() / .ilike() / == work in queries.
    """

    def __init__(self, attr):
        self._attr = attr

    def desc(self):
        return ("_ORDER", self._attr, "desc")

    def asc(self):
        return ("_ORDER", self._attr, "asc")

    def ilike(self, pattern):
        import re
        re_pat = re.compile(re.escape(pattern).replace(r"\%", ".*"), re.IGNORECASE)
        attr = self._attr
        return lambda row: bool(re_pat.search(str(getattr(row, attr) or "")))

    def __eq__(self, value):
        attr = self._attr
        return lambda row: getattr(row, attr) == value

    def __ne__(self, value):
        attr = self._attr
        return lambda row: getattr(row, attr) != value

    def __hash__(self):
        return hash(("_Col", self._attr))


# ──────────────────────────────────────────────────────────────────────────────
# Fake ORM objects
# ──────────────────────────────────────────────────────────────────────────────

class FakeChatTemplate:
    """Fake ORM row with class-level column descriptors matching the real model."""

    # Class-level descriptors for use in order_by / filter expressions
    id = _Col("id")
    name = _Col("name")
    is_default = _Col("is_default")
    created_at = _Col("created_at")
    updated_at = _Col("updated_at")
    participants = _Col("participants")

    def __init__(self, *, id=1, name="Default", participants=None,
                 is_default=False, created_at=None, updated_at=None):
        # Instance attributes shadow the class-level _Col descriptors on reads
        self.id = id
        self.name = name
        self.participants = participants if participants is not None else []
        self.is_default = is_default
        self.created_at = created_at or datetime(2025, 1, 1)
        self.updated_at = updated_at or datetime(2025, 1, 1)


class FakeQuery:
    """SQLAlchemy query stub backed by a mutable list."""

    def __init__(self, rows):
        self._rows = rows
        self._filters = []
        self._order = []

    def filter(self, *criteria):
        self._filters.extend(criteria)
        return self

    def order_by(self, *criteria):
        self._order = list(criteria)
        return self

    def count(self):
        return len(self._filtered())

    def first(self):
        rows = self._filtered()
        return rows[0] if rows else None

    def all(self):
        rows = self._filtered()
        for criterion in reversed(self._order):
            if isinstance(criterion, tuple) and len(criterion) == 3 and criterion[0] == "_ORDER":
                _, attr, direction = criterion
                rows = sorted(rows, key=lambda r, a=attr: getattr(r, a),
                              reverse=(direction == "desc"))
        return rows

    def update(self, values):
        for row in self._filtered():
            for k, v in values.items():
                setattr(row, k, v)

    def _filtered(self):
        result = list(self._rows)
        for f in self._filters:
            if callable(f):
                result = [r for r in result if f(r)]
        return result


class FakeSession:
    """SQLAlchemy session stub."""

    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.added = []
        self.deleted = []
        self.committed = False

    def query(self, _model):
        return FakeQuery(self._rows)

    def add(self, obj):
        self.added.append(obj)
        self._rows.append(obj)

    def delete(self, obj):
        self.deleted.append(obj)
        self._rows = [r for r in self._rows if r is not obj]

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


# ──────────────────────────────────────────────────────────────────────────────
# API handler loader
# ──────────────────────────────────────────────────────────────────────────────

def _load_handler(session, module_name="chat_templates"):
    """Load api/v2/<module_name>.py with all deps stubbed.

    After loading, callers patch mod.request before calling handler methods.
    """
    pd_mod = _load_pd_module()

    @contextlib.contextmanager
    def get_session(_pid):
        yield session

    tools_mod = types.ModuleType("tools")
    tools_mod.db = types.SimpleNamespace(get_session=get_session)
    tools_mod.auth = types.ModuleType("tools.auth")
    tools_mod.auth.decorators = types.SimpleNamespace(
        check_api=lambda *a, **kw: (lambda func: func),
    )
    tools_mod.config = types.SimpleNamespace(
        DEFAULT_MODE="prompt_lib", ADMINISTRATION_MODE="default",
    )
    api_tools = types.ModuleType("tools.api_tools")
    api_tools.APIModeHandler = object
    api_tools.APIBase = object
    api_tools.endpoint_metrics = lambda func: func
    api_tools.with_modes = lambda urls: urls
    tools_mod.api_tools = api_tools

    flask_mod = types.ModuleType("flask")
    flask_mod.request = types.SimpleNamespace(json={})

    extra = {
        "flask": flask_mod,
        "tools": tools_mod,
        "tools.api_tools": api_tools,
        "tools.auth": tools_mod.auth,
        "tools.config": tools_mod.config,
        "tools.db": tools_mod.db,
    }

    handler_path = os.path.join(PLUGIN_ROOT, "api", "v2", f"{module_name}.py")

    with _with_stubs(extra):
        for pkg_name in ("elitea_core", "elitea_core.api", "elitea_core.api.v2",
                         "elitea_core.models", "elitea_core.models.pd", "elitea_core.utils"):
            m = types.ModuleType(pkg_name)
            m.__path__ = [PLUGIN_ROOT]
            sys.modules[pkg_name] = m

        ct_orm = types.ModuleType("elitea_core.models.chat_template")
        ct_orm.ChatTemplate = FakeChatTemplate
        ct_pd = types.ModuleType("elitea_core.models.pd.chat_template")
        ct_pd.ChatTemplateCreate = pd_mod.ChatTemplateCreate
        ct_pd.ChatTemplateRead = pd_mod.ChatTemplateRead
        ct_pd.ChatTemplateUpdate = pd_mod.ChatTemplateUpdate
        constants = types.ModuleType("elitea_core.utils.constants")
        constants.PROMPT_LIB_MODE = "prompt_lib"
        sys.modules.update({
            "elitea_core.models.chat_template": ct_orm,
            "elitea_core.models.pd.chat_template": ct_pd,
            "elitea_core.utils.constants": constants,
        })

        spec = importlib.util.spec_from_file_location(
            f"elitea_core.api.v2.{module_name}", handler_path,
        )
        handler_mod = importlib.util.module_from_spec(spec)
        handler_mod.__package__ = "elitea_core.api.v2"
        sys.modules[f"elitea_core.api.v2.{module_name}"] = handler_mod
        spec.loader.exec_module(handler_mod)

    return handler_mod


def _call(session, request_json, method, module_name="chat_templates", **kwargs):
    """Load handler, patch request, call method, return (body, status)."""
    mod = _load_handler(session, module_name)
    # Patch the module-level `request` name; handler methods read it from globals
    mod.request = types.SimpleNamespace(json=request_json)
    return getattr(mod.PromptLibAPI(), method)(project_id=1, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# 1. Pydantic model validation
# ──────────────────────────────────────────────────────────────────────────────

class TestChatTemplateCreateValidation(unittest.TestCase):

    def setUp(self):
        self.pd = _load_pd_module()

    def test_valid_name_accepted(self):
        obj = self.pd.ChatTemplateCreate(name="Sprint Review")
        self.assertEqual(obj.name, "Sprint Review")

    def test_name_is_stripped(self):
        obj = self.pd.ChatTemplateCreate(name="  Review  ")
        self.assertEqual(obj.name, "Review")

    def test_blank_name_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            self.pd.ChatTemplateCreate(name="   ")

    def test_empty_name_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            self.pd.ChatTemplateCreate(name="")

    def test_max_length_boundary_accepted(self):
        obj = self.pd.ChatTemplateCreate(name="x" * 64)
        self.assertEqual(len(obj.name), 64)

    def test_over_max_length_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            self.pd.ChatTemplateCreate(name="x" * 65)

    def test_empty_participants_is_default(self):
        obj = self.pd.ChatTemplateCreate(name="A")
        self.assertEqual(obj.participants, [])

    def test_valid_participant_accepted(self):
        obj = self.pd.ChatTemplateCreate(
            name="A",
            participants=[{"id": 1, "entity_name": "application"}],
        )
        self.assertEqual(len(obj.participants), 1)
        self.assertEqual(obj.participants[0].id, 1)

    def test_participant_missing_required_field_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            self.pd.ChatTemplateCreate(
                name="A",
                participants=[{"entity_name": "application"}],  # id missing
            )

    def test_participant_optional_fields_default_to_none(self):
        obj = self.pd.ChatTemplateCreate(
            name="A",
            participants=[{"id": 5, "entity_name": "application"}],
        )
        p = obj.participants[0]
        self.assertIsNone(p.name)
        self.assertIsNone(p.project_id)
        self.assertIsNone(p.agent_type)


class TestChatTemplateUpdateValidation(unittest.TestCase):

    def setUp(self):
        self.pd = _load_pd_module()

    def test_same_validation_as_create(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            self.pd.ChatTemplateUpdate(name="  ")


class TestChatTemplateReadFromAttributes(unittest.TestCase):

    def setUp(self):
        self.pd = _load_pd_module()

    def test_from_orm_row(self):
        row = FakeChatTemplate(id=3, name="Weekly", participants=[], is_default=True)
        read = self.pd.ChatTemplateRead.model_validate(row)
        self.assertEqual(read.id, 3)
        self.assertEqual(read.name, "Weekly")
        self.assertTrue(read.is_default)

    def test_model_dump_json_mode_serialises_datetimes(self):
        row = FakeChatTemplate(id=1, name="X", is_default=False,
                               created_at=datetime(2025, 6, 1, 12, 0),
                               updated_at=datetime(2025, 6, 1, 12, 0))
        data = self.pd.ChatTemplateRead.model_validate(row).model_dump(mode='json')
        self.assertIsInstance(data["created_at"], str)
        self.assertIn("2025-06-01", data["created_at"])


# ──────────────────────────────────────────────────────────────────────────────
# 2. API handler business logic
# ──────────────────────────────────────────────────────────────────────────────

class TestApiGet(unittest.TestCase):

    def test_returns_200_with_list(self):
        rows = [
            FakeChatTemplate(id=1, name="Default", is_default=True),
            FakeChatTemplate(id=2, name="Other", is_default=False),
        ]
        body, status = _call(FakeSession(rows), {}, "get")
        self.assertEqual(status, 200)
        self.assertEqual(len(body), 2)

    def test_default_template_appears_first(self):
        rows = [
            FakeChatTemplate(id=2, name="Other", is_default=False,
                             created_at=datetime(2025, 1, 1)),
            FakeChatTemplate(id=1, name="Default", is_default=True,
                             created_at=datetime(2025, 1, 2)),
        ]
        body, _ = _call(FakeSession(rows), {}, "get")
        self.assertTrue(body[0]["is_default"])

    def test_empty_project_returns_empty_list(self):
        body, status = _call(FakeSession([]), {}, "get")
        self.assertEqual(status, 200)
        self.assertEqual(body, [])


class TestApiPostCreate(unittest.TestCase):

    def test_first_create_returns_201(self):
        _, status = _call(FakeSession([]), {"name": "Default"}, "post")
        self.assertEqual(status, 201)

    def test_first_template_is_not_auto_default(self):
        session = FakeSession([])
        _call(session, {"name": "First"}, "post")
        self.assertFalse(session.added[0].is_default)

    def test_template_created_when_none_is_default_stays_non_default(self):
        existing = [FakeChatTemplate(id=1, name="Existing", is_default=False)]
        session = FakeSession(existing)
        _call(session, {"name": "Second"}, "post")
        self.assertFalse(session.added[0].is_default)
        self.assertFalse(existing[0].is_default)

    def test_limit_5_returns_400(self):
        rows = [FakeChatTemplate(id=i, name=f"T{i}") for i in range(1, 6)]
        _, status = _call(FakeSession(rows), {"name": "One More"}, "post")
        self.assertEqual(status, 400)

    def test_blank_name_returns_400(self):
        _, status = _call(FakeSession([]), {"name": ""}, "post")
        self.assertEqual(status, 400)

    def test_name_conflict_returns_400(self):
        # Subclass FakeQuery so all rows always match (simulates ilike hit)
        class _AlwaysMatchQuery(FakeQuery):
            def _filtered(self):
                return list(self._rows)

        class _MatchSession(FakeSession):
            def query(self, _model):
                return _AlwaysMatchQuery(self._rows)

        existing = [FakeChatTemplate(id=1, name="Sprint", is_default=True)]
        _, status = _call(_MatchSession(existing), {"name": "Sprint"}, "post")
        self.assertEqual(status, 400)


def _call_default(session, method, **kwargs):
    return _call(session, {}, method, module_name="chat_template_default", **kwargs)


class TestApiPostSetDefault(unittest.TestCase):

    def test_set_default_returns_200(self):
        default_tpl = FakeChatTemplate(id=1, name="Old Default", is_default=True)
        other_tpl = FakeChatTemplate(id=2, name="Other", is_default=False)
        _, status = _call_default(FakeSession([default_tpl, other_tpl]), "post", template_id=2)
        self.assertEqual(status, 200)

    def test_set_default_clears_previous(self):
        default_tpl = FakeChatTemplate(id=1, name="Old Default", is_default=True)
        other_tpl = FakeChatTemplate(id=2, name="New Default", is_default=False)
        session = FakeSession([default_tpl, other_tpl])
        _call_default(session, "post", template_id=2)
        self.assertFalse(default_tpl.is_default)
        self.assertTrue(other_tpl.is_default)

    def test_set_default_idempotent_when_already_default(self):
        tpl = FakeChatTemplate(id=7, name="Default", is_default=True)
        body, status = _call_default(FakeSession([tpl]), "post", template_id=7)
        self.assertEqual(status, 200)
        self.assertTrue(body["is_default"])

    def test_set_default_not_found_returns_404(self):
        _, status = _call_default(FakeSession([]), "post", template_id=99)
        self.assertEqual(status, 404)


class TestApiDeleteUnsetDefault(unittest.TestCase):

    def test_unset_default_clears_flag(self):
        tpl = FakeChatTemplate(id=1, name="Default", is_default=True)
        session = FakeSession([tpl])
        body, status = _call_default(session, "delete", template_id=1)
        self.assertEqual(status, 200)
        self.assertFalse(body["is_default"])
        self.assertFalse(tpl.is_default)
        self.assertTrue(session.committed)

    def test_unset_leaves_no_default(self):
        default_tpl = FakeChatTemplate(id=1, name="Default", is_default=True)
        other_tpl = FakeChatTemplate(id=2, name="Other", is_default=False)
        _call_default(FakeSession([default_tpl, other_tpl]), "delete", template_id=1)
        self.assertFalse(default_tpl.is_default)
        self.assertFalse(other_tpl.is_default)

    def test_unset_non_default_is_noop(self):
        tpl = FakeChatTemplate(id=2, name="Other", is_default=False)
        session = FakeSession([tpl])
        body, status = _call_default(session, "delete", template_id=2)
        self.assertEqual(status, 200)
        self.assertFalse(body["is_default"])
        self.assertFalse(session.committed)

    def test_unset_not_found_returns_404(self):
        _, status = _call_default(FakeSession([]), "delete", template_id=99)
        self.assertEqual(status, 404)


class TestApiDelete(unittest.TestCase):

    def test_delete_non_default_returns_204(self):
        tpl = FakeChatTemplate(id=2, name="Other", is_default=False)
        _, status = _call(FakeSession([tpl]), {}, "delete", template_id=2)
        self.assertEqual(status, 204)

    def test_delete_default_returns_204(self):
        tpl = FakeChatTemplate(id=1, name="Default", is_default=True)
        session = FakeSession([tpl])
        _, status = _call(session, {}, "delete", template_id=1)
        self.assertEqual(status, 204)
        self.assertIn(tpl, session.deleted)

    def test_delete_commits(self):
        tpl = FakeChatTemplate(id=2, name="Other", is_default=False)
        session = FakeSession([tpl])
        _, status = _call(session, {}, "delete", template_id=2)
        self.assertEqual(status, 204)
        self.assertTrue(session.committed)

    def test_delete_missing_returns_404(self):
        _, status = _call(FakeSession([]), {}, "delete", template_id=99)
        self.assertEqual(status, 404)

    def test_delete_removes_from_session(self):
        tpl = FakeChatTemplate(id=2, name="Other", is_default=False)
        session = FakeSession([tpl])
        _call(session, {}, "delete", template_id=2)
        self.assertIn(tpl, session.deleted)


class TestApiPut(unittest.TestCase):

    def test_put_updates_name(self):
        tpl = FakeChatTemplate(id=1, name="Old", is_default=True)
        session = FakeSession([tpl])
        _call(session, {"name": "New", "participants": []}, "put", template_id=1)
        self.assertEqual(tpl.name, "New")

    def test_put_not_found_returns_404(self):
        _, status = _call(FakeSession([]), {"name": "X"}, "put", template_id=99)
        self.assertEqual(status, 404)

    def test_put_blank_name_returns_400(self):
        tpl = FakeChatTemplate(id=1, name="Old", is_default=True)
        _, status = _call(FakeSession([tpl]), {"name": "  "}, "put", template_id=1)
        self.assertEqual(status, 400)


# ──────────────────────────────────────────────────────────────────────────────
# 3. Migration task — migrate_project_chat_config
# ──────────────────────────────────────────────────────────────────────────────

class _FakeRpc:
    """Fake rpc_manager.call for migration tests."""

    def __init__(self, projects, existing_cfg=None):
        self._projects = projects
        self._existing_cfg = existing_cfg
        self.created_configs = []

    def project_list(self, filter_=None):
        return self._projects

    def configurations_get_first_filtered_project(self, project_id, filter_fields):
        return self._existing_cfg

    def configurations_create_if_not_exists(self, payload):
        self.created_configs.append(payload)


def _make_migration_stubs(sessions):
    """Build the stubs dict used when loading and calling the migration method."""
    @contextlib.contextmanager
    def get_session(project_id):
        yield sessions.get(project_id, FakeSession([]))

    tools_mod = types.ModuleType("tools")
    tools_mod.db = types.SimpleNamespace(get_session=get_session)
    tools_mod.config = types.SimpleNamespace(get=lambda k, d=None: d)

    scripts_icons = types.ModuleType("elitea_core.scripts.tool_icons")
    scripts_icons.download_github_repo_zip = lambda **kw: {"ok": False}
    scripts_icons.unzip_file = lambda *a, **kw: None

    toolkit_mig = types.ModuleType("elitea_core.utils.toolkit_migration")
    toolkit_mig.run_selected_tools_migration = lambda *a, **kw: None
    toolkit_mig.migrate_project_pipeline_instructions = lambda *a, **kw: None

    llm_mig = types.ModuleType("elitea_core.utils.llm_migration_utils")
    for fn in (
        "parse_migration_params", "resolve_target_project_ids",
        "validate_target_model", "lookup_source_model_capabilities",
        "build_new_llm_settings", "migrate_application_versions",
        "migrate_participant_mappings", "parse_heal_params",
        "heal_family_conflict_versions", "heal_family_conflict_mappings",
    ):
        setattr(llm_mig, fn, lambda *a, **kw: None)

    emb_mig = types.ModuleType("elitea_core.utils.embedding_migration_utils")
    emb_mig.validate_target_embedding_model = lambda *a, **kw: None
    emb_mig.migrate_toolkit_embedding_models = lambda *a, **kw: None

    backfill = types.ModuleType("elitea_core.utils.trace_step_backfill_utils")
    backfill.parse_backfill_params = lambda *a, **kw: None
    backfill.backfill_project = lambda *a, **kw: None

    utils_utils = types.ModuleType("elitea_core.utils.utils")
    utils_utils.get_public_project_id = lambda *a, **kw: None
    utils_utils.make_yield_to_hub = lambda *a, **kw: None

    ct_orm = types.ModuleType("elitea_core.models.chat_template")
    ct_orm.ChatTemplate = FakeChatTemplate

    return {
        **_pylon_stubs(),
        "tools": tools_mod,
        "tools.db": tools_mod.db,
        "tools.config": tools_mod.config,
        "elitea_core.scripts.tool_icons": scripts_icons,
        "elitea_core.utils.toolkit_migration": toolkit_mig,
        "elitea_core.utils.llm_migration_utils": llm_mig,
        "elitea_core.utils.embedding_migration_utils": emb_mig,
        "elitea_core.utils.trace_step_backfill_utils": backfill,
        "elitea_core.utils.utils": utils_utils,
        "elitea_core.models.chat_template": ct_orm,
    }


def _load_migration_fn(rpc, sessions):
    """Load admin_tasks.py; return a callable for migrate_project_chat_config.

    The callable re-installs stubs around each invocation so the local imports
    inside the method body (`from tools import db`, `from ..models...`) resolve.
    """
    stubs = _make_migration_stubs(sessions)

    # Install stubs and package hierarchy for the load phase
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)

    for pkg in ("elitea_core", "elitea_core.scripts", "elitea_core.utils",
                "elitea_core.models", "elitea_core.methods"):
        if pkg not in sys.modules:
            m = types.ModuleType(pkg)
            m.__path__ = [PLUGIN_ROOT]
            sys.modules[pkg] = m

    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks",
        os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"),
    )
    admin_mod = importlib.util.module_from_spec(spec)
    admin_mod.__package__ = "elitea_core.methods"
    sys.modules["elitea_core.methods.admin_tasks"] = admin_mod
    spec.loader.exec_module(admin_mod)

    # Restore sys.modules after loading
    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v

    fake_self = types.SimpleNamespace(
        context=types.SimpleNamespace(
            rpc_manager=types.SimpleNamespace(call=rpc),
        ),
    )

    def call(*args, **kwargs):
        # Re-install stubs for the method call duration
        call_saved = {k: sys.modules.get(k) for k in stubs}
        sys.modules.update(stubs)
        try:
            return admin_mod.Method.migrate_project_chat_config(fake_self, *args, **kwargs)
        finally:
            for k, v in call_saved.items():
                if v is None:
                    sys.modules.pop(k, None)
                else:
                    sys.modules[k] = v

    return call


class TestMigrationEntityIdRemapping(unittest.TestCase):
    """entity_id in the legacy format must become id in the new template."""

    def test_entity_id_becomes_id(self):
        cfg = {"data": {"chat_config": {"participants": [
            {"entity_id": 42, "name": "Agent A", "entity_name": "application",
             "project_id": 1, "agent_type": None},
        ]}}}
        rpc = _FakeRpc([{"id": 1}], existing_cfg=cfg)
        session = FakeSession([])

        result = _load_migration_fn(rpc, {1: session})(param="project_id=1")

        self.assertEqual(result["errors"], 0)
        self.assertEqual(len(session.added), 1)
        p = session.added[0].participants[0]
        self.assertEqual(p["id"], 42)
        self.assertNotIn("entity_id", p)

    def test_multiple_participants_all_remapped(self):
        cfg = {"data": {"chat_config": {"participants": [
            {"entity_id": 10, "entity_name": "application", "project_id": 1},
            {"entity_id": 20, "entity_name": "application", "project_id": 1},
        ]}}}
        rpc = _FakeRpc([{"id": 1}], existing_cfg=cfg)
        session = FakeSession([])

        _load_migration_fn(rpc, {1: session})(param="project_id=1")

        seeded = session.added[0].participants
        self.assertEqual([p["id"] for p in seeded], [10, 20])


class TestMigrationIdempotency(unittest.TestCase):

    def test_skips_step2_when_templates_already_exist(self):
        cfg = {"data": {"chat_config": {"participants": []}}}
        rpc = _FakeRpc([{"id": 1}], existing_cfg=cfg)
        session = FakeSession([FakeChatTemplate(id=1, name="Default", is_default=True)])

        result = _load_migration_fn(rpc, {1: session})(param="project_id=1")

        self.assertEqual(result["templates_skipped"], 1)
        self.assertEqual(len(session.added), 0)

    def test_seeds_when_no_templates_exist(self):
        rpc = _FakeRpc([{"id": 1}], existing_cfg=None)
        session = FakeSession([])

        result = _load_migration_fn(rpc, {1: session})(param="project_id=1")

        self.assertEqual(result["templates_seeded"], 1)


class TestMigrationDryRun(unittest.TestCase):

    def test_dry_run_does_not_add_to_session(self):
        rpc = _FakeRpc([{"id": 1}], existing_cfg=None)
        session = FakeSession([])

        result = _load_migration_fn(rpc, {1: session})(param="project_id=1;dry_run")

        self.assertTrue(result["dry_run"])
        self.assertEqual(len(session.added), 0)

    def test_dry_run_reports_would_seed(self):
        rpc = _FakeRpc([{"id": 1}], existing_cfg=None)
        result = _load_migration_fn(rpc, {1: FakeSession([])})(param="project_id=1;dry_run")
        self.assertIn("templates_would_seed", result)
        self.assertEqual(result["templates_would_seed"], 1)

    def test_dry_run_does_not_create_legacy_config(self):
        rpc = _FakeRpc([{"id": 1}], existing_cfg=None)
        _load_migration_fn(rpc, {1: FakeSession([])})(param="project_id=1;dry_run")
        self.assertEqual(len(rpc.created_configs), 0)


class TestMigrationParamParsing(unittest.TestCase):

    def test_invalid_project_id_returns_error(self):
        rpc = _FakeRpc([{"id": 1}])
        result = _load_migration_fn(rpc, {})(param="project_id=abc")
        self.assertIn("error", result)

    def test_nonexistent_project_id_returns_error(self):
        rpc = _FakeRpc([{"id": 1}])
        result = _load_migration_fn(rpc, {})(param="project_id=999")
        self.assertIn("error", result)

    def test_all_projects_iterates_over_all(self):
        rpc = _FakeRpc([{"id": 1}, {"id": 2}], existing_cfg=None)
        result = _load_migration_fn(rpc, {1: FakeSession([]), 2: FakeSession([])})(
            param="project_id=all"
        )
        self.assertEqual(result["templates_seeded"], 2)


class TestMigrationEmptyParticipants(unittest.TestCase):

    def test_no_config_seeds_empty_default_template(self):
        rpc = _FakeRpc([{"id": 1}], existing_cfg=None)
        session = FakeSession([])

        _load_migration_fn(rpc, {1: session})(param="project_id=1")

        template = session.added[0]
        self.assertEqual(template.name, "Default")
        self.assertTrue(template.is_default)
        self.assertEqual(template.participants, [])

    def test_empty_participants_list_seeds_empty_template(self):
        cfg = {"data": {"chat_config": {"participants": []}}}
        rpc = _FakeRpc([{"id": 1}], existing_cfg=cfg)
        session = FakeSession([])

        _load_migration_fn(rpc, {1: session})(param="project_id=1")

        self.assertEqual(session.added[0].participants, [])


# ──────────────────────────────────────────────────────────────────────────────
# 4. chat_template_utils — delete and update helpers
# ──────────────────────────────────────────────────────────────────────────────

def _load_utils_fns(sessions):
    """Load utils/chat_template_utils.py with stubs; return (delete_fn, update_fn)."""
    @contextlib.contextmanager
    def get_session(project_id):
        yield sessions.get(project_id, FakeSession([]))

    tools_mod = types.ModuleType("tools")
    tools_mod.db = types.SimpleNamespace(get_session=get_session)

    ct_orm_mod = types.ModuleType("elitea_core.models.chat_template")
    ct_orm_mod.ChatTemplate = FakeChatTemplate

    stubs = {
        **_pylon_stubs(),
        "tools": tools_mod,
        "tools.db": tools_mod.db,
        "elitea_core.models.chat_template": ct_orm_mod,
    }

    path = os.path.join(PLUGIN_ROOT, "utils", "chat_template_utils.py")
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)

    for pkg in ("elitea_core", "elitea_core.models", "elitea_core.utils"):
        if pkg not in sys.modules:
            m = types.ModuleType(pkg)
            m.__path__ = [PLUGIN_ROOT]
            sys.modules[pkg] = m

    sys.modules["elitea_core.models.chat_template"] = ct_orm_mod

    spec = importlib.util.spec_from_file_location(
        "elitea_core.utils.chat_template_utils", path,
    )
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "elitea_core.utils"
    sys.modules["elitea_core.utils.chat_template_utils"] = mod
    spec.loader.exec_module(mod)

    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v

    return mod.delete_entity_from_templates, mod.update_entity_name_in_templates


class TestDeleteEntityFromTemplates(unittest.TestCase):

    def test_removes_matching_participant(self):
        tpl = FakeChatTemplate(id=1, name="T", participants=[
            {"entity_name": "application", "id": 42, "project_id": 1, "name": "App A"},
        ])
        delete_fn, _ = _load_utils_fns({1: FakeSession([tpl])})
        delete_fn(1, ["application"], 42, 1)
        self.assertEqual(tpl.participants, [])

    def test_noop_when_id_does_not_match(self):
        tpl = FakeChatTemplate(id=1, name="T", participants=[
            {"entity_name": "application", "id": 99, "project_id": 1},
        ])
        delete_fn, _ = _load_utils_fns({1: FakeSession([tpl])})
        delete_fn(1, ["application"], 42, 1)
        self.assertEqual(len(tpl.participants), 1)

    def test_removes_only_matching_entity_name(self):
        tpl = FakeChatTemplate(id=1, name="T", participants=[
            {"entity_name": "application", "id": 42, "project_id": 1},
            {"entity_name": "toolkit", "id": 42, "project_id": 1},
        ])
        delete_fn, _ = _load_utils_fns({1: FakeSession([tpl])})
        delete_fn(1, ["application"], 42, 1)
        self.assertEqual(len(tpl.participants), 1)
        self.assertEqual(tpl.participants[0]["entity_name"], "toolkit")

    def test_removes_across_multiple_templates(self):
        tpl1 = FakeChatTemplate(id=1, name="A", participants=[
            {"entity_name": "application", "id": 7, "project_id": 1},
        ])
        tpl2 = FakeChatTemplate(id=2, name="B", participants=[
            {"entity_name": "application", "id": 7, "project_id": 1},
        ])
        delete_fn, _ = _load_utils_fns({1: FakeSession([tpl1, tpl2])})
        delete_fn(1, ["application"], 7, 1)
        self.assertEqual(tpl1.participants, [])
        self.assertEqual(tpl2.participants, [])


class TestUpdateEntityNameInTemplates(unittest.TestCase):

    def test_updates_matching_participant_name(self):
        tpl = FakeChatTemplate(id=1, name="T", participants=[
            {"entity_name": "application", "id": 5, "project_id": 1, "name": "Old Name"},
        ])
        _, update_fn = _load_utils_fns({1: FakeSession([tpl])})
        update_fn(1, ["application"], 5, 1, "New Name")
        self.assertEqual(tpl.participants[0]["name"], "New Name")

    def test_noop_when_id_does_not_match(self):
        tpl = FakeChatTemplate(id=1, name="T", participants=[
            {"entity_name": "application", "id": 99, "project_id": 1, "name": "Unchanged"},
        ])
        _, update_fn = _load_utils_fns({1: FakeSession([tpl])})
        update_fn(1, ["application"], 42, 1, "New Name")
        self.assertEqual(tpl.participants[0]["name"], "Unchanged")

    def test_updates_only_correct_entity_id(self):
        tpl = FakeChatTemplate(id=1, name="T", participants=[
            {"entity_name": "application", "id": 5, "project_id": 1, "name": "Target"},
            {"entity_name": "application", "id": 6, "project_id": 1, "name": "Other"},
        ])
        _, update_fn = _load_utils_fns({1: FakeSession([tpl])})
        update_fn(1, ["application"], 5, 1, "Updated")
        self.assertEqual(tpl.participants[0]["name"], "Updated")
        self.assertEqual(tpl.participants[1]["name"], "Other")

    def test_does_not_dirty_unaffected_template(self):
        tpl1 = FakeChatTemplate(id=1, name="A", participants=[
            {"entity_name": "application", "id": 5, "project_id": 1, "name": "Target"},
        ])
        tpl2 = FakeChatTemplate(id=2, name="B", participants=[
            {"entity_name": "application", "id": 9, "project_id": 1, "name": "Untouched"},
        ])
        original_list_obj = tpl2.participants
        _, update_fn = _load_utils_fns({1: FakeSession([tpl1, tpl2])})
        update_fn(1, ["application"], 5, 1, "Updated")
        # tpl2 had no match — its participants list object must not be replaced
        self.assertIs(tpl2.participants, original_list_obj)

    def test_updates_across_multiple_templates(self):
        tpl1 = FakeChatTemplate(id=1, name="A", participants=[
            {"entity_name": "application", "id": 5, "project_id": 1, "name": "Old"},
        ])
        tpl2 = FakeChatTemplate(id=2, name="B", participants=[
            {"entity_name": "application", "id": 5, "project_id": 1, "name": "Old"},
        ])
        _, update_fn = _load_utils_fns({1: FakeSession([tpl1, tpl2])})
        update_fn(1, ["application"], 5, 1, "Updated")
        self.assertEqual(tpl1.participants[0]["name"], "Updated")
        self.assertEqual(tpl2.participants[0]["name"], "Updated")


# ──────────────────────────────────────────────────────────────────────────────
# 5. Event handlers — _affected_projects scope and rename propagation
# ──────────────────────────────────────────────────────────────────────────────

def _load_events_module(public_project_id=None):
    """Load events/chat_template.py with stubs.

    Returns (Event instance, calls dict) where calls["delete"] and
    calls["update"] accumulate the project_id arguments passed to the
    mocked utility functions.
    """
    calls = {"delete": [], "update": []}

    def mock_delete(project_id, entity_names, entity_id, entity_project_id):
        calls["delete"].append(project_id)

    def mock_update(project_id, entity_names, entity_id, entity_project_id, new_name):
        calls["update"].append(project_id)

    ct_utils_mod = types.ModuleType("elitea_core.utils.chat_template_utils")
    ct_utils_mod.delete_entity_from_templates = mock_delete
    ct_utils_mod.update_entity_name_in_templates = mock_update

    utils_utils_mod = types.ModuleType("elitea_core.utils.utils")
    utils_utils_mod.get_public_project_id = lambda: public_project_id

    events_enums_mod = types.ModuleType("elitea_core.models.enums.events")
    events_enums_mod.ApplicationEvents = types.SimpleNamespace(
        application_deleted="application_deleted",
        toolkit_deleted="toolkit_deleted",
        application_updated="application_updated",
        toolkit_updated="toolkit_updated",
    )

    stubs = {
        **_pylon_stubs(),
        "elitea_core.utils.chat_template_utils": ct_utils_mod,
        "elitea_core.utils.utils": utils_utils_mod,
        "elitea_core.models.enums.events": events_enums_mod,
    }

    path = os.path.join(PLUGIN_ROOT, "events", "chat_template.py")
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)

    for pkg in ("elitea_core", "elitea_core.models", "elitea_core.models.enums",
                "elitea_core.utils", "elitea_core.events"):
        if pkg not in sys.modules:
            m = types.ModuleType(pkg)
            m.__path__ = [PLUGIN_ROOT]
            sys.modules[pkg] = m

    spec = importlib.util.spec_from_file_location(
        "elitea_core.events.chat_template", path,
    )
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "elitea_core.events"
    sys.modules["elitea_core.events.chat_template"] = mod
    spec.loader.exec_module(mod)

    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v

    return mod.Event(), calls


def _make_context(project_ids):
    """Build a fake pylon context whose rpc_manager returns the given project list."""
    rpc = types.SimpleNamespace(
        call=types.SimpleNamespace(
            project_list=lambda filter_=None: [{"id": p} for p in project_ids],
        )
    )
    return types.SimpleNamespace(rpc_manager=rpc)


class TestEventHandlerDeletePropagation(unittest.TestCase):

    def test_delete_propagates_to_all_projects_for_public_app(self):
        PUBLIC_ID = 0
        handler, calls = _load_events_module(public_project_id=PUBLIC_ID)
        handler.on_application_deleted(_make_context([1, 2, 3]), None, {
            "owner_id": PUBLIC_ID, "id": 42,
        })
        self.assertCountEqual(calls["delete"], [1, 2, 3])

    def test_delete_propagates_to_all_projects_for_public_toolkit(self):
        PUBLIC_ID = 0
        handler, calls = _load_events_module(public_project_id=PUBLIC_ID)
        handler.on_toolkit_deleted(_make_context([1, 2, 3]), None, {
            "owner_id": PUBLIC_ID, "id": 42,
        })
        self.assertCountEqual(calls["delete"], [1, 2, 3])

    def test_delete_only_touches_owning_project_for_private_entity(self):
        handler, calls = _load_events_module(public_project_id=99)
        handler.on_application_deleted(_make_context([1, 2, 3]), None, {
            "owner_id": 5, "id": 42,
        })
        self.assertEqual(calls["delete"], [5])


class TestEventHandlerRenamePropagation(unittest.TestCase):

    def test_rename_propagates_to_all_projects_for_public_app(self):
        PUBLIC_ID = 0
        handler, calls = _load_events_module(public_project_id=PUBLIC_ID)
        handler.on_application_updated(_make_context([1, 2, 3]), None, {
            "owner_id": PUBLIC_ID, "id": 7, "data": {"name": "Renamed App"},
        })
        self.assertCountEqual(calls["update"], [1, 2, 3])

    def test_rename_propagates_to_all_projects_for_public_toolkit(self):
        PUBLIC_ID = 0
        handler, calls = _load_events_module(public_project_id=PUBLIC_ID)
        handler.on_toolkit_updated(_make_context([1, 2, 3]), None, {
            "owner_id": PUBLIC_ID, "id": 7, "data": {"name": "Renamed Toolkit"},
        })
        self.assertCountEqual(calls["update"], [1, 2, 3])

    def test_rename_only_touches_owning_project_for_private_app(self):
        handler, calls = _load_events_module(public_project_id=99)
        handler.on_application_updated(_make_context([1, 2, 3]), None, {
            "owner_id": 5, "id": 7, "data": {"name": "Renamed"},
        })
        self.assertEqual(calls["update"], [5])

    def test_rename_only_touches_owning_project_for_private_toolkit(self):
        handler, calls = _load_events_module(public_project_id=99)
        handler.on_toolkit_updated(_make_context([1, 2, 3]), None, {
            "owner_id": 5, "id": 7, "data": {"name": "Renamed"},
        })
        self.assertEqual(calls["update"], [5])

    def test_missing_name_in_payload_skips_update(self):
        handler, calls = _load_events_module(public_project_id=99)
        handler.on_application_updated(_make_context([1, 2, 3]), None, {
            "owner_id": 5, "id": 7, "data": {},
        })
        self.assertEqual(calls["update"], [])

    def test_null_data_field_skips_update(self):
        handler, calls = _load_events_module(public_project_id=99)
        handler.on_application_updated(_make_context([1, 2, 3]), None, {
            "owner_id": 5, "id": 7, "data": None,
        })
        self.assertEqual(calls["update"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
