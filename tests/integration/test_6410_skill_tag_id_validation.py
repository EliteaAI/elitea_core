"""Issue #6410 (Defect B) - `_apply_tags_to_version()` accepted a tag `id` but never checked it.

`PromptTagUpdateModel` carries both `name` and `id`, but tags were always matched/created by
`name` alone - a caller-supplied `id` that didn't exist, or belonged to a different tag, was
silently tolerated instead of surfacing a validation error.

This loads the real `utils/skill_utils.py` standalone (same technique as
`test_5978_runtime_skills.py`: minimal stubs for `tools`/ORM-adjacent modules, but the function
under test - `_apply_tags_to_version` - is the genuine one) and drives it with a fake
SQLAlchemy-ish session/`Tag` model to pin:

  1. `id`-less tags keep matching/creating by name (regression guard, unchanged behavior).
  2. A tag `id` that matches an existing row's `name` is still accepted (positive case).
  3. A tag `id` that doesn't exist raises ``SkillTagMismatchError`` (was: silently created a
     brand-new same-named-or-different tag by name).
  4. A tag `id` that exists but whose `name` doesn't match raises ``SkillTagMismatchError``
     (was: silently fell back to a name-based lookup/create, keeping the wrong tag attached).
  5. Both tag models callers actually pass work: `PromptTagUpdateModel` (update paths) *and*
     `TagBaseModel` (create/import/publish paths), which declares no `id` field at all - so
     `id` has to be read defensively rather than as a plain attribute.

The tags are the real Pydantic models, not stand-ins: a `SimpleNamespace` always has `.id`,
so it would hide the `AttributeError` -> 500 that `TagBaseModel` triggers.

Run via:
    python tests/run_tests.py integration/test_6410_skill_tag_id_validation.py -v
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG_ROOT = 'plugins'  # matches the package prefix skill_utils.py's relative imports expect


def _register(name, module):
    sys.modules[name] = module
    return module


# --- Fake SQLAlchemy-ish plumbing, enough for `session.query(Tag).filter(Tag.x.in_(...))` ----

class _InCriterion:
    def __init__(self, attr, values):
        self.attr = attr
        self.values = set(values)


class _FakeColumn:
    def __init__(self, attr):
        self.attr = attr

    def in_(self, values):
        return _InCriterion(self.attr, values)


class FakeTag:
    id = _FakeColumn('id')
    name = _FakeColumn('name')

    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows
        self._criterion = None

    def filter(self, criterion):
        self._criterion = criterion
        return self

    def all(self):
        if self._criterion is None:
            return list(self._rows)
        return [r for r in self._rows if getattr(r, self._criterion.attr) in self._criterion.values]


class FakeSession:
    """Minimal session: only what `_apply_tags_to_version` calls."""

    def __init__(self, existing_tags):
        self.rows = list(existing_tags)
        self.added = []
        self.flushed = 0

    def query(self, model):
        assert model is FakeTag
        return _FakeQuery(self.rows)

    def add(self, obj):
        self.added.append(obj)
        self.rows.append(obj)
        if getattr(obj, 'id', None) is None:
            obj.id = 1000 + len(self.added)  # mimic autoincrement on flush

    def flush(self):
        self.flushed += 1


class FakeVersion:
    def __init__(self):
        self.tags = []


@pytest.fixture(scope='module')
def skill_utils_module():
    """Load skill_utils.py standalone with minimal stubs (mirrors test_5978's technique)."""
    saved = {k: v for k, v in sys.modules.items()
             if k == 'tools' or k.startswith(f'{PKG_ROOT}.elitea_core')}

    for name in (
        PKG_ROOT, f'{PKG_ROOT}.elitea_core', f'{PKG_ROOT}.elitea_core.models',
        f'{PKG_ROOT}.elitea_core.models.pd', f'{PKG_ROOT}.elitea_core.models.enums',
        f'{PKG_ROOT}.elitea_core.utils',
    ):
        mod = types.ModuleType(name)
        mod.__path__ = []
        _register(name, mod)

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    for name in ('pylon', 'pylon.core'):
        m = types.ModuleType(name)
        m.__path__ = []
        _register(name, m)
    _register('pylon.core.tools', pylon_tools)

    tools_pkg = types.ModuleType('tools')
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(), current_user=lambda: {'id': 1},
    )
    tools_pkg.serialize = lambda obj, **k: obj
    tools_pkg.rpc_tools = types.SimpleNamespace()
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace(event_manager=types.SimpleNamespace(
        fire_event=lambda *a, **k: None))
    _register('tools', tools_pkg)

    authors = types.ModuleType(f'{PKG_ROOT}.elitea_core.utils.authors')
    authors.get_authors_data = lambda author_ids: []
    _register(f'{PKG_ROOT}.elitea_core.utils.authors', authors)

    utils_helpers = types.ModuleType(f'{PKG_ROOT}.elitea_core.utils.utils')
    utils_helpers.set_columns_as_attrs = lambda *a, **k: None
    utils_helpers.get_public_project_id = lambda: 1
    utils_helpers.parse_ids_filter = lambda *a, **k: None
    _register(f'{PKG_ROOT}.elitea_core.utils.utils', utils_helpers)

    like_utils = types.ModuleType(f'{PKG_ROOT}.elitea_core.utils.like_utils')
    for n in ('add_likes', 'add_my_liked', 'add_trending_likes', 'get_like_model'):
        setattr(like_utils, n, lambda *a, **k: None)
    _register(f'{PKG_ROOT}.elitea_core.utils.like_utils', like_utils)

    models_skill = types.ModuleType(f'{PKG_ROOT}.elitea_core.models.skill')
    models_skill.Skill = type('Skill', (), {})
    models_skill.SkillVersion = type('SkillVersion', (), {})
    models_skill.EntitySkillMapping = type('EntitySkillMapping', (), {})
    _register(f'{PKG_ROOT}.elitea_core.models.skill', models_skill)

    models_all = types.ModuleType(f'{PKG_ROOT}.elitea_core.models.all')
    models_all.Tag = FakeTag
    models_all.ApplicationVersion = type('ApplicationVersion', (), {})
    models_all.Application = type('Application', (), {})
    _register(f'{PKG_ROOT}.elitea_core.models.all', models_all)

    def _load_real(rel_path, name):
        spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / rel_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    _load_real('models/enums/all.py', f'{PKG_ROOT}.elitea_core.models.enums.all')
    _load_real('models/enums/events.py', f'{PKG_ROOT}.elitea_core.models.enums.events')

    from pydantic import BaseModel, ConfigDict

    class _Permissive(BaseModel):
        model_config = ConfigDict(extra='allow')

    pd_skill = types.ModuleType(f'{PKG_ROOT}.elitea_core.models.pd.skill')
    for n in ('SkillCreateModel', 'SkillDetailModel', 'SkillUpdateModel',
              'SkillImportResultModel', 'AgentsWithSkillItemModel'):
        setattr(pd_skill, n, type(n, (_Permissive,), {}))
    _register(f'{PKG_ROOT}.elitea_core.models.pd.skill', pd_skill)

    pd_skill_version = types.ModuleType(f'{PKG_ROOT}.elitea_core.models.pd.skill_version')
    for n in ('SkillVersionCreateModel', 'SkillVersionUpdateModel', 'SkillVersionDetailModel'):
        setattr(pd_skill_version, n, type(n, (_Permissive,), {}))
    _register(f'{PKG_ROOT}.elitea_core.models.pd.skill_version', pd_skill_version)

    folder_access = types.ModuleType(f'{PKG_ROOT}.elitea_core.utils.folder_access')
    folder_access.folder_exclusion_clause = lambda *a, **k: None
    _register(f'{PKG_ROOT}.elitea_core.utils.folder_access', folder_access)

    module = _load_real('utils/skill_utils.py', f'{PKG_ROOT}.elitea_core.utils.skill_utils')

    yield module

    for key in [k for k in sys.modules
                if k == 'tools' or k.startswith(f'{PKG_ROOT}.elitea_core') or k.startswith('pylon')]:
        del sys.modules[key]
    sys.modules.update(saved)


@pytest.fixture(scope='module')
def tag_models():
    """The real `TagBaseModel` / `PromptTagUpdateModel` - `collection_base.py` imports only
    typing and pydantic, so it loads standalone.

    `TagBaseModel` is what `create_skill`, `create_skill_version`, import and publish all pass
    (see `skill_publish_utils.py`), and it declares no `id` field - `t.id` on one is an
    `AttributeError`, which is not a `SkillError` and so escapes as a 500.
    """
    spec = importlib.util.spec_from_file_location(
        '_6410_collection_base', PLUGIN_ROOT / 'models' / 'pd' / 'collection_base.py'
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- Regression guard: id-less tags behave exactly as before ------------------------------

def test_idless_update_tag_reuses_an_existing_row_by_name(skill_utils_module, tag_models):
    session = FakeSession(existing_tags=[FakeTag(id=1, name='aqa')])
    version = FakeVersion()

    skill_utils_module._apply_tags_to_version(
        session, version, [tag_models.PromptTagUpdateModel(name='aqa')]
    )

    assert [t.id for t in version.tags] == [1]
    assert session.added == []


def test_idless_update_tag_creates_a_new_row_when_no_name_match(skill_utils_module, tag_models):
    session = FakeSession(existing_tags=[])
    version = FakeVersion()

    skill_utils_module._apply_tags_to_version(
        session, version, [tag_models.PromptTagUpdateModel(name='brand-new')]
    )

    assert len(session.added) == 1
    assert version.tags[0].name == 'brand-new'


# --- The create/import/publish paths pass TagBaseModel, which has no `id` field -----------

def test_base_tag_model_without_an_id_field_reuses_an_existing_row(skill_utils_module, tag_models):
    """`create_skill` with a single tag: `TagBaseModel` has no `id`, so reading it must not
    raise `AttributeError` (which would surface as a 500, not a `SkillError`)."""
    session = FakeSession(existing_tags=[FakeTag(id=1, name='aqa')])
    version = FakeVersion()

    skill_utils_module._apply_tags_to_version(
        session, version, [tag_models.TagBaseModel(name='aqa')]
    )

    assert [t.id for t in version.tags] == [1]
    assert session.added == []


def test_base_tag_model_without_an_id_field_creates_a_new_row(skill_utils_module, tag_models):
    session = FakeSession(existing_tags=[])
    version = FakeVersion()

    skill_utils_module._apply_tags_to_version(
        session, version, [tag_models.TagBaseModel(name='brand-new')]
    )

    assert len(session.added) == 1
    assert version.tags[0].name == 'brand-new'


# --- New behavior: a supplied id is validated against the matched tag's name --------------

def test_matching_id_and_name_is_accepted(skill_utils_module, tag_models):
    session = FakeSession(existing_tags=[FakeTag(id=22, name='aqa')])
    version = FakeVersion()

    skill_utils_module._apply_tags_to_version(
        session, version, [tag_models.PromptTagUpdateModel(id=22, name='aqa')]
    )

    assert [t.id for t in version.tags] == [22]
    assert session.added == []  # never falls back to create


def test_nonexistent_id_is_rejected_not_silently_created(skill_utils_module, tag_models):
    session = FakeSession(existing_tags=[])
    version = FakeVersion()

    with pytest.raises(skill_utils_module.SkillTagMismatchError) as exc_info:
        skill_utils_module._apply_tags_to_version(
            session, version, [tag_models.PromptTagUpdateModel(id=999, name='aqa')]
        )

    assert exc_info.value.http_status == 400
    assert session.added == []
    assert version.tags == []


def test_id_belonging_to_a_different_name_is_rejected_not_silently_reused(skill_utils_module, tag_models):
    """The ticket's own payload: tag id 22 is really named 'aqa', but the caller pairs it
    with a different name here - this must not silently resolve to either tag."""
    session = FakeSession(existing_tags=[FakeTag(id=22, name='aqa')])
    version = FakeVersion()

    with pytest.raises(skill_utils_module.SkillTagMismatchError):
        skill_utils_module._apply_tags_to_version(
            session, version, [tag_models.PromptTagUpdateModel(id=22, name='attach')]
        )

    assert version.tags == []


def test_mixed_tag_models_in_one_call(skill_utils_module, tag_models):
    """An id-carrying update tag, an id-less update tag, and an id-less `TagBaseModel`
    together - the mix `_apply_tags_to_version` has to tolerate across its callers."""
    session = FakeSession(existing_tags=[FakeTag(id=22, name='aqa')])
    version = FakeVersion()

    skill_utils_module._apply_tags_to_version(session, version, [
        tag_models.PromptTagUpdateModel(id=22, name='aqa'),
        tag_models.PromptTagUpdateModel(name='attach'),
        tag_models.TagBaseModel(name='publish'),
    ])

    names = {t.name for t in version.tags}
    assert names == {'aqa', 'attach', 'publish'}
