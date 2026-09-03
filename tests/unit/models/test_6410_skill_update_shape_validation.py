"""Issue #6410 - `PUT` on the Skill endpoint silently no-op'd on a body/URL shape mismatch.

`PromptLibAPI.put()` picks between two mutually-exclusive request-body schemas purely on
whether `version_id` is in the URL path: the flat `SkillVersionUpdateModel` (versioned URL)
or the nested `SkillUpdateModel` (version-less URL). Neither model rejected unknown keys and
every field defaulted to `None`, so sending the *other* shape validated cleanly into an
all-`None` model and produced a silent HTTP 200 no-op - `instructions` never updated, tags
collection never touched.

This test loads the real `models/pd/skill.py` and `models/pd/skill_version.py` (only `tools`,
`flask`, and the authors RPC helper are stubbed - everything else, including the real
`collection_base`/`tag`/`enums/all`/`constants` modules, is the genuine module) and pins:

  1. The flat model now rejects a nested `{"version": {...}}` body (was: silent no-op).
  2. The nested model now rejects a flat `{"instructions": ...}` body (was: silent no-op).
  3. Both models still accept their own correct shape unchanged (regression guard for the
     one caller - EliteaUI - that already sends the flat shape correctly).
  4. A stray top-level `version_id` alongside a correct nested body no longer 400s, matching
     the `put()` handler popping it before validation.
  5. The flat model still accepts a whole fetched `version_details` spread back into the body -
     EliteaUI's compare-versions save (`useCompareSkillVersions.hooks.js`) does exactly that,
     so the server-owned keys it echoes (`id`, `status`, `author_id`, `author`, `created_at`)
     are dropped rather than rejected. The nested model keeps its `id`, which selects the
     version to update.

Run via:
    python tests/run_tests.py unit/models/test_6410_skill_update_shape_validation.py -v
"""
import importlib.util
import pathlib
import sys
import types

import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'skillpkg_6410_models'


def _register(name, module):
    sys.modules[name] = module
    return module


def _load_real(rel_path, name):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def pd_module():
    saved = {k: v for k, v in sys.modules.items() if k.startswith(PKG) or k in ('tools', 'flask')}

    for name in (PKG, f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.models.enums', f'{PKG}.utils'):
        mod = types.ModuleType(name)
        mod.__path__ = []
        _register(name, mod)

    tools_pkg = types.ModuleType('tools')
    tools_pkg.rpc_tools = types.SimpleNamespace()
    _register('tools', tools_pkg)

    # Real, dependency-free modules - no reason to fake these.
    _load_real('models/pd/collection_base.py', f'{PKG}.models.pd.collection_base')
    _load_real('models/pd/tag.py', f'{PKG}.models.pd.tag')
    _load_real('models/enums/all.py', f'{PKG}.models.enums.all')
    constants = _load_real('utils/constants.py', f'{PKG}.utils.constants')

    authors = types.ModuleType(f'{PKG}.utils.authors')
    authors.get_authors_data = lambda author_ids: []
    _register(f'{PKG}.utils.authors', authors)

    skill_version = _load_real('models/pd/skill_version.py', f'{PKG}.models.pd.skill_version')
    skill = _load_real('models/pd/skill.py', f'{PKG}.models.pd.skill')

    yield types.SimpleNamespace(skill=skill, skill_version=skill_version)

    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask'):
        sys.modules.pop(key, None)
    sys.modules.update(saved)


# --- Defect A: flat model (versioned URL / `update_skill_version`) -----------------------

def test_flat_model_accepts_its_own_shape(pd_module):
    model = pd_module.skill_version.SkillVersionUpdateModel.model_validate({
        'instructions': 'new instructions',
        'tags': [{'name': 'aqa'}],
    })
    assert model.instructions == 'new instructions'
    assert model.tags[0].name == 'aqa'


def test_flat_model_rejects_the_nested_body_instead_of_silently_defaulting_to_none(pd_module):
    """The exact failure mode from the ticket: version_id in the path, nested body sent."""
    with pytest.raises(ValidationError) as exc_info:
        pd_module.skill_version.SkillVersionUpdateModel.model_validate({
            'version': {'id': 18, 'instructions': '7896541', 'tags': [{'name': 'aqa', 'id': 22}]},
        })
    assert any(e['type'] == 'extra_forbidden' for e in exc_info.value.errors())


# --- Defect A: nested model (version-less URL / `update_skill`) --------------------------

def test_nested_model_accepts_its_own_shape(pd_module):
    model = pd_module.skill.SkillUpdateModel.model_validate({
        'project_id': 13, 'user_id': 25,
        'version': {'id': 18, 'instructions': '7896541', 'tags': [{'name': 'aqa', 'id': 22}]},
    })
    assert model.version.instructions == '7896541'


def test_nested_model_rejects_a_flat_body_instead_of_silently_defaulting_to_none(pd_module):
    with pytest.raises(ValidationError) as exc_info:
        pd_module.skill.SkillUpdateModel.model_validate({
            'project_id': 13, 'user_id': 25,
            'instructions': '7896541', 'tags': [{'name': 'aqa', 'id': 22}],
        })
    errors = exc_info.value.errors()
    assert any(e['type'] == 'extra_forbidden' for e in errors)


def test_nested_model_still_tolerates_a_stray_version_id_key(pd_module):
    """`put()` pops `version_id` from the payload before validating the nested model, so a
    caller that includes it alongside a correct nested body (as the ticket's own payload did)
    must not be newly 400'd by `extra="forbid"`."""
    payload = {
        'project_id': 13, 'user_id': 25,
        'version': {'id': 18, 'instructions': '7896541'},
    }
    # Simulate what put() does before validating: pop the stray key, then validate.
    payload.pop('version_id', None)
    model = pd_module.skill.SkillUpdateModel.model_validate(payload)
    assert model.version.id == 18


# --- Round-tripped version_details bodies (the compare-versions save) --------------------

# What GET /skill/prompt_lib/<pid>/<sid>/<vid> returns, which the compare-versions save
# spreads wholesale into its PUT body: `...w.version_details ?? {}` in the deployed bundle.
FETCHED_VERSION_DETAILS = {
    'id': 203,
    'name': 'base',
    'instructions': 'edited instructions',
    'status': 'draft',
    'author_id': 25,
    'author': {'id': 25, 'email': 'a@b.c', 'name': 'A B'},
    'tags': [{'id': 22, 'name': 'aqa'}],
    'created_at': '2026-09-02T12:00:00',
    'meta': {'icon_meta': {}},
}


def test_flat_model_accepts_a_round_tripped_version_details_body(pd_module):
    """`useCompareSkillVersions` spreads the fetched version_details into the versioned PUT,
    so the body carries server-owned keys. Those are dropped, not 400'd."""
    model = pd_module.skill_version.SkillVersionUpdateModel.model_validate(
        dict(FETCHED_VERSION_DETAILS)
    )
    assert model.instructions == 'edited instructions'
    assert model.name == 'base'
    assert [(t.id, t.name) for t in model.tags] == [(22, 'aqa')]
    # Server-owned keys were dropped, not absorbed as extras.
    assert not hasattr(model, 'status')
    assert model.model_extra in (None, {})


def test_flat_model_still_rejects_the_nested_body_after_dropping_server_owned_keys(pd_module):
    """Dropping read-only keys must not weaken the shape-mismatch guard: `version` isn't one
    of them, so a nested body sent to the versioned URL still 400s."""
    with pytest.raises(ValidationError) as exc_info:
        pd_module.skill_version.SkillVersionUpdateModel.model_validate({
            'id': 203, 'status': 'draft',
            'version': {'id': 18, 'instructions': '7896541'},
        })
    assert any(e['type'] == 'extra_forbidden' for e in exc_info.value.errors())


def test_nested_version_id_survives_the_drop(pd_module):
    """`SkillVersionNestedUpdateModel` declares `id` and `update_skill()` uses it to pick the
    version to update, so the drop must not strip it - `useSaveSkill` relies on it."""
    model = pd_module.skill.SkillUpdateModel.model_validate({
        'project_id': 13, 'user_id': 25,
        'version': {'id': 18, 'instructions': '7896541', 'status': 'draft', 'created_at': 'x'},
    })
    assert model.version.id == 18
    assert model.version.instructions == '7896541'
