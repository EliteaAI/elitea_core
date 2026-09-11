"""Issue #6410, round two - `put_elitea_core_skill` was uncallable via MCP.

The #6410 fix (PR #403) gave `SkillVersionUpdateModel` `extra="forbid"` so that a body shaped
for the *other* branch stops validating into a silent all-`None` no-op. But the published tool
schema derives its `required` list from the HTTP request-body model, which inherits
`project_id`/`user_id` from `SkillArgsForwardingModel` as **required** fields, and the MCP
executor routes every argument that is not a declared path/query parameter into the request
**body**. So a version-targeted call had to send the one key the server now rejects:

    omit user_id  -> the SDK refuses to start the tool ("required user_id argument was missing")
    send user_id  -> 400 extra_forbidden: user_id

The fix has two halves, and this file exercises both against the **real** pydantic models:

  * `mcp_request_body=SkillMcpUpdateModel` keeps transport fields out of the tool's `required`;
  * `normalize_version_update_body` drops the transport keys a caller sends anyway (after
    cross-checking them against the URL) and unwraps a `{"version": {...}}` envelope onto the
    flat shape the version branch validates.

Why a separate file: `tests/unit/test_6412_skill_mcp_version_param.py` stubs the request models
with a permissive stand-in, so a handler test there cannot observe an `extra_forbidden` at all -
it keeps passing with the fix fully reverted. Here the models are real, so the deadlock is
reproducible and every assertion records **which writer ran and with what**, because a status
code alone distinguishes neither branch (both return 200) nor a real write from an all-`None`
no-op - the original #6410 failure mode.

Run via:
    python tests/run_tests.py unit/test_6410b_mcp_body_deadlock.py -v
"""
import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'skillpkg_6410b'

# Which skill_utils writer ran, and with what.
CALLS = {'update_skill': [], 'update_skill_version': [],
         'delete_skill': [], 'delete_skill_version': []}

LOGGED = {'warning': [], 'info': []}


class _Args(dict):
    pass


class _Request:
    args = _Args()
    json = {}
    method = 'PUT'
    path = '/api/v2/elitea_core/skill/prompt_lib/1/2'
    environ = {}

    @classmethod
    def get_json(cls, silent=False):
        return cls.json


def _with_modes(url_params):
    params = []
    for i in url_params:
        if not i.startswith('<string:mode>'):
            params.append('<string:mode>' if i == '' else f'<string:mode>/{i}')
        params.append(i)
    return params


def _register(name, module):
    sys.modules[name] = module
    return module


def _load_real(rel_path, name):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _install_package():
    """Stub only the runtime edges; load the real models and the real `api/v2/skill.py`."""
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        warning=lambda *a, **k: LOGGED['warning'].append(a),
        info=lambda *a, **k: LOGGED['info'].append(a),
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    for name in ('pylon', 'pylon.core'):
        mod = sys.modules.get(name) or types.ModuleType(name)
        mod.__path__ = []
        _register(name, mod)
    _register('pylon.core.tools', pylon_tools)

    def _register_openapi(**meta):
        def decorator(func):
            func._openapi = meta
            return func
        return decorator

    def _endpoint_metrics(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    class _ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        with_modes = staticmethod(_with_modes)
        endpoint_metrics = staticmethod(_endpoint_metrics)

    # The union of what the two loaders need: `api/v2/skill.py` wants api_tools/config/auth/
    # register_openapi, the real `models/pd/skill.py` wants rpc_tools.
    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
    tools.rpc_tools = types.SimpleNamespace()
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration',
                                         DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)),
        current_user=lambda: {'id': 1},
    )
    tools.register_openapi = _register_openapi
    _register('tools', tools)

    flask = types.ModuleType('flask')
    flask.request = _Request
    _register('flask', flask)

    for name in (PKG, f'{PKG}.api', f'{PKG}.models', f'{PKG}.models.pd',
                 f'{PKG}.models.enums', f'{PKG}.utils'):
        mod = types.ModuleType(name)
        mod.__path__ = []
        _register(name, mod)

    v2_pkg = types.ModuleType(f'{PKG}.api.v2')
    v2_pkg.__path__ = [str(PLUGIN_ROOT / 'api' / 'v2')]
    _register(f'{PKG}.api.v2', v2_pkg)

    # Real, dependency-free modules - no reason to fake these.
    _load_real('models/pd/collection_base.py', f'{PKG}.models.pd.collection_base')
    _load_real('models/pd/tag.py', f'{PKG}.models.pd.tag')
    _load_real('models/enums/all.py', f'{PKG}.models.enums.all')
    _load_real('utils/constants.py', f'{PKG}.utils.constants')

    authors = types.ModuleType(f'{PKG}.utils.authors')
    authors.get_authors_data = lambda author_ids: []
    _register(f'{PKG}.utils.authors', authors)

    # The models under test: real pydantic, so extra="forbid" actually fires.
    _load_real('models/pd/skill_version.py', f'{PKG}.models.pd.skill_version')
    _load_real('models/pd/skill.py', f'{PKG}.models.pd.skill')

    def _record(name):
        def call(*args, **kwargs):
            CALLS[name].append(kwargs)
            return {'id': 1}
        return call

    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    for name in ('create_skill_version', 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: {'id': 1})
    skill_utils.update_skill = _record('update_skill')
    skill_utils.update_skill_version = _record('update_skill_version')
    # Recorded, not a silent no-op: a test has to be able to assert that a malformed selector
    # did NOT reach the irreversible skill-wide delete.
    skill_utils.delete_skill = _record('delete_skill')
    skill_utils.delete_skill_version = _record('delete_skill_version')
    # Returns a SkillVersion row whose `.id` the handler reads - deliberately a minimal class
    # rather than SimpleNamespace, which would answer to any attribute typo.
    skill_utils.get_skill_version_by_id = (
        lambda *a, **k: type('SkillVersion', (), {'id': k.get('version_id')})())
    skill_utils.get_skill_details = lambda *a, **k: {'data': {'id': 1}}
    skill_utils.SkillError = type('SkillError', (Exception,), {'http_status': 400})
    _register(f'{PKG}.utils.skill_utils', skill_utils)

    folder_access = types.ModuleType(f'{PKG}.utils.folder_access')
    folder_access.require_folder_access = lambda *a, **k: (lambda f: f)
    _register(f'{PKG}.utils.folder_access', folder_access)

    mcp_versioning = types.ModuleType(f'{PKG}.utils.mcp_versioning')
    mcp_versioning.INTERNAL_MCP_ENVIRON_KEY = 'elitea.internal_mcp_request'
    _register(f'{PKG}.utils.mcp_versioning', mcp_versioning)

    return _load_real('api/v2/skill.py', f'{PKG}.api.v2.skill')


@pytest.fixture()
def skill_api():
    saved = {k: v for k, v in sys.modules.items()
             if k in ('tools', 'flask') or k.startswith(PKG)}
    module = _install_package()
    _Request.args = _Args()
    _Request.json = {}
    _Request.environ = {}
    LOGGED['warning'].clear()
    LOGGED['info'].clear()
    for calls in CALLS.values():
        calls.clear()
    yield module
    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask'):
        sys.modules.pop(key, None)
    sys.modules.update(saved)


def _put(skill_api, body, *, query=None, path_version_id=None):
    _Request.json = body
    _Request.args = _Args(query or {})
    kwargs = {'project_id': 1, 'skill_id': 2}
    if path_version_id is not None:
        kwargs['version_id'] = path_version_id
    return skill_api.PromptLibAPI.put(None, **kwargs)


def _version_writes():
    return [(c['version_id'], c['update_data']) for c in CALLS['update_skill_version']]


# --- the deadlock itself ----------------------------------------------------------------

def test_version_targeted_put_accepts_an_mcp_body_carrying_user_id(skill_api):
    """Probe P1 / reporter screenshot 4. The tool schema forced `user_id` into the body and the
    server rejected it; now it is dropped. Asserting the status is not enough - the metadata
    branch also returns 200, and an all-`None` model would too."""
    body, status = _put(skill_api, {'user_id': 1, 'instructions': 'x'}, query={'version_id': '8'})

    assert status == 200, body
    (version_id, update_data), = _version_writes()
    assert version_id == 8
    assert update_data.instructions == 'x'
    assert CALLS['update_skill'] == []


def test_version_targeted_put_accepts_the_nested_envelope(skill_api):
    """Probe P3 / reporter screenshot 2. Once `user_id` leaves the schema the body properties are
    exactly {name, description, version, meta} under `additionalProperties: false`, so
    `{"version": {...}}` is the only body a schema-conforming client can build for a content
    edit. Returning the envelope unchanged would 400 with `extra_forbidden: version`."""
    body, status = _put(
        skill_api,
        {'user_id': 1, 'version': {'id': 8, 'instructions': 'x'}},
        query={'version_id': '8'},
    )

    assert status == 200, body
    (version_id, update_data), = _version_writes()
    assert version_id == 8
    assert update_data.instructions == 'x'


def test_agreeing_transport_keys_are_dropped(skill_api):
    """A direct HTTP client echoing its own URL parameters back into the body. `'8'` vs `8` is
    the same version, so string/int coercion must not read as a mismatch."""
    body, status = _put(
        skill_api,
        {'project_id': 1, 'skill_id': 2, 'version_id': 8, 'user_id': 1, 'instructions': 'x'},
        query={'version_id': '8'},
    )

    assert status == 200, body
    (_, update_data), = _version_writes()
    assert update_data.instructions == 'x'


def test_materialized_nulls_do_not_break_the_canonical_shape(skill_api):
    """The published properties carry `"default": null`, and the SDK materializes schema defaults
    the model never authored (see `utils/mcp_versioning.sanitize_mcp_settings_update`). A
    null-valued sibling is therefore not authored content and must not make the canonical
    envelope a 400."""
    body, status = _put(
        skill_api,
        {'version': {'instructions': 'x'}, 'name': None, 'description': None, 'meta': None},
        query={'version_id': '8'},
    )

    assert status == 200, body
    (_, update_data), = _version_writes()
    assert update_data.instructions == 'x'


# --- the version-vs-skill ambiguity, documented and pinned ------------------------------

def test_a_top_level_name_with_a_version_selector_renames_the_version_not_the_skill(skill_api):
    """`SkillVersionUpdateModel` declares `name` (models/pd/skill_version.py) and
    `_update_version_fields` (utils/skill_utils.py) writes it onto the version, so a top-level
    `name` on a version-addressed URL edits the VERSION. That is unchanged behaviour - it is what
    the shipped `/{version_id}` path form has always done, and EliteaUI's compare-versions save
    relies on it - but the fix newly makes the call reachable, so it is stated in the
    mcp_description, in the `version_id` parameter description (which
    `build_mcp_input_schema` copies verbatim into the tool schema) and on the `name` property
    itself. Pinned here so a later 'tidy-up' cannot quietly break the compare-dialog save."""
    body, status = _put(skill_api, {'name': 'renamed'}, query={'version_id': '8'})

    assert status == 200, body
    (version_id, update_data), = _version_writes()
    assert version_id == 8
    assert update_data.name == 'renamed'
    assert CALLS['update_skill'] == [], 'the SKILL must not have been renamed'


def test_a_top_level_description_with_a_version_selector_is_a_guided_400(skill_api):
    """`description` belongs to the skill and to no version, so it is the one skill-only key
    that is genuinely invalid here. It gets a message naming the way out instead of the opaque
    `extra_forbidden` list that made #6410 hard to read in the first place."""
    body, status = _put(skill_api, {'description': 'd'}, query={'version_id': '8'})

    assert status == 400
    assert 'description' in body['error']
    assert 'version selector' in body['error']
    assert CALLS['update_skill_version'] == []


# --- the shape guards the #6410 fix depends on ------------------------------------------

def test_a_nested_envelope_targeting_another_version_is_rejected(skill_api):
    """`id` is not a field of `SkillVersionUpdateModel` and *is* in
    `SERVER_OWNED_VERSION_FIELDS`, so an unwrapped `version.id` would be silently stripped and
    the URL's version written instead - a fresh #6410-class silent misapplication."""
    body, status = _put(
        skill_api, {'version': {'id': 999, 'instructions': 'x'}}, query={'version_id': '8'})

    assert status == 400
    assert '999' in body['error'] and '8' in body['error']
    assert CALLS['update_skill_version'] == []


def test_a_disagreeing_body_version_id_is_rejected(skill_api):
    body, status = _put(
        skill_api, {'version_id': 999, 'instructions': 'x'}, query={'version_id': '8'})

    assert status == 400
    assert '999' in body['error']
    assert CALLS['update_skill_version'] == []


def test_a_disagreeing_body_project_id_is_rejected(skill_api):
    body, status = _put(
        skill_api, {'project_id': 77, 'instructions': 'x'}, query={'version_id': '8'})

    assert status == 400
    assert 'project_id' in body['error']
    assert CALLS['update_skill_version'] == []


def test_skill_level_fields_cannot_ride_along_with_a_version_envelope(skill_api):
    """Top-level `name` means the SKILL in the enveloped shape and the VERSION in the flat one.
    Merging the two would silently drop one of two conflicting intents."""
    body, status = _put(
        skill_api,
        {'name': 'renamed', 'version': {'instructions': 'x'}},
        query={'version_id': '8'},
    )

    assert status == 400
    assert 'name' in body['error']
    assert CALLS['update_skill_version'] == []
    assert CALLS['update_skill'] == []


def test_a_non_dict_version_envelope_is_rejected(skill_api):
    body, status = _put(skill_api, {'version': 'base'}, query={'version_id': '8'})

    assert status == 400
    assert 'version' in body['error']
    assert CALLS['update_skill_version'] == []


def test_a_null_version_key_is_treated_as_absent(skill_api):
    body, status = _put(
        skill_api, {'version': None, 'instructions': 'x'}, query={'version_id': '8'})

    assert status == 200, body
    (_, update_data), = _version_writes()
    assert update_data.instructions == 'x'


def test_a_stray_top_level_id_is_warned_not_rejected(skill_api):
    """Pre-existing behaviour, deliberately left alone: `drop_server_owned_fields` strips a
    top-level `id` and the URL's version is written. Not promoted to a 400 because
    `useCompareSkillVersions` picks its `version_details` by ref lookup with a fallback to the
    other pane, so a stale ref would legitimately carry a mismatched id and a 400 would surface
    as a user-visible 'Failed to save'. Pinned with the warning so the deliberate asymmetry with
    the envelope check above is not later 'fixed'."""
    body, status = _put(skill_api, {'id': 999, 'instructions': 'x'}, query={'version_id': '8'})

    assert status == 200, body
    (version_id, update_data), = _version_writes()
    assert version_id == 8
    assert update_data.instructions == 'x'
    assert any('999' in str(call) for call in LOGGED['warning'])


def test_a_stray_top_level_id_cannot_forge_a_log_line(skill_api):
    """The stray id is caller-supplied JSON, and it is rendered into a WARNING - so a string
    carrying a newline would let a caller write their own log lines."""
    _put(skill_api, {'id': '\n2026-09-01 ERROR forged', 'instructions': 'x'},
         query={'version_id': '8'})

    warning, = [call for call in LOGGED['warning'] if 'forged' in str(call)]
    rendered = warning[0] % warning[1:]
    assert '\n' not in rendered
    assert '\\n2026-09-01 ERROR forged' in rendered


# --- the #6410 core guard, unweakened ---------------------------------------------------

def test_a_flat_body_is_still_rejected_on_the_versionless_branch(skill_api):
    """The original defect: the flat shape sent to the version-less URL used to validate into an
    all-`None` no-op and return 200."""
    body, status = _put(skill_api, {'instructions': 'x'})

    assert status == 400
    assert any(e['type'] == 'extra_forbidden' for e in body)
    assert CALLS['update_skill'] == []


def test_an_unknown_key_is_still_rejected_on_the_version_branch(skill_api):
    """The transport-key drop is a bounded allowlist, not a blanket `extra="ignore"` relapse."""
    body, status = _put(skill_api, {'instrucitons': 'x'}, query={'version_id': '8'})

    assert status == 400
    assert any(e['type'] == 'extra_forbidden' for e in body)
    assert CALLS['update_skill_version'] == []


def test_a_versionless_envelope_still_writes_through_the_metadata_branch(skill_api):
    """Probe P4b - the one MCP path that worked before the fix, and the branch every shipped
    EliteaUI caller uses. `user_id` is simply absent from the body now and injected server-side."""
    body, status = _put(skill_api, {'version': {'id': 204, 'instructions': 'x'}})

    assert status == 200, body
    update_data, = [c['update_data'] for c in CALLS['update_skill']]
    assert update_data.version.id == 204
    assert update_data.version.instructions == 'x'
    assert update_data.user_id == 1, 'the server must still resolve the author'
    assert CALLS['update_skill_version'] == []


# --- shipped callers -------------------------------------------------------------------

# The exact body EliteaUI's Compare-versions dialog sends, captured off the wire - the shape PR
# #403's `extra="forbid"` first broke and `SERVER_OWNED_VERSION_FIELDS` then rescued.
COMPARE_DIALOG_BODY = {
    'id': 209, 'name': 'base', 'instructions': 'text', 'status': 'draft', 'author_id': 3,
    'author': {'id': 3, 'email': 'admin@centry.user', 'name': 'admin@centry.user',
               'avatar': None},
    'tags': [], 'created_at': '2026-09-03T06:14:58.695546', 'meta': {},
}


def test_the_compare_dialog_body_still_saves(skill_api):
    """PR #403's regression #2, re-pinned against the real models rather than a permissive stub.
    Its top-level `id` equals the addressed version by construction, so no warning is logged."""
    body, status = _put(skill_api, dict(COMPARE_DIALOG_BODY), path_version_id=209)

    assert status == 200, body
    (version_id, update_data), = _version_writes()
    assert version_id == 209
    assert update_data.instructions == 'text'
    assert update_data.name == 'base'
    assert LOGGED['warning'] == []


def test_the_normal_version_save_still_writes(skill_api):
    """`useSaveSkill` - skill metadata and version content in one nested body."""
    body, status = _put(skill_api, {
        'name': 'my-skill', 'description': 'd',
        'version': {'id': 18, 'instructions': 'x', 'tags': [{'name': 'aqa'}]},
    })

    assert status == 200, body
    update_data, = [c['update_data'] for c in CALLS['update_skill']]
    assert update_data.name == 'my-skill'
    assert update_data.version.instructions == 'x'


# --- malformed bodies -------------------------------------------------------------------

@pytest.mark.parametrize('raw', [None, [1, 2], 'text'])
@pytest.mark.parametrize('query', [{'version_id': '8'}, {}])
def test_a_non_object_or_absent_body_is_a_clean_400_on_both_branches(skill_api, raw, query):
    """`get_json(silent=True)` deliberately, not `or {}`: turning malformed JSON into an empty
    dict would resurrect the silent all-`None` no-op. This edge is newly reachable - the MCP
    executor sets no CONTENT_TYPE when a call carries only path/query arguments, which before
    the fix could not happen because the schema forced `user_id` into every body."""
    body, status = _put(skill_api, raw, query=query)

    assert status == 400
    assert 'JSON object' in body['error']
    assert CALLS['update_skill'] == []
    assert CALLS['update_skill_version'] == []


# --- code-review round: the new intolerances on the now-reachable path --------------------

def test_a_null_version_selector_in_the_query_means_no_version_selected(skill_api):
    """A model that fills every published property emits `version_id: null`; the executor used to
    urlencode that as the literal string 'None'. Read as a bad value it made every metadata-only
    edit uncallable - the original #6410 complaint, relocated to the other branch."""
    for spelling in ('None', 'null', 'NULL', ''):
        for calls in CALLS.values():
            calls.clear()
        body, status = _put(skill_api, {'name': 'meta-only'}, query={'version_id': spelling})

        assert status == 200, f'{spelling!r} -> {body}'
        assert CALLS['update_skill_version'] == [], f'{spelling!r} must not target a version'
        update_data, = [c['update_data'] for c in CALLS['update_skill']]
        assert update_data.name == 'meta-only'


def test_a_real_version_selector_is_still_honoured(skill_api):
    """Mutation guard for the test above: tolerating 'None' must not swallow a genuine value."""
    body, status = _put(skill_api, {'instructions': 'x'}, query={'version_id': '8'})

    assert status == 200, body
    assert [v for v, _ in _version_writes()] == [8]


def test_a_non_numeric_version_selector_is_still_rejected(skill_api):
    """The other half of the guard: 'base' is a typo, not an absent selector."""
    body, status = _put(skill_api, {'instructions': 'x'}, query={'version_id': 'base'})

    assert status == 400
    assert 'must be an integer' in body['error']


@pytest.mark.parametrize('spelling', [8, '8', 8.0])
def test_the_same_id_spelled_three_ways_is_one_id(skill_api, spelling):
    """JSON carries no integer type hint, so a model may spell one id as int, string or float.
    Only a genuinely different id is a mismatch."""
    body, status = _put(
        skill_api,
        {'version': {'id': spelling, 'instructions': 'x'}},
        query={'version_id': '8'},
    )

    assert status == 200, f'{spelling!r} -> {body}'
    (version_id, update_data), = _version_writes()
    assert (version_id, update_data.instructions) == (8, 'x')


def test_a_genuinely_different_id_is_still_a_mismatch(skill_api):
    """Mutation guard: loosening the comparison must not stop it catching a wrong target."""
    body, status = _put(
        skill_api,
        {'version': {'id': 9, 'instructions': 'x'}},
        query={'version_id': '8'},
    )

    assert status == 400
    assert CALLS['update_skill_version'] == []


@pytest.mark.parametrize('spelling,url_version', [(True, '1'), (False, '0')])
def test_a_boolean_id_never_addresses_the_numerically_equal_version(skill_api, spelling, url_version):
    """`bool` subclasses `int`, so `float(True) == float(1)`. Without the explicit guard in
    `is_same_id`, `{"id": true}` silently addresses version 1 and the write lands on the wrong
    record - and a guard that nothing pins is exactly the line a later tidy-up removes."""
    body, status = _put(
        skill_api,
        {'version': {'id': spelling, 'instructions': 'x'}},
        query={'version_id': url_version},
    )

    assert status == 400, f'{spelling!r} addressed version {url_version}: {body}'
    assert _version_writes() == []


def test_the_metadata_branch_tolerates_echoed_transport_keys(skill_api):
    """The version branch drops these; the metadata branch used to 400 on `skill_id`, so the same
    direct HTTP client was accepted on one branch and rejected on the other."""
    body, status = _put(
        skill_api,
        {'project_id': 1, 'skill_id': 2, 'user_id': 99, 'name': 'echoed'},
    )

    assert status == 200, body
    update_data, = [c['update_data'] for c in CALLS['update_skill']]
    assert update_data.name == 'echoed'


@pytest.mark.parametrize('key,value', [('project_id', 7), ('skill_id', 7)])
def test_the_metadata_branch_still_rejects_a_disagreeing_transport_key(skill_api, key, value):
    """Tolerating an echo must not become a silent write to whatever the body names."""
    body, status = _put(skill_api, {key: value, 'name': 'x'})

    assert status == 400
    assert 'does not match' in body['error']
    assert CALLS['update_skill'] == []


@pytest.mark.parametrize('key', ['project_id', 'skill_id'])
def test_both_branches_reject_a_disagreeing_transport_key_identically(skill_api, key):
    """The two branches read the same key off the same URL, so a caller must not have to learn
    two error texts for one mistake. They diverged once already - the metadata branch grew its
    own copy of the cross-check with its own wording - and only `pop_url_owned_keys` being the
    single implementation keeps them together. Comparing the texts fails if it is re-inlined."""
    metadata_body, metadata_status = _put(skill_api, {key: 999, 'name': 'x'})
    version_body, version_status = _put(
        skill_api, {key: 999, 'instructions': 'x'}, query={'version_id': '8'},
    )

    assert metadata_status == version_status == 400
    assert metadata_body['error'] == version_body['error']
    assert repr(999) in metadata_body['error']
    assert CALLS['update_skill'] == [] and _version_writes() == []


# --- the null-tolerance must not reach the destructive call site -------------------------

def _delete(skill_api, *, query=None, path_version_id=None):
    _Request.json = {}
    _Request.args = _Args(query or {})
    kwargs = {'project_id': 1, 'skill_id': 2}
    if path_version_id is not None:
        kwargs['version_id'] = path_version_id
    return skill_api.PromptLibAPI.delete(None, **kwargs)


@pytest.mark.parametrize('spelling', ['null', 'None', 'NULL'])
def test_a_null_version_selector_never_deletes_the_whole_skill(skill_api, spelling):
    """`resolve_version_id` is shared with DELETE, where an ABSENT selector means "delete the
    skill and every version of it". Reading a malformed selector as absent there would turn a
    400 into an irreversible deletion, so the PUT-side tolerance must be opt-in per call site."""
    result = _delete(skill_api, query={'version_id': spelling})

    assert CALLS['delete_skill'] == [], f'{spelling!r} deleted the entire skill'
    assert CALLS['delete_skill_version'] == []
    body, status = result
    assert status == 400, result
    assert 'must be an integer' in body['error']


def test_an_absent_selector_still_deletes_the_skill(skill_api):
    """Mutation guard: the opt-in must not accidentally disable the real skill-wide delete."""
    _delete(skill_api)

    assert len(CALLS['delete_skill']) == 1


def test_a_real_selector_still_deletes_only_that_version(skill_api):
    _delete(skill_api, query={'version_id': '8'})

    assert CALLS['delete_skill'] == []
    assert [c['version_id'] for c in CALLS['delete_skill_version']] == [8]
