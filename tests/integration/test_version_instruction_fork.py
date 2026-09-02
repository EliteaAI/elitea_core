"""Integration tests for the enhancement fork-apply endpoint (ENH-6b, §6.1.1).

This is the write path: whatever a user accepted in the review dialog lands here and becomes agent
instructions. The properties worth pinning are therefore about what must *not* happen —

* the source version is never modified, including when it is published (the case the in-place patch
  endpoint refuses outright);
* a rejected batch creates no version, so a stale hash cannot leave an orphan draft behind;
* a repeated default name yields a second fork rather than an integrity error.

The real request model and the real patch primitives are loaded; only the session and the clone
helper are faked, since the ORM write itself is not what these cases are about.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_instruction_fork_test'

SOURCE_INSTRUCTIONS = 'Answer support tickets politely. Never cite sources.'


class _Request:
    _payload = {}

    @classmethod
    def get_json(cls, silent=False):
        return cls._payload


class _Version:
    def __init__(self, version_id, name, instructions, status='published', application_id=1):
        self.id = version_id
        self.name = name
        self.instructions = instructions
        self.status = status
        self.application_id = application_id
        self.agent_type = 'openai'


class _Query:
    """Just enough of a SQLAlchemy query to serve the two reads the endpoint makes."""

    def __init__(self, session, entity):
        self._session = session
        self._entity = entity

    def filter(self, *_criteria):
        return self

    def one_or_none(self):
        if self._entity == 'version':
            return self._session.source_version
        return self._session.application

    def all(self):
        return [(version.name,) for version in self._session.versions]


class _Session:
    def __init__(self, source_version, extra_versions=()):
        self.source_version = source_version
        self.application = types.SimpleNamespace(id=1, owner_id=1, name='Support Bot')
        self.versions = [source_version, *extra_versions]
        self.committed = False
        self.rolled_back = False
        self.flushed = 0

    def query(self, entity):
        # The two model classes each read one row; ApplicationVersion.name (a column) reads the
        # existing name list for de-duplication.
        if entity is _ApplicationVersion:
            return _Query(self, 'version')
        if entity is _Application:
            return _Query(self, 'application')
        return _Query(self, 'names')

    def flush(self):
        self.flushed += 1

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Column:
    pass


class _ApplicationVersion:
    id = _Column()
    name = _Column()
    application_id = _Column()


class _Application:
    id = _Column()


def _install_package(session, update_result=None):
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    api_pkg = types.ModuleType(f'{PKG}.api')
    api_pkg.__path__ = []
    v2_pkg = types.ModuleType(f'{PKG}.api.v2')
    v2_pkg.__path__ = [str(PLUGIN_ROOT / 'api' / 'v2')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    pd_pkg = types.ModuleType(f'{PKG}.models.pd')
    pd_pkg.__path__ = [str(PLUGIN_ROOT / 'models' / 'pd')]
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]

    models_all = types.ModuleType(f'{PKG}.models.all')
    models_all.Application = _Application
    models_all.ApplicationVersion = _ApplicationVersion

    predict_llm = types.ModuleType(f'{PKG}.models.pd.predict_llm')

    from pydantic import BaseModel as _BaseModel

    class LLMSettingsRequest(_BaseModel):
        model_config = {'extra': 'allow'}

    predict_llm.LLMSettingsRequest = LLMSettingsRequest

    pd_version = types.ModuleType(f'{PKG}.models.pd.version')

    class ApplicationVersionUpdateModel(_BaseModel):
        model_config = {'extra': 'allow'}

    pd_version.ApplicationVersionUpdateModel = ApplicationVersionUpdateModel

    state = types.SimpleNamespace(
        clone_calls=[],
        update_calls=[],
        update_result=update_result or {'updated': True, 'data': {'id': 99}},
        clone_exc=None,
    )

    application_utils = types.ModuleType(f'{PKG}.utils.application_utils')

    def _update(update_data, _session, commit=False):
        state.update_calls.append(update_data)
        return dict(state.update_result)

    application_utils.applications_update_version = _update

    create_utils = types.ModuleType(f'{PKG}.utils.create_utils')

    def _clone(*, source_version, application, new_version_name, author_id, session):
        if state.clone_exc:
            raise state.clone_exc
        state.clone_calls.append(new_version_name)
        clone = _Version(99, new_version_name, source_version.instructions, status='draft')
        session.versions.append(clone)
        return clone

    create_utils.clone_persisted_application_version = _clone

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'

    flask = types.ModuleType('flask')
    flask.request = _Request

    pylon_core_tools = types.ModuleType('pylon.core.tools')
    pylon_core_tools.log = types.SimpleNamespace(
        debug=lambda *a, **k: None, warning=lambda *a, **k: None,
        exception=lambda *a, **k: None, info=lambda *a, **k: None)
    pylon_core = types.ModuleType('pylon.core')
    pylon_core.tools = pylon_core_tools
    pylon = types.ModuleType('pylon')
    pylon.core = pylon_core

    class _ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        @staticmethod
        def with_modes(params):
            return params

        @staticmethod
        def endpoint_metrics(func):
            return func

    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)),
        current_user=lambda: {'id': 7},
    )
    tools.register_openapi = lambda *a, **k: (lambda f: f)
    tools.db = types.SimpleNamespace(with_project_schema_session=lambda project_id: session)

    for name, mod in {
        PKG: pkg,
        f'{PKG}.api': api_pkg,
        f'{PKG}.api.v2': v2_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.all': models_all,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.pd.predict_llm': predict_llm,
        f'{PKG}.models.pd.version': pd_version,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.application_utils': application_utils,
        f'{PKG}.utils.create_utils': create_utils,
        f'{PKG}.utils.constants': constants,
        'flask': flask,
        'pylon': pylon,
        'pylon.core': pylon_core,
        'pylon.core.tools': pylon_core_tools,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    full = f'{PKG}.api.v2.version_instruction_fork'
    spec = importlib.util.spec_from_file_location(
        full, PLUGIN_ROOT / 'api' / 'v2' / 'version_instruction_fork.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module, state


@pytest.fixture
def fork():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools', 'pylon')}
    sessions = []

    def _make(source_instructions=SOURCE_INSTRUCTIONS, status='published', extra_versions=(),
              update_result=None):
        source = _Version(7, 'v3', source_instructions, status=status)
        session = _Session(source, extra_versions=extra_versions)
        sessions.append(session)
        module, state = _install_package(session, update_result=update_result)
        return module, state, session, source

    yield _make

    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _sha(text):
    from hashlib import sha256
    return sha256(text.encode('utf-8')).hexdigest()


def _post(module, body):
    _Request._payload = body
    handler = module.PromptLibAPI()
    return handler.post(1, 1, 7)


def _body(**overrides):
    payload = {
        'expected_instructions_sha256': _sha(SOURCE_INSTRUCTIONS),
        'patches': [{'old_text': 'Never cite sources.', 'replacement': 'Always cite sources.'}],
    }
    payload.update(overrides)
    return payload


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

def test_fork_applies_patches_without_touching_the_source(fork):
    module, state, session, source = fork()

    payload, status = _post(module, _body())

    assert status == 201
    assert payload['forked_from_version_id'] == 7
    assert source.instructions == SOURCE_INSTRUCTIONS
    assert state.update_calls[0].id == 99
    assert state.update_calls[0].instructions == (
        'Answer support tickets politely. Always cite sources.'
    )
    assert payload['instructions_sha256'] == _sha(state.update_calls[0].instructions)
    assert session.committed is True


def test_a_published_source_can_be_forked(fork):
    """The in-place patch endpoint refuses published and embedded versions; being able to enhance
    one anyway is why this endpoint exists."""
    module, _state, _session, source = fork(status='published')

    _payload, status = _post(module, _body())

    assert status == 201
    assert source.status == 'published'


def test_patches_apply_in_order_and_chain(fork):
    module, state, _session, _source = fork()

    _payload, status = _post(module, _body(patches=[
        {'old_text': 'Never cite sources.', 'replacement': 'Always cite sources.'},
        {'old_text': 'politely', 'replacement': 'politely and precisely'},
    ]))

    assert status == 201
    assert state.update_calls[0].instructions == (
        'Answer support tickets politely and precisely. Always cite sources.'
    )


def test_replace_all_discards_the_original_instructions(fork):
    module, state, _session, _source = fork()

    _payload, status = _post(module, _body(patches=[
        {'replace_all': True, 'replacement': 'You are a concise support assistant.'},
    ]))

    assert status == 201
    assert state.update_calls[0].instructions == 'You are a concise support assistant.'


# ---------------------------------------------------------------------------
# naming
# ---------------------------------------------------------------------------

def test_default_name_is_a_generated_enhanced_name(fork):
    module, state, _session, _source = fork()

    _payload, status = _post(module, _body())

    assert status == 201
    assert state.clone_calls[0].startswith('enhanced-7-')


def test_requested_name_is_used_verbatim(fork):
    module, state, _session, _source = fork()

    _payload, status = _post(module, _body(new_version_name='  cited-sources  '))

    assert status == 201
    assert state.clone_calls[0] == 'cited-sources'


def test_a_taken_name_is_suffixed_rather_than_colliding(fork):
    """Accepting two proposals and keeping the suggested name both times must produce a second
    fork, not an integrity error."""
    module, state, _session, _source = fork(extra_versions=[_Version(8, 'cited-sources', 'x')])

    _payload, status = _post(module, _body(new_version_name='cited-sources'))

    assert status == 201
    assert state.clone_calls[0] == 'cited-sources-2'


def test_a_blank_requested_name_falls_back_to_the_default(fork):
    module, state, _session, _source = fork()

    _payload, status = _post(module, _body(new_version_name='   '))

    assert status == 201
    assert state.clone_calls[0].startswith('enhanced-7-')


# ---------------------------------------------------------------------------
# rejections — none of these may create a version
# ---------------------------------------------------------------------------

def test_stale_hash_returns_409_and_creates_nothing(fork):
    module, state, session, source = fork()

    payload, status = _post(module, _body(expected_instructions_sha256=_sha('older text')))

    assert status == 409
    assert 'changed' in payload['error']
    assert state.clone_calls == [] and session.committed is False
    assert source.instructions == SOURCE_INSTRUCTIONS


def test_ambiguous_anchor_returns_409_naming_the_patch(fork):
    module, state, _session, _source = fork(source_instructions='cite. cite.')

    payload, status = _post(module, {
        'expected_instructions_sha256': _sha('cite. cite.'),
        'patches': [{'old_text': 'cite.', 'replacement': 'always cite.'}],
    })

    assert status == 409
    assert 'Patch 0' in payload['error'] and 'found 2' in payload['error']
    assert state.clone_calls == []


def test_a_later_patch_failing_discards_the_whole_batch(fork):
    """All-or-nothing matters most here: a half-applied batch is instructions no one reviewed."""
    module, state, session, _source = fork()

    payload, status = _post(module, _body(patches=[
        {'old_text': 'Never cite sources.', 'replacement': 'Always cite sources.'},
        {'old_text': 'text that is not there', 'replacement': 'x'},
    ]))

    assert status == 409
    assert 'Patch 1' in payload['error']
    assert state.clone_calls == [] and session.committed is False


def test_unknown_version_returns_404(fork):
    module, state, session, _source = fork()
    session.source_version = None

    payload, status = _post(module, _body())

    assert status == 404
    assert 'not found' in payload['error']
    assert state.clone_calls == []


def test_missing_hash_returns_400(fork):
    module, _state, _session, _source = fork()

    payload, status = _post(module, {'patches': [{'old_text': 'a', 'replacement': 'b'}]})

    assert status == 400
    assert isinstance(payload, list)


def test_empty_patch_list_returns_400(fork):
    module, _state, _session, _source = fork()

    payload, status = _post(module, _body(patches=[]))

    assert status == 400
    assert isinstance(payload, list)


def test_anchorless_patch_returns_400(fork):
    module, _state, _session, _source = fork()

    payload, status = _post(module, _body(patches=[{'replacement': 'Always cite sources.'}]))

    assert status == 400


def test_too_many_patches_returns_400(fork):
    module, _state, _session, _source = fork()

    payload, status = _post(module, _body(patches=[
        {'old_text': f'anchor {i}', 'replacement': f'fix {i}'} for i in range(9)
    ]))

    assert status == 400


def test_failed_version_update_rolls_back(fork):
    module, _state, session, _source = fork(
        update_result={'updated': False, 'msg': {'error': 'nope'}})

    payload, status = _post(module, _body())

    assert status == 400
    assert payload == {'error': 'nope'}
    assert session.rolled_back is True and session.committed is False
