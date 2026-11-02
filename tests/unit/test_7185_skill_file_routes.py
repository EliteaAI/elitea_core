"""#7185: the skill file endpoints - what they accept, which permission they need, how errors come back.

Loads the real ``api/v2/skill_file.py`` and ``api/v2/skill_files.py`` over a harness that records the
storage calls and the OpenAPI registration, so the handlers run for real.
"""
import functools
import importlib.util
import pathlib
import sys
import types

import pytest
from werkzeug.exceptions import RequestEntityTooLarge

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
READ = 'models.applications.skills.details'
WRITE = 'models.applications.skills.update'
MAX_FILE_BYTES = 1000
ENVELOPE_BYTES = 64 * 1024
MAX_BODY_BYTES = MAX_FILE_BYTES + ENVELOPE_BYTES
ENTRY = {'path': 'forms.md', 'size': 3, 'sha256': 'abc', 'content_type': 'text/markdown', 'is_text': True,
         'created_at': '2026-02-02T00:00:00'}


class SkillError(Exception):
    http_status = 400


class Refusal(SkillError):
    http_status = 404


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class SentFile:
    def __init__(self, stream, **kwargs):
        self.content = stream.read()
        self.kwargs = kwargs
        self.headers = {}


class FakeRequest:
    """Flask's request as far as an upload goes: the body is read when ``files`` or ``get_json`` is first touched,
    and reading more than ``max_content_length`` raises 413 the way Werkzeug's limited stream does."""

    def __init__(self, harness):
        self.harness = harness
        self.max_content_length = None
        self.body_reads = []

    @property
    def content_length(self):
        return self.harness.declared_length

    def _read_body(self):
        self.body_reads.append(self.max_content_length)
        if self.max_content_length is not None and self.harness.body_size > self.max_content_length:
            raise RequestEntityTooLarge()

    @property
    def files(self):
        self._read_body()
        return self.harness.files

    def get_json(self, silent=False):
        self._read_body()
        return self.harness.json


class Harness:
    def __init__(self):
        self.body_size = 0
        self.declared_length = None
        self.user_permissions = set()
        self.folder_writes = []
        self.calls = []
        self.openapi = {}
        self.outcome = ENTRY
        self.raises = None
        self.files = {}
        self.json = None

    def check_api(self, descriptor):
        required = set(descriptor['permissions'])

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not required & self.user_permissions:
                    return {'error': 'access_denied'}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def register_openapi(self, **meta):
        def decorator(func):
            self.openapi[(func.__module__.rsplit('.', 1)[-1], func.__name__)] = meta
            return func
        return decorator

    def require_folder_access(self, entity_types, id_param, write=False, **kwargs):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(handler, *args, **call_kwargs):
                self.folder_writes.append((func.__name__, write))
                return func(handler, *args, **call_kwargs)
            return wrapper
        return decorator

    def storage(self, name):
        def call(**kwargs):
            self.calls.append((name, kwargs))
            if self.raises:
                raise self.raises
            return self.outcome
        return call


@pytest.fixture
def harness(isolated_sys_modules):
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2', f'{PACKAGE}.utils',
                 f'{PACKAGE}.models', f'{PACKAGE}.models.pd'):
        _package(name)

    flask = types.ModuleType('flask')
    harness.request = FakeRequest(harness)
    flask.request = harness.request
    flask.send_file = SentFile
    sys.modules['flask'] = flask

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        APIBase=type('APIBase', (), {}),
        endpoint_metrics=lambda func: func,
        with_modes=lambda params: params,
    )
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=harness.check_api))
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.register_openapi = harness.register_openapi
    sys.modules['tools'] = tools

    _load('models/pd/skill_file.py', f'{PACKAGE}.models.pd.skill_file')
    stubs = {
        'utils.constants': {'PROMPT_LIB_MODE': 'prompt_lib'},
        'utils.folder_access': {'require_folder_access': harness.require_folder_access},
        'utils.skill_utils': {'SkillError': SkillError, 'get_max_file_bytes': lambda: MAX_FILE_BYTES},
        'utils.skill_files': {'describe_size': lambda size: f'{size} B', **{name: harness.storage(name) for name in (
            'list_version_files', 'read_version_file', 'write_version_file', 'rename_version_file',
            'delete_version_file')}},
    }
    for name, attrs in stubs.items():
        module = types.ModuleType(f'{PACKAGE}.{name}')
        module.__dict__.update(attrs)
        sys.modules[module.__name__] = module

    harness.file = _load('api/v2/skill_file.py', f'{PACKAGE}.api.v2.skill_file')
    harness.files_api = _load('api/v2/skill_files.py', f'{PACKAGE}.api.v2.skill_files')
    return harness


BODIES = {'put': {'content': 'x'}, 'patch': {'new_path': 'y'}}


def _call(harness, method, path='forms.md', **kwargs):
    handler = harness.file.PromptLibAPI()
    return getattr(handler, method)(project_id=7, skill_id=3, version_id=10, path=path, **kwargs)


def _is_denied(result):
    return isinstance(result, tuple) and result[1] == 403


class FakeUpload:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


class TestPermissions:
    @pytest.mark.parametrize('method, permission', [('get', READ), ('put', WRITE), ('patch', WRITE), ('delete', WRITE)])
    def test_each_method_needs_its_permission(self, harness, method, permission):
        harness.json = BODIES.get(method)
        harness.outcome = (ENTRY, b'x')
        assert _is_denied(_call(harness, method))

        harness.user_permissions = {permission}
        assert not _is_denied(_call(harness, method))

    def test_the_manifest_needs_read(self, harness):
        handler = harness.files_api.PromptLibAPI()
        assert handler.get(project_id=7, skill_id=3, version_id=10)[1] == 403
        harness.user_permissions = {READ}
        harness.outcome = [ENTRY]
        assert handler.get(project_id=7, skill_id=3, version_id=10) == ({'files': [ENTRY]}, 200)

    def test_writes_ask_for_folder_write_access_and_reads_do_not(self, harness):
        harness.user_permissions = {READ, WRITE}
        harness.outcome = (ENTRY, b'x')
        for method in ('get', 'put', 'patch', 'delete'):
            harness.json = BODIES.get(method)
            _call(harness, method)
        assert harness.folder_writes == [('get', False), ('put', True), ('patch', True), ('delete', True)]


class TestRead:
    def test_get_streams_the_bytes_with_the_type_name_and_etag(self, harness):
        harness.user_permissions = {READ}
        harness.outcome = ({**ENTRY, 'path': 'references/forms.md'}, b'# F')

        sent = _call(harness, 'get', path='references/FORMS.md')

        assert harness.calls == [('read_version_file', {'project_id': 7, 'skill_id': 3, 'version_id': 10,
                                                        'path': 'references/FORMS.md'})]
        assert sent.content == b'# F'
        assert sent.kwargs == {'mimetype': 'text/markdown', 'download_name': 'forms.md', 'as_attachment': False}
        assert sent.headers == {'ETag': 'abc', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': 'sandbox'}

    def test_a_storage_refusal_keeps_its_status(self, harness):
        harness.user_permissions = {READ, WRITE}
        harness.raises = Refusal("File 'x' not found in skill version 10")

        for method in ('get', 'put', 'patch', 'delete'):
            harness.json = BODIES.get(method)
            assert _call(harness, method) == ({'error': "File 'x' not found in skill version 10"}, 404)


class TestWrite:
    def test_a_multipart_part_is_stored_as_is(self, harness):
        harness.user_permissions = {WRITE}
        harness.files['file'] = FakeUpload(b'\x89PNG')

        assert _call(harness, 'put', path='assets/logo.png') == (ENTRY, 200)
        assert harness.calls == [('write_version_file', {'project_id': 7, 'skill_id': 3, 'version_id': 10,
                                                         'path': 'assets/logo.png', 'data': b'\x89PNG'})]

    def test_a_json_body_stores_the_text_as_utf8(self, harness):
        harness.user_permissions = {WRITE}
        harness.json = {'content': 'héllo'}

        assert _call(harness, 'put') == (ENTRY, 200)
        assert harness.calls[0][1]['data'] == 'héllo'.encode('utf-8')

    def test_a_multipart_part_wins_over_a_json_body(self, harness):
        harness.user_permissions = {WRITE}
        harness.files['file'] = FakeUpload(b'part')
        harness.json = {'content': 'json'}

        _call(harness, 'put')

        assert harness.calls[0][1]['data'] == b'part'

    @pytest.mark.parametrize('body', [None, 'text', {}, {'content': 5}, {'content': 'x', 'extra': 1}])
    def test_neither_a_part_nor_valid_json_is_a_400_before_storage(self, harness, body):
        harness.user_permissions = {WRITE}
        harness.json = body

        response, status = _call(harness, 'put')

        assert status == 400 and 'error' in response and harness.calls == []


class TestUploadSize:
    TOO_LARGE = ({'error': f'the upload is too large: a skill file can be at most {MAX_FILE_BYTES} B'}, 413)

    def _send(self, harness, kind, size, declared):
        harness.user_permissions = {WRITE}
        harness.body_size = size
        harness.declared_length = size if declared else None
        if kind == 'multipart':
            harness.files['file'] = FakeUpload(b'x')
        else:
            harness.json = {'content': 'x'}
        return _call(harness, 'put')

    @pytest.mark.parametrize('declared', [True, False])
    @pytest.mark.parametrize('kind', ['multipart', 'json'])
    def test_a_body_up_to_the_cap_plus_its_envelope_is_read(self, harness, kind, declared):
        assert self._send(harness, kind, MAX_BODY_BYTES, declared) == (ENTRY, 200)
        assert harness.request.body_reads[0] == MAX_BODY_BYTES

    @pytest.mark.parametrize('declared', [True, False])
    @pytest.mark.parametrize('kind', ['multipart', 'json'])
    def test_one_byte_more_is_a_413_and_nothing_is_stored(self, harness, kind, declared):
        assert self._send(harness, kind, MAX_BODY_BYTES + 1, declared) == self.TOO_LARGE
        assert harness.calls == []

    def test_a_declared_length_over_the_bound_is_refused_before_the_body_is_touched(self, harness):
        self._send(harness, 'multipart', MAX_BODY_BYTES + 1, declared=True)

        assert harness.request.body_reads == []


class TestRenameAndDelete:
    def test_patch_renames_to_new_path(self, harness):
        harness.user_permissions = {WRITE}
        harness.json = {'new_path': 'references/forms.md'}

        assert _call(harness, 'patch') == (ENTRY, 200)
        assert harness.calls == [('rename_version_file', {'project_id': 7, 'skill_id': 3, 'version_id': 10,
                                                          'path': 'forms.md', 'new_path': 'references/forms.md'})]

    @pytest.mark.parametrize('body', [None, {}, {'new_path': ''}, {'path': 'x'}, {'new_path': 'x', 'extra': 1}])
    def test_a_rename_without_a_valid_new_path_is_a_400(self, harness, body):
        harness.user_permissions = {WRITE}
        harness.json = body

        response, status = _call(harness, 'patch')

        assert status == 400 and harness.calls == []

    def test_delete_removes_the_file(self, harness):
        harness.user_permissions = {WRITE}
        harness.outcome = None

        assert _call(harness, 'delete', path='scripts/a.py') == ({'ok': True}, 200)
        assert harness.calls == [('delete_version_file', {'project_id': 7, 'skill_id': 3, 'version_id': 10,
                                                          'path': 'scripts/a.py'})]


class TestRegistration:
    def test_every_handler_is_registered_under_the_skills_tag_with_its_path(self, harness):
        file_path = '<string:mode>/<int:project_id>/<int:skill_id>/<int:version_id>/<path:path>'
        for method in ('get', 'put', 'patch', 'delete'):
            meta = harness.openapi[('skill_file', method)]
            assert meta['tags'] == ['elitea_core/skills'] and meta['path_suffix_override'] == file_path
        manifest = harness.openapi[('skill_files', 'get')]
        assert manifest['path_suffix_override'] == '<string:mode>/<int:project_id>/<int:skill_id>/<int:version_id>'

    def test_the_agent_facing_tools_are_the_manifest_write_and_delete(self, harness):
        exposed = {key for key, meta in harness.openapi.items() if meta['mcp_tool']}
        assert exposed == {('skill_files', 'get'), ('skill_file', 'put'), ('skill_file', 'delete')}

    def test_the_write_tool_documents_the_json_shape(self, harness):
        model = harness.openapi[('skill_file', 'put')]['request_body']
        assert set(model.model_fields) == {'content'}

    def test_the_url_params_route_the_file_path_as_a_path_converter(self, harness):
        assert harness.file.API.url_params == ['<int:project_id>/<int:skill_id>/<int:version_id>/<path:path>']
        assert harness.files_api.API.url_params == ['<int:project_id>/<int:skill_id>/<int:version_id>']
