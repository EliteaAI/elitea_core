"""#7185: the skill file endpoints - what they accept, which permission they need, how errors come back.

Loads the real ``api/v2/skill_file.py`` and ``api/v2/skill_files.py`` over a harness that records the
storage calls and the OpenAPI registration, so the handlers run for real.
"""
import functools
import json
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
MAX_JSON_BODY_BYTES = 6 * MAX_FILE_BYTES + ENVELOPE_BYTES
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


class FakeFiles(dict):
    def to_dict(self):
        return dict(self)


class FakeRequest:
    """Flask's request as far as an upload goes. The body is read once, when ``files``, ``get_data`` or ``get_json``
    is first touched, and kept. Over ``max_content_length``, a body with a declared length is a 413 the way
    Werkzeug's limited stream raises it; a chunked one is cut at the limit, and its JSON then does not parse."""

    def __init__(self, harness):
        self.harness = harness
        self.max_content_length = None
        self.body_reads = []
        self.read_size = None

    @property
    def content_length(self):
        return self.harness.declared_length

    @property
    def mimetype(self):
        return self.harness.mimetype

    def _read_body(self):
        if self.read_size is not None:
            return self.read_size
        self.body_reads.append(self.max_content_length)
        limit = self.max_content_length
        if limit is not None and self.harness.body_size > limit:
            if self.harness.declared_length is not None or self.harness.mimetype == 'multipart/form-data':
                raise RequestEntityTooLarge()
            self.read_size = limit
        else:
            self.read_size = self.harness.body_size
        return self.read_size

    @property
    def files(self):
        self._read_body()
        return FakeFiles(self.harness.files)

    def get_data(self):
        return b'x' * self._read_body()

    def get_json(self, silent=False):
        return self.harness.json if self._read_body() == self.harness.body_size else None


class Harness:
    def __init__(self):
        self.body_size = 0
        self.max_file_bytes = MAX_FILE_BYTES
        self.declared_length = None
        self.mimetype = 'application/json'
        self.user_permissions = set()
        self.folder_writes = []
        self.calls = []
        self.openapi = {}
        self.outcome = ENTRY
        self.raises = None
        self.files = {}
        self.json = None
        self.metrics_bodies = []

    def endpoint_metrics(self, func):
        """Like the platform's: for a JSON request it reads the body, before the handler, to log it."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if self.request.mimetype == 'application/json':
                self.metrics_bodies.append(self.request.get_json(silent=True))
            return func(*args, **kwargs)
        return wrapper

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
        endpoint_metrics=harness.endpoint_metrics,
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
        'utils.skill_utils': {'SkillError': SkillError, 'get_max_file_bytes': lambda: harness.max_file_bytes},
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
            harness.mimetype = 'multipart/form-data'
            harness.files['file'] = FakeUpload(b'x')
        else:
            harness.mimetype = 'application/json'
            harness.json = {'content': 'x'}
        return _call(harness, 'put')

    LIMITS = {'multipart': MAX_BODY_BYTES, 'json': MAX_JSON_BODY_BYTES}
    READ_LIMITS = {'multipart': MAX_BODY_BYTES, 'json': MAX_JSON_BODY_BYTES + 1}

    @pytest.mark.parametrize('declared', [True, False])
    @pytest.mark.parametrize('kind', ['multipart', 'json'])
    def test_a_body_up_to_its_bound_is_read_once_bounded_and_reaches_the_handler(self, harness, kind, declared):
        assert self._send(harness, kind, self.LIMITS[kind], declared) == (ENTRY, 200)
        assert harness.request.body_reads == [self.READ_LIMITS[kind]]

    @pytest.mark.parametrize('declared', [True, False])
    @pytest.mark.parametrize('kind', ['multipart', 'json'])
    def test_one_byte_more_is_a_413_and_nothing_is_stored(self, harness, kind, declared):
        assert self._send(harness, kind, self.LIMITS[kind] + 1, declared) == self.TOO_LARGE
        assert harness.calls == []

    @pytest.mark.parametrize('declared', [True, False])
    def test_a_json_body_is_bounded_before_the_metrics_wrapper_reads_it(self, harness, declared):
        assert self._send(harness, 'json', 10 * MAX_JSON_BODY_BYTES, declared) == self.TOO_LARGE

        assert harness.request.body_reads in ([], [MAX_JSON_BODY_BYTES + 1])
        assert harness.metrics_bodies == [] and harness.calls == []

    def test_under_the_bound_the_metrics_wrapper_and_the_handler_share_the_one_read(self, harness):
        assert self._send(harness, 'json', MAX_FILE_BYTES, declared=False) == (ENTRY, 200)

        assert harness.metrics_bodies == [{'content': 'x'}] and harness.request.body_reads == [MAX_JSON_BODY_BYTES + 1]

    @pytest.mark.parametrize('declared', [True, False])
    def test_escaped_json_text_under_the_cap_is_accepted_though_its_body_is_over_cap_plus_envelope(self, harness,
                                                                                                  declared):
        harness.max_file_bytes = 1024 * 1024
        text = 'ж' * (harness.max_file_bytes // 4)
        body = json.dumps({'content': text})
        harness.user_permissions = {WRITE}
        harness.mimetype = 'application/json'
        harness.json = json.loads(body)
        harness.body_size = len(body)
        harness.declared_length = len(body) if declared else None

        assert len(text.encode('utf-8')) < harness.max_file_bytes < harness.max_file_bytes + ENVELOPE_BYTES < len(body)
        assert _call(harness, 'put') == (ENTRY, 200)
        assert harness.calls[0][1]['data'] == text.encode('utf-8')

    @pytest.mark.parametrize('kind', ['multipart', 'json'])
    def test_a_declared_length_over_the_bound_is_refused_before_the_body_is_touched(self, harness, kind):
        assert self._send(harness, kind, self.LIMITS[kind] + 1, declared=True) == self.TOO_LARGE

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


class NamedUpload(FakeUpload):
    def __init__(self, filename, data):
        super().__init__(data)
        self.filename = filename


class SentResponse:
    def __init__(self, content, mimetype=None, headers=None):
        self.content, self.mimetype, self.headers = content, mimetype, headers


@pytest.fixture
def package_routes(harness):
    calls = types.SimpleNamespace(imported=[], unzipped=[], zip_error=None, export={})

    def read_skill_zip(data):
        calls.unzipped.append(data)
        if calls.zip_error:
            raise calls.zip_error
        return '---\nname: forms\n---\nbody', ['entry']

    def import_skill_md(**kwargs):
        calls.imported.append(kwargs)
        return {'id': 3}

    def validate_skill_import_filename(filename):
        if not filename.lower().endswith(('.md', '.zip')):
            raise ValueError('Skill import only accepts .md or .zip files.')

    module = types.ModuleType(f'{PACKAGE}.utils.skill_export_import')
    module.__dict__.update(
        read_skill_zip=read_skill_zip, import_skill_md=import_skill_md,
        validate_skill_import_filename=validate_skill_import_filename,
        export_skill_md=lambda **kwargs: calls.export,
    )
    sys.modules[module.__name__] = module
    sys.modules['tools'].auth.current_user = lambda: {'id': 1}
    sys.modules['flask'].request.is_json = False
    sys.modules['flask'].Response = SentResponse
    sys.modules['pylon'] = types.ModuleType('pylon')
    sys.modules['pylon.core'] = types.ModuleType('pylon.core')
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(error=lambda *a, **k: None)
    sys.modules['pylon.core.tools'] = pylon_tools
    skill_api = types.ModuleType(f'{PACKAGE}.api.v2.skill')
    skill_api.SKILL_PATH, skill_api.resolve_version_id = 'path', lambda version_id: (version_id, None)
    sys.modules[skill_api.__name__] = skill_api
    export_utils = types.ModuleType(f'{PACKAGE}.utils.export_import_utils')
    export_utils.content_disposition_attachment = lambda filename: f'attachment; filename={filename}'
    sys.modules[export_utils.__name__] = export_utils
    calls.importer = _load('api/v2/skill_import.py', f'{PACKAGE}.api.v2.skill_import').PromptLibAPI()
    calls.exporter = _load('api/v2/skill_export.py', f'{PACKAGE}.api.v2.skill_export').PromptLibAPI()
    return calls


class TestSkillPackageRoutes:
    def test_a_zip_upload_is_unpacked_and_its_files_go_to_the_import(self, harness, package_routes):
        harness.user_permissions = {'models.applications.skills.create'}
        harness.files['file'] = NamedUpload('forms.ZIP', b'PK\x03\x04zip')

        assert package_routes.importer.post(project_id=7) == ({'id': 3}, 201)
        assert package_routes.unzipped == [b'PK\x03\x04zip']
        assert package_routes.imported == [{'project_id': 7, 'content': '---\nname: forms\n---\nbody', 'author_id': 1,
                                            'files': ['entry']}]

    def test_a_markdown_upload_imports_without_files(self, harness, package_routes):
        harness.user_permissions = {'models.applications.skills.create'}
        harness.files['file'] = NamedUpload('forms.md', b'---\nname: forms\n---\nbody')

        package_routes.importer.post(project_id=7)

        assert package_routes.unzipped == [] and package_routes.imported[0]['files'] == ()

    def test_a_refused_zip_keeps_its_status_and_imports_nothing(self, harness, package_routes):
        harness.user_permissions = {'models.applications.skills.create'}
        harness.files['file'] = NamedUpload('forms.zip', b'PK')
        package_routes.zip_error = SkillError("'../x' cannot be unpacked safely")

        assert package_routes.importer.post(project_id=7) == ({'error': "'../x' cannot be unpacked safely"}, 400)
        assert package_routes.imported == []

    def test_the_export_answers_with_the_type_the_package_has(self, harness, package_routes):
        harness.user_permissions = {'models.applications.skills.export'}
        package_routes.export.update(ok=True, content=b'PK', filename='forms.zip', mimetype='application/zip')

        response = package_routes.exporter.get(project_id=7, skill_id=3)

        assert (response.content, response.mimetype) == (b'PK', 'application/zip')
        assert response.headers['Content-Disposition'] == 'attachment; filename=forms.zip'
