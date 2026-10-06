import ast
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PUBLIC_TWIN = {
    'id': 64,
    'name': 'Release Helper',
    'description': 'helps',
    'owner_id': 1,
    'shared_owner_id': 2,
    'shared_id': 94,
    'project_id': 1,
    'user_id': 3,
    'created_at': datetime(2026, 10, 6, 9, 0),
    'versions': [{
        'id': 122,
        'name': 'v1',
        'author_id': 3,
        'instructions': 'Help with releases.',
        'llm_settings': {'model_name': 'gpt-4o'},
        'meta': {'step_limit': 25},
        'tools': [],
        'status': 'published',
        'created_at': datetime(2026, 10, 6, 9, 0),
        'project_id': 1,
        'user_id': 3,
    }],
}


def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _package(name):
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


def _load(dotted_name, relative_path):
    spec = importlib.util.spec_from_file_location(dotted_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[dotted_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def fork_model():
    saved = {}

    def install(name, module):
        if name not in saved:
            saved[name] = sys.modules.get(name)
        sys.modules[name] = module

    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(
        info=noop, error=noop, warning=noop, debug=noop, exception=noop,
    )

    for pkg in (
        'plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
        'plugins.elitea_core.models.pd', 'plugins.elitea_core.models.enums',
        'plugins.elitea_core.utils',
    ):
        install(pkg, _package(pkg))

    install('pylon', _package('pylon'))
    install('pylon.core', _package('pylon.core'))
    install('pylon.core.tools', _module('pylon.core.tools', log=log))
    install('tools', _module(
        'tools',
        auth=types.SimpleNamespace(current_user=lambda: {'id': 3}),
        this=types.SimpleNamespace(module=None, descriptor=None),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
        serialize=lambda value: value,
        db=types.SimpleNamespace(get_session=noop),
        SecretString=str,
    ))

    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': type('Application', (), {}),
            'ApplicationVersion': type('ApplicationVersion', (), {}),
        },
        'plugins.elitea_core.utils.authors': {'get_authors_data': lambda *a, **k: []},
        'plugins.elitea_core.utils.toolkits_utils': {'get_mcp_schemas': lambda *a, **k: {}},
        'plugins.elitea_core.utils.pipeline_utils': {'validate_yaml_from_str': noop},
        'plugins.elitea_core.utils.application_utils': {
            'apply_selected_tools_intersection': noop,
            'check_if_usable_attachment_toolkit': lambda *a, **k: True,
        },
        'plugins.elitea_core.utils.application_tools': {
            'expand_toolkit_settings': lambda type_, settings, project_id, user_id: settings,
            'ValidatorNotSupportedError': type('ValidatorNotSupportedError', (Exception,), {}),
            'ConfigurationExpandError': type('ConfigurationExpandError', (Exception,), {}),
            'raise_validation_error_if_any': noop,
            'find_suggested_toolkit_name_field': lambda *a, **k: None,
            'find_suggested_toolkit_max_length': lambda *a, **k: None,
        },
    }
    for name, attrs in stubs.items():
        install(name, _module(name, **attrs))

    _load('plugins.elitea_core.utils.constants', 'utils/constants.py')
    _load('plugins.elitea_core.models.pd.collection_base', 'models/pd/collection_base.py')
    _load('plugins.elitea_core.models.pd.llm', 'models/pd/llm.py')
    _load('plugins.elitea_core.models.pd.tag', 'models/pd/tag.py')
    _load('plugins.elitea_core.models.enums.all', 'models/enums/all.py')
    _load('plugins.elitea_core.models.pd.tool', 'models/pd/tool.py')
    _load('plugins.elitea_core.models.pd.version', 'models/pd/version.py')
    _load('plugins.elitea_core.models.pd.application', 'models/pd/application.py')
    module = _load('plugins.elitea_core.models.pd.export_import', 'models/pd/export_import.py')

    yield module.ApplicationForkModel

    for name, previous in saved.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def test_fork_payload_does_not_carry_the_public_twin_link(fork_model):
    payload = fork_model.model_validate(deepcopy(PUBLIC_TWIN)).model_dump(mode='json')

    assert 'shared_id' not in payload
    assert 'shared_owner_id' not in payload


def test_fork_payload_keeps_the_fields_the_fork_lineage_needs(fork_model):
    payload = fork_model.model_validate(deepcopy(PUBLIC_TWIN)).model_dump(mode='json')

    assert payload['id'] == 64
    assert payload['owner_id'] == 1


class _ShareOriginTaken(Exception):
    pass


def _import_application_function(create_application, logged):
    source = (PLUGIN_ROOT / 'rpc' / 'application.py').read_text()
    tree = ast.parse(source)
    [function] = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'applications_import_application'
    ]
    function.decorator_list = []
    module = ast.Module(body=[function], type_ignores=[])

    @contextmanager
    def get_session(project_id):
        yield types.SimpleNamespace(commit=lambda: None)

    class _ImportModel:
        @staticmethod
        def model_validate(payload):
            return payload

    namespace = {
        'Tuple': tuple,
        'deepcopy': deepcopy,
        'db': types.SimpleNamespace(get_session=get_session),
        'ApplicationImportModel': _ImportModel,
        'ValidationError': type('ValidationError', (Exception,), {}),
        'create_application': create_application,
        'log': types.SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: logged.append(a),
        ),
    }
    exec(compile(ast.fix_missing_locations(module), 'rpc/application.py', 'exec'), namespace)
    return namespace['applications_import_application']


def _payload():
    return {
        'name': 'Release Helper',
        'versions': [{'name': 'base', 'author_id': 3, 'meta': {}}],
    }


RAW_DATABASE_ERROR = (
    'duplicate key value violates unique constraint "application_shared_origin" '
    '[SQL: INSERT INTO p_3.applications (name, owner_id) VALUES (%(name)s, %(owner_id)s)]'
)


def _import_rejected_insert():
    def create_application(*a, **k):
        raise _ShareOriginTaken(RAW_DATABASE_ERROR)

    logged = []
    import_application = _import_application_function(create_application, logged)
    result, errors = import_application(None, _payload(), project_id=3, author_id=3)
    return result, errors, logged


def test_a_rejected_insert_is_reported_instead_of_crashing():
    result, errors, _ = _import_rejected_insert()

    assert result == ''
    assert errors == ['Import function has been failed']


def test_a_rejected_insert_does_not_expose_the_database_error():
    _, errors, _ = _import_rejected_insert()

    assert not any('INSERT INTO' in error or 'application_shared_origin' in error for error in errors)


def test_a_rejected_insert_is_logged_on_the_server():
    _, _, logged = _import_rejected_insert()

    assert len(logged) == 1
