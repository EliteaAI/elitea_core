"""#6356 - `clean_up_schedule_in_toolkit` must always return an unpackable (body, code).

`remove_index()` takes `index_name` by default, so the no-argument "remove every index"
call reaches the platform with an empty index_name. The model that validates the event
puts no length constraint on it, so the handler runs and unpacks the result — and the
old guard `if index_name:` fell off the end of the function returning None, raising
TypeError out of the event_node subscriber.

Artifact toolkits were shielded from this only because their `toolkit_id` was 0 and
failed validation first; fixing that injection makes this path reachable for them too.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _drop_stubbed_modules(prefix):
    """Earlier tests in a full-suite run replace real packages with stand-ins that have no
    ``__file__``. ``application_tools`` imports the real sqlalchemy at module scope, so those
    stand-ins have to go before it is loaded or the import fails with "unknown location"."""
    for name in [n for n in list(sys.modules) if n == prefix or n.startswith(f'{prefix}.')]:
        if getattr(sys.modules[name], '__file__', None) is None:
            del sys.modules[name]


@pytest.fixture(scope='module')
def application_tools():
    _drop_stubbed_modules('sqlalchemy')
    _drop_stubbed_modules('pydantic')
    for name in (
        'plugins',
        'plugins.elitea_core',
        'plugins.elitea_core.models',
        'plugins.elitea_core.utils',
    ):
        module = sys.modules.setdefault(name, types.ModuleType(name))
        module.__path__ = []

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    pylon_tools.web = types.SimpleNamespace(
        method=lambda *a, **k: (lambda f: f), rpc=lambda *a, **k: (lambda f: f),
    )
    sys.modules.setdefault('pylon', types.ModuleType('pylon'))
    sys.modules.setdefault('pylon.core', types.ModuleType('pylon.core'))
    sys.modules['pylon.core.tools'] = pylon_tools

    tools_pkg = types.ModuleType('tools')
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=_unreachable_session)
    tools_pkg.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    tools_pkg.VaultClient = type('VaultClient', (), {'get_secrets': lambda self: {}})
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules['tools'] = tools_pkg

    models_all = types.ModuleType('plugins.elitea_core.models.all')
    for name in ('EliteATool', 'EntityToolMapping', 'ApplicationVersion'):
        setattr(models_all, name, type(name, (), {}))
    sys.modules['plugins.elitea_core.models.all'] = models_all

    models_indexer = types.ModuleType('plugins.elitea_core.models.indexer')
    models_indexer.EmbeddingStore = type('EmbeddingStore', (), {})
    models_indexer.IndexRun = type('IndexRun', (), {})
    models_indexer.INDEX_RUN_CANCELLED = 'cancelled'
    models_indexer.INDEX_RUN_PENDING = 'pending'
    models_indexer.INDEX_RUN_LIVE_INDEX_NAME = 'live'
    models_indexer.INDEX_RUN_LIVE_INDEX_PREDICATE = None
    models_indexer.INDEX_RUN_STATUSES = ()
    sys.modules['plugins.elitea_core.models.indexer'] = models_indexer

    enums = types.ModuleType('plugins.elitea_core.models.enums.all')
    enums.ToolEntityTypes = type('ToolEntityTypes', (), {})
    enums.AgentTypes = type('AgentTypes', (), {})
    enums.InitiatorType = type('InitiatorType', (), {'user': 'user'})
    enums.IndexDataStatus = type('IndexDataStatus', (), {
        'in_progress': types.SimpleNamespace(value='in_progress'),
        'cancelled': types.SimpleNamespace(value='cancelled'),
        'failed': types.SimpleNamespace(value='failed'),
    })
    sys.modules['plugins.elitea_core.models.enums.all'] = enums

    exceptions = types.ModuleType('plugins.elitea_core.utils.exceptions')
    exceptions.PoolSaturationError = type('PoolSaturationError', (Exception,), {})
    exceptions.MaintenanceInProgressError = type('MaintenanceInProgressError', (Exception,), {})
    sys.modules['plugins.elitea_core.utils.exceptions'] = exceptions

    utils_utils = types.ModuleType('plugins.elitea_core.utils.utils')
    utils_utils.parse_ids_filter = lambda *a, **k: None
    sys.modules['plugins.elitea_core.utils.utils'] = utils_utils

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.utils.application_tools', PLUGIN_ROOT / 'utils' / 'application_tools.py'
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _unreachable_session(project_id):
    raise AssertionError(f"clean-up opened a session for project {project_id} with no index_name")


class TestEmptyIndexName:
    @pytest.mark.parametrize('index_name', ['', None])
    def test_it_returns_an_unpackable_pair(self, application_tools, index_name):
        result, code = application_tools.clean_up_schedule_in_toolkit(1, 44, index_name)

        assert code == 400
        assert result['ok'] is False

    def test_it_never_opens_a_session(self, application_tools):
        # The stubbed session factory raises, so reaching the DB fails the test outright.
        application_tools.clean_up_schedule_in_toolkit(1, 44, '')
