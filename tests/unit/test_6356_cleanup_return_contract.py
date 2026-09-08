"""#6356 - `clean_up_schedule_in_toolkit` must always return an unpackable (body, code).

`remove_index()` takes `index_name` by default, so the no-argument "remove every index" call
reaches the platform with an empty index_name. The model validating the event puts no length
constraint on that field, so the handler runs and unpacks the result — and the old guard
`if index_name:` fell off the end of the function returning None, raising TypeError out of the
event_node subscriber. Artifact toolkits were shielded from that only because their toolkit_id
was 0 and failed validation first.

Only the event caller reaches the empty-name branch. `index_data` rejects an empty index_name
(`base_indexer_toolkit.py`), so no stored index_meta row can carry one and the REST caller's
`cmetadata["collection"]` is never falsy. The branch reports a failure so the event handler logs
it: every schedule on the toolkit has just outlived the collections it pointed at.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def opened_sessions():
    return []


def _drop_stubbed_modules(prefix):
    """Drop stand-in modules an earlier test left behind under ``prefix``.

    ``isolated_sys_modules`` stops this module leaking its own stubs, but it does not undo what
    ran before it. ``application_tools`` imports the real sqlalchemy at module scope, and a
    stand-in installed earlier in the session makes that import fail with "unknown location".
    Stand-ins have no ``__file__``; real modules do. Whatever is removed here is put back by
    ``isolated_sys_modules`` on teardown, so later tests see what they expect.
    """
    for name in [n for n in list(sys.modules) if n == prefix or n.startswith(f'{prefix}.')]:
        if getattr(sys.modules[name], '__file__', None) is None:
            del sys.modules[name]


@pytest.fixture
def application_tools(isolated_sys_modules, opened_sessions):
    """Load application_tools behind `isolated_sys_modules`, which restores sys.modules after
    the test. Without it a standalone module load leaks its stubs and later modules in the same
    session fail to import the real sqlalchemy. The shared `pylon_stubs`/`tools_stubs` fixtures
    are not used: both raise ModuleNotFoundError on `tests.stubs` under this runner.
    """
    _drop_stubbed_modules('sqlalchemy')
    _drop_stubbed_modules('pydantic')

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None,
        debug=lambda *a, **k: None, exception=lambda *a, **k: None,
    )
    pylon_tools.web = types.SimpleNamespace(
        method=lambda *a, **k: (lambda f: f), rpc=lambda *a, **k: (lambda f: f),
    )
    sys.modules.setdefault('pylon', types.ModuleType('pylon'))
    sys.modules.setdefault('pylon.core', types.ModuleType('pylon.core'))
    sys.modules['pylon.core.tools'] = pylon_tools

    tools_pkg = types.ModuleType('tools')
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(
        get_session=lambda project_id: opened_sessions.append(project_id)
    )
    tools_pkg.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    tools_pkg.VaultClient = type('VaultClient', (), {'get_secrets': lambda self: {}})
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules['tools'] = tools_pkg

    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
                 'plugins.elitea_core.utils'):
        module = sys.modules.setdefault(name, types.ModuleType(name))
        module.__path__ = []

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
        'plugins.elitea_core.utils.application_tools',
        PLUGIN_ROOT / 'utils' / 'application_tools.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestEmptyIndexName:
    @pytest.mark.parametrize('index_name', ['', None])
    def test_it_returns_an_unpackable_pair(self, application_tools, index_name):
        result, code = application_tools.clean_up_schedule_in_toolkit(1, 44, index_name)

        # Asserted on the guard's own message: the generic `except Exception` tail returns a
        # 400 with ok False too, so a status-only assertion would pass with the guard removed.
        assert code == 400
        assert result['ok'] is False
        assert 'No index_name supplied' in result['error']

    def test_it_short_circuits_before_touching_the_database(self, application_tools, opened_sessions):
        # Asserted on a recorded call, not on an exception raised from the stub: the function
        # body is wrapped in `except Exception`, which would swallow a raising stub and let this
        # test pass with the guard removed.
        application_tools.clean_up_schedule_in_toolkit(1, 44, '')

        assert opened_sessions == []

    def test_the_event_handler_logs_it(self, application_tools):
        # methods/stream.py logs `result['error']` whenever `ok` is falsy, so the message has to
        # carry enough to identify the toolkit whose schedules were left behind.
        result, _ = application_tools.clean_up_schedule_in_toolkit(1, 44, '')

        assert 'schedules left in place' in result['error']
        assert 'toolkit_id=44' in result['error']
