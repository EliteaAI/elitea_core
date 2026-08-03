import datetime
import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _load_module(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


for package_name in (
    'plugins',
    'plugins.elitea_core',
    'plugins.elitea_core.models',
    'plugins.elitea_core.utils',
):
    package = types.ModuleType(package_name)
    package.__path__ = []
    sys.modules.setdefault(package_name, package)


class _MessageTraceStep:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


message_trace_step = types.ModuleType('plugins.elitea_core.models.message_trace_step')
message_trace_step.MessageTraceStep = _MessageTraceStep
sys.modules['plugins.elitea_core.models.message_trace_step'] = message_trace_step

_load_module(
    'plugins.elitea_core.utils.tool_call_dedup',
    'utils/tool_call_dedup.py',
)
_load_module(
    'plugins.elitea_core.utils.trace_step_writer',
    'utils/trace_step_writer.py',
)


# llm_migration_utils pulls in ORM models (ApplicationVersion, ParticipantMapping) that aren't
# needed for parse_project_id_spec; stub the module directly rather than loading the real thing.
llm_migration_utils = types.ModuleType('plugins.elitea_core.utils.llm_migration_utils')


def _parse_project_id_spec(raw, param_name='project_id'):
    raw = raw.strip()
    if raw == 'all':
        return ('all', None)
    if '-' in raw:
        lo, hi = raw.split('-', 1)
        return ('range', (int(lo), int(hi)))
    return ('single', int(raw))


llm_migration_utils.parse_project_id_spec = _parse_project_id_spec
sys.modules['plugins.elitea_core.utils.llm_migration_utils'] = llm_migration_utils

backfill_utils = _load_module(
    'plugins.elitea_core.utils.trace_step_backfill_utils',
    'utils/trace_step_backfill_utils.py',
)

parse_backfill_params = backfill_utils.parse_backfill_params
backfill_project = backfill_utils.backfill_project
DEFAULT_BATCH_SIZE = backfill_utils.DEFAULT_BATCH_SIZE
MAX_BATCH_SIZE = backfill_utils.MAX_BATCH_SIZE


# --- parse_backfill_params: batch_size ---

def test_batch_size_defaults_when_omitted():
    parsed = parse_backfill_params('project_ids=all')
    assert parsed['batch_size'] == DEFAULT_BATCH_SIZE


def test_batch_size_parses_valid_int():
    parsed = parse_backfill_params('project_ids=all;batch_size=250')
    assert parsed['batch_size'] == 250


def test_batch_size_rejects_non_int():
    with pytest.raises(ValueError):
        parse_backfill_params('project_ids=all;batch_size=abc')


def test_batch_size_rejects_below_one():
    with pytest.raises(ValueError):
        parse_backfill_params('project_ids=all;batch_size=0')


def test_batch_size_clamps_above_max():
    parsed = parse_backfill_params(f'project_ids=all;batch_size={MAX_BATCH_SIZE + 1000}')
    assert parsed['batch_size'] == MAX_BATCH_SIZE


# --- backfill_project: batching loop ---

class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._scalar_value


class _CandidateRow:
    """Mimics a text()-query Row: no tool_calls/thinking_steps -> skipped_no_keys, never migrated."""

    def __init__(self, row_id, created_at):
        self.id = row_id
        self.created_at = created_at
        self.tool_calls = None
        self.thinking_steps = None
        self.any_truncated = False


class _FakeSession:
    """Queues one fetchall() result per execute() call matching the candidate query; counts/scalar
    queries return 0 so the loop only exercises the paginated candidate fetch + expunge_all()."""

    def __init__(self, batches):
        self._batches = list(batches)
        self.expunge_all_calls = 0
        self.executed_params = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        if 'LIMIT :batch_size' in sql:
            self.executed_params.append(params)
            rows = self._batches.pop(0) if self._batches else []
            return _FakeResult(rows=rows)
        return _FakeResult(scalar_value=0)

    def expunge_all(self):
        self.expunge_all_calls += 1

    def commit(self):
        pass

    def rollback(self):
        pass


def _ts(i):
    return datetime.datetime(2026, 1, 1) - datetime.timedelta(minutes=i)


def test_backfill_project_stops_when_batch_smaller_than_batch_size():
    batches = [
        [_CandidateRow(1, _ts(1)), _CandidateRow(2, _ts(2))],
    ]
    session = _FakeSession(batches)

    counters = backfill_project(session, 1, 'all', dry_run=True, yield_to_hub=lambda: None, batch_size=5)

    assert counters['batches'] == 1
    assert counters['skipped_no_keys'] == 2
    assert session.expunge_all_calls == 1


def test_backfill_project_pages_through_multiple_full_batches():
    batches = [
        [_CandidateRow(1, _ts(1)), _CandidateRow(2, _ts(2))],
        [_CandidateRow(3, _ts(3)), _CandidateRow(4, _ts(4))],
        [_CandidateRow(5, _ts(5))],
    ]
    session = _FakeSession(batches)

    counters = backfill_project(session, 1, 'all', dry_run=True, yield_to_hub=lambda: None, batch_size=2)

    assert counters['batches'] == 3
    assert counters['skipped_no_keys'] == 5
    assert session.expunge_all_calls == 3


def test_backfill_project_cursor_follows_last_row_of_prior_batch():
    batches = [
        [_CandidateRow(1, _ts(1)), _CandidateRow(2, _ts(2))],
        [_CandidateRow(3, _ts(3))],
    ]
    session = _FakeSession(batches)

    backfill_project(session, 1, 'all', dry_run=True, yield_to_hub=lambda: None, batch_size=2)

    assert 'last_created_at' not in session.executed_params[0]
    assert session.executed_params[1]['last_created_at'] == _ts(2)
    assert session.executed_params[1]['last_id'] == 2


def test_backfill_project_no_candidates_returns_zero_batches():
    session = _FakeSession([])

    counters = backfill_project(session, 1, 'all', dry_run=True, yield_to_hub=lambda: None, batch_size=400)

    assert counters['batches'] == 0
    assert session.expunge_all_calls == 0
