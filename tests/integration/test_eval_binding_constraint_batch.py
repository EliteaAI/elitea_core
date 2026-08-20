"""Integration test for `migrate_eval_binding_constraints`'s batch isolation (review #336).

The documented way to run this migration is `project_id=all` (`db_migrations.txt`). The task used
to run one duplicate scan across the whole batch and bail on the first hit, so a single project
carrying duplicate bindings left *every other* project unguarded — and nothing said so: the
operator saw "duplicate bindings exist" and had to retry each project by hand to find out.

`admin_tasks.py` imports the eval schema helpers lazily inside the method, so only the module-level
pylon/toolkit imports need stubbing here.
"""
import importlib.abc
import importlib.util
import pathlib
import sys
import types
from unittest.mock import MagicMock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evaladminpkg_batch_test'

_STUBBED = ('redis', 'sqlalchemy', f'{PKG}.scripts', f'{PKG}.utils')


class _MockFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, fullname, path=None, target=None):  # noqa: ARG002
        if any(fullname == root or fullname.startswith(root + '.') for root in _STUBBED):
            return importlib.util.spec_from_loader(fullname, self)
        return None

    def create_module(self, spec):
        mock = MagicMock()
        mock.__name__ = spec.name
        mock.__spec__ = spec
        mock.__path__ = []
        return mock

    def exec_module(self, module):
        pass


@pytest.fixture
def admin_tasks():
    finder = _MockFinder()
    sys.meta_path.insert(0, finder)

    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    sys.modules[PKG] = pkg
    methods_pkg = types.ModuleType(f'{PKG}.methods')
    methods_pkg.__path__ = []
    sys.modules[f'{PKG}.methods'] = methods_pkg

    try:
        full = f'{PKG}.methods.admin_tasks'
        spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / 'methods/admin_tasks.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
    finally:
        sys.meta_path.remove(finder)

    yield module

    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]


class _Task:
    """Stand-in for the Method mixin's `self`: only `project_list` is reached."""

    def __init__(self, project_ids):
        self.context = types.SimpleNamespace(
            rpc_manager=types.SimpleNamespace(
                call=types.SimpleNamespace(
                    project_list=lambda **kwargs: [{'id': pid} for pid in project_ids],
                ),
            ),
        )


@pytest.fixture
def schema_stub():
    """Fake the lazily-imported `..utils.eval_binding_schema`, recording what it was asked to do."""
    stub = types.ModuleType(f'{PKG}.utils.eval_binding_schema')
    stub.applied = []
    stub.duplicates = {}

    def _find(project_ids):
        return {pid: [{'row': 1}] for pid in project_ids if pid in stub.duplicates}

    def _apply(project_ids):
        stub.applied.extend(project_ids)
        return list(project_ids), []

    stub.find_duplicate_bindings = _find
    stub.apply_eval_binding_constraints = _apply
    sys.modules[f'{PKG}.utils.eval_binding_schema'] = stub
    yield stub
    del sys.modules[f'{PKG}.utils.eval_binding_schema']


def _run(admin_tasks, project_ids, param='project_id=all'):
    return admin_tasks.Method.migrate_eval_binding_constraints(_Task(project_ids), param=param)


def test_clean_projects_are_migrated_even_when_another_has_duplicates(admin_tasks, schema_stub):
    schema_stub.duplicates = {2: True}

    result = _run(admin_tasks, [1, 2, 3])

    assert schema_stub.applied == [1, 3]
    assert result['migrated'] == 2


def test_the_dirty_project_is_reported_not_silently_dropped(admin_tasks, schema_stub):
    schema_stub.duplicates = {2: True}

    result = _run(admin_tasks, [1, 2, 3])

    assert result['skipped'] == 1
    assert 2 in result['duplicates']


def test_a_fully_clean_batch_reports_no_skips(admin_tasks, schema_stub):
    result = _run(admin_tasks, [1, 2, 3])

    assert schema_stub.applied == [1, 2, 3]
    assert 'duplicates' not in result


def test_dry_run_still_only_reports(admin_tasks, schema_stub):
    schema_stub.duplicates = {2: True}

    result = _run(admin_tasks, [1, 2, 3], param='project_id=all;dry_run')

    assert result['dry_run'] is True
    assert schema_stub.applied == []
