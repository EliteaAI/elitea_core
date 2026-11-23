"""An on-demand toolkit run that writes an index needs the index rights, on the stored toolkit of the project (#7450).

The real ``test_toolkit_tool_sio`` and its guard helpers are compiled from ``rpc/application.py`` (too heavy to import
here), with the real rule (``check_index_rights``) and table (``INDEX_WRITE_TOOLS``); only I/O is stubbed.
"""
import ast
import dataclasses
import sys
import types
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
PACKAGE = "plugins_7450"
PROJECT = 2
VIEWER = frozenset({"models.applications.index_meta.details", "models.applications.predict.post"})
EDITOR = VIEWER | {"models.applications.index_meta.edit", "models.applications.index_meta.delete"}
STORED_SETTINGS = {"pgvector_configuration": {"elitea_title": "stored"}}


class SioValidationError(Exception):
    def __init__(self, sio=None, sid=None, event=None, error=None, stream_id=None, message_id=None, **kwargs):
        super().__init__(error)
        self.error, self.stream_id, self.message_id = error, stream_id, message_id


class AdmissionDenied(Exception):
    def __init__(self, reason_code, message, audit=None):
        super().__init__(message)
        self.reason_code, self.message = reason_code, message


def _nodes(relative_path, names, class_name=None):
    tree = ast.parse((PLUGIN_ROOT / relative_path).read_text())
    body = tree.body
    if class_name:
        body = next(n for n in body if isinstance(n, ast.ClassDef) and n.name == class_name).body
    found = []
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names:
            node.decorator_list = [d for d in node.decorator_list if "dataclass" in ast.unparse(d)]
            found.append(node)
        elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names for t in node.targets):
            found.append(node)
    return found


def _compile(namespace, relative_path, names, class_name=None):
    module = ast.Module(_nodes(relative_path, names, class_name), [])
    exec(compile(module, relative_path, "exec"), namespace)  # pylint: disable=W0122


class Env:
    """The dispatch with the caller's rights, the folder answer and the stored toolkit under the test's control."""

    def __init__(self):
        self.permissions = EDITOR
        self.folder_refusal = None
        self.stored = {5: {"id": 5, "name": "Docs", "type": "confluence", "settings": STORED_SETTINGS}}
        self.permission_reads = []
        self.started_indexes = []
        self.started_tasks = []
        self.expanded = []
        namespace = {"__name__": f"{PACKAGE}.utils.schedule_service", "__package__": f"{PACKAGE}.utils",
                     "Optional": Optional, "dataclass": dataclasses.dataclass}
        _compile(namespace, "utils/schedule_access.py",
                 {"INDEX_EDIT_PERMISSION", "INDEX_DELETE_PERMISSION", "INDEX_WRITE_TOOLS", "ScheduleScope"})
        namespace.update(entity_access_error=self._folder)
        _compile(namespace, "utils/schedule_service.py",
                 {"INDEX_READ_ONLY", "NOT_FOUND_TEXT", "refusal", "ScheduleError", "ScheduleNotFound",
                  "check_index_rights"})
        namespace.update(self._io())
        _compile(namespace, "rpc/application.py", {"_toolkit_run_refused", "_stored_index_toolkit",
                                                   "_index_write_refusal"})
        _compile(namespace, "rpc/application.py", {"test_toolkit_tool_sio"}, class_name="RPC")
        self.namespace = namespace
        self.module = mock.MagicMock()
        self.module.task_node.start_task = self._start_task
        self.module.toolkit_settings_validator = lambda *a, **k: {"ok": True}

    def _folder(self, project_id, entity_type, entity_id, write=False, user_id=None):
        return self.folder_refusal

    def _resolve_permissions(self, mode=None, project_id=None, auth_data=None):
        self.permission_reads.append((project_id, auth_data))
        return self.permissions

    def _start_index(self, task_node, data, sio_event, admission=None):
        self.started_indexes.append(data)
        return "task-1"

    def _start_task(self, *args, **kwargs):
        self.started_tasks.append(kwargs)
        return "task-2"

    def _expand(self, toolkit_type, settings, project_id=None, user_id=None):
        self.expanded.append(settings)
        return settings

    def _session(self, project_id):
        stored = self.stored

        class Query:
            def __init__(self, model):
                self.row = None

            def where(self, clause):
                self.row = stored.get(clause)
                return self

            def first(self):
                return self.row

        class Session:
            def __enter__(self):
                return types.SimpleNamespace(query=Query)

            def __exit__(self, *exc):
                return False

        assert project_id == PROJECT
        return Session()

    def _io(self):
        sio_users = {"sid-1": types.SimpleNamespace(type="user", id=42)}
        auth = types.SimpleNamespace(
            sio_users=sio_users, is_sio_user_in_project=lambda sid, project_id: True,
            current_user=lambda auth_data=None: {"id": 42}, resolve_permissions=self._resolve_permissions)
        stubs = {name: mock.MagicMock() for name in (
            "CREDENTIAL_FREE", "ENTRY_INDEX_ON_DEMAND", "ENTRY_TOOLKIT_TEST", "IndexMetaLockTimeoutError",
            "IndexRunInProgressError", "InternalMcpPatError", "MaintenanceInProgressError", "PoolSaturationError",
            "VaultClient", "_cancel_abandoned_task", "_toolkit_config_log_summary", "get_event_room",
            "get_predict_base_url", "get_public_base_url", "get_session_cookie_name", "guard_test_stream",
            "has_free_form_secret", "has_secret_template", "log", "mark_free_form_secret",
            "redact_internal_mcp_secrets", "require_internal_mcp_pat", "resolve_internal_mcp_tools", "rpc_tools",
            "web", "admission_request_from")}
        stubs.update(
            IndexMetaLockTimeoutError=type("IndexMetaLockTimeoutError", (Exception,), {}),
            IndexRunInProgressError=type("IndexRunInProgressError", (Exception,), {}),
            InternalMcpPatError=type("InternalMcpPatError", (Exception,), {}),
            MaintenanceInProgressError=type("MaintenanceInProgressError", (Exception,), {}),
            PoolSaturationError=type("PoolSaturationError", (Exception,), {}),
            AdmissionDenied=AdmissionDenied, SioValidationError=SioValidationError, auth=auth,
            c=types.SimpleNamespace(DEFAULT_MODE="default"), uuid4=lambda: "uuid",
            strip_platform_fields=lambda data: [], run_identity_applies=lambda sid: False,
            expand_toolkit_settings=self._expand, start_index_task=self._start_index,
            get_predict_token_and_session=lambda *a, **k: ("token", None), is_governed=lambda module: True,
            db=types.SimpleNamespace(get_session=self._session),
            ToolDetails=types.SimpleNamespace(from_orm=lambda row: row), serialize=lambda value: value,
            EliteATool=types.SimpleNamespace(id=_Column()),
        )
        return stubs

    def run(self, tool_name="index_data", sid="sid-1", **toolkit_config):
        config = {"toolkit_id": 5, "type": "confluence", "toolkit_name": "Docs", "settings": {"from": "payload"}}
        config.update(toolkit_config)
        data = {"toolkit_config": config, "tool_name": tool_name, "project_id": PROJECT,
                "tool_params": {"index_name": "idx"}, "stream_id": "stream", "message_id": "msg"}
        return self.namespace["test_toolkit_tool_sio"](self.module, sid, data)

    @property
    def dispatched(self):
        return self.started_indexes + self.started_tasks


class _Column:  # pylint: disable=R0903
    def __eq__(self, other):
        return other


@pytest.fixture
def env(monkeypatch):
    for name in ("plugins_7450", f"{PACKAGE}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    folder_access = types.ModuleType(f"{PACKAGE}.utils.folder_access")
    folder_access.TOOLKIT_ENTITY_TYPES = ["toolkit", "mcp"]
    monkeypatch.setitem(sys.modules, folder_access.__name__, folder_access)
    return Env()


def refusal_of(env, **kwargs):
    with pytest.raises(SioValidationError) as err:
        env.run(**kwargs)
    return err.value.error


@pytest.mark.parametrize("tool_name", ["index_data", "remove_index"])
def test_a_viewer_is_refused_every_index_write_before_any_credential_is_resolved(env, tool_name):
    env.permissions = VIEWER
    assert refusal_of(env, tool_name=tool_name) == {
        "code": "INDEX_READ_ONLY", "error": "You have read-only access to this index"}
    assert env.dispatched == [] and env.expanded == []


def test_building_needs_the_edit_right_and_removing_the_delete_right(env):
    env.permissions = VIEWER | {"models.applications.index_meta.edit"}
    env.run(tool_name="index_data")
    assert len(env.started_indexes) == 1
    assert refusal_of(env, tool_name="remove_index")["code"] == "INDEX_READ_ONLY"


def test_the_rights_are_read_for_the_socket_user_in_the_dispatch_project(env):
    env.run()
    assert env.permission_reads == [(PROJECT, env.namespace["auth"].sio_users["sid-1"])]


def test_an_editor_on_a_read_only_folder_is_refused_and_a_hidden_toolkit_is_not_found(env):
    env.folder_refusal = ({"ok": False, "error": "You have read-only access to this folder"}, 403)
    assert refusal_of(env) == {"code": "INDEX_READ_ONLY", "error": "You have read-only access to this folder"}
    env.folder_refusal = ({"ok": False, "error": "Not found"}, 404)
    assert refusal_of(env) == {"code": "INDEX_NOT_FOUND", "error": "The requested item could not be found"}
    assert env.dispatched == []


def test_the_run_uses_the_stored_toolkit_not_the_settings_the_payload_carries(env):
    env.run(settings={"pgvector_configuration": {"elitea_title": "someone-elses"}})
    assert env.expanded == [STORED_SETTINGS]
    assert env.started_indexes[0]["toolkit_config"]["settings"] == STORED_SETTINGS


@pytest.mark.parametrize("toolkit_config", [{"toolkit_id": None}, {"toolkit_id": 99}, {"project_id": 1}])
def test_an_index_write_on_no_stored_toolkit_of_this_project_is_not_found_without_reading_rights(env, toolkit_config):
    assert refusal_of(env, **toolkit_config) == {"code": "INDEX_NOT_FOUND", "error": "The requested item could not be found"}
    assert env.permission_reads == [] and env.dispatched == []


def test_a_toolkit_of_this_project_named_by_the_participant_runs(env):
    env.run(project_id=PROJECT)
    assert len(env.started_indexes) == 1


@pytest.mark.parametrize("tool_name", ["search_index", "list_indexes"])
def test_a_viewer_still_runs_the_read_tools_of_a_toolkit_of_another_project(env, tool_name):
    env.permissions = VIEWER
    env.run(tool_name=tool_name, project_id=1, settings={"from": "payload"})
    assert env.permission_reads == [] and len(env.started_tasks) == 1
    assert env.expanded == [{"from": "payload"}]


def test_a_rest_caller_without_a_socket_gets_the_refusal_back_with_its_own_rights(env):
    env.permissions = VIEWER
    assert env.run(sid=None) == {"code": "INDEX_READ_ONLY", "error": "You have read-only access to this index"}
    assert env.permission_reads == [(PROJECT, None)] and env.dispatched == []
