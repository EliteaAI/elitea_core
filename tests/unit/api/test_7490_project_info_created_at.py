"""#7490: the project-info GET returns the project's creation time as `created_at`.

The handler body is compiled on its own, with the RPC layer replaced, so the test covers only the field:
it is requested like the others, comes from project_get_created_at, and a failing lookup yields None
instead of breaking the endpoint.
"""
import ast
import pathlib
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PROJECT_INFO = PLUGIN_ROOT / "api" / "v2" / "project_info.py"
CREATED_AT = "2026-07-13T09:18:11.904953+00:00"


def _function(path, class_name, func_name):
    tree = ast.parse(path.read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    func = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == func_name)
    func.decorator_list = []
    return func


class FakeRpc:
    def __init__(self):
        self.created_at = CREATED_AT
        self.error = None
        self.calls = []

    def timeout(self, _):
        return self

    def project_get_created_at(self, project_id):
        self.calls.append(project_id)
        if self.error:
            raise self.error
        return self.created_at

    def admin_get_users_ids_in_project(self, project_id, filter_system_user=True):
        return [1, 2]

    def configurations_get_first_filtered_project(self, **kwargs):
        return None


@pytest.fixture
def rpc():
    return FakeRpc()


@pytest.fixture
def get(rpc):
    request = types.SimpleNamespace(args={})
    namespace = {
        "request": request,
        "rpc_tools": types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        "ProjectChatConfig": lambda: types.SimpleNamespace(model_dump=lambda: {}),
        "can_manage_project_settings": lambda project_id, user_id: True,
        "auth": types.SimpleNamespace(current_user=lambda: {"id": 1}),
    }
    func = _function(PROJECT_INFO, "PromptLibAPI", "get")
    exec(compile(ast.Module(body=[func], type_ignores=[]), str(PROJECT_INFO), "exec"), namespace)  # pylint: disable=W0122

    def call(project_id, fields=None):
        request.args = {"fields": fields} if fields else {}
        return namespace["get"](None, project_id)

    return call


def test_created_at_is_returned_for_the_project(get, rpc):
    body, status = get(10)

    assert status == 200
    assert body["created_at"] == CREATED_AT
    assert rpc.calls == [10]


def test_it_can_be_requested_alone(get):
    body, _ = get(10, fields="created_at")

    assert body == {"created_at": CREATED_AT}


def test_it_is_not_looked_up_when_other_fields_are_requested(get, rpc):
    body, _ = get(10, fields="teammates_count")

    assert "created_at" not in body
    assert rpc.calls == []


def test_a_project_without_a_creation_time_returns_none(get, rpc):
    rpc.created_at = None

    body, _ = get(10, fields="created_at")

    assert body == {"created_at": None}


def test_a_failing_lookup_returns_none_and_the_rest(get, rpc):
    rpc.error = TimeoutError("rpc timeout")

    body, status = get(10)

    assert status == 200
    assert body["created_at"] is None
    assert body["teammates_count"] == 2
