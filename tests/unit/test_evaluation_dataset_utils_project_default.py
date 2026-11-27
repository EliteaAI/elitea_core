"""The project's default eval dataset: one shared dataset flagged in its meta, set by project admins only,
and never carried in by a create or a plain meta update. Read from source, so it runs without the plugin's
imports."""
import ast
import pathlib
from typing import Optional

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
UTILS = PLUGIN_ROOT / "utils" / "evaluation_dataset_utils.py"
API = PLUGIN_ROOT / "api" / "v2" / "eval_project_default_dataset.py"


def _function(path, name):
    tree = ast.parse(path.read_text())
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)


def _source(path, name):
    return ast.unparse(_function(path, name))


def _decorators(name):
    return [ast.unparse(d) for d in _function(API, name).decorator_list]


def _helpers(*names):
    namespace = {"SYSTEM_META_KEYS": ("project_default", "auto_eval_generated"), "Optional": Optional}
    body = [_function(UTILS, name) for name in names]
    exec(compile(ast.Module(body=body, type_ignores=[]), "utils", "exec"), namespace)
    return [namespace[name] for name in names]


def test_the_system_flags_are_stripped_from_caller_meta():
    (strip,) = _helpers("_without_system_keys")
    assert strip({"project_default": True, "auto_eval_generated": True, "tags": ["a"]}) == {"tags": ["a"]}
    assert strip(None) == {}


def test_a_meta_edit_keeps_the_stored_flags_and_cannot_add_new_ones():
    strip, keep = _helpers("_without_system_keys", "_system_keys")
    stored = {"auto_eval_generated": True, "tags": ["old"]}
    sent = {"project_default": True, "tags": ["new"]}
    assert {**strip(sent), **keep(stored)} == {"tags": ["new"], "auto_eval_generated": True}


def test_a_create_cannot_set_the_flags():
    assert "_without_system_keys(data.meta)" in _source(UTILS, "create_dataset")


def test_a_meta_update_keeps_the_flags_and_an_unshare_drops_it():
    update = _source(UTILS, "update_dataset")
    assert "{**_without_system_keys(value), **system_meta}" in update
    assert "_drop_project_default(dataset)" in update


def test_only_a_shared_dataset_can_be_the_project_default():
    setter = _source(UTILS, "set_project_default_dataset")
    assert "EvalDatasetNotSharedError" in setter
    # Every other flagged dataset is unflagged, so there is one default at most.
    assert "_drop_project_default(current)" in setter


def test_the_scheduled_run_reads_the_project_default():
    service = (PLUGIN_ROOT / "utils" / "eval_auto_service.py").read_text()
    assert "get_project_default_dataset(project_id, session=session)" in service


def test_only_project_admins_can_set_it():
    put = _decorators("put")
    assert any("models.project_context.edit" in d for d in put)
    assert "require_project_settings_admin" in put
    assert any("models.applications.evaluation.dataset.read" in d for d in _decorators("get"))
