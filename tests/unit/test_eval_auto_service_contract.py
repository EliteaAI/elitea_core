"""The scheduled evaluation's wiring: the platform schedules that drive it, the RPCs they name, and the
seed constants the health check depends on. Read from source, so it runs without the plugin's imports."""
import ast
import pathlib
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _module_method(name):
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    module_class = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Module")
    return next(node for node in module_class.body if isinstance(node, ast.FunctionDef) and node.name == name)


def _rpc_names(path):
    names = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                        and decorator.func.attr == "rpc"):
                    names.update(arg.value for arg in decorator.args if isinstance(arg, ast.Constant))
    return names


def _service_constant(name):
    tree = ast.parse((PLUGIN_ROOT / "utils" / "eval_auto_service.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found")


@pytest.fixture
def platform_schedules():
    created = []
    managed = [{"name": name, "rpc_func": f"{name}_rpc", "cron": f"{index} 1 * * *", "active": index % 2 == 0}
               for index, name in enumerate(("index_scheduling", "pipeline_scheduling", "eval_auto_run"))]
    namespace = {
        "build_scheduler_sync_plan": lambda config: managed,
        "Empty": type("Empty", (Exception,), {}),
        "log": types.SimpleNamespace(warning=lambda *args: None, exception=lambda *args: None),
    }
    exec(compile(ast.Module(body=[_module_method("_bootstrap_platform_schedules")], type_ignores=[]), "module.py",
                 "exec"), namespace)
    plugin = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={}),
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(timeout=lambda seconds: types.SimpleNamespace(
            scheduling_create_if_not_exists=created.append))),
        _apply_scheduler_runtime_config=lambda plan=None: None,
        _release_managed_schedules=lambda: None,
    )
    namespace["_bootstrap_platform_schedules"](plugin)
    return {payload["name"]: payload for payload in created}


def test_the_run_schedule_takes_its_cadence_from_the_admin_settings(platform_schedules):
    assert platform_schedules["eval_auto_run"] == {
        "rpc_func": "eval_auto_run_rpc", "rpc_kwargs": {}, "name": "eval_auto_run",
        "cron": "2 1 * * *", "active": True, "match_handler": True,
    }


def test_the_drain_runs_every_minute_whatever_the_settings(platform_schedules):
    # It is not a managed row: turning the run off must still let an open job finish.
    assert platform_schedules["eval_auto_drain"] == {
        "rpc_func": "elitea_core_eval_auto_drain", "rpc_kwargs": {}, "name": "eval_auto_drain",
        "cron": "* * * * *", "active": True,
    }


def test_every_rpc_the_schedules_name_is_registered():
    registered = _rpc_names(PLUGIN_ROOT / "rpc" / "eval_auto.py")
    bindings = (PLUGIN_ROOT / "utils" / "scheduler_bindings.py").read_text()
    assert "elitea_core_eval_auto_tick" in bindings
    assert {"elitea_core_eval_auto_tick", "elitea_core_eval_auto_drain"} <= registered


def test_the_dashboard_rpcs_are_registered():
    assert {
        "elitea_core_eval_auto_run_now", "elitea_core_eval_auto_get_config", "elitea_core_eval_auto_set_config",
        "elitea_core_eval_auto_list", "elitea_core_eval_auto_history",
    } <= _rpc_names(PLUGIN_ROOT / "rpc" / "eval_auto.py")


def test_the_structure_checks_the_health_check_binds_are_seeded_checks(utils_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("trajectory_checks", utils_path / "evaluation_trajectory_checks.py")
    checks = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checks)
    structure = _service_constant("STRUCTURE_CHECKS")
    assert set(structure) <= set(checks.CHECKS)
    for key in structure:
        assert checks.CHECKS[key]["scope"] == checks.STRUCTURE_SCOPE


def test_probe_and_dimension_keys_are_unique():
    probes = _service_constant("PROBES")
    assert len({key for key, _ in probes}) == len(probes)
    assert all(text.strip() for _, text in probes)
    dimensions = _service_constant("AI_DIMENSIONS")
    assert len({d["key"] for d in dimensions}) == len({d["name"] for d in dimensions}) == len(dimensions)


def test_the_run_acts_as_the_version_author_not_the_application_owner():
    # ``Application.owner_id`` holds the project id, so a run as that "user" always fails access.
    tree = ast.parse((PLUGIN_ROOT / "utils" / "eval_auto_service.py").read_text())
    candidates = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_candidates")
    owner = next(value for node in ast.walk(candidates) if isinstance(node, ast.Dict)
                 for key, value in zip(node.keys, node.values)
                 if isinstance(key, ast.Constant) and key.value == "owner_id")
    assert ast.unparse(owner) == "version.author_id"


def _service_function(name):
    tree = ast.parse((PLUGIN_ROOT / "utils" / "eval_auto_service.py").read_text())
    return ast.unparse(next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name))


def test_a_drain_that_died_after_claiming_does_not_hold_the_job_open():
    # A claimed item without a run is never settled; only the reclaim puts it back in the queue.
    assert "_reclaim_stale(session)" in _service_function("drain")
    reclaim = _service_function("_reclaim_stale")
    assert "eval_run_id.is_(None)" in reclaim and "STALE_CLAIM" in reclaim and "MAX_ATTEMPTS" in reclaim
    # The launching drain holds the row, so the reclaim's skip_locked passes over a slow launch.
    assert "skip_locked=True" in reclaim
    assert "with_for_update()" in _service_function("_process_claimed")


def test_opening_a_job_is_serialised():
    tick = _service_function("tick")
    assert tick.index("session.execute(_JOB_LOCK)") < tick.index("_open_job(session)")


def test_a_refused_start_closes_the_run_it_created():
    launch = _service_function("_launch")
    assert "mark_run_unstarted(item.project_id, run_id" in launch


def test_the_owner_needs_the_run_permission_and_the_folder():
    launch = _service_function("_launch")
    assert "_check_owner_rights(item.project_id, item.owner_id, item.application_id)" in launch


def test_the_generated_suite_is_found_by_its_flag_not_its_name():
    assert "trigger_config['generated']" in _service_function("_generated_suite")


def test_the_dashboard_search_escapes_its_term_and_matches_an_agent_id():
    latest = _service_function("list_latest")
    assert "rules.like_pattern(term)" in latest and "escape=" in latest
    assert "term.isdigit()" in latest and "EvalAutoItem.application_id == int(term)" in latest
    assert "EvalAutoItem.reason_code == reason" in latest


def test_the_history_reports_the_agent_total_not_the_page_size():
    history = _service_function("list_history")
    assert "total = query.count()" in history
    assert history.index("total = query.count()") < history.index(".limit(")


def test_the_platform_dataset_rpcs_are_registered():
    names = _rpc_names(PLUGIN_ROOT / "rpc" / "eval_auto.py")
    assert {"elitea_core_eval_auto_get_platform_dataset", "elitea_core_eval_auto_set_platform_probes",
            "elitea_core_eval_auto_resync_platform_dataset"} <= names


def test_an_edited_probe_list_survives_the_boot_seed():
    assert "_KEY = 'customized'" in (PLUGIN_ROOT / "utils" / "eval_auto_service.py").read_text()
    seed = _service_function("seed_platform_health_check")
    assert "[] if (dataset.meta or {}).get(CUSTOMIZED_KEY) else PROBES" in seed
    assert "CUSTOMIZED_KEY: True" in _service_function("set_platform_probes")


def test_the_resync_rewrites_only_generated_copies_and_skips_unchanged_ones():
    assert "EvalDataset.meta[GENERATED_KEY]" in _service_function("_generated_datasets")
    rewrite = _service_function("_rewrite_generated")
    assert rewrite.index("== inputs") < rewrite.index("session.delete(case)")
    resync = _service_function("resync_platform_dataset")
    assert "_rewrite_generated(session, dataset, probes)" in resync
    # No probes means a missing or inactive registry: refuse rather than empty every copy.
    assert resync.index("if not probes:") < resync.index("_active_project_ids()")
