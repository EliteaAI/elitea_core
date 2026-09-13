"""Issue #6556 — the Configuration → Runtime section owns these two schedules.

The elitea_core config is re-pushed onto the `schedule` rows at every boot and
reconfig, so the Admin Portal's Schedules tab must show them as read-only. The
binding declared here is what tells the portal where each row is edited, and
it is only useful while it keeps matching the admin schema.
"""

import ast
import collections
import json
import types
import sys
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module  # noqa: E402


_LogLine = collections.namedtuple("_LogLine", "level template args rendered")


def _recording_log(records):
    """Every branch this change logs sits on a path something swallows --
    ready()'s bare except, a fallback that quietly substitutes a default, or
    reconfig()'s caller -- so the line is the only evidence an operator gets.

    Template and args are kept apart on purpose. In the rendered text a literal
    and an interpolated argument are indistinguishable, so an assertion on it
    cannot tell ``"...for %s...", name`` from the same words with the name
    baked in -- which is exactly the edit that sends an operator to the wrong
    field. Wording is asserted against the template, identity against the args.
    """
    def _at(level):
        def _log(message, *args, **_kwargs):
            records.append(_LogLine(
                level, message, args, message % args if args else message,
            ))
        return _log
    return types.SimpleNamespace(
        info=_at("info"), warning=_at("warning"), error=_at("error"),
        exception=_at("exception"), debug=_at("debug"), critical=_at("critical"),
    )


@pytest.fixture(scope="module")
def bindings():
    """Self-sufficient: run_tests.py installs pylon stubs globally, but this
    file should not need the whole suite to have gone first."""
    import types
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools = types.ModuleType("pylon.core.tools")
    records = []
    tools.log = _recording_log(records)
    module = load_utils_module(
        PLUGIN_ROOT / "utils", "scheduler_bindings",
        extra_stubs={
            "pylon": pylon, "pylon.core": core, "pylon.core.tools": tools,
        },
    )
    module.LOG_RECORDS = records
    return module


@pytest.fixture
def logged(bindings):
    """The stub is module-scoped; give each test an empty ledger."""
    bindings.LOG_RECORDS.clear()
    return bindings.LOG_RECORDS


@pytest.fixture(scope="module")
def admin_schema():
    return json.loads((PLUGIN_ROOT / "admin_schema.json").read_text())["properties"]


def test_both_scheduler_rows_are_declared(bindings):
    assert set(bindings.SCHEDULER_CONFIG_BINDINGS) == {
        "index_scheduling",
        "pipeline_scheduling",
    }


def test_every_declared_field_exists_in_the_admin_schema(bindings, admin_schema):
    """A renamed prop would silently orphan the binding."""
    for name, binding in bindings.SCHEDULER_CONFIG_BINDINGS.items():
        for field in binding["fields"]:
            assert field in admin_schema, field
            assert admin_schema[field]["section"] == binding["section"]
            assert admin_schema[field]["path"].startswith(f"scheduler.{name}.")


def test_cron_props_declare_the_cron_format(admin_schema):
    """Without this the config layer accepts a cron the schedule row rejects."""
    for field in ("index_scheduling_cron", "pipeline_scheduling_cron"):
        assert admin_schema[field].get("format") == "cron"


def test_plan_carries_cron_active_and_the_binding(bindings):
    plan = bindings.build_scheduler_sync_plan({
        "index_scheduling": {"enabled": True, "cron": "*/15 * * * *"},
        "pipeline_scheduling": {"enabled": False, "cron": "* * * * *"},
    })
    by_name = {entry["name"]: entry for entry in plan}
    assert by_name["index_scheduling"]["cron"] == "*/15 * * * *"
    assert by_name["index_scheduling"]["active"] is True
    assert by_name["pipeline_scheduling"]["active"] is False
    assert by_name["index_scheduling"]["managed_by"] == \
        bindings.SCHEDULER_CONFIG_BINDINGS["index_scheduling"]


def test_malformed_scheduler_structures_do_not_raise(bindings):
    """Config is operator-editable. A scalar where a block belongs would raise
    out of ready(), which pylon swallows whole -- the platform would come up
    missing everything after the schedule bootstrap, silently."""
    for cfg in ("invalid", 7, ["a"], {"index_scheduling": "invalid"},
                {"index_scheduling": 5}):
        plan = bindings.build_scheduler_sync_plan(cfg)
        assert {e["name"] for e in plan} == set(bindings.SCHEDULER_CONFIG_BINDINGS), cfg
        assert all(e["cron"] == "* * * * *" for e in plan), cfg


def test_non_boolean_enabled_falls_back_instead_of_coercing(bindings):
    """bool("false") is True; persisting that string would start a schedule
    the operator meant to stop."""
    for bad in ("false", "no", 0, 1, "true", []):
        plan = {e["name"]: e for e in bindings.build_scheduler_sync_plan(
            {"index_scheduling": {"enabled": bad}}
        )}
        assert plan["index_scheduling"]["active"] is True, bad


def test_real_booleans_are_honoured(bindings):
    for value, expected in ((True, True), (False, False)):
        plan = {e["name"]: e for e in bindings.build_scheduler_sync_plan(
            {"index_scheduling": {"enabled": value}}
        )}
        assert plan["index_scheduling"]["active"] is expected


def test_plan_carries_the_expected_handler(bindings):
    """Reconciliation uses it to pick the canonical row out of duplicates."""
    plan = {e["name"]: e for e in bindings.build_scheduler_sync_plan({})}
    assert plan["index_scheduling"]["rpc_func"] == "applications_check_index_scheduling"
    assert plan["pipeline_scheduling"]["rpc_func"] == "pipelines_check_scheduling"


def test_index_handler_matches_the_rpc_this_plugin_registers(bindings):
    """A handler that drifts makes the canonical-row check pick the wrong row
    and creates the schedule pointing at nothing."""
    registered = {
        arg.value
        for node in ast.walk(ast.parse((PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text()))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "rpc"
        for arg in node.args
        if isinstance(arg, ast.Constant)
    }
    assert bindings.SCHEDULER_CONFIG_RPC_FUNCS["index_scheduling"] in registered


def test_every_binding_declares_a_handler(bindings):
    """pipeline_scheduling's handler belongs to the pipelines plugin, which is
    not installed in every deployment, so only its presence can be checked."""
    assert set(bindings.SCHEDULER_CONFIG_RPC_FUNCS) == set(
        bindings.SCHEDULER_CONFIG_BINDINGS
    )
    assert all(
        isinstance(value, str) and value
        for value in bindings.SCHEDULER_CONFIG_RPC_FUNCS.values()
    )


def test_each_schedule_creation_is_isolated():
    """An existing row that no longer validates raises inside
    create_if_not_exists; one try around all of them would skip every later
    row and the reconcile that repairs the bad one."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    bootstrap = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "_bootstrap_platform_schedules"
    )
    loops = [node for node in ast.walk(bootstrap) if isinstance(node, ast.For)]
    assert loops, "creation is no longer a loop over payloads"

    creation_tries = [
        stmt for loop in loops for stmt in loop.body
        if isinstance(stmt, ast.Try)
        and any(
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Attribute)
            and child.func.attr == "scheduling_create_if_not_exists"
            for child in ast.walk(stmt)
        )
    ]
    assert creation_tries, "create_if_not_exists is not individually guarded"

    for node in creation_tries:
        tolerant = [
            handler for handler in node.handlers
            if getattr(handler.type, "id", None) == "Exception"
        ]
        assert tolerant, "a failing row aborts the remaining creations"
        for handler in tolerant:
            assert not any(
                isinstance(child, (ast.Return, ast.Break))
                for child in ast.walk(handler)
            ), "the handler stops the loop instead of continuing"


def test_reconcile_runs_even_if_a_creation_failed():
    """The reconcile is what repairs a row create_if_not_exists choked on."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    bootstrap = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "_bootstrap_platform_schedules"
    )
    loops = [node for node in ast.walk(bootstrap) if isinstance(node, ast.For)]
    reconcile_lines = [
        child.lineno for child in ast.walk(bootstrap)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
        and child.func.attr == "_apply_scheduler_runtime_config"
    ]
    assert reconcile_lines
    assert all(line > max(loop.end_lineno for loop in loops) for line in reconcile_lines)


def test_unusable_persisted_cron_falls_back_to_the_default(bindings):
    """An invalid stored value reaches ScheduleModelPD and raises inside
    ready(), which pylon swallows whole -- the platform then comes up missing
    everything after the schedule bootstrap, with nothing in the log."""
    for bad in ("9 *", "not a cron", "", 5, ["* * * * *"]):
        plan = {e["name"]: e for e in bindings.build_scheduler_sync_plan(
            {"index_scheduling": {"cron": bad, "enabled": True}}
        )}
        assert plan["index_scheduling"]["cron"] == "* * * * *", bad


def test_valid_persisted_cron_is_passed_through(bindings):
    plan = {e["name"]: e for e in bindings.build_scheduler_sync_plan(
        {"index_scheduling": {"cron": "*/15 * * * *"}}
    )}
    assert plan["index_scheduling"]["cron"] == "*/15 * * * *"


def test_every_planned_cron_is_accepted_by_the_schedule_model(bindings):
    """The plan feeds create_if_not_exists directly, so anything it emits has
    to survive ScheduleModelPD's cron validator."""
    from croniter import croniter
    for cfg in ({}, {"index_scheduling": {"cron": None}},
                {"index_scheduling": {"cron": "bogus"}}):
        for entry in bindings.build_scheduler_sync_plan(cfg):
            assert croniter.is_valid(entry["cron"]), (cfg, entry)


def test_bootstrap_failures_cannot_abort_ready(bindings):
    """ready() is wrapped by pylon in a bare except, so an unexpected failure
    in the bootstrap would silently cost the chat thread and load_providers."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    bootstrap = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "_bootstrap_platform_schedules"
    )
    handlers = [
        handler for node in ast.walk(bootstrap)
        if isinstance(node, ast.Try) for handler in node.handlers
    ]
    caught = {getattr(h.type, "id", None) for h in handlers}
    assert "Exception" in caught, caught


def test_silent_config_pushes_the_schema_default(bindings):
    """Runtime shows the default for a missing or null value and a save of an
    unchanged value is a no-op, so skipping here would strand the schedule row
    on a value neither screen could correct."""
    for cfg in ({}, None, {"index_scheduling": {}}, {"index_scheduling": {"cron": None}}):
        plan = {entry["name"]: entry for entry in bindings.build_scheduler_sync_plan(cfg)}
        assert set(plan) == set(bindings.SCHEDULER_CONFIG_BINDINGS), cfg
        assert plan["index_scheduling"]["cron"] == "* * * * *", cfg
        assert plan["index_scheduling"]["active"] is True, cfg


def test_absent_enabled_flag_falls_back_to_the_default(bindings):
    plan = bindings.build_scheduler_sync_plan({"index_scheduling": {"cron": "0 * * * *"}})
    entry = next(e for e in plan if e["name"] == "index_scheduling")
    assert entry["cron"] == "0 * * * *"
    assert entry["active"] is True


def test_declared_defaults_match_the_admin_schema(bindings, admin_schema):
    """A default that drifts from the schema recreates the same divergence."""
    for name, binding in bindings.SCHEDULER_CONFIG_BINDINGS.items():
        defaults = bindings.SCHEDULER_CONFIG_DEFAULTS[name]
        for field in binding["fields"]:
            key = "cron" if field.endswith("_cron") else "enabled"
            assert admin_schema[field]["default"] == defaults[key], field


def test_managed_by_stays_free_of_reconciliation_detail(bindings):
    """managed_by is shipped to the browser; it describes where to edit, and
    nothing more."""
    for binding in bindings.SCHEDULER_CONFIG_BINDINGS.values():
        assert set(binding) == {"section", "fields"}


def test_ready_registers_the_bindings_unconditionally():
    """The schedule bootstrap sits behind an early return and a try/except
    Empty; registration must not inherit either, or a deployment with
    skill_publish_auto_migrate off leaves the rows editable."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    ready = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "ready"
    )
    top_level_calls = {
        stmt.value.func.attr for stmt in ready.body
        if isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Call)
        and isinstance(stmt.value.func, ast.Attribute)
    }
    assert "_register_managed_schedules" in top_level_calls


def test_registration_does_not_go_over_rpc():
    """The registry is per-process; an RPC would reach an arbitrary replica."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    helper = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "_register_managed_schedules"
    )
    assert not any(
        isinstance(child, ast.Attribute) and child.attr == "rpc_manager"
        for child in ast.walk(helper)
    )


def test_row_creation_uses_the_same_normalisation_as_reconciliation():
    """A persisted `cron: null` reaches create_if_not_exists as None, which
    fails ScheduleModelPD and aborts the rest of ready(). The creation payload
    has to take its values from the plan, not from raw config."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    bootstrap = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "_bootstrap_platform_schedules"
    )
    assert any(
        isinstance(child, ast.Call)
        and isinstance(child.func, ast.Name)
        and child.func.id == "build_scheduler_sync_plan"
        for child in ast.walk(bootstrap)
    )
    raw_reads = {
        child.args[0].value
        for child in ast.walk(bootstrap)
        if isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and child.func.attr == "get"
        and child.args
        and isinstance(child.args[0], ast.Constant)
    }
    assert "cron" not in raw_reads
    assert "enabled" not in raw_reads


def test_schedule_bootstrap_is_not_gated_on_skill_publish_migration():
    """_ensure_skill_publish_schema returns early when that unrelated flag is
    off; leaving the bootstrap there locks the managed rows without ever
    syncing them."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    methods = {
        item.name: item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef)
    }

    def calls(node, name):
        return any(
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Attribute)
            and child.func.attr == name
            for child in ast.walk(node)
        )

    assert calls(methods["ready"], "_bootstrap_platform_schedules")
    gated = methods["_ensure_skill_publish_schema"]
    assert not calls(gated, "_apply_scheduler_runtime_config")
    assert not any(
        isinstance(child, ast.Constant) and child.value == "index_scheduling"
        for child in ast.walk(gated)
    )


@pytest.mark.parametrize(
    "call", ["_register_managed_schedules", "_bootstrap_platform_schedules"]
)
def test_schedule_setup_precedes_the_unguarded_tenant_ddl(call):
    """ensure_trace_step_schema takes an advisory lock and issues DDL per
    tenant with no handler; anything it raises aborts ready().

    Registering behind it leaves the rows editable. Registering in front of it
    but bootstrapping behind it is worse: the tab locks both rows while nothing
    ever creates or reconciles them.
    """
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    ready = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "ready"
    )
    call_line = min(
        child.lineno for child in ast.walk(ready)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
        and child.func.attr == call
    )
    ddl_line = min(
        child.lineno for child in ast.walk(ready)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
        and child.func.id == "ensure_trace_step_schema"
    )
    assert call_line < ddl_line


def test_croniter_is_declared_by_this_plugin():
    """The module-level import runs at plugin load. croniter resolves today
    only because pylon leaves other plugins' requirement dirs on sys.path; if
    one drops it, elitea_core fails to import and pylon skips it entirely.
    """
    declared = (PLUGIN_ROOT / "requirements.txt").read_text()
    assert any(
        line.split("==")[0].split(">=")[0].strip() == "croniter"
        for line in declared.splitlines()
    ), declared


def test_reconciliation_does_not_pass_a_kwarg_older_scheduling_lacks():
    """rpc_func rides on register_managed_schedule, which only exists on a
    scheduling plugin new enough to accept it. Adding it to the RPC instead
    would TypeError against an older one, and the per-target handler swallows
    that -- cadence changes would stop reaching the rows silently."""
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    apply_config = next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "_apply_scheduler_runtime_config"
    )
    call = next(
        child for child in ast.walk(apply_config)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
        and child.func.attr == "scheduling_update_schedule"
    )
    assert {kw.arg for kw in call.keywords} == {"name", "cron", "active"}


def test_pulled_bindings_carry_the_handler(bindings):
    """The scheduling plugin rebuilds its whole registry from this after a hot
    reload, so a handler missing here would be lost with it."""
    for entry in bindings.advertised_bindings(True).values():
        assert entry["rpc_func"]


def _method(name):
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    return next(
        item for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "Module"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == name
    )


def test_a_planning_failure_releases_the_rows_it_cannot_drive():
    """Registration has already locked them by then. Falling through to a
    reconcile that re-derives the same plan just raises again on the same
    cause, so the rows would sit read-only with nothing behind them."""
    planning = next(
        node for node in _method("_bootstrap_platform_schedules").body
        if isinstance(node, ast.Try)
    )
    released = [
        child for handler in planning.handlers for child in ast.walk(handler)
        if isinstance(child, ast.Call)
        and getattr(child.func, "attr", None) == "_release_managed_schedules"
    ]
    assert released, "a planning failure leaves the rows locked"


def test_the_reconcile_is_handed_the_plan_that_was_already_built():
    """Re-deriving it would raise again on whatever stopped the bootstrap."""
    bootstrap = _method("_bootstrap_platform_schedules")
    call = next(
        node for node in ast.walk(bootstrap)
        if isinstance(node, ast.Call)
        and getattr(node.func, "attr", None) == "_apply_scheduler_runtime_config"
    )
    assert {kw.arg for kw in call.keywords} == {"plan"}, ast.dump(call)


def test_the_reconcile_only_derives_a_plan_when_it_is_not_given_one():
    apply_config = _method("_apply_scheduler_runtime_config")
    assert "plan" in {a.arg for a in apply_config.args.args}
    derivations = [
        node for node in ast.walk(apply_config)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "build_scheduler_sync_plan"
    ]
    assert len(derivations) == 1
    guard = next(
        node for node in ast.walk(apply_config)
        if isinstance(node, ast.If)
        and any(d.lineno == c.lineno for d in derivations for c in ast.walk(node))
    )
    assert isinstance(guard.test, ast.Compare)


def _get_managed_schedules(bindings, active):
    """Drive Module.get_managed_schedules itself, not just what it delegates
    to -- nothing else calls it, and scheduling calls nothing else."""
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[
        src.index("    def get_managed_schedules"):
        src.index("    def _release_managed_schedules")
    ]
    namespace = {"advertised_bindings": bindings.advertised_bindings}
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)
    holder = types.SimpleNamespace(_scheduler_bindings_active=active)
    return namespace["_M"].get_managed_schedules(holder)


def test_a_released_binding_is_not_re_advertised(bindings):
    """The scheduling plugin rebuilds its registry from this on its own
    ready(), so a static answer re-locks rows we released for being
    undrivable."""
    assert bindings.advertised_bindings(False) == {}
    assert _get_managed_schedules(bindings, False) == {}
    assert set(_get_managed_schedules(bindings, True)) == set(
        bindings.SCHEDULER_CONFIG_BINDINGS
    )


def test_active_bindings_carry_both_schedules_and_their_handlers(bindings):
    advertised = bindings.advertised_bindings(True)
    assert set(advertised) == set(bindings.SCHEDULER_CONFIG_BINDINGS)
    for name, entry in advertised.items():
        assert entry["managed_by"] == bindings.SCHEDULER_CONFIG_BINDINGS[name]
        assert entry["rpc_func"] == bindings.SCHEDULER_CONFIG_RPC_FUNCS[name]


def test_the_release_is_remembered_and_reversible():
    """Without the re-arm on a successful registration, one planning failure
    unlocks both rows permanently while the reconcile keeps overwriting them."""
    release = _method("_release_managed_schedules")
    register = _method("_register_managed_schedules")

    def _sets(node, value):
        return any(
            isinstance(child, ast.Assign)
            and any(
                getattr(t, "attr", None) == "_scheduler_bindings_active"
                for t in child.targets
            )
            and isinstance(child.value, ast.Constant)
            and child.value.value is value
            for child in ast.walk(node)
        )

    assert _sets(release, False), "a release is not remembered"
    assert _sets(register, True), "there is no way back out of the released state"


def test_enabled_props_declare_strict_type(bindings, admin_schema):
    """Without it the switch reports the stored value verbatim while the plan
    runs the default -- the ticket's own divergence, on the other field."""
    for field in ("index_scheduling_enabled", "pipeline_scheduling_enabled"):
        assert admin_schema[field].get("strict_type") is True, field


def test_creation_declares_its_own_identity_rather_than_asking_the_registry():
    """Read from a per-process registry, a replica whose ready() has not run
    matches on name alone, declines to create the real row, and leaves the
    reconcile raising for a row nothing ever makes."""
    bootstrap = _method("_bootstrap_platform_schedules")
    payloads = [
        node for node in ast.walk(bootstrap) if isinstance(node, ast.Dict)
        and any(
            isinstance(k, ast.Constant) and k.value == "name" for k in node.keys
        )
    ]
    owned = [
        d for d in payloads
        if any(
            isinstance(k, ast.Constant) and k.value == "match_handler"
            for k in d.keys
        )
    ]
    assert len(owned) == 2, "the config-owned payloads do not declare their identity"


def _explode(_name):
    raise RuntimeError("scheduling is not loaded")


def _drive_release(bindings, *, explode=False):
    """Run _release_managed_schedules against a stand-in scheduling module."""
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[
        src.index("    def _release_managed_schedules"):
        src.index("    def _register_managed_schedules")
    ]
    calls, logs = [], []
    scheduling = types.SimpleNamespace(
        register_managed_schedules=lambda owner, payload: calls.append(
            (owner, payload)
        ),
    )
    namespace = {
        "log": _recording_log(logs),
        "this": types.SimpleNamespace(
            for_module=_explode if explode else (
                lambda _name: types.SimpleNamespace(module=scheduling)
            ),
        ),
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)
    holder = types.SimpleNamespace(_scheduler_bindings_active=True)
    namespace["_M"]._release_managed_schedules(holder)
    return holder, calls, logs


def _drive_register(bindings, *, supported=True, explode=False):
    """Run _register_managed_schedules against a stand-in scheduling module.

    ``supported`` drops the method a pre-#6556 scheduling plugin does not have.
    """
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[
        src.index("    def _register_managed_schedules"):
        src.index("    def _apply_scheduler_runtime_config")
    ]
    calls, logs = [], []
    scheduling = types.SimpleNamespace()
    if supported:
        scheduling.register_managed_schedules = lambda owner, payload: (
            calls.append((owner, payload))
        )
    namespace = {
        "log": _recording_log(logs),
        "this": types.SimpleNamespace(
            for_module=_explode if explode else (
                lambda _name: types.SimpleNamespace(module=scheduling)
            ),
        ),
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)
    holder = types.SimpleNamespace(
        _scheduler_bindings_active=False,
        get_managed_schedules=lambda: dict(bindings.SCHEDULER_CONFIG_BINDINGS),
    )
    namespace["_M"]._register_managed_schedules(holder)
    return holder, calls, logs


def test_a_release_hands_over_nothing(bindings):
    """Re-registering the bindings here would re-lock the rows the release
    just declared undrivable."""
    holder, calls, _ = _drive_release(bindings)
    assert calls == [("elitea_core", {})]
    assert holder._scheduler_bindings_active is False


def test_an_unreachable_plugin_stops_creating_without_skipping_the_reconcile():
    """Returning here costs the reconcile; falling through costs the full RPC
    timeout once per remaining payload, in front of the rest of ready()."""
    bootstrap = _method("_bootstrap_platform_schedules")
    loop = next(node for node in ast.walk(bootstrap) if isinstance(node, ast.For))
    handlers = [
        handler for stmt in loop.body if isinstance(stmt, ast.Try)
        for handler in stmt.handlers
        if getattr(handler.type, "id", None) == "Empty"
    ]
    assert handlers, "the unreachable-plugin case is no longer handled"
    for handler in handlers:
        assert any(isinstance(c, ast.Break) for c in ast.walk(handler))
        assert not any(isinstance(c, ast.Return) for c in ast.walk(handler))


class _Empty(Exception):
    """Stands in for queue.Empty, which arbiter raises on an RPC timeout."""


def _drive_bootstrap(bindings, *, fail_after=0, error=_Empty, config=None,
                     plan_raises=False, reconcile_raises=False,
                     want_holder=False, want_created=False, want_logs=False):
    """Run _bootstrap_platform_schedules against a stand-in rpc_manager.

    ``error`` picks which failure the creates raise: an arbiter timeout, which
    means stop trying, or anything else -- an existing row that no longer
    validates -- which means carry on to the rest.
    """
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[
        src.index("    def _bootstrap_platform_schedules"):
        src.index("    def get_managed_schedules")
    ]
    attempts, reconciled, created, logs = [], [], [], []

    def _plan(cfg):
        if plan_raises:
            raise RuntimeError("the stored config cannot be planned")
        return bindings.build_scheduler_sync_plan(cfg)

    def _reconcile(plan=None):
        reconciled.append(plan)
        if reconcile_raises:
            raise RuntimeError("the scheduling plugin is unhappy")

    def _create(payload):
        attempts.append(payload["name"])
        created.append(dict(payload))
        if len(attempts) > fail_after:
            raise error()

    namespace = {
        "Empty": _Empty,
        "log": _recording_log(logs),
        "build_scheduler_sync_plan": _plan,
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)

    holder = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config=config or {}),
        context=types.SimpleNamespace(
            rpc_manager=types.SimpleNamespace(
                timeout=lambda _s: types.SimpleNamespace(
                    scheduling_create_if_not_exists=_create,
                ),
            ),
        ),
        _apply_scheduler_runtime_config=_reconcile,
        _release_managed_schedules=lambda: reconciled.append("released"),
        _scheduler_bindings_active=True,
    )
    namespace["_M"]._bootstrap_platform_schedules(holder)
    if want_holder:
        return holder, reconciled
    if want_created:
        return created, reconciled
    if want_logs:
        return logs, reconciled
    return attempts, reconciled


def test_an_unreachable_plugin_stops_at_the_first_timeout(bindings):
    """Every further call costs the full RPC timeout, in front of everything
    else ready() still has to do."""
    attempts, _ = _drive_bootstrap(bindings)
    assert len(attempts) == 1


def test_it_still_reconciles_with_a_real_plan_after_a_timeout(bindings):
    """Stopping the loop must not cost the reconcile, or the rows stay locked
    with their cadence unpushed. A plan carrying names alone is no better:
    target['cron'] raises inside the per-target try and nothing is pushed."""
    _, reconciled = _drive_bootstrap(bindings, config=CONFIGURED)
    assert len(reconciled) == 1
    assert _drive_apply(bindings, plan=reconciled[0]) == _expected_pushes(
        bindings, CONFIGURED
    )


def test_every_payload_is_attempted_when_the_plugin_answers(bindings):
    attempts, reconciled = _drive_bootstrap(bindings, fail_after=99)
    assert "index_scheduling" in attempts and "pipeline_scheduling" in attempts
    assert len(reconciled) == 1


def test_a_release_survives_the_lookup_raising(bindings):
    """Set inside the try, a raising for_module leaves the flag True and the
    next collect re-locks both rows."""
    holder, _, _ = _drive_release(bindings, explode=True)
    assert holder._scheduler_bindings_active is False


# Deliberately literal: were this derived from the method, deleting a payload
# would move both sides together and the schedule would silently stop being
# created.
BOOTSTRAPPED_SCHEDULES = [
    "empty_agent_state",
    "index_scheduling",
    "cleanup_stale_chunks",
    "pgvector_engine_reap",
    "eval_run_reap",
    "pat_expiration_check",
    "pipeline_scheduling",
]


def test_the_bootstrap_creates_exactly_these_schedules(bindings):
    """Nothing raises here, so this pins the payload list alone -- driven with
    a failure it would pass on the strength of the loop's own error handling."""
    attempts, _ = _drive_bootstrap(
        bindings, fail_after=len(BOOTSTRAPPED_SCHEDULES),
    )
    assert attempts == BOOTSTRAPPED_SCHEDULES


def test_a_row_that_no_longer_validates_does_not_stop_the_others(bindings):
    """Unlike an unreachable plugin, this says nothing about the next row --
    and truncating the loop here skips schedules nothing else creates. Failing
    mid-list is what separates this from the payload-list test above."""
    attempts, _ = _drive_bootstrap(bindings, fail_after=3, error=RuntimeError)
    assert attempts == BOOTSTRAPPED_SCHEDULES


def test_a_failing_create_still_reaches_the_reconcile(bindings):
    """A raise here escapes the method: the outer try covers only the payload
    building, so the reconcile never runs and registration has already left
    both rows locked with their cadence unpushed."""
    _, reconciled = _drive_bootstrap(bindings, error=RuntimeError, config=CONFIGURED)
    assert len(reconciled) == 1
    assert _drive_apply(bindings, plan=reconciled[0]) == _expected_pushes(
        bindings, CONFIGURED
    )


def test_a_failing_create_does_not_release_the_rows(bindings):
    """One row refusing to be created is not the configuration failing to
    describe them; releasing here would unlock schedules it can still drive."""
    holder, reconciled = _drive_bootstrap(bindings, error=RuntimeError, want_holder=True)
    assert "released" not in reconciled
    assert holder._scheduler_bindings_active is True


def _drive_apply(bindings, plan=None, config=None, raise_on=None,
                 want_registrations=False, want_logs=False):
    """Run the real _apply_scheduler_runtime_config and record what it pushes.

    ``raise_on`` makes that schedule's RPC fail, so the per-target guard is
    driven rather than assumed.
    """
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[
        src.index("    def _apply_scheduler_runtime_config"):
        src.index("    def create_scheduling")
    ]
    pushed, registrations, logs = [], [], []

    def _push(**kw):
        if raise_on is not None and kw.get("name") == raise_on:
            raise RuntimeError("this row's RPC is unhappy")
        pushed.append(kw)

    namespace = {
        "log": _recording_log(logs),
        "build_scheduler_sync_plan": bindings.build_scheduler_sync_plan,
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)

    holder = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config=config or {}),
        context=types.SimpleNamespace(
            rpc_manager=types.SimpleNamespace(
                timeout=lambda _s: types.SimpleNamespace(
                    scheduling_update_schedule=_push,
                ),
            ),
        ),
        _register_managed_schedules=lambda: registrations.append(True),
    )
    namespace["_M"]._apply_scheduler_runtime_config(holder, plan=plan)
    if want_registrations:
        return pushed, registrations
    if want_logs:
        return pushed, logs
    return pushed


CONFIGURED = {
    "scheduler": {
        "index_scheduling": {"cron": "*/15 * * * *", "enabled": True},
        "pipeline_scheduling": {"cron": "0 * * * *", "enabled": False},
    },
}


def _expected_pushes(bindings, config):
    entries = [
        {"name": e["name"], "cron": e["cron"], "active": e["active"]}
        for e in bindings.build_scheduler_sync_plan(config["scheduler"])
    ]
    return sorted(entries, key=lambda e: e["name"])


def test_the_configured_cadence_reaches_every_managed_row(bindings):
    """The ticket itself: what Configuration holds is what the schedule row
    ends up running. A plan carrying names alone pushes nothing at all, since
    target['cron'] raises inside the per-target try and is swallowed."""
    pushed = _drive_apply(bindings, config=CONFIGURED)
    assert sorted(pushed, key=lambda e: e["name"]) == _expected_pushes(
        bindings, CONFIGURED
    )


def test_every_managed_row_is_pushed_a_cron_and_an_active_flag(bindings):
    pushed = _drive_apply(bindings, config=CONFIGURED)
    assert len(pushed) == len(bindings.SCHEDULER_CONFIG_BINDINGS)
    for call in pushed:
        assert call["cron"] and isinstance(call["active"], bool)


def test_the_plan_the_bootstrap_builds_pushes_that_same_cadence(bindings):
    """End to end: the plan handed to the reconcile is one that actually
    carries cadence, not just names."""
    _, reconciled = _drive_bootstrap(bindings, error=RuntimeError, config=CONFIGURED)
    pushed = _drive_apply(bindings, plan=reconciled[0])
    assert sorted(pushed, key=lambda e: e["name"]) == _expected_pushes(
        bindings, CONFIGURED
    )


def test_the_reconcile_re_registers_the_bindings(bindings):
    """After a planning failure released the rows this is the only recovery
    short of a restart: a scheduling reload re-pulls get_managed_schedules(),
    which still answers nothing while the release stands."""
    _, registrations = _drive_apply(
        bindings, config=CONFIGURED, want_registrations=True,
    )
    assert registrations, "the release is never undone"


def test_one_unhappy_row_does_not_cost_the_other_its_cadence(bindings):
    """reconfig() calls this bare, so an unguarded raise would also take the
    MCP refresh and the guardrail reload with it."""
    pushed = _drive_apply(
        bindings, config=CONFIGURED, raise_on="index_scheduling",
    )
    assert [call["name"] for call in pushed] == ["pipeline_scheduling"]


def _drive_reconfig(bindings, apply_raises=False):
    """Run the real reconfig() and record whether the cadence push happened."""
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[src.index("    def reconfig(self):"):src.index("    def _bootstrap_platform_schedules")]
    done = []

    def _apply():
        done.append("applied")
        if apply_raises:
            raise RuntimeError("cannot reach the scheduling plugin")

    logs = []
    recorder = _recording_log(logs)

    def _exception(*args, **kwargs):
        done.append("logged")
        recorder.exception(*args, **kwargs)

    namespace = {"log": types.SimpleNamespace(
        info=recorder.info, exception=_exception,
    )}
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)
    holder = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={"mcp_exposure": {}}),
        _configure_elitea_ui=lambda: done.append("elitea_ui"),
        _apply_scheduler_runtime_config=_apply,
        _init_publishing_guardrail=lambda: done.append("guardrail"),
    )
    namespace["_M"].reconfig(holder)
    return done, holder, logs


def test_saving_runtime_config_pushes_the_cadence(bindings):
    """With the rows read-only, reconfig is the only route an operator's edit
    has to them -- without it the tab shows a value that is not what runs, and
    nothing on the tab can correct it."""
    done, _, _ = _drive_reconfig(bindings)
    assert done == ["elitea_ui", "applied", "guardrail"]


def test_a_failed_cadence_push_does_not_cost_the_rest_of_reconfig(bindings):
    """The MCP refresh and the guardrail reload run after it. reconfig()'s only
    caller swallows without logging, so the log.exception here is the sole
    trace a failed push leaves -- without it the tab keeps showing a cadence
    that is not the one running, silently."""
    done, holder, logs = _drive_reconfig(bindings, apply_raises=True)
    assert done == ["elitea_ui", "applied", "logged", "guardrail"]
    assert holder.mcp_exposure_enabled is True
    line = _only(logs, "exception")
    assert "apply scheduler runtime config" in line.template


def test_the_managed_payloads_carry_the_configured_cadence(bindings):
    """Asserted by name alone, a hardcoded cron or handler passes -- and a
    hardcoded cron slips past the normalisation tests too, since those only
    check the plan's shape."""
    created, _ = _drive_bootstrap(
        bindings, error=RuntimeError, config=CONFIGURED, want_created=True,
    )
    by_name = {payload["name"]: payload for payload in created}
    for entry in bindings.build_scheduler_sync_plan(CONFIGURED["scheduler"]):
        payload = by_name[entry["name"]]
        assert payload["cron"] == entry["cron"]
        assert payload["active"] == entry["active"]
        assert payload["rpc_func"] == entry["rpc_func"]
        assert payload["match_handler"] is True


def test_the_managed_payloads_name_the_handler_the_registry_declares(bindings):
    created, _ = _drive_bootstrap(
        bindings, error=RuntimeError, want_created=True,
    )
    by_name = {payload["name"]: payload for payload in created}
    for name, rpc_func in bindings.SCHEDULER_CONFIG_RPC_FUNCS.items():
        assert by_name[name]["rpc_func"] == rpc_func


# Every branch below is swallowed by its caller -- pylon's bare except around
# ready(), or a fallback that substitutes a default and carries on. The log
# line is the whole of what an operator or an on-call engineer ever sees, so
# deleting one has to be a test failure rather than a tidy-up.

def _messages(logs, level):
    return [line.rendered for line in logs if line.level == level]


def _only(logs, level):
    """The one line logged at this level, template and args still separate."""
    line, = [entry for entry in logs if entry.level == level]
    return line


@pytest.mark.parametrize("stopped_at", [0, 3])
def test_an_unreachable_plugin_names_the_schedule_it_stopped_at(bindings, stopped_at):
    """Driven at one position the name could be a literal, and a warning
    hoisted out of the loop would name the wrong schedule."""
    logs, _ = _drive_bootstrap(bindings, fail_after=stopped_at, want_logs=True)
    line = _only(logs, "warning")
    assert "No scheduling plugin responded" in line.template
    assert "skipping the rest" in line.template
    assert line.args == (BOOTSTRAPPED_SCHEDULES[stopped_at],)


def test_every_row_that_cannot_be_created_is_named(bindings):
    """The loop carries on past these, so without a line per row a partial
    bootstrap looks exactly like a whole one."""
    logs, _ = _drive_bootstrap(
        bindings, fail_after=3, error=RuntimeError, want_logs=True,
    )
    assert _messages(logs, "exception") == [
        f"Failed to create schedule: name={name}"
        for name in BOOTSTRAPPED_SCHEDULES[3:]
    ]


def test_a_planning_failure_says_so_before_it_releases_the_rows(bindings):
    logs, reconciled = _drive_bootstrap(
        bindings, plan_raises=True, want_logs=True,
    )
    assert _messages(logs, "exception") == ["Failed to plan platform schedules"]
    assert reconciled == ["released"]


def test_a_failed_reconcile_is_logged(bindings):
    """The rows are locked by then, so this is the line that explains why the
    tab shows a cadence nothing is pushing."""
    logs, _ = _drive_bootstrap(bindings, reconcile_raises=True, want_logs=True)
    assert "Failed to reconcile platform schedules" in _messages(logs, "exception")


def test_a_failed_push_names_the_row_it_could_not_reach(bindings):
    _, logs = _drive_apply(
        bindings, config=CONFIGURED, raise_on="index_scheduling", want_logs=True,
    )
    line = _only(logs, "error")
    assert "scheduling_update_schedule" in line.template
    assert "name=" in line.template and "error=" in line.template
    name, error = line.args
    assert name == "index_scheduling"
    assert str(error) == "this row's RPC is unhappy", "the cause is the line"
    assert "pipeline_scheduling" not in line.rendered


def test_a_release_says_the_rows_are_no_longer_driven(bindings):
    _, _, logs = _drive_release(bindings)
    line = _only(logs, "warning")
    assert "read-only" in line.template
    assert "cadence" in line.template, "why they must not stay locked"
    assert line.args == ()


def test_a_release_that_cannot_reach_the_plugin_is_logged(bindings):
    _, _, logs = _drive_release(bindings, explode=True)
    line = _only(logs, "error")
    assert "release" in line.template and "managed schedules" in line.template
    cause, = line.args
    assert str(cause) == "scheduling is not loaded", "the cause is the line"


def test_an_old_scheduling_plugin_is_logged_and_nothing_is_locked(bindings):
    """The rows stay editable on this path, which is the right outcome -- but
    silently it reads as a working lock that simply never took."""
    holder, calls, logs = _drive_register(bindings, supported=False)
    assert calls == []
    assert holder._scheduler_bindings_active is False
    line = _only(logs, "warning")
    assert "no managed-schedule support" in line.template
    assert "editable" in line.template, "what the operator actually sees happen"


def test_a_failed_registration_is_logged(bindings):
    _, calls, logs = _drive_register(bindings, explode=True)
    assert calls == []
    line = _only(logs, "error")
    assert "register" in line.template and "managed schedules" in line.template
    cause, = line.args
    assert str(cause) == "scheduling is not loaded", "the cause is the line"


def test_a_registration_hands_over_the_bindings_and_logs_nothing(bindings):
    holder, calls, logs = _drive_register(bindings)
    assert calls == [("elitea_core", dict(bindings.SCHEDULER_CONFIG_BINDINGS))]
    assert holder._scheduler_bindings_active is True
    assert logs == []


@pytest.mark.parametrize("name", ["index_scheduling", "pipeline_scheduling"])
def test_an_unusable_cron_names_the_field_and_the_fallback(bindings, logged, name):
    """Naming the schedule is not enough on either axis: three fallbacks log
    against the same name and two names share each message, so the line has to
    identify both the field to fix and the value now running in its place."""
    other, = set(bindings.SCHEDULER_CONFIG_BINDINGS) - {name}
    bindings.build_scheduler_sync_plan({name: {"cron": "9 *"}})
    line = _only(logged, "error")
    assert "unusable cron" in line.template
    assert "Configuration -> Runtime" in line.template
    assert line.args == (name, bindings.SCHEDULER_CONFIG_DEFAULTS[name]["cron"])
    assert other not in line.rendered


@pytest.mark.parametrize("name", ["index_scheduling", "pipeline_scheduling"])
def test_a_non_boolean_flag_names_the_field_and_the_fallback(bindings, logged, name):
    other, = set(bindings.SCHEDULER_CONFIG_BINDINGS) - {name}
    bindings.build_scheduler_sync_plan({name: {"enabled": "false"}})
    line = _only(logged, "error")
    assert "non-boolean enabled flag" in line.template
    assert "Configuration -> Runtime" in line.template
    assert line.args == (name, bindings.SCHEDULER_CONFIG_DEFAULTS[name]["enabled"])
    assert other not in line.rendered


@pytest.mark.parametrize("name", ["index_scheduling", "pipeline_scheduling"])
def test_a_scalar_where_a_block_belongs_names_the_field(bindings, logged, name):
    other, = set(bindings.SCHEDULER_CONFIG_BINDINGS) - {name}
    bindings.build_scheduler_sync_plan({name: "* * * * *"})
    line = _only(logged, "error")
    assert "not a mapping" in line.template
    assert line.args == (name,)
    assert other not in line.rendered


def test_a_healthy_config_logs_nothing(bindings, logged):
    """A fallback that fires on a good value would drown the real ones."""
    bindings.build_scheduler_sync_plan(CONFIGURED["scheduler"])
    assert logged == []
