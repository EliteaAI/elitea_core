"""C-1009 (#6947): who a trigger acts for, and the read-only report of the triggers that run on the system user."""
import json
import sys
import types
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import NOW, OWNER, add_mandate  # noqa: E402  pylint: disable=C0413

AGENT_KINDS = ("pipeline_schedule", "pipeline_webhook", "agent_webhook")
STATES = ("draft", "pending_approval", "active", "suspended", "expired", "revoked")


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("trigger_principal"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    return mandate_env.prepare(ns, ns.env)


def resolve(ns, env, trigger_kind, project_id=1, **kwargs):
    with env.get_session(project_id) as session:
        return ns.trigger_principal.resolve_trigger_principal(
            session, trigger_kind=trigger_kind, project_id=project_id, **kwargs)


# --- resolver ---------------------------------------------------------------------------------

def test_the_kinds_and_operations_are_the_designed_ones(ns):
    tp = ns.trigger_principal
    assert tp.TRIGGER_KINDS == ("pipeline_schedule", "pipeline_webhook", "agent_webhook", "index_schedule")
    assert tp.OPERATION_OF == {
        "pipeline_schedule": "run:schedule", "pipeline_webhook": "run:webhook", "agent_webhook": "run:webhook",
        "index_schedule": "run:index_schedule"}   # a toolkit mandate's (#7305)
    assert tp.AGENT_OPERATIONS == {"run:schedule", "run:webhook"} and tp.TOOLKIT_OPERATIONS == {"run:index_schedule"}


@pytest.mark.parametrize("key", ["-1", -1, None])
def test_a_team_index_schedule_runs_as_the_system_user(ns, key):
    principal = ns.trigger_principal.resolve_trigger_principal(
        None, trigger_kind="index_schedule", project_id=1, schedule_key=key)  # no session: nothing is read
    assert principal == ns.trigger_principal.TriggerPrincipal("legacy_system", None, None, None)


@pytest.mark.parametrize("key", ["7", 7])
def test_a_per_user_index_schedule_is_a_delegation(ns, key):
    principal = ns.trigger_principal.resolve_trigger_principal(
        None, trigger_kind="index_schedule", project_id=1, schedule_key=key)
    assert principal == ns.trigger_principal.TriggerPrincipal("delegation", "user:7", None, 7)


def test_an_odd_schedule_key_is_still_a_delegation_never_the_system_user(ns):
    principal = ns.trigger_principal.principal_of("index_schedule", schedule_key="abc")
    assert (principal.kind, principal.ref, principal.owner_user_id) == ("delegation", "user:abc", None)


@pytest.mark.parametrize("kind", AGENT_KINDS)
def test_a_trigger_without_a_mandate_runs_as_the_system_user(ns, env, kind):
    add_mandate(env, ns, version=71)
    assert resolve(ns, env, kind, version_id=70).kind == "legacy_system"
    assert resolve(ns, env, kind, project_id=2, version_id=71).kind == "legacy_system"
    assert resolve(ns, env, kind).kind == "legacy_system"  # no version at all


@pytest.mark.parametrize("kind", AGENT_KINDS)
@pytest.mark.parametrize("state", STATES)
def test_a_mandate_of_the_version_is_the_principal_whatever_its_state(ns, env, kind, state):
    mandate_id = add_mandate(env, ns, state=state, approved_by=8 if state in ("expired", "revoked") else None,
                             suspended_reason="MANUAL" if state == "suspended" else None)
    principal = resolve(ns, env, kind, version_id=70)
    assert principal == ns.trigger_principal.TriggerPrincipal("mandate", f"mandate:{mandate_id}", state, OWNER)


def test_the_latest_revision_wins(ns, env):
    add_mandate(env, ns, state="revoked", revision=1, approved_by=8, owner=6)
    second = add_mandate(env, ns, state="revoked", revision=2, approved_by=8, owner=7)
    third = add_mandate(env, ns, state="draft", revision=3, owner=9)
    principal = resolve(ns, env, "pipeline_schedule", version_id=70)
    assert (principal.ref, principal.mandate_state, principal.owner_user_id) == (f"mandate:{third}", "draft", 9)
    assert second != third


def test_a_revoked_latest_revision_is_not_replaced_by_an_older_live_looking_one(ns, env):
    add_mandate(env, ns, state="expired", revision=1, approved_by=8)
    last = add_mandate(env, ns, state="revoked", revision=2, approved_by=8)
    assert resolve(ns, env, "agent_webhook", version_id=70).ref == f"mandate:{last}"


def test_resolving_is_one_read_and_never_a_write(ns, env):
    add_mandate(env, ns, state="active")
    statements = []
    with env.get_session(1) as session:
        original = session.execute

        def counting(statement, *args, **kwargs):
            statements.append(statement)
            return original(statement, *args, **kwargs)

        session.execute = counting
        ns.trigger_principal.resolve_trigger_principal(
            session, trigger_kind="pipeline_schedule", project_id=1, version_id=70)
    assert len(statements) == 1 and statements[0].is_select


def test_only_the_newest_revision_leaves_the_database(ns, env):
    for revision in (1, 2, 3):
        add_mandate(env, ns, version=70, state="revoked" if revision < 3 else "draft", revision=revision,
                    approved_by=8 if revision < 3 else None)
    seen = []
    with env.get_session(1) as session:
        original = session.execute
        session.execute = lambda statement, *a, **k: seen.append(statement) or original(statement, *a, **k)
        latest = ns.trigger_principal.latest_mandates(session, 1)
    assert len(seen) == 1 and "max(" in str(seen[0].compile(dialect=ns.postgresql.dialect()))
    assert [row.revision for row in latest.values()] == [3]


def test_an_index_schedule_reads_nothing(ns, env):
    env.poison()
    ns.trigger_principal.resolve_trigger_principal(
        types.SimpleNamespace(execute=lambda *a, **k: (_ for _ in ()).throw(AssertionError("read"))),
        trigger_kind="index_schedule", project_id=1, schedule_key="7")
    env.unpoison()


def test_an_unknown_kind_is_refused(ns, env):
    with pytest.raises(ValueError):
        resolve(ns, env, "cron", version_id=70)


def test_the_bulk_read_is_one_query_for_many_versions(ns, env):
    add_mandate(env, ns, version=70, state="revoked", approved_by=8, revision=1)
    two = add_mandate(env, ns, version=70, state="draft", revision=2)
    three = add_mandate(env, ns, version=71, state="draft")
    add_mandate(env, ns, version=72, state="draft")
    with env.get_session(1) as session:
        latest = ns.trigger_principal.latest_mandates(session, 1, [70, 71])
    assert {version: row.id for version, row in latest.items()} == {70: two, 71: three}
    with env.get_session(1) as session:
        assert set(ns.trigger_principal.latest_mandates(session, 1)) == {70, 71, 72}
        assert ns.trigger_principal.latest_mandates(session, 1, []) == {}
        assert ns.trigger_principal.latest_mandates(session, 2) == {}


# --- report ---------------------------------------------------------------------------------------

@pytest.fixture
def report(ns, env, monkeypatch):
    """The report over canned trigger rows (the queries have their own test below) and real mandate rows."""
    canned = types.SimpleNamespace(pipelines=[], webhooks=[], indexes=[])
    monkeypatch.setattr(ns.trigger_report, "_pipeline_triggers", lambda session: list(canned.pipelines))
    monkeypatch.setattr(ns.trigger_report, "_agent_webhooks", lambda session: list(canned.webhooks))
    monkeypatch.setattr(ns.trigger_report, "_index_schedules", lambda session: list(canned.indexes))
    monkeypatch.setattr(ns.trigger_report, "datetime", types.SimpleNamespace(now=lambda tz: NOW))
    canned.run = lambda project_ids=(1,), principal=None: ns.trigger_report.build_trigger_report(list(project_ids), principal)
    return canned


def only(rows, **match):
    [row] = [row for row in rows if all(row[key] == value for key, value in match.items())]
    return row


def test_every_outcome_the_admission_layer_would_give(ns, env, report):
    admissible = add_mandate(env, ns, version=70, state="active", operations=("run:schedule", "run:webhook"))
    wrong_operation = add_mandate(env, ns, version=71, state="active")
    draft = add_mandate(env, ns, version=73, state="draft")
    expired = add_mandate(env, ns, version=74, state="expired", approved_by=8)
    overdue = add_mandate(env, ns, version=75, state="active", valid_until=NOW)
    overdue_suspended = add_mandate(env, ns, version=76, state="suspended", valid_until=NOW - timedelta(days=1))
    suspended = add_mandate(env, ns, version=77, state="suspended")
    revoked = add_mandate(env, ns, version=78, state="revoked", approved_by=8)
    report.pipelines = [(70, 7, "schedule", "3"), (71, 7, "webhook", "3"), (72, 7, "schedule", "3"),
                        (73, 7, "schedule", "3"), (74, 7, "schedule", None), (75, 7, "schedule", "3"),
                        (76, 7, "schedule", "3"), (77, 7, "webhook", "3"), (78, 7, "schedule", "3")]
    rows, failed = report.run()
    assert failed == []
    expected = {
        70: ("mandate", admissible, "active", "ADMISSIBLE"),
        71: ("mandate", wrong_operation, "active", "DENIED_OPERATION"),
        72: ("legacy_system", None, None, "MANDATE_REQUIRED"),
        73: ("mandate", draft, "draft", "MANDATE_INACTIVE"),
        74: ("mandate", expired, "expired", "MANDATE_EXPIRED"),
        75: ("mandate", overdue, "active", "MANDATE_EXPIRED"),
        76: ("mandate", overdue_suspended, "suspended", "MANDATE_EXPIRED"),
        77: ("mandate", suspended, "suspended", "MANDATE_INACTIVE"),
        78: ("mandate", revoked, "revoked", "MANDATE_INACTIVE"),
    }
    assert len(rows) == len(expected)
    for version, (principal, mandate_id, state, outcome) in expected.items():
        row = only(rows, version_id=version)
        assert (row["principal"], row["mandate_ref"], row["mandate_state"], row["governed_outcome"]) == (
            principal, None if mandate_id is None else str(mandate_id), state, outcome), version
    assert only(rows, version_id=71)["trigger_kind"] == "pipeline_webhook"
    assert only(rows, version_id=74)["owner_user_id"] is None and only(rows, version_id=70)["owner_user_id"] == 3


def test_the_boundary_belongs_to_expired_in_the_report_too(ns, env, report):
    add_mandate(env, ns, version=70, state="active", valid_until=NOW + timedelta(microseconds=1))
    report.pipelines = [(70, 7, "schedule", "3")]
    assert report.run()[0][0]["governed_outcome"] == "ADMISSIBLE"
    add_mandate(env, ns, version=71, state="active", valid_until=NOW)
    report.pipelines = [(71, 7, "schedule", "3")]
    assert report.run()[0][0]["governed_outcome"] == "MANDATE_EXPIRED"


def test_agent_webhooks_and_index_schedules(ns, env, report):
    add_mandate(env, ns, version=80, state="active", operations=("run:webhook",))
    report.webhooks = [(80, 8, 4), (81, 8, 4)]
    report.indexes = [(11, "docs", "-1", True, 3), (11, "docs", "7", False, 7), (12, "wiki", "-1", True, None)]
    rows, _ = report.run()
    assert only(rows, version_id=80) == {
        "project_id": 1, "trigger_kind": "agent_webhook", "application_id": 8, "version_id": 80, "toolkit_id": None,
        "index_name": None, "owner_user_id": 4, "enabled": True, "principal": "mandate",
        "mandate_ref": rows[0]["mandate_ref"], "mandate_state": "active", "governed_outcome": "ADMISSIBLE"}
    assert only(rows, version_id=81)["governed_outcome"] == "MANDATE_REQUIRED"
    team = only(rows, toolkit_id=11, enabled=True)
    assert (team["trigger_kind"], team["index_name"], team["principal"], team["owner_user_id"], team["governed_outcome"],
            team["version_id"], team["mandate_ref"]) == (
        "index_schedule", "docs", "legacy_system", 3, "MANDATE_REQUIRED", None, None)
    user = only(rows, toolkit_id=11, enabled=False)
    assert (user["principal"], user["owner_user_id"], user["governed_outcome"]) == ("delegation", 7, "AUTHORIZATION_REQUIRED")
    assert only(rows, toolkit_id=12)["owner_user_id"] is None


def test_rows_have_exactly_the_documented_fields(ns, env, report):
    report.pipelines = [(70, 7, "schedule", "3")]
    report.indexes = [(11, "docs", "7", True, 7)]
    rows, _ = report.run()
    assert all(set(row) == {
        "project_id", "trigger_kind", "application_id", "version_id", "toolkit_id", "index_name", "owner_user_id",
        "enabled", "principal", "mandate_ref", "mandate_state", "governed_outcome"} for row in rows)


def test_a_filter_keeps_only_the_requested_principal(ns, env, report):
    add_mandate(env, ns, version=70, state="active")
    report.pipelines = [(70, 7, "schedule", "3"), (72, 7, "schedule", "3")]
    report.indexes = [(11, "docs", "7", True, 7)]
    for principal, versions in (("mandate", [70]), ("legacy_system", [72]), ("delegation", [None])):
        rows, _ = report.run(principal=principal)
        assert [row["version_id"] for row in rows] == versions
    assert len(report.run()[0]) == 3


def test_a_project_that_cannot_be_read_is_reported_and_the_others_still_are(ns, env, report):
    report.pipelines = [(70, 7, "schedule", "3")]
    real = env.get_session

    def get_session(project_id):
        if project_id == 2:
            raise RuntimeError("no connection")
        return real(project_id)

    ns.tools.db.get_session = get_session
    rows, failed = report.run(project_ids=(1, 2, 3))
    assert failed == [2] and [row["project_id"] for row in rows] == [1, 3]
    assert any("failed to read project 2" in line for line in ns.log.lines)


def test_a_missing_mandate_table_still_reports_the_project(ns, env, report, monkeypatch):
    report.pipelines = [(70, 7, "schedule", "3")]
    report.webhooks = [(80, 8, 4)]
    report.indexes = [(11, "docs", "7", True, 7)]

    def missing(session, project_id, ids=None):
        raise ns.sa.exc.OperationalError("select", {}, Exception("no such table: autonomous_mandate"))

    monkeypatch.setattr(ns.trigger_report, "latest_mandates", missing)
    rows, failed = report.run()
    assert failed == [] and len(rows) == 3
    assert {row["trigger_kind"]: row["governed_outcome"] for row in rows} == {
        "pipeline_schedule": "MANDATE_TABLE_MISSING", "agent_webhook": "MANDATE_TABLE_MISSING",
        "index_schedule": "AUTHORIZATION_REQUIRED"}
    assert only(rows, trigger_kind="pipeline_schedule")["mandate_ref"] is None


def test_another_database_error_fails_the_project_not_the_report(ns, env, report, monkeypatch):
    report.pipelines = [(70, 7, "schedule", "3")]

    def broken(session, project_id, ids=None):
        raise ns.sa.exc.OperationalError("select", {}, Exception("connection reset"))

    monkeypatch.setattr(ns.trigger_report, "latest_mandates", broken)
    rows, failed = report.run(project_ids=(1, 2))
    assert rows == [] and failed == [1, 2]


def test_one_mandate_query_per_project_whatever_the_number_of_triggers(ns, env, report):
    report.pipelines = [(70 + n, 7, "schedule", "3") for n in range(20)]
    report.webhooks = [(100 + n, 8, 4) for n in range(20)]
    seen = []
    real = ns.trigger_report.latest_mandates
    ns.trigger_report.latest_mandates = lambda session, project_id, ids=None: seen.append(sorted(ids)) or real(session, project_id, ids)
    try:
        rows, _ = report.run()
    finally:
        ns.trigger_report.latest_mandates = real
    assert len(rows) == 40 and len(seen) == 1 and len(seen[0]) == 40


def test_no_row_holds_a_secret_shaped_value(ns, env, report):
    add_mandate(env, ns, version=70, state="active")
    report.pipelines = [(70, 7, "webhook", "3")]
    report.webhooks = [(80, 8, 4)]
    report.indexes = [(11, "docs", "-1", True, 3)]
    rows, _ = report.run()
    values = [value for row in rows for value in row.values() if isinstance(value, str)]
    assert values
    for value in values:
        if ns.event_model is not None:  # the tracing plugin's own secret-shape check, when it is checked out
            ns.event_model.assert_no_secret_shaped(value)
        assert not value.startswith(("eyJ", "sk-", "ghp_"))
        assert len(value) < 128 and " " not in value or value.count(" ") < 8


def test_outcomes_are_the_documented_set(ns, env, report):
    report.pipelines = [(70, 7, "schedule", "3")]
    assert report.run()[0][0]["governed_outcome"] in {
        "ADMISSIBLE", "MANDATE_REQUIRED", "MANDATE_INACTIVE", "MANDATE_EXPIRED", "AUTHORIZATION_REQUIRED",
        "DENIED_OPERATION"}


# --- the queries, on SQLite with the same column names ------------------------------------------------

@pytest.fixture
def tables(ns, env, monkeypatch):
    sa, orm = ns.sa, ns.sa_orm
    base = orm.declarative_base()

    class Application(base):
        __tablename__ = "applications"
        id = sa.Column(sa.Integer, primary_key=True)
        owner_id = sa.Column(sa.Integer, nullable=False)
        webhook_secret = sa.Column(sa.String, nullable=True)

    class ApplicationVersion(base):
        __tablename__ = "application_versions"
        id = sa.Column(sa.Integer, primary_key=True)
        application_id = sa.Column(sa.Integer, nullable=False)
        agent_type = sa.Column(sa.String, nullable=False)
        pipeline_settings = sa.Column(ns.postgresql.JSONB, nullable=True)

    class EliteATool(base):
        __tablename__ = "elitea_tools"
        id = sa.Column(sa.Integer, primary_key=True)
        meta = sa.Column(ns.postgresql.JSONB, nullable=False)

    models_all = sys.modules[f"{mandate_env.admission_env.PKG}.models.all"]
    monkeypatch.setattr(models_all, "Application", Application, raising=False)
    monkeypatch.setattr(models_all, "ApplicationVersion", ApplicationVersion)
    enums = sys.modules[f"{mandate_env.admission_env.PKG}.models.enums.all"]
    monkeypatch.setattr(enums, "AgentTypes", types.SimpleNamespace(pipeline=types.SimpleNamespace(value="pipeline")),
                        raising=False)
    tool_models = sys.modules[f"{mandate_env.admission_env.PKG}.models.elitea_tools"]
    monkeypatch.setattr(tool_models, "EliteATool", EliteATool)
    # Project 5 gets tables of its own: the harness stand-in of application_versions has two columns only
    engine = sa.create_engine(f"sqlite:///{env.dir}/p5.db").execution_options(schema_translate_map={"tenant": None})
    sa.event.listen(engine, "connect", lambda conn, _: conn.create_function(
        "jsonb_array_length", 1, lambda value: len(json.loads(value)), deterministic=True))
    ns.Mandate.__table__.create(engine)
    base.metadata.create_all(engine)
    env.engines[5] = engine
    with orm.Session(engine) as session:
        session.add_all([
            Application(id=7, owner_id=2, webhook_secret=None), Application(id=8, owner_id=4, webhook_secret="{{secret.hook}}"),
            Application(id=9, owner_id=4, webhook_secret=""),
            ApplicationVersion(id=70, application_id=7, agent_type="pipeline", pipeline_settings={
                "trigger": {"type": "schedule", "cron": "* * * * *", "timezone": "UTC", "created_by": 3}}),
            ApplicationVersion(id=71, application_id=7, agent_type="pipeline", pipeline_settings={
                "trigger": {"type": "webhook", "webhook_type": "custom", "webhook_secret": "{{secret.x}}", "created_by": 4}}),
            ApplicationVersion(id=72, application_id=7, agent_type="pipeline", pipeline_settings={
                "trigger": {"type": "chat_message"}}),
            ApplicationVersion(id=73, application_id=7, agent_type="pipeline", pipeline_settings=None),
            ApplicationVersion(id=80, application_id=8, agent_type="openai", pipeline_settings=None),
            ApplicationVersion(id=81, application_id=8, agent_type="pipeline", pipeline_settings=None),
            ApplicationVersion(id=82, application_id=7, agent_type="openai", pipeline_settings=None),
            ApplicationVersion(id=90, application_id=9, agent_type="openai", pipeline_settings=None),
            EliteATool(id=11, meta={"indexes_meta": {"docs": {"schedules": {
                "-1": {"cron": "0 3 * * *", "enabled": True, "created_by": 3, "credentials": {"elitea_title": "my-gh-token"}},
                "7": {"cron": "0 4 * * *", "enabled": False, "created_by": 7}}}, "empty": {}}}),
            EliteATool(id=12, meta={"other": 1}),
        ])
        session.commit()
    return base


def test_the_pipeline_query_returns_schedule_and_webhook_triggers_only(ns, env, tables):
    with env.get_session(5) as session:
        rows = [(*row[:3], str(row[3])) for row in ns.trigger_report._pipeline_triggers(session)]  # pylint: disable=W0212
    assert rows == [(70, 7, "schedule", "3"), (71, 7, "webhook", "4")]  # PostgreSQL returns the text, SQLite the number


def test_the_agent_webhook_query_returns_non_pipeline_versions_of_applications_with_a_secret(ns, env, tables):
    with env.get_session(5) as session:
        rows = [tuple(row) for row in ns.trigger_report._agent_webhooks(session)]  # pylint: disable=W0212
    assert rows == [(80, 8, 4)]


def test_the_index_query_returns_keys_and_flags_never_the_body(ns, env, tables):
    with env.get_session(5) as session:
        rows = ns.trigger_report._index_schedules(session)  # pylint: disable=W0212
    assert rows == [(11, "docs", "-1", True, 3), (11, "docs", "7", False, 7)]
    assert "my-gh-token" not in repr(rows)


def test_the_whole_report_over_real_queries(ns, env, tables):
    add_mandate(env, ns, 5, version=70, state="active", valid_until=datetime.now(timezone.utc) + timedelta(days=1))
    rows, failed = ns.trigger_report.build_trigger_report([5])
    assert failed == []
    assert [(row["trigger_kind"], row["version_id"], row["toolkit_id"], row["principal"]) for row in rows] == [
        ("pipeline_schedule", 70, None, "mandate"), ("pipeline_webhook", 71, None, "legacy_system"),
        ("agent_webhook", 80, None, "legacy_system"), ("index_schedule", None, 11, "legacy_system"),
        ("index_schedule", None, 11, "delegation")]
    assert "{{secret" not in repr(rows) and "my-gh-token" not in repr(rows) and "* * * * *" not in repr(rows)


def test_the_report_selects_no_column_that_holds_a_secret_or_a_schedule_body():
    source = (PLUGIN_ROOT / "utils" / "trigger_report.py").read_text()
    assert "EliteATool.settings" not in source and "Application.webhook_secret)" not in source
    assert source.count("webhook_secret") == 2 and "webhook_secret !=" in source and "webhook_secret.isnot(None)" in source
