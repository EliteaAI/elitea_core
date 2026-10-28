"""C-1009 (#6947, WP-1C PR C): the trigger paths of autonomous runs - tickets, the scheduler tick, the webhook API,
the agent webhook, ``predict_sio``, scheduled indexing and the dispatch wrapper.

The admission code, its SQL, the ticket store and the trigger principal are the real files (see
``fixtures/mandate_env.py``). The tick, the pipeline execution helpers, the webhook API and ``do_predict`` are the
real source too, loaded with the rest of the plugin stubbed; ``predict_sio`` is a stand-in that takes the ticket and
admits the dispatch exactly as the real one does (and the real one is run, with mocks around it, for the flag-off
equality and the ticket hand-off).
"""
import ast
import builtins
import copy
import json
import sys
import textwrap
import types
import uuid
from collections import OrderedDict
from datetime import timedelta
from pathlib import Path
from unittest import mock

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env, mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import NOW, OWNER, add_mandate  # noqa: E402  pylint: disable=C0413

PKG = admission_env.PKG
CREATOR = 3


def source_of(relative):
    return (PLUGIN_ROOT / relative).read_text()


def stub(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


class FakeVersion:
    def __init__(self, version_id=70, application_id=7, cron="*/2 * * * *"):
        self.id, self.application_id = version_id, application_id
        self.agent_type = "pipeline"
        self.pipeline_settings = {"trigger": {"type": "schedule", "cron": cron, "timezone": "UTC",
                                              "last_run": "2026-10-20T11:00:00+00:00", "created_by": CREATOR}}


class FakeSession:
    def __init__(self):
        self.commits = self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture(scope="module")
def ns(tmp_path_factory):  # pylint: disable=R0914,R0915
    saved = dict(sys.modules)
    ns = mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_triggers"))
    sa = ns.sa
    sys.modules["pylon.core.tools"].web = types.SimpleNamespace(
        rpc=lambda *a, **k: (lambda func: func), method=lambda *a, **k: (lambda func: func))

    class Application(ns.Base):   # the one column the tick reads
        __tablename__ = "applications"
        __table_args__ = {"schema": "tenant"}
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String)

    ns.Application = Application
    for name in ("plugins.elitea_core.rpc", "plugins.elitea_core.api", "plugins.elitea_core.api.v2",
                 "plugins.elitea_core.models.pd", "plugins.elitea_core.models.message_items"):
        package = sys.modules.get(name) or types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    models_all = sys.modules[f"{PKG}.models.all"]
    models_all.Application = Application
    ns.state = types.SimpleNamespace(due=True, conversations=[], events=[], predicts=[], tickets_seen=[])
    ns.parse = lambda trigger: types.SimpleNamespace(**trigger)
    stub(f"{PKG}.models.enums.all", AgentTypes=types.SimpleNamespace(pipeline=types.SimpleNamespace(value="pipeline")),
         ParticipantTypes=types.SimpleNamespace(application="application", user="user"),
         ToolEntityTypes=types.SimpleNamespace(agent=types.SimpleNamespace(value="agent")))
    stub(f"{PKG}.models.pd.pipeline_trigger",
         TriggerType=types.SimpleNamespace(schedule=types.SimpleNamespace(value="schedule"),
                                           webhook=types.SimpleNamespace(value="webhook")),
         PipelineTriggerSchedule=type("PipelineTriggerSchedule", (), {
             "parse_obj": classmethod(lambda cls, trigger: ns.parse(trigger))}))
    stub(f"{PKG}.models.conversation", Conversation=object)
    stub(f"{PKG}.models.message_group", ConversationMessageGroup=object)
    stub(f"{PKG}.models.message_items.text", TextMessageItem=object)
    stub(f"{PKG}.models.pd.participant", ParticipantEntityApplication=object, ParticipantEntityUser=object)
    stub(f"{PKG}.utils.participant_utils", get_or_create_one=None)
    stub(f"{PKG}.utils.sio_utils", SioEvents=types.SimpleNamespace(chat_predict=types.SimpleNamespace(value="chat_predict")))
    stub(f"{PKG}.utils.cron_utils", is_cron_due=lambda cron, last, tz: ns.state.due)
    stub(f"{PKG}.utils.maintenance_gate", is_maintenance_active=lambda: False)
    stub(f"{PKG}.utils.input_guardrail", redacted_input=lambda text: text, apply_input_guardrail=lambda kwargs: None,
         inspect_and_unsecret=lambda payload, vault, *args: vault.unsecret(payload))   # inert guardrail (#6886)
    stub(f"{PKG}.utils.utils", make_yield_to_hub=lambda runtime: (lambda: None), end_ambient_transaction=lambda: None)
    stub("sqlalchemy.orm.attributes", flag_modified=lambda obj, key: None)

    def predict_sio(**kwargs):
        """What ``applications_predict_sio`` does with a ticket: take it, else the ordinary request, then admit."""
        module = ns.tools.this.module
        ns.state.predicts.append(kwargs)
        data = kwargs["data"]
        admission = ns.mandate_admission.take_trigger_admission(
            module, kwargs.get("mandate_ticket"), project_id=data["project_id"], binding_project_id=data["project_id"],
            version_id=data["version_id"],
        ) or ns.pd_admission.admission_request_from(
            module, sid=None, project_id=data["project_id"], binding_project_id=data["project_id"],
            version_row=types.SimpleNamespace(id=data["version_id"], application_id=data["application_id"]),
            run_kind="system" if kwargs["is_system_user"] else "interactive", entry_point="chat_sio")
        call = {"args": [data["stream_id"], data["message_id"]], "pool": "agents",
                "kwargs": {"message_id": data["message_id"], "envelope": {"security_profile": "governed", "context_ref": None}},
                "meta": {"project_id": data["project_id"]}}
        ns.run_id.stamp_predict_run_id(call)
        try:
            context_ref = ns.adm.admit_dispatch(module, "indexer_agent", call, admission) if admission else None
        except ns.exceptions.AdmissionDenied as denied:
            return {"code": denied.reason_code, "error": denied.message}
        ns.state.contexts = getattr(ns.state, "contexts", []) + [context_ref]
        return {"task_id": f"task-{len(ns.state.predicts)}"}

    ns.tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
        timeout=lambda seconds: types.SimpleNamespace(applications_predict_sio=predict_sio))))
    ns.tools.this.module = None   # per test
    ns.tools.VaultClient = lambda *a, **k: types.SimpleNamespace(unsecret=lambda value: value)

    def load(kind, name, package):
        return load_module_with_stubs(PLUGIN_ROOT / kind / f"{name}.py", f"{PKG}.{package}.{name}")

    ns.execution = load("utils", "pipeline_execution", "utils")
    ns.scheduling = load("rpc", "pipeline_scheduling", "rpc")
    ns.webhook_rpc = load("rpc", "pipeline_webhook", "rpc")
    for module in (ns.execution, ns.scheduling, ns.webhook_rpc):
        setattr(module, "create_trigger_run_conversation", lambda **kwargs: (
            ns.state.conversations.append(kwargs), ("conv-uuid", 55, f"response-{len(ns.state.conversations)}"))[1])
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    env = mandate_env.prepare(ns, ns.env)
    ns.state.due, ns.state.conversations, ns.state.events, ns.state.predicts = True, [], [], []
    ns.state.contexts = []
    return env


def arm(ns, env, governed=True, **settings):
    """The module the tick and ``predict_sio`` find as ``this.module``, with an event manager that records."""
    module = env.module(governed=governed, **settings)
    module.context = types.SimpleNamespace(event_manager=types.SimpleNamespace(
        fire_event=lambda name, payload: ns.state.events.append((name, payload))))
    ns.tools.this.module = module
    return module


@pytest.fixture
def agent(ns, env):
    binding = env.seed_agent()
    mandate = add_mandate(env, ns, state="active", binding_ref=binding, operations=("run:schedule", "run:webhook"))
    return types.SimpleNamespace(binding=binding, mandate=mandate)


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def tick(ns, version=None):
    version, session = version or FakeVersion(), FakeSession()
    ns.scheduling._process_pipeline_version(1, version, session)   # pylint: disable=W0212
    return version, session


def set_mandate(env, ns, mandate, **values):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == mandate).values(**values))
        session.commit()


# --- the ticket store ---------------------------------------------------------------------------

class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def a_request(ns, env, mandate=None, **over):
    values = dict(project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="mandate",
                  entry_point="schedule", caller_type="trigger", mandate_ref=str(mandate or uuid.uuid4()),
                  trigger_ref="schedule:version:70")
    values.update(over)
    return ns.AdmissionRequest(**values)


def test_a_ticket_is_single_use_and_unguessable(ns, env):
    store = ns.mandate_admission.TicketStore()
    request = a_request(ns, env)
    ticket = store.mint(request)
    assert isinstance(ticket, str) and len(ticket) >= 40 and ticket != store.mint(request)
    assert store.take(ticket) is request
    assert store.take(ticket) is None   # replay
    for junk in (None, "", "x", 5, b"bytes", ["a"], {"a": 1}):
        assert store.take(junk) is None


def test_a_ticket_lives_sixty_seconds(ns, env):
    assert ns.mandate_admission.TICKET_TTL_S == 60 and ns.mandate_admission.MAX_TICKETS == 1000
    clock = Clock()
    store = ns.mandate_admission.TicketStore(clock=clock)
    request = a_request(ns, env)
    fresh, stale = store.mint(request), store.mint(request)
    clock.now += 59.9
    assert store.take(fresh) is request
    clock.now += 0.1   # exactly the ttl: gone
    assert store.take(stale) is None and len(store) == 0


def test_the_store_is_bounded_and_drops_the_oldest_ticket_first(ns, env):
    store = ns.mandate_admission.TicketStore(capacity=3)
    request = a_request(ns, env)
    tickets = [store.mint(request) for _ in range(5)]
    assert len(store) == 3
    assert [store.take(ticket) is not None for ticket in tickets] == [False, False, True, True, True]


def test_expired_tickets_are_purged_when_a_new_one_is_minted(ns, env):
    clock = Clock()
    store = ns.mandate_admission.TicketStore(clock=clock)
    request = a_request(ns, env)
    for _ in range(10):
        store.mint(request)
    clock.now += 61
    store.mint(request)
    assert len(store) == 1


def test_take_trigger_admission_gives_the_request_back_only_for_the_version_the_caller_loaded(ns, env, agent):
    module = arm(ns, env)
    request = a_request(ns, env, agent.mandate)
    take = ns.mandate_admission.take_trigger_admission
    ticket = ns.mandate_admission.mint_trigger_admission(module, request)
    assert take(module, ticket, project_id=1, binding_project_id=1, version_id=70) is request
    assert take(module, ticket, project_id=1, binding_project_id=1, version_id=70) is None   # spent
    for over in ({"version_id": 71}, {"project_id": 2}, {"binding_project_id": 2}):
        ticket = ns.mandate_admission.mint_trigger_admission(module, request)
        kwargs = {"project_id": 1, "binding_project_id": 1, "version_id": 70, **over}
        assert take(module, ticket, **kwargs) is None
        assert take(module, ticket, project_id=1, binding_project_id=1, version_id=70) is None   # spent by the miss


def test_only_a_mandate_request_gets_a_ticket(ns, env):
    module = arm(ns, env)
    interactive = ns.AdmissionRequest(project_id=1, binding_project_id=1, run_kind="interactive", entry_point="chat_sio",
                                      caller_type="user", caller_id=1)
    assert ns.mandate_admission.mint_trigger_admission(module, interactive) is None
    assert len(module._mandate_tickets) == 0   # pylint: disable=W0212


def test_flag_off_no_ticket_is_minted_or_taken_and_the_store_is_not_touched(ns, env, agent):
    module = arm(ns, env, governed=False)
    module._mandate_tickets = mock.Mock(spec=[])   # any use fails   # pylint: disable=W0212
    assert ns.mandate_admission.mint_trigger_admission(module, a_request(ns, env, agent.mandate)) is None
    assert ns.mandate_admission.take_trigger_admission(module, "ticket", project_id=1, version_id=70) is None


def test_the_request_builder_refuses_a_trigger_without_a_mandate(ns, env, agent):
    row = types.SimpleNamespace(id=70, application_id=7)
    legacy = ns.trigger_principal.LEGACY
    with pytest.raises(ValueError):
        ns.mandate_admission.mandate_admission_request(
            project_id=1, version_row=row, principal=legacy, entry_point="schedule", trigger_ref="schedule:version:70")
    principal = ns.trigger_principal.TriggerPrincipal("mandate", f"mandate:{agent.mandate}", "active", OWNER)
    request = ns.mandate_admission.mandate_admission_request(
        project_id=1, version_row=row, principal=principal, entry_point="webhook", trigger_ref="webhook:version:70")
    assert (request.run_kind, request.entry_point, request.caller_type, request.mandate_ref, request.application_id,
            request.version_id, request.binding_project_id) == ("mandate", "webhook", "trigger", str(agent.mandate), 7, 70, 1)


def test_prepare_trigger_run_flag_off_reads_nothing_and_changes_nothing(ns, env, agent):
    arm(ns, env, governed=False)
    env.poison()
    assert ns.mandate_admission.prepare_trigger_run(
        trigger_kind="pipeline_schedule", project_id=1, version=FakeVersion(), entry_point="schedule",
        trigger_ref="schedule:version:70", default_user_id=CREATOR) == (CREATOR, None)


def test_prepare_trigger_run_governed_gives_the_owner_and_a_ticket_only_with_a_mandate(ns, env, agent):
    module = arm(ns, env)
    prepare = ns.mandate_admission.prepare_trigger_run
    user_id, ticket = prepare(trigger_kind="pipeline_schedule", project_id=1, version=FakeVersion(), entry_point="schedule",
                              trigger_ref="schedule:version:70", default_user_id=CREATOR)
    assert user_id == OWNER and isinstance(ticket, str)
    request = module._mandate_tickets.take(ticket)   # pylint: disable=W0212
    assert (request.mandate_ref, request.entry_point, request.trigger_ref) == (str(agent.mandate), "schedule",
                                                                                 "schedule:version:70")
    # a version with no mandate stays a system run: the old user, no ticket
    assert prepare(trigger_kind="pipeline_schedule", project_id=1, version=FakeVersion(71, 8), entry_point="schedule",
                   trigger_ref="schedule:version:71", default_user_id=CREATOR) == (CREATOR, None)
    assert len(module._mandate_tickets) == 0   # pylint: disable=W0212


def test_a_mandate_that_is_not_active_still_gives_a_ticket_so_admission_can_say_why(ns, env, agent):
    arm(ns, env)
    set_mandate(env, ns, agent.mandate, state="suspended", suspended_reason="MANUAL")
    user_id, ticket = ns.mandate_admission.prepare_trigger_run(
        trigger_kind="pipeline_schedule", project_id=1, version=FakeVersion(), entry_point="schedule",
        trigger_ref="schedule:version:70", default_user_id=CREATOR)
    assert user_id == OWNER and ticket is not None


# --- forgery: what predict_sio does with a ticket -----------------------------------------------

def forged_call(ns, ticket, version_id=70, user_id=999):
    predict = ns.tools.rpc_tools.RpcMixin().rpc.timeout(5).applications_predict_sio
    kwargs = dict(sid=None, data={"project_id": 1, "application_id": 7, "version_id": version_id, "message_id": str(uuid.uuid4()),
                                  "stream_id": "s"}, user_id=user_id, is_system_user=True)
    if ticket is not None:
        kwargs["mandate_ticket"] = ticket
    return predict(**kwargs)


def test_a_system_run_with_a_forged_ticket_is_refused_as_required_whoever_it_claims_to_be(ns, env, agent):
    arm(ns, env)
    for ticket in (None, "forged", "x" * 43, uuid.uuid4().hex):
        assert forged_call(ns, ticket)["code"] == "MANDATE_REQUIRED"
    assert contexts(env, ns) == []


def test_a_real_ticket_admits_once_and_a_replay_is_refused(ns, env, agent):
    module = arm(ns, env)
    ticket = ns.mandate_admission.mint_trigger_admission(module, a_request(ns, env, agent.mandate))
    assert "task_id" in forged_call(ns, ticket)
    assert forged_call(ns, ticket)["code"] == "MANDATE_REQUIRED"
    assert len(contexts(env, ns)) == 1


def test_a_ticket_for_one_version_does_not_admit_another(ns, env, agent):
    module = arm(ns, env)
    env.seed_agent(version_id=71)
    ticket = ns.mandate_admission.mint_trigger_admission(module, a_request(ns, env, agent.mandate))
    assert forged_call(ns, ticket, version_id=71)["code"] == "MANDATE_REQUIRED"
    assert contexts(env, ns) == []


def test_a_ticket_older_than_its_ttl_is_refused(ns, env, agent):
    module = arm(ns, env)
    clock = Clock()
    module._mandate_tickets = ns.mandate_admission.TicketStore(clock=clock)   # pylint: disable=W0212
    ticket = ns.mandate_admission.mint_trigger_admission(module, a_request(ns, env, agent.mandate))
    clock.now += 61
    assert forged_call(ns, ticket)["code"] == "MANDATE_REQUIRED"


def test_a_ticket_from_another_replica_is_just_an_unknown_string(ns, env, agent):
    module = arm(ns, env)
    other = arm(ns, env)   # a second process: its own store
    ticket = ns.mandate_admission.mint_trigger_admission(other, a_request(ns, env, agent.mandate))
    ns.tools.this.module = module
    assert forged_call(ns, ticket)["code"] == "MANDATE_REQUIRED"


def test_the_cross_pylon_webhook_rpc_never_passes_a_ticket(ns, env, agent):
    arm(ns, env)
    text = source_of("rpc/pipeline_webhook.py")
    rpc_part = text[text.index("class RPC"):]
    assert "mandate_ticket" not in rpc_part
    result = ns.webhook_rpc.execute_pipeline_webhook(1, FakeVersion(), 999, "github", {"action": "opened"})
    assert result["code"] == "MANDATE_REQUIRED" and set(result) == {"code", "error"}
    assert contexts(env, ns) == []


# --- the scheduler tick -------------------------------------------------------------------------

def test_a_due_pipeline_with_an_active_mandate_runs_as_its_owner_under_one_admission(ns, env, agent):
    arm(ns, env)
    version, session = tick(ns)
    [predict] = ns.state.predicts
    assert predict["user_id"] == OWNER and predict["is_system_user"] is True and predict["trigger_source"] == "scheduled"
    assert isinstance(predict["mandate_ticket"], str)
    assert ns.state.conversations[0]["user_id"] == OWNER   # History and author: the owner, not the schedule creator
    [row] = contexts(env, ns)
    assert (row.entry_point, row.caller["ref"], row.subject["ref"]) == ("schedule", "schedule:version:70",
                                                                         f"mandate:{agent.mandate}")
    assert ns.state.events == [] and session.commits == 1
    assert version.pipeline_settings["trigger"]["last_run"] != "2026-10-20T11:00:00+00:00"


def test_two_firings_are_two_runs_with_two_contexts_and_two_tickets(ns, env, agent):
    module = arm(ns, env)
    tick(ns)
    tick(ns)
    rows = contexts(env, ns)
    assert len(rows) == 2 and len({row.run_id for row in rows}) == 2
    tickets = [predict["mandate_ticket"] for predict in ns.state.predicts]
    assert len(set(tickets)) == 2 and len(module._mandate_tickets) == 0   # pylint: disable=W0212


@pytest.mark.parametrize("name, change, code", [
    ("no_mandate", lambda ns, env, agent: None, "MANDATE_REQUIRED"),
    ("suspended", lambda ns, env, agent: set_mandate(env, ns, agent.mandate, state="suspended", suspended_reason="MANUAL"),
     "MANDATE_INACTIVE"),
    ("revoked", lambda ns, env, agent: set_mandate(env, ns, agent.mandate, state="revoked"), "MANDATE_INACTIVE"),
    ("overdue", lambda ns, env, agent: set_mandate(env, ns, agent.mandate, valid_until=NOW - timedelta(seconds=1)),
     "MANDATE_EXPIRED"),
    ("webhook_only", lambda ns, env, agent: set_mandate(env, ns, agent.mandate, allowed_operations=["run:webhook"]),
     "DENIED_OPERATION"),
])
def test_a_refused_firing_writes_the_readable_refusal_into_its_history_and_consumes_the_slot(ns, env, agent, name, change, code):
    arm(ns, env)
    if name == "no_mandate":
        with ns.sa_orm.Session(env.engine(1)) as session:
            session.execute(ns.sa.delete(ns.Mandate))
            session.commit()
    change(ns, env, agent)
    version, session = tick(ns)
    [predict] = ns.state.predicts
    assert ("mandate_ticket" in predict) == (name != "no_mandate")
    assert predict["user_id"] == (CREATOR if name == "no_mandate" else OWNER)
    [(event_name, payload)] = ns.state.events
    meta = payload["response_metadata"]
    assert event_name == "chat_message_stream_end" and payload["message_id"] == "response-1"
    assert meta["is_error"] is True and meta["code"] == code
    assert ns.adm.MESSAGES[code] in payload["content"] and ns.adm.MESSAGES[code] in meta["error"]
    assert meta["project_id"] == meta["chat_project_id"] == 1
    assert contexts(env, ns) == []
    assert version.pipeline_settings["trigger"]["last_run"] != "2026-10-20T11:00:00+00:00" and session.commits == 1
    assert env.denied[-1]["reason_code"] == code


def test_a_refused_slot_is_not_fired_again_on_the_next_tick(ns, env, agent, monkeypatch):
    arm(ns, env)
    stale = "2026-10-20T11:00:00+00:00"
    monkeypatch.setattr(ns.scheduling, "is_cron_due", lambda cron, last_run, tz: last_run == stale)
    set_mandate(env, ns, agent.mandate, state="revoked")
    version, session = FakeVersion(), FakeSession()
    for _ in range(4):
        ns.scheduling._process_pipeline_version(1, version, session)   # pylint: disable=W0212
    assert len(ns.state.predicts) == 1 and len(ns.state.events) == 1


def test_an_exception_while_preparing_a_firing_does_not_open_a_conversation(ns, env, agent, monkeypatch):
    arm(ns, env)
    monkeypatch.setattr(ns.scheduling, "prepare_trigger_run", mock.Mock(side_effect=RuntimeError("db down")))
    version, session = tick(ns)
    assert ns.state.conversations == [] and ns.state.predicts == [] and session.commits == 1


def test_a_failed_dispatch_of_a_mandate_run_is_closed_like_any_other(ns, env, agent, monkeypatch):
    arm(ns, env)

    def boom(**kwargs):
        raise RuntimeError("pool down")

    monkeypatch.setattr(ns.execution, "execute_pipeline_via_predict_sio", boom)
    monkeypatch.setattr(ns.scheduling, "execute_pipeline_via_predict_sio", boom)
    _, session = tick(ns)
    [(name, payload)] = ns.state.events
    assert name == "chat_message_stream_end" and "pool down" in payload["content"] and session.commits == 1


# --- flag off: today's behaviour, no added calls ------------------------------------------------

def test_flag_off_the_tick_sends_exactly_the_old_call_and_reads_nothing_new(ns, env, agent):
    module = arm(ns, env, governed=False)
    module._mandate_tickets = mock.Mock(spec=[])   # any use fails   # pylint: disable=W0212
    calls = []
    real = ns.tools.db.get_session
    ns.tools.db.get_session = lambda project_id: (calls.append(project_id), real(project_id))[1]
    resolved = mock.Mock()
    try:
        original = ns.mandate_admission.resolve_trigger_principal
        ns.mandate_admission.resolve_trigger_principal = resolved
        version, session = tick(ns)
    finally:
        ns.mandate_admission.resolve_trigger_principal = original
        ns.tools.db.get_session = real
    [predict] = ns.state.predicts
    assert predict == {
        "sid": None, "sio_event": "chat_predict", "chat_project_id": 1,
        "data": {"project_id": 1, "application_id": 7, "version_id": 70, "user_input": "",
                 "chat_history": [], "message_id": "response-1", "stream_id": "conv-uuid", "conversation_id": "conv-uuid"},
        "await_task_timeout": -1, "user_id": CREATOR, "is_system_user": True, "non_interactive": True,
        "trigger_source": "scheduled", "auto_approve_sensitive_actions": False}
    assert calls == [1] and resolved.call_count == 0   # only the application-name read of today's tick
    assert ns.state.conversations[0]["user_id"] == CREATOR and ns.state.events == [] and session.commits == 1


def test_flag_off_the_pipeline_helper_adds_the_ticket_only_when_there_is_one(ns, env, agent):
    arm(ns, env, governed=False)
    send = ns.execution.execute_pipeline_via_predict_sio
    common = dict(project_id=1, version=FakeVersion(), user_id=CREATOR, conversation_uuid="c", response_message_id="m")
    send(**common)
    send(**common, mandate_ticket=None)
    send(**common, mandate_ticket="ticket")
    plain, explicit_none, ticketed = ns.state.predicts[-3:]
    assert plain == explicit_none and "mandate_ticket" not in plain
    assert ticketed == {**plain, "mandate_ticket": "ticket"}


# The governed deadline sweep runs in the schedule tick: tests/unit/rpc/test_7011_entity_schedules_check.py


# --- the webhook API ----------------------------------------------------------------------------

@pytest.fixture
def webhook(ns, env, monkeypatch):
    """The real ``api/v2/webhook.py`` with the web framework stubbed; ``post`` is called for real."""
    api = types.SimpleNamespace(APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func,
                                with_modes=lambda urls: urls)
    monkeypatch.setitem(sys.modules, "flask", types.SimpleNamespace(
        request=None, has_request_context=lambda: False, g=types.SimpleNamespace()))
    monkeypatch.setitem(sys.modules, "pydantic.v1", types.SimpleNamespace(ValidationError=type("ValidationError", (Exception,), {})))
    monkeypatch.setattr(ns.tools, "api_tools", api, raising=False)
    monkeypatch.setattr(ns.tools, "register_openapi", lambda **kwargs: (lambda func: func), raising=False)
    for name, attrs in {
        f"{PKG}.utils.constants": {"PROMPT_LIB_MODE": "prompt_lib"},
        f"{PKG}.utils.folder_access": {"require_folder_access": lambda *a, **k: (lambda func: func),
                                       "APPLICATION_ENTITY_TYPES": ()},
        f"{PKG}.utils.pipeline_trigger": {
            "WEBHOOK_TYPE_CONFIG": {"github": {"signature_header": "X-Hub-Signature-256", "signing_id_header": "x"}},
            "normalize_secret_value": lambda s: s, "validate_webhook_secret": None,
            "get_webhook_creator_id": lambda trigger, application: trigger.get("created_by") or 1,
            "get_webhook_signature_header": lambda *a: "X-Hub-Signature-256", "is_gitlab_signing_mode": lambda *a: False},
    }.items():
        monkeypatch.setitem(sys.modules, name, stub(name, **attrs))
    monkeypatch.setitem(sys.modules, f"{PKG}.rpc.pipeline_webhook", ns.webhook_rpc)
    module = load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / "webhook.py", f"{PKG}.api.v2.webhook")
    version = FakeVersion()
    version.pipeline_settings = {"trigger": {"type": "webhook", "webhook_type": "github", "created_by": CREATOR}}
    handler = module.WebHookAPI()
    state = types.SimpleNamespace(valid=True, signature_checks=0)

    def validate(project_id, version_id, webhook_type, raw_data):
        state.signature_checks += 1
        if not state.valid:
            return False, ({"error": "Signature verification failed"}, 400)
        return True, (version, types.SimpleNamespace(owner_id=9))

    handler._validate_webhook_signature = validate   # pylint: disable=W0212
    module.request = types.SimpleNamespace(data=b'{"action": "opened"}', headers={})
    module.handler, module.version, module.state = handler, version, state
    return module


def post(webhook):
    return webhook.handler.post(project_id=1, version_id=70, webhook_type="github")


def test_a_webhook_with_a_good_signature_runs_the_pipeline_as_the_mandate_owner(ns, env, agent, webhook):
    arm(ns, env)
    result, status = post(webhook)
    assert status == 200 and result["status"] == "started" and result["task_id"]
    [predict] = ns.state.predicts
    assert predict["user_id"] == OWNER and isinstance(predict["mandate_ticket"], str) and predict["trigger_source"] == "webhook"
    [row] = contexts(env, ns)
    assert (row.entry_point, row.caller["ref"], row.caller["user_id"]) == ("webhook", "webhook:version:70", OWNER)


def test_a_bad_signature_mints_nothing_and_runs_nothing(ns, env, agent, webhook):
    module = arm(ns, env)
    webhook.state.valid = False
    result, status = post(webhook)
    assert status == 400 and ns.state.predicts == [] and ns.state.conversations == []
    assert len(module._mandate_tickets) == 0   # pylint: disable=W0212


@pytest.mark.parametrize("name, change, code", [
    ("no_mandate", None, "MANDATE_REQUIRED"),
    ("suspended", {"state": "suspended", "suspended_reason": "MANUAL"}, "MANDATE_INACTIVE"),
    ("schedule_only", {"allowed_operations": ["run:schedule"]}, "DENIED_OPERATION"),
    ("overdue", {"valid_until": NOW - timedelta(seconds=1)}, "MANDATE_EXPIRED"),
])
def test_a_refused_webhook_answers_403_with_the_fixed_code_and_text_and_closes_its_run(ns, env, agent, webhook, name, change, code):
    arm(ns, env)
    if change is None:
        with ns.sa_orm.Session(env.engine(1)) as session:
            session.execute(ns.sa.delete(ns.Mandate))
            session.commit()
    else:
        set_mandate(env, ns, agent.mandate, **change)
    result, status = post(webhook)
    assert status == 403 and result == {"code": code, "error": ns.adm.MESSAGES[code]}
    [(event_name, payload)] = ns.state.events
    assert event_name == "chat_message_stream_end" and payload["response_metadata"]["code"] == code
    assert contexts(env, ns) == [] and env.denied[-1]["reason_code"] == code


@pytest.mark.parametrize("name, change, code", [
    ("audit_down", lambda ns, env, agent: setattr(env, "audit_allowed", False), "AUDIT_UNAVAILABLE"),
    ("identity_down", lambda ns, env, agent: env.epochs.pop(OWNER), "DEPENDENCY_UNAVAILABLE"),
])
def test_a_webhook_the_platform_could_not_decide_answers_503_so_the_sender_retries(ns, env, agent, webhook, name, change, code):
    arm(ns, env)
    change(ns, env, agent)
    result, status = post(webhook)
    assert status == 503 and result == {"code": code, "error": ns.adm.MESSAGES[code]}
    assert contexts(env, ns) == []


def test_every_other_refusal_stays_403(ns, env):
    status = ns.mandate_admission.refusal_http_status
    assert {status(code) for code in ns.adm.MESSAGES if code not in ("DEPENDENCY_UNAVAILABLE", "AUDIT_UNAVAILABLE")} == {403}
    assert status("DEPENDENCY_UNAVAILABLE") == status("AUDIT_UNAVAILABLE") == 503
    assert status("SOMETHING_NEW") == 403 and status(None) == 403


@pytest.mark.parametrize("code, expected", [
    ("AUDIT_UNAVAILABLE", 503), ("DEPENDENCY_UNAVAILABLE", 503), ("MANDATE_REQUIRED", 403), ("MANDATE_INACTIVE", 403),
    ("DENIED_OPERATION", 403), ("DENIED_BINDING", 403),
])
def test_the_agent_webhook_refusal_uses_the_same_status_mapping(ns, env, webhook, code, expected):
    webhook.version.agent_type = "agent"   # not a pipeline: the do_predict route
    webhook.handler.module = types.SimpleNamespace(do_predict=mock.Mock(
        side_effect=ns.exceptions.AdmissionDenied(code, ns.adm.MESSAGES[code])))
    result, status = post(webhook)
    assert status == expected and result == {"code": code, "error": ns.adm.MESSAGES[code]}


def test_flag_off_a_webhook_runs_as_before_with_no_ticket(ns, env, agent, webhook):
    arm(ns, env, governed=False)
    result, status = post(webhook)
    assert status == 200 and result["status"] == "started"
    [predict] = ns.state.predicts
    assert predict["user_id"] == CREATOR and "mandate_ticket" not in predict and predict["trigger_source"] == "webhook"
    assert ns.state.conversations[0]["user_id"] == CREATOR


def test_a_governed_webhook_with_no_signature_check_cannot_reach_a_mandate(ns, env):
    text = source_of("api/v2/webhook.py")
    assert text.index("self._validate_webhook_signature(") < text.index("prepare_trigger_run(")
    assert text.count("prepare_trigger_run(") == 1


# --- the agent webhook (do_predict) -------------------------------------------------------------

class SessionProxy:
    """A real SQLite session whose legacy ``query`` answers with the agent version row of the test."""

    def __init__(self, real, version):
        self._real, self._version = real, version

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self._real.__exit__(*exc)

    def query(self, *args):
        chain = mock.MagicMock()
        chain.options.return_value.get.return_value = self._version
        return chain

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.fixture
def do_predict(ns, env, monkeypatch):
    """The real ``methods/predict.py`` with the request/payload machinery mocked."""
    heavy = {name: mock.MagicMock() for name in (
        "opentelemetry", "opentelemetry.trace", "sqlalchemy.orm", f"{PKG}.models.pd.chat", f"{PKG}.utils.predict_utils",
        f"{PKG}.utils.budget_door", f"{PKG}.utils.vectorstore")}
    for name, module in heavy.items():
        monkeypatch.setitem(sys.modules, name, module)
    sys.modules["opentelemetry"].trace = sys.modules["opentelemetry.trace"]
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm", sys.modules["sqlalchemy.orm"])
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.utils", stub(f"{PKG}.utils.utils", verify_signature=lambda *a: None))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.input_guardrail", stub(
        f"{PKG}.utils.input_guardrail", inspect_and_unsecret=lambda payload, vault, *a: vault.unsecret(payload)))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.sio_utils", stub(f"{PKG}.utils.sio_utils", SioEvents=mock.MagicMock()))
    monkeypatch.setattr(sys.modules[f"{PKG}.models.all"].ApplicationVersion, "application", mock.MagicMock(), raising=False)
    monkeypatch.setattr(ns.tools, "serialize", lambda value: value, raising=False)
    monkeypatch.setattr(ns.tools, "VaultClient", lambda *a, **k: mock.MagicMock(), raising=False)
    web = types.SimpleNamespace(method=lambda *a, **k: (lambda func: func))
    sys.modules["pylon.core.tools"].web = web
    module = load_module_with_stubs(PLUGIN_ROOT / "methods" / "predict.py", f"{PKG}.methods.predict")
    return module


def run_do_predict(ns, env, do_predict, governed=True, signature="sig", version=None, user_id=9):
    tools = ns.tools
    module = arm(ns, env, governed=governed)
    module.task_node = mock.MagicMock()
    module.task_node.start_task.return_value = "task-1"
    version = version or types.SimpleNamespace(
        id=70, application_id=7, llm_settings={"model_name": "m"}, agent_type="agent", instructions="x",
        application=types.SimpleNamespace(name="agent", webhook_secret="secret"))
    real = env.get_session

    def get_session(project_id):
        return _Scope(real(project_id), version)

    class _Scope:
        def __init__(self, scope, version_):
            self.scope, self.version = scope, version_

        def __enter__(self):
            return SessionProxy(self.scope.__enter__(), self.version)

        def __exit__(self, *exc):
            return self.scope.__exit__(*exc)

    tools.db.get_session = get_session
    try:
        result = do_predict.Method.do_predict(
            module, project_id=1, user_id=user_id, version_id=70, payload_in={"chat_history": [], "user_input": "{}"},
            raw=b"{}", webhook_signature=signature, webhook_type="github")
    finally:
        tools.db.get_session = real
    return result, module.task_node.start_task.call_args.kwargs


def test_a_governed_agent_webhook_with_a_mandate_runs_as_the_owner_with_a_mandate_request(ns, env, agent, do_predict):
    do_predict_module = do_predict
    result, kwargs = run_do_predict(ns, env, do_predict_module)
    assert result["task_id"] == "task-1"
    admission = kwargs["admission"]
    assert (admission.run_kind, admission.entry_point, admission.caller_type, admission.trigger_ref) == (
        "mandate", "webhook", "trigger", "webhook:version:70")
    assert admission.mandate_ref == str(agent.mandate) and (admission.project_id, admission.binding_project_id) == (1, 1)
    assert (admission.application_id, admission.version_id) == (7, 70)
    sys.modules[f"{PKG}.utils.predict_utils"].generate_predict_payload.assert_called_once()
    assert sys.modules[f"{PKG}.utils.predict_utils"].generate_predict_payload.call_args.kwargs["user_id"] == OWNER


def test_a_governed_agent_webhook_without_a_mandate_stays_a_system_run(ns, env, do_predict):
    env.seed_agent()
    _, kwargs = run_do_predict(ns, env, do_predict)
    admission = kwargs["admission"]
    assert (admission.run_kind, admission.caller_type, admission.mandate_ref) == ("system", "public", None)
    assert sys.modules[f"{PKG}.utils.predict_utils"].generate_predict_payload.call_args.kwargs["user_id"] == 9


def test_an_inactive_mandate_still_makes_a_mandate_request_so_admission_can_say_why(ns, env, agent, do_predict):
    set_mandate(env, ns, agent.mandate, state="suspended", suspended_reason="MANUAL")
    _, kwargs = run_do_predict(ns, env, do_predict)
    assert kwargs["admission"].run_kind == "mandate"


def test_the_api_call_with_no_signature_is_never_a_mandate_run(ns, env, agent, do_predict):
    _, kwargs = run_do_predict(ns, env, do_predict, signature=None)
    assert kwargs["admission"].run_kind == "interactive"
    assert sys.modules[f"{PKG}.utils.predict_utils"].generate_predict_payload.call_args.kwargs["user_id"] == 9


def test_flag_off_the_agent_webhook_reads_no_mandate_and_sends_no_admission(ns, env, agent, do_predict, monkeypatch):
    resolved = mock.Mock()
    monkeypatch.setattr(do_predict, "resolve_trigger_principal", resolved)
    _, kwargs = run_do_predict(ns, env, do_predict, governed=False)
    assert kwargs["admission"] is None and resolved.call_count == 0
    assert sys.modules[f"{PKG}.utils.predict_utils"].generate_predict_payload.call_args.kwargs["user_id"] == 9


# --- predict_sio itself -------------------------------------------------------------------------

def lift(relative, name, **names):
    """A function lifted out of a heavy module by its AST, run against a namespace of mocks for every name it uses."""
    tree = ast.parse(source_of(relative))
    function = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)
    glob = {used.id: mock.MagicMock() for used in ast.walk(function)
            if isinstance(used, ast.Name) and not hasattr(builtins, used.id)}
    glob.update(names)
    function.decorator_list = []
    exec(compile(ast.Module([function], []), relative, "exec"), glob)   # pylint: disable=W0122
    return glob[name], glob


@pytest.fixture
def predict_sio(ns, env):
    function, glob = lift("rpc/application.py", "predict_sio",
                          take_trigger_admission=ns.mandate_admission.take_trigger_admission,
                          admission_request_from=ns.pd_admission.admission_request_from,
                          AdmissionDenied=ns.exceptions.AdmissionDenied, SioEvents=types.SimpleNamespace(
                              application_predict="application_predict", chat_predict=types.SimpleNamespace(value="chat_predict")),
                          log=ns.log)
    return function, glob


def new_self(ns, env, governed):
    module = env.module(governed=governed)
    module.task_node = mock.MagicMock()
    module.task_node.start_task.return_value = "task-1"
    module.context = mock.MagicMock()
    module.stream_response = mock.MagicMock()
    return module


def sio_data(message="m-1"):
    return {"project_id": 1, "application_id": 7, "version_id": 70, "message_id": message, "stream_id": "s-1"}


def wire(glob, version_id=70):
    """The loaded version row and the request that merges with it, as the function reads them."""
    parsed = mock.MagicMock(project_id=1, version_id=version_id, application_id=7, version_details=None,
                            stream_id="s-1", message_id="m-1")
    glob["ApplicationChatRequest"].model_validate.return_value = parsed
    glob["ApplicationChatRequest"].from_orm.return_value.merge_update.return_value = parsed
    row = types.SimpleNamespace(id=version_id, application_id=7, llm_settings={"model_name": "m"},
                                application=types.SimpleNamespace(name="agent"))
    glob["db"].get_session.return_value.__enter__.return_value.query.return_value.get.return_value = row
    glob["auth"].sio_users = {}
    return parsed


def run_sio(function, module, **kwargs):
    function(module, sid=None, data=sio_data(), user_id=5, is_system_user=True, **kwargs)
    return module.task_node.start_task.call_args


def test_flag_off_predict_sio_calls_start_task_identically_with_and_without_mandate_ticket(ns, env, predict_sio):
    function, glob = predict_sio
    wire(glob)
    module = new_self(ns, env, governed=False)
    module._mandate_tickets = mock.Mock(spec=[])   # any use fails   # pylint: disable=W0212
    plain = run_sio(function, module)
    explicit = run_sio(function, module, mandate_ticket=None)
    assert plain == explicit and plain.kwargs["admission"] is None
    assert module.task_node.start_task.call_count == 2


def test_governed_predict_sio_takes_the_ticket_for_the_row_it_loaded(ns, env, agent, predict_sio):
    function, glob = predict_sio
    wire(glob)
    module = new_self(ns, env, governed=True)
    request = a_request(ns, env, agent.mandate)
    ticket = ns.mandate_admission.mint_trigger_admission(module, request)
    assert run_sio(function, module, mandate_ticket=ticket).kwargs["admission"] is request
    replay = run_sio(function, module, mandate_ticket=ticket).kwargs["admission"]
    assert replay is not request and (replay.run_kind, replay.caller_type) == ("system", "public")


@pytest.mark.parametrize("ticket", [None, "forged"])
def test_governed_predict_sio_without_a_valid_ticket_builds_the_ordinary_system_request(ns, env, predict_sio, ticket):
    function, glob = predict_sio
    wire(glob)
    module = new_self(ns, env, governed=True)
    admission = run_sio(function, module, mandate_ticket=ticket).kwargs["admission"]
    assert (admission.run_kind, admission.caller_type, admission.mandate_ref, admission.version_id) == (
        "system", "public", None, 70)


def test_a_ticket_for_another_version_is_not_taken_by_predict_sio(ns, env, agent, predict_sio):
    function, glob = predict_sio
    wire(glob, version_id=71)
    module = new_self(ns, env, governed=True)
    ticket = ns.mandate_admission.mint_trigger_admission(module, a_request(ns, env, agent.mandate))
    admission = run_sio(function, module, mandate_ticket=ticket).kwargs["admission"]
    assert admission.run_kind == "system" and admission.version_id == 71


def test_predict_sio_takes_the_ticket_after_it_loaded_the_row_and_passes_nothing_from_the_payload():
    text = source_of("rpc/application.py")
    chat = text[text.index("def predict_sio("):text.index("def predict_sio_llm(")]
    assert "mandate_ticket: Optional[str] = None," in chat
    assert chat.index("admission_version_row = application_version") < chat.index("take_trigger_admission(")
    call = chat[chat.index("take_trigger_admission("):chat.index("admission=admission,")]
    assert "version_id=getattr(admission_version_row, \"id\", None)" in call and "parsed.project_id" in call
    assert "mandate_ticket" not in text[text.index("def predict_sio_llm("):]


# --- scheduled indexing -------------------------------------------------------------------------

def test_a_team_schedule_is_refused_as_mandate_required_and_a_per_user_one_as_authorization_required(ns, env):
    module = arm(ns, env)
    team = ns.trigger_principal.principal_of("index_schedule", schedule_key="-1")
    user = ns.trigger_principal.principal_of("index_schedule", schedule_key="5")
    assert ns.adm.index_schedule_refusal_code(team) == "MANDATE_REQUIRED"
    assert ns.adm.index_schedule_refusal_code(user) == "AUTHORIZATION_REQUIRED"
    assert env.denied == []   # deciding the code records nothing
    for code in ("MANDATE_REQUIRED", "AUTHORIZATION_REQUIRED"):
        ns.adm.record_index_schedule_refusal(module, code, project_id=1, toolkit_id=12)
    assert [(event["reason_code"], event["caller_ref"], event["project_id"], event["decision"], event["event_type"])
            for event in env.denied] == [("MANDATE_REQUIRED", "schedule:index:12", 1, "deny", "admission"),
                                         ("AUTHORIZATION_REQUIRED", "schedule:index:12", 1, "deny", "admission")]
    assert "security_metric name=admission_decision_total decision=deny reason_code=AUTHORIZATION_REQUIRED" in env.log.lines


@pytest.fixture
def index_tick(ns, env, monkeypatch):
    """The real ``rpc/index_scheduling.py``, its imports mocked; one toolkit with one due schedule."""
    names = ("models.elitea_tools", "models.indexer", "models.enums", "models.enums.indexer", "models.pd.index",
             "utils.application_tools", "utils.index_scheduling", "utils.predict_utils")
    for name in names:
        monkeypatch.setitem(sys.modules, f"{PKG}.{name}", mock.MagicMock(name=name))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.cron_utils", stub(f"{PKG}.utils.cron_utils", is_cron_due=lambda *a: True))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.utils", stub(
        f"{PKG}.utils.utils", make_yield_to_hub=lambda runtime: (lambda: None), end_ambient_transaction=lambda: None))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.maintenance_gate", stub(
        f"{PKG}.utils.maintenance_gate", is_maintenance_active=lambda: False))
    web = types.SimpleNamespace(rpc=lambda *a, **k: (lambda func: func))
    monkeypatch.setattr(sys.modules["pylon.core.tools"], "web", web, raising=False)
    monkeypatch.setattr(ns.tools, "VaultClient", mock.MagicMock(), raising=False)
    module = load_module_with_stubs(PLUGIN_ROOT / "rpc" / "index_scheduling.py", f"{PKG}.rpc.index_scheduling")
    helpers = sys.modules[f"{PKG}.utils.index_scheduling"]
    tools_module = sys.modules[f"{PKG}.utils.application_tools"]
    pd_index = sys.modules[f"{PKG}.models.pd.index"]
    pd_index.MISSING_SINCE_KEY = "missing_since"
    pd_index.ToolkitIndexingSchedule.parse_obj.side_effect = lambda body: types.SimpleNamespace(
        created_by=body["created_by"], enabled=True, cron="* * * * *", last_run=None, timezone="UTC", retry_since=None)
    helpers.handle_schedule_expiry.return_value = False
    helpers.handle_failed_index_schedule.return_value = True
    helpers.index_log_context.return_value = "[idx]"
    helpers.resolve_credentials.side_effect = AssertionError("credentials were resolved")
    predict_utils = sys.modules[f"{PKG}.utils.predict_utils"]
    predict_utils.get_system_user_token.side_effect = AssertionError("the system token was fetched")
    tools_module.start_index_task.side_effect = AssertionError("an index task was started")
    toolkit = types.SimpleNamespace(id=12, type="github", settings={"pgvector_configuration": {}},
                                    meta={"indexes_meta": {"idx": {"schedules": {}}}})
    session = mock.MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [toolkit]
    scope = mock.MagicMock()
    scope.__enter__.return_value = session
    ns.tools.db.get_session = lambda project_id: scope
    ns.tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
        timeout=lambda seconds: types.SimpleNamespace(project_list=lambda filter_=None: [{"id": 1}]))))
    monkeypatch.setattr(module, "rpc_tools", ns.tools.rpc_tools)
    return types.SimpleNamespace(module=module, helpers=helpers, tools_module=tools_module, predict_utils=predict_utils,
                                 toolkit=toolkit, session=session)


def run_index_tick(ns, env, index_tick, schedules, governed=True):
    module = arm(ns, env, governed=governed)
    module.context = types.SimpleNamespace(web_runtime=None)
    index_tick.toolkit.meta = {"indexes_meta": {"idx": {"schedules": schedules}}}
    index_tick.module.RPC.check_index_scheduling(module)
    return module


@pytest.mark.parametrize("key, code", [("-1", "MANDATE_REQUIRED"), ("5", "AUTHORIZATION_REQUIRED")])
def test_a_governed_index_tick_refuses_the_schedule_before_any_credential_or_token_is_touched(ns, env, index_tick, key, code):
    run_index_tick(ns, env, index_tick, {key: {"created_by": CREATOR}})
    helpers = index_tick.helpers
    [call] = helpers.handle_failed_index_schedule.call_args_list
    assert call.args[5] == ns.adm.MESSAGES[code] and call.args[2] == CREATOR and call.args[3] is index_tick.toolkit
    assert call.kwargs == {"expand_user_id": CREATOR}
    helpers.stamp_schedule_last_run.assert_called_once()
    assert helpers.resolve_credentials.call_count == 0
    assert index_tick.predict_utils.get_system_user_token.call_count == 0
    assert index_tick.tools_module.start_index_task.call_count == 0
    [event] = env.denied
    assert (event["reason_code"], event["caller_ref"]) == (code, "schedule:index:12")


def test_a_refusal_that_concluded_nothing_leaves_the_slot_open(ns, env, index_tick):
    index_tick.helpers.handle_failed_index_schedule.return_value = False   # a live run holds the row: retry next scan
    run_index_tick(ns, env, index_tick, {"-1": {"created_by": CREATOR}})
    assert index_tick.helpers.stamp_schedule_last_run.call_count == 0
    assert env.denied == []   # not concluded: no deny per minute for a slot that stays due
    assert "reason_code=MANDATE_REQUIRED" not in " ".join(env.log.lines)
    assert index_tick.tools_module.start_index_task.call_count == 0


def test_a_failure_while_recording_the_refusal_never_breaks_the_tick_or_dispatches(ns, env, index_tick):
    index_tick.helpers.handle_failed_index_schedule.side_effect = RuntimeError("pgvector down")
    run_index_tick(ns, env, index_tick, {"-1": {"created_by": CREATOR}, "5": {"created_by": CREATOR}})
    assert index_tick.helpers.handle_failed_index_schedule.call_count == 2   # the second schedule was still reached
    assert index_tick.tools_module.start_index_task.call_count == 0


def test_flag_off_the_index_tick_goes_on_to_resolve_credentials_as_today(ns, env, index_tick):
    run_index_tick(ns, env, index_tick, {"-1": {"created_by": CREATOR}}, governed=False)
    assert index_tick.helpers.resolve_credentials.call_count == 1   # the old path (its sentinel failure is the tick's own catch)
    assert index_tick.helpers.handle_failed_index_schedule.call_count == 0 and env.denied == []


def test_the_governed_branch_sits_right_after_the_due_count_and_before_any_resolution():
    text = source_of("rpc/index_scheduling.py")
    due, governed = text.index("stats['due'] += 1"), text.index("if is_governed(self):")
    assert due < governed < text.index("resolve_credentials(") < text.index("get_system_user_token(project_id)")
    assert text.count("is_governed(self)") == 1


# --- the start_task wrapper and the owner event module ------------------------------------------

def build_wrapper(ns, env, module, original):
    """The real ``_maintenance_gated_start_task`` source from module.py, run against the real gates."""
    source = source_of("module.py")
    text = source[source.index("def _maintenance_gated_start_task"):source.index("self.task_node.start_task = _maintenance_gated_start_task")]
    text = textwrap.dedent("        " + text).replace("from .utils.", f"from {PKG}.utils.")
    gate = types.ModuleType(f"{PKG}.utils.maintenance_gate")
    gate.is_maintenance_active = lambda: False
    budget = types.ModuleType(f"{PKG}.utils.budget_door")
    budget.closed_budget_scope = lambda *a, **k: None
    budget.dispatch_may_use_llm = lambda kwargs: False
    budget.dispatch_owner = lambda kwargs: (None, None)
    budget.dispatch_uses_own_model = lambda *a, **k: False
    guardrail = types.ModuleType(f"{PKG}.utils.input_guardrail")
    guardrail.apply_input_guardrail = lambda kwargs: None   # inert, as with input_guardrail.enabled false (#6886)
    sys.modules[gate.__name__], sys.modules[budget.__name__] = gate, budget
    sys.modules[guardrail.__name__] = guardrail
    module.worker_capabilities = types.SimpleNamespace(has_capable_worker=lambda required: True)
    namespace = {
        "self": module, "log": ns.log, "_original_start_task": original,
        "apply_envelope_gate": ns.caps.apply_envelope_gate, "stamp_predict_run_id": ns.run_id.stamp_predict_run_id,
        "admit_dispatch": ns.adm.admit_dispatch, "track_context": ns.adm.track_context,
    }
    exec(compile(text, "module.py", "exec"), namespace)  # pylint: disable=W0122
    return namespace["_maintenance_gated_start_task"]


class Original:
    def __init__(self, task_id="task-1"):
        self.calls, self.task_id = [], task_id

    def __call__(self, *args, **kwargs):
        self.calls.append((args, copy.deepcopy(kwargs)))
        return self.task_id


def mandate_dispatch():
    return ("indexer_agent",), {"args": ["thread", 5], "kwargs": {"project_id": 1, "message_id": 5},
                                "pool": "agents", "meta": {"project_id": 1, "task_name": "indexer_agent"}}


def test_the_wrapper_tracks_a_mandate_run_with_its_deadline_and_never_sends_the_deadline_to_the_worker(ns, env, agent):
    module, original = arm(ns, env), Original("task-9")
    wrapper = build_wrapper(ns, env, module, original)
    args, kwargs = mandate_dispatch()
    assert wrapper(*args, admission=a_request(ns, env, agent.mandate), **kwargs) == "task-9"
    [(_, sent)] = original.calls
    [row] = contexts(env, ns)
    assert "context_deadline" not in sent["meta"] and "admission" not in sent
    assert sent["kwargs"]["envelope"]["context_ref"] == str(row.id) == sent["meta"]["context_ref"]
    assert module._context_by_task == OrderedDict({"task-9": (1, str(row.id), ns.adm.RunDeadline(
        NOW + timedelta(seconds=600), "RUNTIME_LIMIT_EXCEEDED"))})   # pylint: disable=W0212
    # and the sweep stops it once the deadline passes
    stopped = []
    module.task_node = types.SimpleNamespace(stop_task=stopped.append)
    assert ns.adm.enforce_run_deadlines(module, now=NOW + timedelta(seconds=601)) == 1 and stopped == ["task-9"]


def test_the_wrapper_tracks_an_interactive_run_without_a_deadline_and_a_refused_mandate_run_not_at_all(ns, env, agent):
    module, original = arm(ns, env), Original("task-1")
    wrapper = build_wrapper(ns, env, module, original)
    args, kwargs = mandate_dispatch()
    set_mandate(env, ns, agent.mandate, state="revoked")
    with pytest.raises(ns.exceptions.AdmissionDenied):
        wrapper(*args, admission=a_request(ns, env, agent.mandate), **kwargs)
    assert original.calls == [] and module._context_by_task == OrderedDict()   # pylint: disable=W0212


def test_flag_off_the_wrapper_output_is_unchanged_by_the_deadline_handling(ns, env):
    outputs = []
    for admission in (None, env.request()):
        module, original = arm(ns, env, governed=False), Original()
        wrapper = build_wrapper(ns, env, module, original)
        args, kwargs = mandate_dispatch()
        env.poison()
        assert wrapper(*args, admission=admission, **kwargs) == "task-1"
        env.unpoison()
        outputs.append(original.calls)
    assert outputs[0] == outputs[1] and json.dumps(outputs[0], sort_keys=True) == json.dumps(outputs[1], sort_keys=True)
    assert "context_deadline" not in outputs[0][0][1]["meta"]


def test_the_module_keeps_a_bounded_ticket_store_and_registers_the_owner_event_handler():
    text = source_of("module.py")
    assert "self._mandate_tickets = TicketStore()" in text
    event = source_of("events/mandate_owner.py")
    assert '@web.event("auth_invalidate")' in event and "on_auth_invalidate(self, payload)" in event
    assert event.count("def ") == 1 and "class Event:" in event
