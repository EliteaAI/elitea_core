"""#7228 follow-up 5: a participant that cannot run is left out of the turn, not the whole chat.

Under enforce a plain chat's participants (agents and pipelines) are prefetched and admitted with the run, each on its
own decision (#7227). One that would be refused (no active binding while it carries a credential, or a refused
credential) is removed from this turn's tools and prefetch, the model is told in one line (its name and the fixed
sentence of its code), and one ``SUBAGENT_SKIPPED`` event is audited. A directly addressed agent and an agent's own
configured sub-agents refuse as before; off sends the payload untouched. Real models and statements on SQLite
(``fixtures/admission_env.py``).
"""
import copy
import sys
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import Credential, call_kwargs, dumped  # noqa: E402  pylint: disable=C0413

LLM_TASK = "indexer_predict_agent"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("participant_skipped"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.facts_version = 5
    return ns.env


def participant(application_id, version_id, participant_id, name, agent_type="openai"):
    """An application participant as ``generate_toolkit_payload`` injects it."""
    return {"type": "application", "name": name, "participant_id": participant_id, "agent_type": agent_type,
            "settings": {"application_id": application_id, "application_version_id": version_id}}


def configured(application_id, version_id):
    """An agent's own configured sub-agent, as its version details carry it (no participant mark)."""
    return {"id": 900 + version_id, "type": "application", "name": f"own {version_id}",
            "settings": {"application_id": application_id, "application_version_id": version_id}}


def prefetched(payload, key, name, *tools):
    payload["application"].setdefault("subagent_version_details", {})[key] = {
        "name": name, "description": "", "version_details": {"tools": list(tools)}}
    return payload


class Chat:  # pylint: disable=R0903
    """A plain chat with participants: ``healthy`` (9:90, bound, its credential granted), ``suspended`` (8:80, its
    binding suspended, a credential), ``inactive`` (6:60, bound, its credential retired), ``pipeline`` (5:50, a pipeline
    with no binding and a credential)."""

    def __init__(self, env, *names):
        self.payload = {"tools": [], "application": {"instructions": "be brief"}}
        for name in names:
            getattr(self, name)(env)

    def add(self, application_id, version_id, name, tool, agent_type="openai"):
        self.payload["tools"].append(participant(application_id, version_id, 100 + version_id, name, agent_type))
        prefetched(self.payload, f"{application_id}:{version_id}", name, tool)

    def healthy(self, env):
        env.add_version(1, 90, 9)
        self.binding = env.add_binding(1, 9, 90)
        self.good = Credential(env, self.binding, tool_id=3, config_id=7, version_id=90)
        self.add(9, 90, "helper-agent", self.good.tool())

    def suspended(self, env):
        env.add_version(1, 80, 8)
        env.add_binding(1, 8, 80, state="suspended")
        unbound = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=80)
        self.add(8, 80, "p7269-admin-agent", unbound.tool())

    def inactive(self, env):
        env.add_version(1, 60, 6)
        binding = env.add_binding(1, 6, 60)
        self.retired = Credential(env, binding, tool_id=4, config_id=8, version_id=60, ref_state="suspended")
        self.add(6, 60, "retired-agent", self.retired.tool())

    def pipeline(self, env):
        env.add_version(1, 50, 5)
        env.digests[50] = "adv1:" + "5" * 64   # a version with no binding still has its digest
        unbound = Credential(env, uuid.uuid4(), tool_id=1, config_id=5, version_id=50)
        self.add(5, 50, "nightly-pipeline", unbound.tool(), agent_type="pipeline")


def plain_chat(env, ns, payload, module_=None):
    call = call_kwargs(payload)
    ref = ns.adm.admit_dispatch(module_ or env.module(), LLM_TASK, call, env.request(
        entry_point="chat_sio_llm", application_id=None, version_id=None))
    return call, ref


def contexts(env, ns):
    return sorted(env.query(1, ns.models.ExecutionSecurityContext), key=lambda row: row.attempt)


def skips(env):
    return [event for event in env.denied if event["reason_code"] == "SUBAGENT_SKIPPED"]


def tool_keys(call):
    return [key for key in map(application_key, call["kwargs"]["tools"]) if key]


def application_key(tool):
    settings = tool.get("settings") if tool.get("type") == "application" else None
    return f"{settings['application_id']}:{settings['application_version_id']}" if settings else None


def prefetch_keys(call):
    return sorted(call["kwargs"]["application"].get("subagent_version_details") or {})


def told(call):
    return call["kwargs"]["application"]["instructions"]


# --- a plain chat turn ------------------------------------------------------------------------------------------------

def test_an_unavailable_participant_is_left_out_and_the_healthy_one_runs_on_its_own_decision(ns, env):
    chat = Chat(env, "suspended", "healthy")
    call, ref = plain_chat(env, ns, chat.payload)
    assert tool_keys(call) == ["9:90"] and prefetch_keys(call) == ["9:90"]
    root, child = contexts(env, ns)
    assert str(root.id) == ref and (child.application_id, child.version_id, child.agent_binding_ref) == (
        9, 90, chat.binding)
    assert child.parent_context_ref == root.id
    [sealed] = call["kwargs"]["application"]["subagent_version_details"]["9:90"]["version_details"]["tools"]
    slot = sealed["settings"]["credentials"]["credential_slot"]
    assert slot["decision_id"] == child.allowed_bindings[0]["decision_id"]
    [event] = skips(env)
    assert (event["event_type"], event["decision"], event["application_id"], event["target_ref"],
            event["operation_id"], event["run_id"]) == ("decision", "deny", 8, "version:80", "DENIED_BINDING", "run-1")
    assert "would_deny" not in event
    assert chat.good.secret not in dumped(call) + dumped(env.denied)


def test_the_model_is_told_with_the_name_and_the_fixed_sentence_only(ns, env):
    call, _ref = plain_chat(env, ns, Chat(env, "suspended", "healthy").payload)
    line = told(call).split("\n\n")[-1]
    assert told(call).startswith("be brief\n\n")
    assert line == ns.adm.UNAVAILABLE_LINE.format(f"“p7269-admin-agent” ({ns.adm.MESSAGES['DENIED_BINDING']})")
    assert "80" not in line and "helper-agent" not in line


def test_a_refused_credential_of_a_bound_participant_skips_it_with_its_own_code(ns, env):
    chat = Chat(env, "inactive", "healthy")
    call, _ref = plain_chat(env, ns, chat.payload)
    assert tool_keys(call) == ["9:90"] and prefetch_keys(call) == ["9:90"]
    [event] = skips(env)
    code = event["operation_id"]
    assert code == "CREDENTIAL_REFERENCE_INACTIVE" and event["credential_ref"] == chat.retired.ref_id
    assert f"“retired-agent” ({ns.adm.MESSAGES[code]})" in told(call)


def test_every_participant_unavailable_the_chat_still_runs_alone(ns, env):
    call, ref = plain_chat(env, ns, Chat(env, "suspended", "inactive").payload)
    assert ref and tool_keys(call) == [] and prefetch_keys(call) == []
    assert [row.parent_context_ref for row in contexts(env, ns)] == [None]
    assert [(e["application_id"], e["operation_id"]) for e in skips(env)] == [
        (8, "DENIED_BINDING"), (6, "CREDENTIAL_REFERENCE_INACTIVE")]
    assert "p7269-admin-agent" in told(call) and "retired-agent" in told(call)


def test_an_unavailable_pipeline_participant_is_skipped_too(ns, env):
    call, _ref = plain_chat(env, ns, Chat(env, "pipeline", "healthy").payload)
    assert tool_keys(call) == ["9:90"] and prefetch_keys(call) == ["9:90"]
    assert [(e["application_id"], e["operation_id"]) for e in skips(env)] == [(5, "DENIED_BINDING")]
    assert "“nightly-pipeline”" in told(call)


def test_a_participant_whose_own_sub_agent_is_refused_is_skipped_with_what_only_it_reaches(ns, env):
    chat = Chat(env, "healthy")
    env.add_version(1, 70, 7)
    env.add_binding(1, 7, 70)
    env.add_version(1, 80, 8)
    env.add_binding(1, 8, 80, state="suspended")
    nested = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=80)
    chat.add(7, 70, "container-agent", configured(8, 80))
    prefetched(chat.payload, "8:80", "nested-agent", nested.tool())
    call, _ref = plain_chat(env, ns, chat.payload)
    assert tool_keys(call) == ["9:90"] and prefetch_keys(call) == ["9:90"]
    assert [(e["application_id"], e["operation_id"]) for e in skips(env)] == [(7, "DENIED_BINDING")]
    assert [row.version_id for row in contexts(env, ns)] == [None, 90]


def test_a_participant_that_carries_no_credential_is_not_read_or_skipped(ns, env):
    env.add_version(1, 80, 8)   # no binding at all: nothing to decide, it runs as before
    payload = prefetched({"tools": [participant(8, 80, 180, "plain-agent")], "application": {"instructions": "x"}},
                         "8:80", "plain-agent", {"id": 2, "type": "memory", "settings": {"namespace": "n"}})
    before = copy.deepcopy(payload)
    call, _ref = plain_chat(env, ns, payload)
    assert tool_keys(call) == ["8:80"] and told(call) == "x" and skips(env) == []
    assert call["kwargs"]["application"] == before["application"]


def test_the_notice_never_carries_exception_text(ns, env):
    chat = Chat(env, "healthy")
    env.secrets_raises = RuntimeError("SELECT secret FROM vault WHERE id=7")
    call, _ref = plain_chat(env, ns, chat.payload)
    [event] = skips(env)
    assert event["operation_id"] == "DEPENDENCY_UNAVAILABLE" and tool_keys(call) == []
    assert ns.adm.MESSAGES["DEPENDENCY_UNAVAILABLE"] in told(call)
    assert "SELECT" not in told(call) + dumped(env.denied)


def test_the_participants_are_decided_in_one_batch_read(ns, env):
    def run(count):
        env.reset()
        env.facts_version = 5
        chat = Chat(env)
        for index in range(count):
            env.add_version(1, 80 + index, 8)
            env.add_binding(1, 8, 80 + index, state="suspended")
            chat.add(8, 80 + index, f"agent-{index}", Credential(
                env, uuid.uuid4(), tool_id=20 + index, config_id=60 + index, version_id=80 + index).tool())
        seen = []
        engine = env.engine(1)
        listener = lambda conn, cursor, statement, *rest: seen.append(statement)   # noqa: E731
        ns.sa.event.listen(engine, "before_cursor_execute", listener)
        try:
            call, _ref = plain_chat(env, ns, chat.payload)
        finally:
            ns.sa.event.remove(engine, "before_cursor_execute", listener)
        assert tool_keys(call) == [] and len(skips(env)) == count
        reads = [s for s in seen if s.lstrip().upper().startswith("SELECT")]
        return len(env.sessions), len(env.configuration_calls), len(env.secrets_calls), len(env.permission_calls), reads

    one, two, five = run(1), run(2), run(5)
    assert one[:4] == two[:4] == five[:4]   # sessions, facts, secrets, permissions: one batch
    assert one[1] == 1 and one[2] == 0
    per_participant = len(two[4]) - len(one[4])
    assert per_participant <= 3 and len(five[4]) - len(one[4]) == 4 * per_participant   # per-version rows only


# --- unchanged -------------------------------------------------------------------------------------------------------

def test_a_directly_addressed_unavailable_agent_is_still_refused(ns, env):
    env.add_version(1, 80, 8)
    env.add_binding(1, 8, 80, state="suspended")
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call_kwargs(
            {"application": {"version_details": {"tools": []}}}), env.request(application_id=8, version_id=80))
    assert err.value.reason_code == "DENIED_BINDING" and skips(env) == [] and contexts(env, ns) == []


def agent_chat(env, ns, chat, own_tools):
    """A bound agent (7:70) whose turn carries ``own_tools`` (configured) and the chat's injected siblings (swarm)."""
    env.add_version(1, 70, 7)
    env.add_binding(1, 7, 70)
    tools = [*own_tools, *chat.payload["tools"]]
    payload = {"tools": copy.deepcopy(tools), "application": {
        "version_details": {"instructions": "agent", "tools": tools},
        "subagent_version_details": chat.payload["application"]["subagent_version_details"]}}
    call = call_kwargs(payload)
    return call, ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())


def test_a_swarm_sibling_that_cannot_run_is_skipped_for_the_selected_agent(ns, env):
    call, ref = agent_chat(env, ns, Chat(env, "suspended", "healthy"), [])
    assert ref and tool_keys(call) == ["9:90"]
    details = call["kwargs"]["application"]["version_details"]
    assert [t["settings"]["application_id"] for t in details["tools"]] == [9]
    assert details["instructions"].startswith("agent\n\n") and "p7269-admin-agent" in details["instructions"]
    assert [e["operation_id"] for e in skips(env)] == ["DENIED_BINDING"]


def test_an_agents_own_configured_sub_agent_still_refuses_the_run(ns, env):
    chat = Chat(env, "inactive")
    own = configured(6, 60)
    chat.payload["tools"] = []   # configured only
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        agent_chat(env, ns, chat, [own])
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_INACTIVE" and skips(env) == []


def test_a_participant_that_is_also_configured_refuses_as_configured(ns, env):
    chat = Chat(env, "inactive")
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        agent_chat(env, ns, chat, [configured(6, 60)])
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_INACTIVE" and skips(env) == []


def test_off_reads_nothing_and_sends_the_payload_untouched(ns, env):
    payload = Chat(env, "suspended", "healthy").payload
    call = call_kwargs(copy.deepcopy(payload))
    before = copy.deepcopy(call)
    opened = len(env.sessions)
    assert ns.adm.admit_dispatch(env.module(governed=False), LLM_TASK, call, env.request(
        entry_point="chat_sio_llm", application_id=None, version_id=None)) is None
    assert call == before and len(env.sessions) == opened and env.denied == [] and env.recorded == []
    assert env.permission_calls == [] and env.configuration_calls == [] and env.secrets_calls == []


def test_audit_only_never_skips_and_records_a_would_skip(ns, env):
    call = call_kwargs(Chat(env, "suspended", "healthy").payload)
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(env.module(governed=False, audit=True), LLM_TASK, call, env.request(
        entry_point="chat_sio_llm", application_id=None, version_id=None)) is None
    assert call == before and contexts(env, ns) == []
    [event] = skips(env)
    assert (event["decision"], event["would_deny"], event["operation_id"]) == ("allow", True, "DENIED_BINDING")


def test_the_participant_mark_admission_reads_is_set_by_the_payload_builder():
    text = (PLUGIN_ROOT / "rpc" / "chat_all.py").read_text()
    builder = text[text.index("def generate_toolkit_payload("):text.index("def process_attachment_message_items(")]
    assert '"participant_id": app_participant.id' in builder
    llm = (PLUGIN_ROOT / "rpc" / "application.py").read_text()
    assert "prefetch_participants=is_governed(self)" in llm[llm.index("def predict_sio_llm("):]
