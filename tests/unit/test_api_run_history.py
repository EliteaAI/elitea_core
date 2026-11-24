"""Agent and pipeline runs started over REST predict or MCP get a Run History row.

Contract under test: utils/api_run_history.py (the opener) and its use in methods/predict.py `Method.do_predict`
(the binding and the cleanup), both loaded from source with their neighbours faked.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "plugins.elitea_core"
TRIGGERS = types.SimpleNamespace(API="api", MCP="mcp", WEBHOOK="webhook", CHAT="chat")


def _install(monkeypatch, name, **attrs):
    stub = types.ModuleType(name)
    stub.__path__ = []
    vars(stub).update(attrs)
    monkeypatch.setitem(sys.modules, name, stub)
    return stub


def _load(monkeypatch, dotted, relative):
    spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / relative)
    loaded = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, dotted, loaded)
    spec.loader.exec_module(loaded)
    return loaded


# --- the opener -----------------------------------------------------------------------------------

@pytest.fixture
def opener(monkeypatch):
    state = types.SimpleNamespace(created=[], deleted=[], stopped=[], added=[], commits=0, fail_create=False,
                                  exceptions=[], response=types.SimpleNamespace(is_streaming=True, message_items=[]))

    def create_trigger_run_conversation(**kwargs):
        if state.fail_create:
            raise RuntimeError("db down")
        state.created.append(kwargs)
        return "conv-1", 11, "msg-1"

    class Session:
        def query(self, model):
            row = state.response if model is message_group else "conversation-row"
            return types.SimpleNamespace(filter=lambda *_: types.SimpleNamespace(first=lambda: row))

        def delete(self, row):
            state.deleted.append(row)

        def add(self, row):
            state.added.append(row)

        def commit(self):
            state.commits += 1

    message_group = types.SimpleNamespace(uuid="uuid-column")
    text_item = type("TextMessageItem", (types.SimpleNamespace,), {
        "__mapper_args__": {"polymorphic_identity": "text_message"}})

    @contextmanager
    def get_session(_project_id):
        yield Session()

    log = types.SimpleNamespace(exception=lambda *a, **k: state.exceptions.append(a))
    for name in (PKG, f"{PKG}.models", f"{PKG}.utils"):
        _install(monkeypatch, name)
    _install(monkeypatch, "pylon.core.tools", log=log)
    _install(monkeypatch, "tools", db=types.SimpleNamespace(get_session=get_session),
             this=types.SimpleNamespace(module=types.SimpleNamespace(mark_chat_run_stopped=state.stopped.append)))
    _install(monkeypatch, f"{PKG}.utils.input_guardrail", redacted_input=lambda value: f"redacted({value})")
    _install(monkeypatch, f"{PKG}.utils.pipeline_execution", TriggerType=TRIGGERS,
             create_trigger_run_conversation=create_trigger_run_conversation,
             run_label=lambda source: "Agent run" if source == "agent" else "Pipeline run")
    _install(monkeypatch, f"{PKG}.utils.predict_utils",
             user_input_preview=lambda value, limit=None: state.__dict__.setdefault("limits", []).append(limit) or value)
    _install(monkeypatch, f"{PKG}.utils.sio_utils", SioEvents=types.SimpleNamespace(
        chat_predict=types.SimpleNamespace(value="chat_predict")))
    _install(monkeypatch, f"{PKG}.models.conversation", Conversation=types.SimpleNamespace(uuid="uuid-column"))
    _install(monkeypatch, f"{PKG}.models.message_group", ConversationMessageGroup=message_group)
    _install(monkeypatch, f"{PKG}.models.message_items")
    _install(monkeypatch, f"{PKG}.models.message_items.text", TextMessageItem=text_item)
    module = _load(monkeypatch, f"{PKG}.utils.api_run_history", "utils/api_run_history.py")
    return types.SimpleNamespace(module=module, state=state)


def _version(agent_type="openai"):
    return types.SimpleNamespace(id=127, application_id=85, agent_type=agent_type)


@pytest.mark.parametrize("trigger", ["api", "mcp"])
def test_a_rest_or_mcp_run_is_recorded(opener, trigger):
    assert opener.module.should_record_api_run(trigger, None, {"user_input": "hi"}) is True


@pytest.mark.parametrize("trigger, signature, payload, enabled", [
    ("api", "sig", {}, True),                        # the legacy signed webhook keeps its own path
    ("webhook", None, {}, True),
    ("chat", None, {}, True),
    ("api", None, {"conversation_id": "c-1"}, True),  # a caller continuing its own conversation
    ("api", None, {"stream_id": "s-1"}, True),
    ("api", None, {}, False),                         # the record_api_runs switch
])
def test_a_run_that_is_not_recorded(opener, trigger, signature, payload, enabled):
    assert opener.module.should_record_api_run(trigger, signature, payload, enabled=enabled) is False


@pytest.mark.parametrize("trigger, agent_type, name, source", [
    ("api", "openai", "API run: Echo", "agent"),
    ("mcp", "openai", "MCP run: Echo", "agent"),
    ("api", "pipeline", "API run: Echo", "pipeline"),
])
def test_the_row_is_named_by_trigger_and_typed_by_agent_type(opener, trigger, agent_type, name, source):
    row = opener.module.open_api_run_conversation(
        project_id=2, version=_version(agent_type), user_id=3, user_input="hi", trigger_source=trigger,
        entity_name="Echo")

    [created] = opener.state.created
    assert (created["trigger_type"], created["conversation_name"], created["source"]) == (trigger, name, source)
    assert (created["project_id"], created["user_id"]) == (2, 3)
    assert row == opener.module.ApiRunConversation(2, "conv-1", "msg-1", source)


def test_the_stored_input_is_the_redacted_one(opener):
    opener.module.open_api_run_conversation(
        project_id=2, version=_version(), user_id=3, user_input="token abc", trigger_source="api", entity_name="Echo")

    assert opener.state.created[0]["trigger_message"] == "redacted(token abc)"


def test_the_stored_input_is_capped(opener):
    opener.module.open_api_run_conversation(
        project_id=2, version=_version(), user_id=3, user_input="x", trigger_source="api", entity_name="Echo")

    assert opener.state.limits == [opener.module.STORED_INPUT_LIMIT] and opener.module.STORED_INPUT_LIMIT > 0


def test_an_unnamed_entity_falls_back_to_its_application_id(opener):
    opener.module.open_api_run_conversation(
        project_id=2, version=_version(), user_id=3, user_input=None, trigger_source="api", entity_name=None)

    assert opener.state.created[0]["conversation_name"] == "API run: 85"


def test_a_row_that_cannot_be_written_never_fails_the_run(opener):
    opener.state.fail_create = True

    assert opener.module.open_api_run_conversation(
        project_id=2, version=_version(), user_id=3, user_input="hi", trigger_source="api", entity_name="Echo") is None
    assert opener.state.exceptions


def test_discard_deletes_the_conversation(opener):
    opener.module.ApiRunConversation(2, "conv-1", "msg-1", "agent").discard()

    assert opener.state.deleted == ["conversation-row"]


def test_task_meta_binds_the_worker_events_to_the_row(opener):
    assert opener.module.ApiRunConversation(2, "conv-1", "msg-1", "agent").task_meta() == {
        "stream_id": "conv-1", "message_id": "msg-1", "sio_event": "chat_predict", "chat_project_id": 2}


@pytest.mark.parametrize("source, text", [("agent", "Agent run timed out"), ("pipeline", "Pipeline run timed out")])
def test_a_time_out_marks_the_run_stopped_and_ends_the_answer(opener, source, text):
    opener.module.ApiRunConversation(2, "conv-1", "msg-1", source).time_out()

    assert opener.state.stopped == ["msg-1"]   # a late end from the stopped worker is ignored
    assert opener.state.response.is_streaming is False
    [item] = opener.state.added
    assert (item.content, item.item_type, item.order_index) == (text, "text_message", 0)
    assert opener.state.commits == 1


def test_a_time_out_keeps_a_partial_answer(opener):
    opener.state.response.message_items = ["partial"]
    opener.module.ApiRunConversation(2, "conv-1", "msg-1", "agent").time_out()

    assert opener.state.added == [] and opener.state.response.is_streaming is False


# --- the binding in do_predict ------------------------------------------------------------------

class _Row:
    def __init__(self):
        self.project_id, self.conversation_uuid, self.response_message_id, self.source = 2, "conv-1", "msg-1", "agent"
        self.discarded, self.timed_out = False, False

    def discard(self):
        self.discarded = True

    def task_meta(self):
        return {"stream_id": self.conversation_uuid, "message_id": self.response_message_id,
                "sio_event": "chat_predict", "chat_project_id": self.project_id}

    def time_out(self):
        self.timed_out = True


class _Saturated(Exception):
    def __init__(self, **kwargs):
        super().__init__(kwargs)


class _Session:
    def __init__(self, version):
        self.version = version

    def query(self, _model):
        return types.SimpleNamespace(options=lambda *_: types.SimpleNamespace(get=lambda _id: self.version))


@pytest.fixture
def predict(monkeypatch):
    """The real do_predict with an opener the test steers; `world.run(**kwargs)` calls it."""
    world = types.SimpleNamespace(row=_Row(), record=True, opened=[], decided=[], starts=[], events=[],
                                  task_id="task-1", start_error=None, joined="answer", stopped=[],
                                  inspect_error=None)
    version = types.SimpleNamespace(
        application=types.SimpleNamespace(name="Echo", webhook_secret=None), application_id=85, id=127,
        llm_settings={"model": "m"}, agent_type="openai", instructions="")
    request = types.SimpleNamespace(project_id=2, context_settings=True, application_name=None,
                                    stream_id="caller-stream", conversation_id=None, message_id="caller-msg")
    world.request = request
    chat_request = types.SimpleNamespace(
        parse_obj=lambda _payload: request,
        from_orm=lambda _row: types.SimpleNamespace(merge_update=lambda parsed: parsed))

    @contextmanager
    def get_session(_project_id):
        yield _Session(version)

    def should_record_api_run(*args, **kwargs):
        world.decided.append((args, kwargs))
        return world.record

    def open_api_run_conversation(**kwargs):
        world.opened.append(kwargs)
        return world.row

    def payload_of(parsed, **_kwargs):
        return {"user_input": "hi", "stream_id": parsed.stream_id, "message_id": parsed.message_id}

    def inspect_and_unsecret(payload, *_args, **_kwargs):
        if world.inspect_error:
            raise world.inspect_error
        return payload

    log = types.SimpleNamespace(warning=lambda *a, **k: None)
    _install(monkeypatch, "opentelemetry", trace=types.SimpleNamespace(get_current_span=lambda: None))
    _install(monkeypatch, "pylon.core.tools", log=log, web=types.SimpleNamespace(method=lambda *a, **k: (lambda f: f)))
    _install(monkeypatch, "sqlalchemy.orm", joinedload=lambda *_: None)
    _install(monkeypatch, "tools", VaultClient=lambda _pid: types.SimpleNamespace(unsecret=lambda value: value),
             db=types.SimpleNamespace(get_session=get_session), serialize=lambda value: value)
    leaves = {
        "models.pd.chat": {"ApplicationChatRequest": chat_request, "ContextStrategyModel": dict},
        "utils.predict_utils": {"generate_predict_payload": payload_of,
                                "load_context_settings_from_conversation": None,
                                "user_input_preview": lambda value: value},
        "models.all": {"ApplicationVersion": types.SimpleNamespace(application=None)},
        "utils.budget_door": {"schema_may_use_llm": lambda *_: True},
        "utils.utils": {"verify_signature": None},
        "utils.exceptions": {"VerifySignatureError": Exception, "PoolSaturationError": _Saturated},
        "utils.api_run_history": {"should_record_api_run": should_record_api_run,
                                  "open_api_run_conversation": open_api_run_conversation},
        "utils.pipeline_execution": {"TriggerType": TRIGGERS},
        "utils.input_guardrail": {"inspect_and_unsecret": inspect_and_unsecret},
        "utils.vectorstore": {"get_pgvector_connection_string": lambda _pid: "connstr"},
        "models.pd.admission": {"admission_request_from": lambda *a, **k: None},
        "utils.credential_slots": {"has_free_form_secret": lambda *a, **k: False,
                                   "mark_free_form_secret": lambda *a, **k: None},
        "utils.governance": {"is_governed": lambda *a, **k: False},
        "utils.mandate_admission": {"mandate_admission_request": None},
        "utils.trigger_principal": {"resolve_trigger_principal": lambda *a, **k: None},
    }
    for name in (PKG, f"{PKG}.methods", f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        _install(monkeypatch, name)
    for dotted, attrs in leaves.items():
        _install(monkeypatch, f"{PKG}.{dotted}", **attrs)
    loaded = _load(monkeypatch, f"{PKG}.methods.predict", "methods/predict.py")

    def start_task(*args, **kwargs):
        if world.start_error:
            raise world.start_error
        world.starts.append(kwargs)
        return world.task_id

    world.config = {}
    owner = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config=world.config),
        context=types.SimpleNamespace(event_manager=types.SimpleNamespace(
            fire_event=lambda name, payload: world.events.append((name, payload)))),
        task_node=types.SimpleNamespace(
            start_task=start_task, join_task=lambda *_a, **_k: world.joined, stop_task=world.stopped.append))

    def run(**kwargs):
        options = {"payload_in": {"user_input": "hi"}, "predict_wait": False, **kwargs}
        return loaded.Method.do_predict(owner, project_id=2, user_id=3, version_id=127, raw=b"", **options)

    world.run = run
    return world


def test_a_recorded_run_is_bound_to_its_row(predict):
    result = predict.run(trigger_source="mcp")

    assert result == {"message": "Task started", "task_id": "task-1"}
    [opened] = predict.opened
    assert (opened["project_id"], opened["user_id"], opened["trigger_source"]) == (2, 3, "mcp")
    assert (opened["user_input"], opened["entity_name"]) == ("hi", "Echo")
    [start] = predict.starts
    assert start["args"] == ["conv-1", "msg-1"]
    assert start["kwargs"]["stream_id"] == "conv-1" and start["kwargs"]["message_id"] == "msg-1"
    assert predict.request.conversation_id == "conv-1"
    meta = start["meta"]
    assert (meta["stream_id"], meta["message_id"], meta["sio_event"], meta["chat_project_id"]) == (
        "conv-1", "msg-1", "chat_predict", 2)
    assert meta["non_interactive"] is True   # still no Socket.IO consumer: no streaming events


def test_the_row_gets_the_task_id_for_stop_and_the_reconcile(predict):
    predict.run()

    assert predict.events == [("applications_predict_task_id", {
        "task_id": "task-1", "project_id": 2, "message_group_id": "msg-1"})]


def test_an_unrecorded_run_starts_unbound_as_before(predict):
    predict.record = False
    predict.run()

    [start] = predict.starts
    assert start["args"] == [None, None]
    assert not {"stream_id", "message_id", "sio_event", "chat_project_id"} & set(start["meta"])
    assert predict.opened == [] and predict.events == []
    assert predict.request.stream_id == "caller-stream"


def test_a_row_that_could_not_be_opened_still_runs_unbound(predict):
    predict.row = None
    predict.run()

    assert predict.starts[0]["args"] == [None, None] and predict.events == []


@pytest.mark.parametrize("config, enabled", [({}, False), ({"record_api_runs": True}, True)])
def test_the_switch_and_the_inputs_reach_the_decision(predict, config, enabled):
    predict.config.update(config)
    predict.run(payload_in={"user_input": "hi"}, webhook_signature=None)

    [(args, kwargs)] = predict.decided
    assert args[0] == "api" and args[1] is None and args[2]["user_input"] == "hi"
    assert kwargs == {"enabled": enabled}


def test_a_saturated_pool_leaves_no_row(predict):
    predict.task_id = None
    with pytest.raises(_Saturated):
        predict.run()

    assert predict.row.discarded and predict.events == []


def test_a_start_that_raises_leaves_no_row(predict):
    predict.start_error = ValueError("refused")
    with pytest.raises(ValueError):
        predict.run()

    assert predict.row.discarded


def test_a_guardrail_refusal_leaves_no_row(predict):
    predict.inspect_error = PermissionError("blocked")
    with pytest.raises(PermissionError):
        predict.run()

    assert predict.row.discarded and predict.starts == []


def test_a_sync_timeout_closes_the_row_with_an_error(predict):
    predict.joined = ...
    result = predict.run(predict_wait=True)

    assert result == {"error": "Timeout"}
    assert predict.stopped == ["task-1"] and predict.row.timed_out


def test_a_sync_run_returns_the_worker_result_unchanged(predict):
    predict.joined = {"chat_history": [{"content": "Blue and red."}]}

    assert predict.run(predict_wait=True) == {"chat_history": [{"content": "Blue and red."}]}
    assert not predict.row.timed_out and not predict.row.discarded
