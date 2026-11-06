"""C-1009 #7228 items 3 and 4: the chat's credential form endpoint and the resume of a credential form.

``utils/credential_form.py`` and ``api/v2/credential_form.py`` are loaded standalone. Their collaborators are fakes that
record what they were asked: the paused message (its persisted interrupts), the configurations create and delete
services, secrets' ``file_credential_requests`` (in the form's own transaction), the owner's value write, the import,
the probe, ``governance_requests.file_requests`` and the audit trail. Every value here is made up; the tests assert
that none of them reaches a reply, an event, a log line or the message's meta.
"""
import ast
import contextlib
import importlib.util
import json
import sys
import types
import uuid
from pathlib import Path

import pytest
from pydantic import SecretStr

PLUGIN_ROOT = Path(__file__).parents[2]
PKG = "cf7228"
PROJECT, MEMBER, OTHER = 6, 21, 22
CONVERSATION = "11111111-1111-4111-8111-111111111111"
MESSAGE = "22222222-2222-4222-8222-222222222222"
INTERRUPT = "hitl_7228aa"
VALUE = "made-up-value-7228-not-a-secret"
SECRET_MARK = "made-up-value-7228"
REF = "33333333-3333-4333-8333-333333333333"
CONFIG_UUID = "44444444-4444-4444-8444-444444444444"


def _module(name, path=None, **attrs):
    module = types.ModuleType(name)
    if path is not None:
        module.__path__ = [str(path)]
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class World:
    """The fakes, and what they were asked."""

    def __init__(self):
        self.governed = True
        self.logs, self.events, self.calls = [], [], []
        self.permissions = {MEMBER: {"governance.request", "configurations.configuration.create"},
                            OTHER: {"governance.request", "configurations.configuration.create"}}
        self.personal_project = True   # (round 5) the ownership follows the project: the personal project's is personal
        self.secret_names = {"access_token"}
        self.properties = {"access_token", "base_url", "refresh_token"}
        self.section = "credentials"
        self.delegated_types = frozenset({"sharepoint"})
        self.store_answer = {"generation": 1, "state": "pending"}
        self.probe_answer = {"state": "validated"}
        self.reference_fails = None
        self.requests_answer = [{"key": "binding", "kind": "binding", "id": "b-1"}]
        self.requests_refused = None
        self.run_version = (PROJECT, 70)
        self.run_user = MEMBER        # the author of the question the paused answer replies to
        self.claims = None            # run-token claims of the request (a resume from a run)
        self.purged = []
        self.commits = self.rollbacks = 0
        self.reference = {"state": "validated", "owner": {"id": MEMBER}, "created_by": {"id": MEMBER}}
        self.meta = {"hitl_interrupt": self.interrupt(), "hitl_interrupts": [self.interrupt()]}
        self.message = types.SimpleNamespace(meta=self.meta)

    @staticmethod
    def interrupt(**over):
        found = {"interrupt_id": INTERRUPT, "guardrail_type": "credential_form", "node_name": "request_credential",
                 "credential_type": "github", "title": "GitHub", "purpose": "list repositories",
                 "available_actions": ["created", "cancel"]}
        found.update(over)
        return found

    def everything_written(self):
        return json.dumps({"events": self.events, "meta": self.message.meta, "logs": self.logs,
                           "calls": [call for call in self.calls if call[0] != "create_configuration"]}, default=str)


class FakeSession:
    def __init__(self, world):
        self.world = world

    def commit(self):
        self.world.commits += 1

    def rollback(self):
        self.world.rollbacks += 1

    def flush(self):
        pass

    def query(self, *args):
        return self

    def filter(self, *args):
        return self

    def scalar(self):
        return self.world.message.meta

    def first(self):
        return self.world.message


@pytest.fixture
def world(monkeypatch):
    saved = dict(sys.modules)
    w = World()

    def record(level):
        return lambda message, *args, **kwargs: w.logs.append((level, message % args if args else message))

    log = types.SimpleNamespace(**{name: record(name) for name in ("info", "warning", "error", "debug", "exception")})
    _module("pylon"), _module("pylon.core"), _module("pylon.core.tools", log=log)

    @contextlib.contextmanager
    def get_session(project_id):
        yield FakeSession(w)

    def file_credential_requests(session, project_id, items, actor):
        w.calls.append(("file_reference", type(session).__name__, project_id, items, actor))
        if w.reference_fails:
            raise RuntimeError(w.reference_fails)
        return [{"key": items[0]["key"], "id": REF}]

    modules = {
        "secrets": types.SimpleNamespace(file_credential_requests=file_credential_requests),
        "tracing": types.SimpleNamespace(security_audit_record_autocommit=w.events.append),
    }
    auth = types.SimpleNamespace(
        get_user_permissions=lambda user_id, mode, project_id: w.permissions.get(user_id, set()),
        get_user=lambda user_id: {"idp_issuer": "kc", "idp_tid": "Tenant-1", "idp_oid": f"oid-{user_id}"},
        current_user=lambda: {"id": w.actor} if getattr(w, "actor", None) else {})
    _module("tools", auth=auth, db=types.SimpleNamespace(get_session=get_session),
            this=types.SimpleNamespace(for_module=lambda name: types.SimpleNamespace(module=modules[name])))
    _module("sqlalchemy"), _module("sqlalchemy.orm")
    _module("sqlalchemy.orm.attributes", flag_modified=lambda obj, key: None)
    _module(PKG, PLUGIN_ROOT), _module(f"{PKG}.utils", PLUGIN_ROOT / "utils")
    _module(f"{PKG}.models", PLUGIN_ROOT / "models"), _module(f"{PKG}.models.pd", PLUGIN_ROOT / "models" / "pd")
    _load(f"{PKG}.models.pd.continue_predict", PLUGIN_ROOT / "models" / "pd" / "continue_predict.py")
    _load(f"{PKG}.models.pd.credential_form", PLUGIN_ROOT / "models" / "pd" / "credential_form.py")
    _module(f"{PKG}.models.message_group", ConversationMessageGroup=types.SimpleNamespace(meta="meta", uuid="uuid"))
    _module(f"{PKG}.utils.governance", is_governed=lambda module: w.governed)

    class RequestRefused(Exception):
        def __init__(self, code, key=None):
            super().__init__(code)
            self.code, self.key = code, key

    class OneTransaction:
        def __init__(self, session):
            self._session = session

    def file_requests(module, project_id, kind, entity_id, actor, keys=None, run_id=None):
        w.calls.append(("file_requests", project_id, kind, entity_id, actor, run_id))
        if w.requests_refused:
            raise RequestRefused(w.requests_refused)
        return w.requests_answer

    _module(f"{PKG}.utils.governance_requests", RequestRefused=RequestRefused, _OneTransaction=OneTransaction,
            file_requests=file_requests)
    _module(f"{PKG}.utils.governance_readiness", _person=lambda user_id: f"idp:tenant-1:oid-{user_id}")
    _module(f"{PKG}.utils.credential_slots", DELEGATED_TYPES=w.delegated_types,
            configuration_secret_fields=lambda types_: {name: sorted(w.secret_names) for name in types_})
    _load(f"{PKG}.utils.parallel_hitl", PLUGIN_ROOT / "utils" / "parallel_hitl.py")
    _module(f"{PKG}.utils.internal_obo", current_claims=lambda: w.claims)

    def create_configuration(payload):
        w.calls.append(("create_configuration", payload["type"], payload["shared"], payload["author_id"],
                        sorted(payload["data"])))
        w.created_payload = payload
        return {"id": 9, "uuid": CONFIG_UUID, "elitea_title": payload["elitea_title"], "type": payload["type"],
                "data": {name: "{{secret.x}}" for name in payload["data"]}}

    def delete_configuration(project_id, config_id):
        w.calls.append(("delete_configuration", project_id, config_id))

    def write_material(project_id, ref_id, fields, expected_generation, actor):
        assert all(isinstance(value, SecretStr) for value in fields.values())
        w.calls.append(("write_material", ref_id, sorted(fields), expected_generation, actor))
        return w.store_answer

    def run_reference_action(values, actor, audit_code=False):
        w.calls.append(("reference_action", values["action"], values.get("check_connection"), actor))
        if values["action"] == "probe":
            return w.probe_answer
        if values["action"] == "import":
            return w.store_answer
        return {"state": "retired"}

    def reference_detail(project_id, ref_id):
        if w.reference is None:
            raise type("E", (Exception,), {"code": "REFERENCE_NOT_FOUND"})()
        return w.reference

    _module("plugins"), _module("plugins.configurations"), _module("plugins.secrets")
    _module("plugins.secrets.utils")
    _module("plugins.configurations.exceptions", ConfigurationError=type("ConfigurationError", (Exception,), {}))
    _module("plugins.configurations.utils", create_configuration=create_configuration,
            delete_configuration=delete_configuration,
            purge_secrets=lambda project_id, references: w.purged.append((project_id, sorted(references))))
    _module("plugins.secrets.utils.credential_material", write_material=write_material)
    _module("plugins.secrets.utils.credential_onboarding", run_reference_action=run_reference_action)
    _module("plugins.secrets.utils.credential_queries", reference_detail=reference_detail)
    cf = _load(f"{PKG}.utils.credential_form", PLUGIN_ROOT / "utils" / "credential_form.py")
    monkeypatch.setattr(cf, "_message", lambda session, conversation_uuid, message_id: w.message
                        if (str(conversation_uuid), str(message_id)) == (CONVERSATION, MESSAGE) else None)
    monkeypatch.setattr(cf, "_run_version", lambda session, message: w.run_version)
    monkeypatch.setattr(cf, "_run_user", lambda session, message: w.run_user if message is not None else None)
    monkeypatch.setattr(cf, "_type_entry", lambda credential_type: _entry(w, credential_type))
    monkeypatch.setattr(cf, "_is_personal_project", lambda project_id, user_id: w.personal_project)
    w.cf = cf
    w.module = types.SimpleNamespace()
    yield w
    sys.modules.clear()
    sys.modules.update(saved)


def _entry(w, credential_type):
    if w.section != "credentials":
        raise w.cf.FormRefused("INVALID_REQUEST", "credential_type")
    return types.SimpleNamespace(section="credentials", config_schema={"properties": {
        name: {} for name in (w.properties if credential_type != "sharepoint" else {"site_url", "client_secret"})}})


def body(w, **over):
    values = {"interrupt_id": INTERRUPT, "message_id": MESSAGE, "credential_type": "github", "title": "GitHub",
              "fields": {"access_token": VALUE, "base_url": "https://api.github.com"}}
    values.update(over)
    return sys.modules[f"{PKG}.models.pd.credential_form"].CredentialFormBody.model_validate(values)


def complete(w, actor=MEMBER, **over):
    return w.cf.complete(w.module, PROJECT, CONVERSATION, body(w, **over), actor)


def refused(w, actor=MEMBER, **over):
    with pytest.raises(w.cf.FormRefused) as found:
        complete(w, actor, **over)
    return found.value


# --- the create endpoint (item 3) --------------------------------------------------------------------------------

def test_a_personal_credential_is_created_registered_valued_checked_and_requested_in_order(world):
    reply = complete(world)
    assert reply == {"reference_id": REF, "configuration_uuid": CONFIG_UUID,
                     "configuration_title": world.created_payload["elitea_title"], "title": "GitHub",
                     "credential_type": "github", "state": "validated", "configuration_id": 9,
                     "requests": [{"key": "binding", "kind": "binding", "id": "b-1"}]}
    assert [call[0] for call in world.calls] == [
        "create_configuration", "file_reference", "write_material", "reference_action", "file_requests"]
    assert world.calls[0] == ("create_configuration", "github", False, MEMBER, ["access_token", "base_url"])
    _, session_kind, project, [item], actor = world.calls[1]
    assert (session_kind, project, actor) == ("OneTransaction", PROJECT, MEMBER)
    assert item == {"kind": "reference", "key": f"reference:{CONFIG_UUID}", "configuration_uuid": CONFIG_UUID,
                    "ownership_class": "personal"}
    assert world.calls[2] == ("write_material", REF, ["access_token"], 0, MEMBER)   # the secret-typed subset only
    assert world.calls[3] == ("reference_action", "probe", True, MEMBER)          # the owner's own check
    assert world.calls[4] == ("file_requests", PROJECT, "version", 70, MEMBER, CONVERSATION)   # run:<conversation>
    assert world.commits >= 2 and world.rollbacks == 0


def test_the_reply_events_meta_and_logs_hold_no_value_and_no_placeholder(world):
    reply = complete(world)
    text = json.dumps(reply) + world.everything_written()
    assert SECRET_MARK not in text and "{{secret." not in text
    [event] = world.events
    assert (event["reason_code"], event["caller_ref"], event["credential_ref"], event["operation_id"]) == (
        "CREDENTIAL_FORM_COMPLETED", f"run:{CONVERSATION}", REF, f"credential_form:{INTERRUPT}")
    stored = world.message.meta["hitl_interrupts"][0]["credential_form_result"]
    assert stored["reference_id"] == REF and stored["created_by"] == MEMBER
    assert world.message.meta["hitl_interrupt"]["credential_form_result"] == stored


def test_a_team_projects_credential_is_the_projects_pending_request_imported_and_never_probed_by_the_member(world):
    """(round 5) in a team project the credential is the project's: never chosen, never marked as anyone's."""
    world.personal_project = False
    reply = complete(world)
    assert "meta" not in world.created_payload               # no owner label on any configuration
    assert world.calls[0][2] is True                         # the configuration is shared
    assert world.calls[1][3][0]["ownership_class"] == "project_shared"
    assert world.calls[2] == ("reference_action", "import", None, MEMBER)
    assert "probe" not in [call[1] for call in world.calls if call[0] == "reference_action"]
    assert reply["state"] == "pending"


def test_the_personal_projects_credential_is_personal_and_unmarked(world):
    complete(world)
    assert world.calls[0][2] is False and "meta" not in world.created_payload
    assert world.calls[1][3][0]["ownership_class"] == "personal"


@pytest.mark.parametrize("personal_project", [True, False])
def test_a_sign_in_credential_is_delegated_personal_stores_nothing_and_is_probed_by_its_owner(world, personal_project):
    """In a team project its configuration is the project's container (shared, no secret inside) and the reference is
    the person's own sign-in for it (#6756 personal references on a project configuration, #7119)."""
    world.personal_project = personal_project
    world.meta["hitl_interrupts"] = [world.interrupt(credential_type="sharepoint")]
    world.meta["hitl_interrupt"] = world.interrupt(credential_type="sharepoint")
    reply = complete(world, credential_type="sharepoint", fields={"site_url": "https://example.sharepoint.com/sites/a"})
    assert world.created_payload["data"] == {"site_url": "https://example.sharepoint.com/sites/a",
                                             "auth_mode": "entra_delegated"}   # the type's own marker
    [item] = world.calls[1][3]
    assert (item["ownership_class"], item["auth_mode"]) == ("personal", "entra_delegated")
    assert item["delegation"] == {"oauth_app_id": "platform", "tenant_id": "tenant-1",
                                  "resource": "https://graph.microsoft.com",
                                  "scopes": ["https://graph.microsoft.com/.default"], "assertion_source": "login"}
    assert "write_material" not in [call[0] for call in world.calls]
    assert ("reference_action", "probe", None, MEMBER) in world.calls
    assert reply["credential_type"] == "sharepoint"
    assert world.calls[0][2] is (not personal_project)


def test_a_sign_in_credential_takes_no_typed_secret(world):
    world.meta["hitl_interrupts"] = [world.interrupt(credential_type="sharepoint")]
    world.secret_names = {"client_secret"}
    found = refused(world, credential_type="sharepoint", fields={"client_secret": VALUE})
    assert (found.code, found.field) == ("INVALID_REQUEST", "fields") and world.calls == []


def test_a_failed_value_write_keeps_nothing(world):
    world.store_answer = {"error": "store down", "code": "STORE_UNAVAILABLE"}
    found = refused(world)
    assert (found.code, found.status) == ("DEPENDENCY_UNAVAILABLE", 503)
    assert ("reference_action", "retire", None, MEMBER) in world.calls
    assert ("delete_configuration", PROJECT, 9) in world.calls
    assert world.purged == [(PROJECT, ["{{secret.x}}"])]   # the Vault secret the configuration create stored
    meta = json.dumps(world.message.meta)
    assert "credential_form_result" not in meta and "credential_form_in_progress" not in meta   # claim released
    [event] = world.events
    assert (event["reason_code"], event["outcome"], event["operation_id"], event["credential_ref"]) == (
        "CREDENTIAL_FORM_FAILED", "failed", f"credential_form:{INTERRUPT}:value", REF)
    assert SECRET_MARK not in json.dumps(world.events)


def test_a_refused_reference_rolls_its_transaction_back_and_deletes_the_configuration(world):
    world.reference_fails = "REFERENCE_EXISTS"
    found = refused(world)
    assert found.code == "DEPENDENCY_UNAVAILABLE" and world.rollbacks == 1
    assert ("delete_configuration", PROJECT, 9) in world.calls
    assert "write_material" not in [call[0] for call in world.calls]


def test_a_failed_probe_keeps_the_credential_pending_and_says_so(world):
    world.probe_answer = {"error": "refused", "code": "CHECK_CONNECTION_FAILED"}
    assert complete(world)["state"] == "pending"


def test_a_refused_request_does_not_undo_the_credential(world):
    world.requests_refused = "STALE_READINESS"
    reply = complete(world)
    assert reply["requests"] == [] and reply["requests_refused"] == "STALE_READINESS"
    assert reply["reference_id"] == REF


def test_no_request_is_filed_for_a_message_that_is_not_an_agents(world):
    world.run_version = None
    assert complete(world)["requests"] == [] and "file_requests" not in [call[0] for call in world.calls]


@pytest.mark.parametrize("change, code, status", [
    ({"interrupt_id": "hitl_other"}, "NOT_FOUND", 404),
    ({"credential_type": "gitlab"}, "INTERRUPT_MISMATCH", 409),
])
def test_an_unknown_interrupt_or_another_type_is_refused_with_nothing_created(world, change, code, status):
    found = refused(world, **change)
    assert (found.code, found.status) == (code, status) and world.calls == []


def test_a_retired_interrupt_is_not_found(world):
    world.message.meta = {}
    assert refused(world).code == "NOT_FOUND" and world.calls == []


def test_an_interrupt_of_another_kind_is_a_mismatch(world):
    world.message.meta = {"hitl_interrupt": world.interrupt(guardrail_type="clarifying_question")}
    assert refused(world).code == "INTERRUPT_MISMATCH" and world.calls == []


def test_the_same_form_sent_twice_creates_nothing_twice(world):
    first = complete(world)
    count = len(world.calls)
    again = complete(world)
    assert len(world.calls) == count and again["reference_id"] == first["reference_id"] and again["requests"] == []
    world.run_user = OTHER
    found = refused(world, actor=OTHER)   # another member never reads the record
    assert found.code == "NOT_RUN_USER"


def test_governance_off_reads_nothing(world):
    world.governed = False
    world.message = None   # poisoned: any read fails
    assert refused(world).code == "PROJECT_GOVERNANCE_OFF" and world.calls == [] and world.events == []


def test_a_member_without_the_configuration_create_permission_is_forbidden(world):
    world.permissions[MEMBER] = {"governance.request"}
    assert refused(world).code == "FORBIDDEN" and world.calls == []


@pytest.mark.parametrize("fields", [
    {"access_token": VALUE, "not_a_field": "x"},       # a name the type does not have
    {"access_token": VALUE, "refresh_token": "x"},     # a stored sign-in is never typed
    {"base_url": "https://api.github.com"},            # nothing secret to keep
    {"access_token": "{{secret.GITHUB}}"},             # a placeholder is not a typed value
])
def test_fields_must_be_the_types_own_and_hold_its_secret(world, fields):
    found = refused(world, fields=fields)
    assert (found.code, found.field) == ("INVALID_REQUEST", "fields") and world.calls == []


@pytest.mark.parametrize("over", [
    {"extra": 1},
    {"fields": {f"f{i}": "x" for i in range(33)}},
    {"fields": {"bad name": "x"}},
    {"fields": {"access_token": "x" * 9000}},
    {"ownership": "personal"},        # (round 5) an old client's ownership choice: the strict body refuses it
    {"ownership": "project_shared"},
    {"auth_mode": "static"},
    {"message_id": "not-a-uuid"},
    {"title": ""},
])
def test_the_body_is_strict_and_its_errors_never_echo_a_value(world, over):
    from pydantic import ValidationError
    values = {"interrupt_id": INTERRUPT, "message_id": MESSAGE, "credential_type": "github", "title": "GitHub",
              "fields": {"access_token": VALUE}, **over}
    model = sys.modules[f"{PKG}.models.pd.credential_form"].CredentialFormBody
    with pytest.raises(ValidationError) as error:
        model.model_validate(values)
    assert SECRET_MARK not in str(error.value) and SECRET_MARK not in repr(error.value.errors())


def test_a_parsed_value_never_shows_in_a_repr(world):
    assert SECRET_MARK not in repr(body(world))


# --- the resume (item 4) -----------------------------------------------------------------------------------------

def resume(w, action="created", value=None, decisions=None, actor=MEMBER):
    return w.cf.check_resume(w.module, PROJECT, MESSAGE, action, value, decisions, actor)


def test_created_is_replaced_by_the_servers_own_record(world):
    complete(world)
    value, decisions = resume(world, value={"reference_id": REF})
    assert value == {"reference_id": REF, "configuration_uuid": CONFIG_UUID,
                     "configuration_title": world.created_payload["elitea_title"], "title": "GitHub",
                     "credential_type": "github", "state": "validated"}
    assert decisions is None and "created_by" not in value


@pytest.mark.parametrize("value", [
    {"reference_id": REF, "title": "my own words for the model"},   # another key
    {"reference_id": "not-a-uuid"},
    {"reference_id": str(uuid.uuid4())},                           # a reference this form did not make
    "free text",
    None,
])
def test_any_other_answer_is_refused_before_anything_is_dispatched(world, value):
    complete(world)
    with pytest.raises(world.cf.FormRefused) as found:
        resume(world, value=value)
    assert (found.value.code, found.value.status) == ("HITL_VALUE_INVALID", 400)


def test_a_reference_of_someone_else_is_refused(world):
    complete(world)
    world.reference = {"state": "validated", "owner": {"id": OTHER}, "created_by": {"id": OTHER}}
    with pytest.raises(world.cf.FormRefused):
        resume(world, value={"reference_id": REF})
    world.reference = None   # retired since
    with pytest.raises(world.cf.FormRefused):
        resume(world, value={"reference_id": REF})


def test_another_member_cannot_answer_with_the_record_of_someone_elses_form(world):
    complete(world)
    with pytest.raises(world.cf.FormRefused):
        resume(world, value={"reference_id": REF}, actor=OTHER)


def test_created_before_the_form_was_sent_is_refused(world):
    with pytest.raises(world.cf.FormRefused):
        resume(world, value={"reference_id": REF})


@pytest.mark.parametrize("action", ["approve", "answer", "edit"])
def test_only_created_and_cancel_answer_a_credential_form(world, action):
    with pytest.raises(world.cf.FormRefused):
        resume(world, action=action, value="x")


def test_cancel_carries_nothing(world):
    assert resume(world, action="cancel") == ({}, None)
    with pytest.raises(world.cf.FormRefused):
        resume(world, action="cancel", value={"reference_id": REF})


def test_a_routed_decision_on_a_credential_form_is_checked_and_replaced_too(world):
    complete(world)
    _, decisions = resume(world, action=None, decisions=[{"interrupt_id": INTERRUPT, "action": "created",
                                                          "value": {"reference_id": REF}}])
    assert decisions[0]["value"]["reference_id"] == REF and decisions[0]["value"]["credential_type"] == "github"
    with pytest.raises(world.cf.FormRefused):
        resume(world, action=None, decisions=[{"interrupt_id": INTERRUPT, "action": "approve"}])


def test_another_kind_of_pause_is_not_touched(world):
    world.message.meta = {"hitl_interrupt": world.interrupt(guardrail_type="sensitive_tool",
                                                            available_actions=["approve", "reject"])}
    assert resume(world, action="approve", value="anything") == ("anything", None)


def test_off_reads_nothing_at_resume(world):
    world.governed = False
    world.message = None
    assert resume(world, value={"reference_id": REF}) == ({"reference_id": REF}, None)


def test_the_rest_resume_takes_the_two_new_actions_and_the_strict_value_model():
    sys.path.insert(0, str(PLUGIN_ROOT))
    spec = importlib.util.spec_from_file_location("cp7228", PLUGIN_ROOT / "models" / "pd" / "continue_predict.py")
    cp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cp)
    for action in ("created", "cancel"):
        assert cp.ContinuePredictPayload.model_validate(
            {"conversation_uuid": CONVERSATION, "message_id": MESSAGE, "hitl_action": action}).hitl_action == action
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        cp.CredentialFormValue.model_validate({"reference_id": REF, "state": "active"})


# --- the route: who may post the form ------------------------------------------------------------------------

def _request_roles():
    tree = ast.parse((PLUGIN_ROOT / "utils" / "binding_api.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "REQUEST_ROLES":
            text = ast.unparse(node.value).replace("c.ADMINISTRATION_MODE", "'administration'").replace(
                "c.DEFAULT_MODE", "'default'")
            return ast.literal_eval(text)
    raise AssertionError("REQUEST_ROLES not found")


def test_the_form_is_a_members_act_not_a_viewers_and_never_an_administration_one():
    roles = _request_roles()
    assert roles["default"] == {"system": False, "super_admin": False, "admin": True, "editor": True, "viewer": False}
    assert not any(roles["administration"].values())


@pytest.fixture
def route(world, monkeypatch):
    holder = types.SimpleNamespace(actor=MEMBER, session=True)
    permissions = {"member": {"governance.request"}, "viewer": set(), "foreign": set(), "platform_admin": set()}

    def check_api(descriptor):
        def decorator(func):
            def wrapper(self, *args, **kwargs):
                if not set(descriptor["permissions"]) & permissions.get(holder.kind, set()):
                    return {"ok": False, "error": "access_denied"}, 403
                return func(self, *args, **kwargs)
            wrapper.descriptor = descriptor
            return wrapper
        return decorator

    def refusal(code, message=None):
        return {"error": message or code, "code": code}, {"PROJECT_GOVERNANCE_OFF": 409, "FORBIDDEN": 403,
                                                            "PAYLOAD_TOO_LARGE": 413}.get(code, 400)

    sys.modules["tools"].auth.decorators = types.SimpleNamespace(check_api=check_api)
    sys.modules["tools"].register_openapi = lambda **meta: (lambda func: func)
    sys.modules["tools"].api_tools = types.SimpleNamespace(
        APIModeHandler=type("APIModeHandler", (), {"__init__": lambda self, module: setattr(self, "module", module)}),
        APIBase=type("APIBase", (), {}), with_modes=lambda params: params)
    _module(f"{PKG}.utils.binding_api", REQUEST="governance.request", REQUEST_ROLES=_request_roles(),
            OPENAPI_TAG="elitea_core/governance", refusal=refusal,
            session_actor=lambda: holder.actor if holder.session else None)
    _module(f"{PKG}.utils.constants", PROMPT_LIB_MODE="prompt_lib")
    _module(f"{PKG}.api", PLUGIN_ROOT / "api"), _module(f"{PKG}.api.v2", PLUGIN_ROOT / "api" / "v2")
    request = types.SimpleNamespace(content_length=None, max_content_length=None, json_body=None)
    request.get_json = lambda silent=False: request.json_body
    _module("flask", request=request)
    _module("werkzeug"), _module("werkzeug.exceptions", RequestEntityTooLarge=type("TooLarge", (Exception,), {}))
    endpoint = _load(f"{PKG}.api.v2.credential_form", PLUGIN_ROOT / "api" / "v2" / "credential_form.py")
    monkeypatch.setattr(endpoint, "complete", world.cf.complete)

    def post(kind, actor=MEMBER, session=True, json_body=None):
        holder.kind, holder.actor, holder.session = kind, actor, session
        request.json_body = json_body if json_body is not None else {
            "interrupt_id": INTERRUPT, "message_id": MESSAGE, "credential_type": "github", "title": "GitHub",
            "fields": {"access_token": VALUE}}
        handler = endpoint.PromptLibAPI(world.module)
        return handler.post(PROJECT, CONVERSATION)

    return types.SimpleNamespace(post=post, endpoint=endpoint)


@pytest.mark.parametrize("kind, actor, session, status", [
    ("member", MEMBER, True, 200),
    ("viewer", 23, True, 403),
    ("foreign", 24, True, 403),          # a member of another project only
    ("platform_admin", 1, True, 403),    # no project role here: governance.request is a project member's
    ("member", MEMBER, False, 403),      # an API token of the member: never on the form
    ("anonymous", None, False, 403),
])
def test_the_routes_authorization_matrix(route, world, kind, actor, session, status):
    answer, code = route.post(kind, actor, session)
    assert code == status, answer
    if status != 200:
        assert world.calls == [] and SECRET_MARK not in json.dumps(answer)


def test_the_route_refuses_under_off_before_anything_is_read(route, world):
    world.governed = False
    answer, code = route.post("member")
    assert (code, answer["code"]) == (409, "PROJECT_GOVERNANCE_OFF") and world.calls == []


def test_a_bad_body_names_fields_only(route, world):
    answer, code = route.post("member", json_body={"interrupt_id": INTERRUPT, "message_id": MESSAGE,
                                                   "credential_type": "github", "title": "GitHub",
                                                   "fields": {"access_token": VALUE}, "smuggled": VALUE})
    assert code == 400 and answer["code"] == "INVALID_REQUEST" and SECRET_MARK not in json.dumps(answer)


def test_the_route_is_not_an_mcp_tool():
    text = (PLUGIN_ROOT / "api" / "v2" / "credential_form.py").read_text()
    assert "mcp_tool=False" in text and "mcp_tool=True" not in text


def test_the_record_must_be_the_callers_own_even_when_the_reference_would_pass(world):
    complete(world)   # the record says MEMBER made it
    world.reference = {"state": "validated", "owner": {"id": OTHER}, "created_by": {"id": OTHER}}
    with pytest.raises(world.cf.FormRefused):
        resume(world, value={"reference_id": REF}, actor=OTHER)



# --- fix round: one Create at a time, the run's own user, nothing usable left -----------------------------------

def test_a_second_create_while_one_is_in_progress_is_refused_and_creates_nothing(world, monkeypatch):
    seen = {}
    real = world.cf._create_configuration

    def slow(*args, **kwargs):   # the second Create arrives while the first holds its claim
        seen["second"] = refused(world)
        return real(*args, **kwargs)

    monkeypatch.setattr(world.cf, "_create_configuration", slow)
    first = complete(world)
    assert (seen["second"].code, seen["second"].status) == ("FORM_IN_PROGRESS", 409)
    assert [call[0] for call in world.calls].count("create_configuration") == 1
    assert first["reference_id"] == REF and "credential_form_in_progress" not in json.dumps(world.message.meta)


def test_a_stale_claim_of_a_crashed_create_is_taken_over(world):
    stale = {"by": MEMBER, "at": "2026-11-03T10:00:00+00:00"}
    world.message.meta = world.cf._remember(world.message.meta, INTERRUPT, stale, world.cf.CLAIM_KEY)  # pylint: disable=W0212
    assert complete(world)["reference_id"] == REF


def test_a_live_claim_refuses_even_the_same_person(world):
    from datetime import datetime, timezone
    live = {"by": MEMBER, "at": datetime.now(timezone.utc).isoformat()}
    world.message.meta = world.cf._remember(world.message.meta, INTERRUPT, live, world.cf.CLAIM_KEY)  # pylint: disable=W0212
    assert refused(world).code == "FORM_IN_PROGRESS" and world.calls == []


def test_a_failed_reference_releases_the_claim_and_purges_the_vault_secret(world):
    world.reference_fails = "REFERENCE_EXISTS"
    refused(world)
    assert world.purged == [(PROJECT, ["{{secret.x}}"])]
    assert "credential_form_in_progress" not in json.dumps(world.message.meta)
    assert [event["operation_id"] for event in world.events] == [f"credential_form:{INTERRUPT}:reference"]
    world.reference_fails = None
    assert complete(world)["reference_id"] == REF   # the person can try again at once


def test_only_the_person_whose_run_paused_may_complete_the_form(world):
    world.run_user = OTHER   # the question was asked by another member of the project (a project-visible chat)
    found = refused(world)
    assert (found.code, found.status) == ("NOT_RUN_USER", 403)
    assert world.calls == [] and "credential_form_in_progress" not in json.dumps(world.message.meta)
    world.run_user = None    # an answer that replies to no person's question
    assert refused(world).code == "NOT_RUN_USER"


def test_another_member_cannot_resume_the_form_and_neither_can_a_run(world):
    complete(world)
    world.run_user = OTHER
    with pytest.raises(world.cf.FormRefused):
        resume(world, value={"reference_id": REF})
    world.run_user, world.claims = MEMBER, {"uid": MEMBER, "scope": ["run"]}
    for action, value in (("created", {"reference_id": REF}), ("cancel", None)):
        with pytest.raises(world.cf.FormRefused) as found:
            resume(world, action=action, value=value)
        assert found.value.code == "HITL_VALUE_INVALID"


def test_the_body_takes_no_delegation_descriptor(world):
    from pydantic import ValidationError
    model = sys.modules[f"{PKG}.models.pd.credential_form"].CredentialFormBody
    with pytest.raises(ValidationError):
        model.model_validate({"interrupt_id": INTERRUPT, "message_id": MESSAGE, "credential_type": "sharepoint",
                              "title": "SP", "fields": {}, "auth_mode": "entra_delegated",
                              "delegation": {"oauth_app_id": "platform"}})


def test_a_failed_check_names_its_code_without_a_value(world):
    world.probe_answer = {"error": "no", "code": "CHECK_CONNECTION_FAILED", "reason": "DENIED_ASSERTION_SOURCE_UNSUPPORTED"}
    reply = complete(world)
    assert (reply["state"], reply["check"]) == ("pending", "DENIED_ASSERTION_SOURCE_UNSUPPORTED")


def test_a_type_a_plugin_owns_is_not_created_here(world, monkeypatch):
    """The real ``_type_entry`` (the fixture replaces it): an owned type is refused before anything is written."""
    registry = types.ModuleType("plugins.configurations.models.pd.registry")
    registry.CONFIG_TYPE_REGISTRY = {
        "s3_api_credentials": types.SimpleNamespace(section="credentials", managed_by="artifacts"),
        "github": types.SimpleNamespace(section="credentials", managed_by=None)}
    for name in ("plugins.configurations.models", "plugins.configurations.models.pd"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, registry.__name__, registry)
    tree = ast.parse((PLUGIN_ROOT / "utils" / "credential_form.py").read_text())
    namespace = {"FormRefused": world.cf.FormRefused, "CREDENTIAL_SECTION": "credentials"}
    exec(compile(ast.Module([node for node in tree.body if isinstance(node, ast.FunctionDef)  # pylint: disable=W0122
                             and node.name == "_type_entry"], []), "credential_form.py", "exec"), namespace)
    with pytest.raises(world.cf.FormRefused) as found:
        namespace["_type_entry"]("s3_api_credentials")
    assert found.value.code == "INVALID_REQUEST"
    assert namespace["_type_entry"]("github").managed_by is None


# --- fix round 2: the claim's nonce and guarded clean-up --------------------------------------------------------

def test_an_attempt_that_outlived_its_claim_keeps_nothing_and_leaves_the_newer_claim(world, monkeypatch):
    real = world.cf._probe   # pylint: disable=W0212
    newer = {"by": MEMBER, "at": "2026-11-03T12:00:00+00:00", "nonce": "newer"}

    def taken_over(*args, **kwargs):   # while this attempt runs, its claim goes stale and another takes the form
        world.message.meta = world.cf._remember(world.message.meta, INTERRUPT, newer, world.cf.CLAIM_KEY)  # pylint: disable=W0212
        return real(*args, **kwargs)

    monkeypatch.setattr(world.cf, "_probe", taken_over)
    found = refused(world)
    assert found.code == "FORM_IN_PROGRESS"
    assert ("reference_action", "retire", None, MEMBER) in world.calls and ("delete_configuration", PROJECT, 9) in world.calls
    meta = world.message.meta["hitl_interrupts"][0]
    assert meta["credential_form_in_progress"] == newer and "credential_form_result" not in meta
    assert [event["operation_id"] for event in world.events] == [f"credential_form:{INTERRUPT}:record"]


def test_a_failing_clean_up_step_does_not_skip_the_others(world, monkeypatch):
    world.store_answer = {"error": "store down", "code": "STORE_UNAVAILABLE"}

    def broken(*args, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(world.cf, "_retire", broken)
    assert refused(world).code == "DEPENDENCY_UNAVAILABLE"
    assert ("delete_configuration", PROJECT, 9) in world.calls and world.purged
    assert "credential_form_in_progress" not in json.dumps(world.message.meta)
    assert [event["reason_code"] for event in world.events] == ["CREDENTIAL_FORM_FAILED"]
