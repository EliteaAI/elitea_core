"""Seeded governed credentials for the reference-only payload tests (#6946, C-1009).

``Credential`` writes one toolkit, its configuration facts, credential reference and grant into the admission harness
(``fixtures/admission_env.py``) and knows the payload expansion would leave for it, with the secret still in it. The
helpers dispatch such a payload through the real ``admit_dispatch`` and bring the admitted run to ``running``.
"""
import json
import uuid

RUN = "run-1"
SUBJECT = "idp:tid-1:oid-42"
TITLE = "jira-main-credential"
SECRET = "plain-token-value-0123456789"
CALLER = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": RUN}
SECRET_PATHS = {"jira": ["token"], "github": ["auth.token", "app_key"]}


class Credential:  # pylint: disable=R0902
    """One governed credential of a toolkit: its rows, the facts that describe them and the payload that carries it."""

    def __init__(self, env, binding, *, tool_id=1, field="credentials", config_id=5, project_id=1, section="credentials",
                 ref_state="active", store_kind="database", config_generation=3, approved_generation=3, grants=True,
                 reference=True, cfg_uuid=None, version_id=70, ctype="jira", secret=SECRET, material=True):
        self.env, self.binding, self.tool_id, self.field, self.config_id = env, binding, tool_id, field, config_id
        self.uuid, self.ref_id, self.grant_id = cfg_uuid or str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        self.project_id, self.ctype, self.secret = project_id, ctype, secret
        env.secret_fields.setdefault(ctype, list(SECRET_PATHS.get(ctype, ["token"])))
        stored = {"elitea_title": TITLE, "private": False, "configuration_uuid": self.uuid,
                  "configuration_project_id": project_id}
        env.add_toolkit(1, version_id, tool_id, ctype, {"url": "https://jira.example", field: stored})
        env.configurations[(project_id, self.uuid)] = {
            "id": config_id, "project_id": project_id, "type": ctype, "section": section, "generation": config_generation}
        if reference:
            env.references.append({
                "project_id": 1, "configuration_id": config_id, "ref_id": self.ref_id, "state": ref_state, "generation": 1,
                "configuration_generation": approved_generation, "store_kind": store_kind,
                "ownership_class": "project_shared", "revocation_epoch": 2, "custodian_user_id": None})
            if material:
                env.material[self.ref_id] = {path: f"{secret}-{path}" for path in env.secret_fields[ctype]}
        if grants:
            env.grants[(1, self.key())] = {
                "grant_id": self.grant_id, "state": "active", "revocation_epoch": 4, "valid_from": None,
                "valid_until": None, "operations": ["*"]}

    def key(self, subject=SUBJECT):
        return "|".join((str(self.binding), str(self.tool_id), self.ref_id, "*", subject))

    @property
    def reference(self):
        return next(row for row in self.env.references if row["ref_id"] == self.ref_id)

    def expanded(self):
        """What expansion leaves in the dispatched settings: the dict with the configuration merged in."""
        config = {"elitea_title": TITLE, "configuration_uuid": self.uuid, "configuration_project_id": self.project_id,
                  "configuration_type": self.ctype, "configuration_generation": 3, "base_url": "https://jira.example"}
        for path in self.env.secret_fields[self.ctype]:
            node = config
            *parents, leaf = path.split(".")
            for part in parents:
                node = node.setdefault(part, {})
            node[leaf] = f"{self.secret}-{path}"
        return config

    def tool(self):
        return {"id": self.tool_id, "type": self.ctype,
                "settings": {"url": "https://jira.example", self.field: self.expanded()}}


def payload_of(*credentials, **extra):
    payload = {"application": {"version_details": {"tools": [credential.tool() for credential in credentials]}}}
    payload.update(extra)
    return payload


def seeded(env, **over):
    binding = env.seed_agent()
    return Credential(env, binding, **over)


def call_kwargs(payload, context_ref=None, task_pool="agents", **meta):
    body = {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": RUN, **payload}
    body["envelope"] = {"schema_version": 1, "security_profile": "governed",
                        "required_capabilities": ["task_envelope.v1"], "context_ref": context_ref}
    return {"args": ["s", "m"], "kwargs": body, "pool": task_pool, "meta": {"project_id": 1, **meta}}


def dispatch(env, ns, *credentials, payload=None, module=None, **request):
    """A governed chat dispatch of these credentials through the real ``admit_dispatch``: ``(call, context_ref)``."""
    call = call_kwargs(payload if payload is not None else payload_of(*credentials))
    context_ref = ns.adm.admit_dispatch(module or env.module(), "indexer_agent", call, env.request(**request))
    return call, context_ref


def run_to_running(env, ns, context_ref, caller=CALLER):
    """The worker's ``validate_context``: the admitted run becomes ``running``."""
    result = ns.adm.validate_context(env.module(), 1, context_ref, caller)
    assert result["ok"] is True, result
    return result


def sealed_tool_settings(call, tool_index=0, field="credentials"):
    tools = call["kwargs"]["application"]["version_details"]["tools"]
    return tools[tool_index]["settings"][field]


def dumped(value) -> str:
    return json.dumps(value, sort_keys=True, default=str)
