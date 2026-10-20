"""A real-SQL harness for the governed admission code (#6756, C-1009).

The plugin's own models, digest, binding state, admission and binding-admin files are loaded from
disk and run against SQLite files (one per project, like the per-project schemas), so the
statements the plugin sends are executed, not mocked. Only the Pylon runtime is faked: ``tools``
(``db``, ``auth``, ``this``), ``flask`` and the tracing audit methods. The audit fake validates each
event against the real ``SecurityAuditEventV1`` model when the tracing plugin is checked out next to
this one, so a wrong field or a secret-shaped value fails here.

The caller saves ``sys.modules`` before ``install`` and restores it after (see ``admission_env`` in
the ``test_6756_*`` files): the stubs must not leak into other suites.
"""
import ast
import importlib
import json
import os
import sys
import types
import uuid
import warnings
from collections import OrderedDict
from pathlib import Path

import pytest

from fixtures import identity
from fixtures.helpers import load_module_with_stubs

PKG = identity.PKG
TRACING_EVENT_MODEL = Path("tracing") / "models" / "security_audit_event.py"


def find_tracing_event_model(plugin_root: Path):
    """The tracing plugin's audit event model, found by walking up to the first ancestor that holds it (the plugin
    directory in a plain checkout, the same one from a ``wt/`` worktree), or None."""
    for directory in (plugin_root, *plugin_root.parents):
        candidate = directory / TRACING_EVENT_MODEL
        if candidate.is_file():
            return candidate
    return None


class RecordingLog:
    def __init__(self):
        self.lines = []

    def _add(self, message, *args):
        try:
            self.lines.append(message % args if args else str(message))
        except Exception:  # pylint: disable=W0703
            self.lines.append(str(message))

    info = warning = error = debug = exception = lambda self, message, *args, **kwargs: self._add(message, *args)


def principal(user_id=42, *, verified=True, epochs=None, tid="tid-1", oid=None):
    """What ``auth.verify_identity`` returns for a good session user."""
    evidence = (
        {"type": "verified_user", "issuer": "https://issuer", "tid": tid, "oid": oid or f"oid-{user_id}",
         "delegation_ref": f"uti-{user_id}", "audience": "api://elitea"}
        if verified else {"type": "asserted_user", "issuer": "elitea:pat", "subject": f"user:{user_id}"}
    )
    return {"kind": "user" if verified else "legacy_pat", "user_id": user_id, "subject_ref": f"user:{user_id}",
            "evidence": evidence, "expires_at": None, "epochs": epochs or {"user": 3, "token": 0}}


class Env:  # pylint: disable=R0902,R0904
    def __init__(self, ns, tmp_path):
        self.ns = ns
        self.tmp_path = tmp_path
        self.runs = 0
        self.reset()

    # --- state ---------------------------------------------------------------------------------

    def reset(self):
        self.runs += 1
        self.dir = self.tmp_path / f"t{self.runs}"
        self.dir.mkdir()
        self.engines = {}
        self.sessions = []              # (project_id, session) in the order they were opened
        self.log = self.ns.log
        self.log.lines.clear()
        self.sio_users = {}
        self.in_request = False
        self.g = types.SimpleNamespace()
        # audit fake
        self.audit_allowed = True
        self.audit_allowed_raises = False
        self.recorded = []              # (session, event) written in the caller's transaction
        self.denied = []                # events written on their own transaction
        self.record_raises = None
        self.autocommit_raises = None
        self.tracing_calls = 0
        # auth fake
        self.verify_calls = []
        self.verify_result = {"ok": True, "principal": principal()}
        self.verify_raises = None
        self.epoch_calls = []
        self.epochs = {42: {"active": True, "user": 3, "token": 0}, 43: {"active": True, "user": 5, "token": 0}}
        self.epochs_raise = False
        self.verifier_configured = True
        self.auth_mode = "rpc"
        self.has_audit_method = True
        # credential governance fakes (the facts methods of `configurations` and `secrets`, #6757)
        self.configurations = {}        # (project_id, uuid) -> {id, project_id, type, section, generation}
        self.references = []            # rows as `credential_use_facts` returns them, plus project_id/configuration_id
        self.grants = {}                # (project_id, grant key string) -> {grant_id, state, ...}
        self.permissions = {42: {"credentials.use"}, 43: {"credentials.use"}}
        self.permission_calls = []
        self.configuration_calls = []   # (project_id, uuids)
        self.secrets_calls = []         # (project_id, config_ids, grant_keys, ref_ids)
        self.missing_modules = set()    # plugin names whose module is not installed
        self.configurations_raises = None
        self.secrets_raises = None
        self.permissions_raise = None
        self.schemas = {}               # toolkit type -> schema (provider hub), as `find_toolkit_schema_by_type_everywhere`
        self.schema_calls = []
        # digests
        self.digests = {}
        self.fail_commit_once = None
        self.digest_calls = 0
        self.ns.adm._now = self.ns.real_now  # pylint: disable=W0212
        if hasattr(self.ns, "install_fakes"):
            self.ns.install_fakes(self)

    def poison(self):
        """Any use of the runtime dependencies from now on fails the test."""
        def boom(*args, **kwargs):
            raise AssertionError("a runtime dependency was used")
        self.sio_users = _Poison()
        self.ns.tools.auth.sio_users = self.sio_users
        self.ns.tools.auth.verify_identity = boom
        self.ns.tools.auth.read_identity_epochs = boom
        self.ns.tools.db.get_session = boom
        self.ns.tools.this.for_module = boom
        self.ns.flask.has_request_context = boom

    def unpoison(self):
        self.sio_users = {}
        self.ns.install_fakes(self)

    # --- databases -----------------------------------------------------------------------------

    def engine(self, project_id):
        if project_id not in self.engines:
            sa = self.ns.sa
            engine = sa.create_engine(f"sqlite:///{self.dir}/p{project_id}.db").execution_options(
                schema_translate_map={"tenant": None})
            # PostgreSQL's jsonb_array_length, used by a CHECK of the mandate table, for SQLite
            sa.event.listen(engine, "connect", lambda conn, _: conn.create_function(
                "jsonb_array_length", 1, lambda value: len(json.loads(value)), deterministic=True))
            self.ns.Base.metadata.create_all(engine)
            self.engines[project_id] = engine
        return self.engines[project_id]

    def get_session(self, project_id):
        env = self

        class _Scope:
            def __enter__(self_inner):
                self_inner.session = env.ns.CountingSession(env, bind=env.engine(project_id))
                env.sessions.append((project_id, self_inner.session))
                return self_inner.session

            def __exit__(self_inner, *exc):
                self_inner.session.close()
                return False

        return _Scope()

    def query(self, project_id, model, *where):
        with self.ns.sa_orm.Session(self.engine(project_id)) as session:
            statement = self.ns.sa.select(model)
            for clause in where:
                statement = statement.where(clause)
            return list(session.execute(statement).scalars())

    # --- seeding -------------------------------------------------------------------------------

    def add_version(self, project_id, version_id, application_id):
        with self.ns.sa_orm.Session(self.engine(project_id)) as session:
            session.add(self.ns.ApplicationVersion(id=version_id, application_id=application_id))
            session.commit()

    def add_binding(self, project_id, application_id, version_id, *, state="active", mode="elitea_native",
                    digest=None, epoch=0, binding_id=None):
        digest = digest or self.digests.setdefault(version_id, "adv1:" + f"{version_id:064x}")
        binding_id = binding_id or uuid.uuid4()
        row = self.ns.models.AgentIdentityBinding(
            id=binding_id, project_id=project_id, application_id=application_id,
            version_id=version_id, identity_mode=mode, definition_digest=digest, state=state,
            revocation_epoch=epoch,
            approval_ref="appr-1" if state == "active" else None,
            policy_revision="rev-1" if state == "active" else None,
        )
        with self.ns.sa_orm.Session(self.engine(project_id)) as session:
            session.add(row)
            session.commit()
        return binding_id

    def seed_agent(self, project_id=1, application_id=7, version_id=70, **binding):
        """An application version with one active binding; returns the binding id."""
        self.add_version(project_id, version_id, application_id)
        return self.add_binding(project_id, application_id, version_id, **binding)

    def add_toolkit(self, project_id, version_id, tool_id, kind, settings, selected=("run",)):
        """A toolkit row of a version, as the digest and the credential decision read it."""
        with self.ns.sa_orm.Session(self.engine(project_id)) as session:
            session.add(self.ns.EliteATool(id=tool_id, type=kind, settings=settings))
            session.add(self.ns.EntityToolMapping(
                tool_id=tool_id, entity_version_id=version_id, entity_type="agent", selected_tools=list(selected)))
            session.commit()

    def module(self, *, governed=True, **settings):
        adm = self.ns.adm
        return types.SimpleNamespace(
            governance_profile=types.SimpleNamespace(
                enabled=governed, policy_revision="rev-1" if governed else None,
                profile="governed" if governed else "legacy"),
            admission_settings=adm.AdmissionSettings(**settings),
            _context_by_task=OrderedDict(),
        )

    def request(self, **over):
        values = dict(project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="interactive",
                      entry_point="chat_sio", caller_type="user", caller_id=42, caller_reference="cookie-secret-value")
        values.update(over)
        return self.ns.AdmissionRequest(**values)


class _Poison(dict):
    def get(self, *args, **kwargs):
        raise AssertionError("sio_users was read")

    __getitem__ = get


def install(plugin_root: Path, tmp_path: Path) -> types.SimpleNamespace:  # pylint: disable=R0914,R0915
    ns = identity.install(plugin_root)
    sa = importlib.import_module("sqlalchemy")
    sa_orm = importlib.import_module("sqlalchemy.orm")
    postgresql = importlib.import_module("sqlalchemy.dialects.postgresql")
    compiler = importlib.import_module("sqlalchemy.ext.compiler")
    tools = ns.tools

    @compiler.compiles(postgresql.JSONB, "sqlite")
    def _jsonb(element, compiler_, **kw):  # pylint: disable=W0613
        return "JSON"

    base = tools.db.Base
    ns.log = RecordingLog()
    sys.modules["pylon.core.tools"].log = ns.log

    class ApplicationVersion(base):  # a stand-in with the two columns admission reads
        __tablename__ = "application_versions"
        __table_args__ = {"schema": "tenant"}
        id = sa.Column(sa.Integer, primary_key=True)
        application_id = sa.Column(sa.Integer, nullable=False)

    class EliteATool(base):  # stand-ins with the columns the credential decision reads
        __tablename__ = "elitea_tools"
        __table_args__ = {"schema": "tenant"}
        id = sa.Column(sa.Integer, primary_key=True)
        type = sa.Column(sa.String, nullable=False)
        settings = sa.Column(postgresql.JSONB, nullable=False)

    class EntityToolMapping(base):
        __tablename__ = "entity_tool_mapping"
        __table_args__ = {"schema": "tenant"}
        id = sa.Column(sa.Integer, primary_key=True)
        tool_id = sa.Column(sa.Integer, nullable=False)
        entity_version_id = sa.Column(sa.Integer, nullable=False)
        entity_type = sa.Column(sa.String, nullable=False)
        selected_tools = sa.Column(postgresql.JSONB, nullable=True)

    @compiler.compiles(sa.sql.elements.BinaryExpression, "sqlite")
    def _jsonb_contains(element, compiler_, **kw):
        """SQLite has no ``@>``: emulate the one probe the plugin sends, a list holding one ``credential_ref``."""
        if getattr(element.operator, "opstring", None) == "@>":
            left, right = compiler_.process(element.left, **kw), compiler_.process(element.right, **kw)
            return (f"EXISTS (SELECT 1 FROM json_each({left}) AS je WHERE "
                    f"json_extract(je.value, '$.credential_ref') = json_extract({right}, '$[0].credential_ref'))")
        return compiler_.visit_binary(element, **kw)

    for table in base.metadata.tables.values():  # PostgreSQL-only defaults do not exist on SQLite
        for column in table.columns:
            if column.server_default is not None and "jsonb" in str(getattr(column.server_default, "arg", "")):
                column.server_default = None

    for name in ("plugins", PKG, f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.models.enums", f"{PKG}.utils"):
        package = sys.modules.get(name) or types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    models_all = types.ModuleType(f"{PKG}.models.all")
    models_all.ApplicationVersion = ApplicationVersion
    sys.modules[f"{PKG}.models.all"] = models_all
    tool_models = types.ModuleType(f"{PKG}.models.elitea_tools")
    tool_models.EliteATool, tool_models.EntityToolMapping = EliteATool, EntityToolMapping
    sys.modules[tool_models.__name__] = tool_models
    enums = types.ModuleType(f"{PKG}.models.enums.all")
    enums.ToolEntityTypes = types.SimpleNamespace(agent=types.SimpleNamespace(value="agent"))
    sys.modules[enums.__name__] = enums
    # the real scanner of expanded configurations, lifted from the module that is too heavy to import here
    scanner = next(node for node in ast.parse((plugin_root / "utils" / "application_utils.py").read_text()).body
                   if isinstance(node, ast.FunctionDef) and node.name == "_find_configurations_in_settings")
    utils_application = types.ModuleType(f"{PKG}.utils.application_utils")
    exec(compile(ast.Module([scanner], []), "application_utils.py", "exec"), utils_application.__dict__)  # pylint: disable=W0122
    sys.modules[utils_application.__name__] = utils_application
    application_tools = types.ModuleType(f"{PKG}.utils.application_tools")
    sys.modules[application_tools.__name__] = application_tools
    ns.application_tools = application_tools
    tools.auth = types.SimpleNamespace()
    tools.this = types.SimpleNamespace()

    def load(kind, name):
        return load_module_with_stubs(plugin_root / kind / f"{name}.py", f"{PKG}.{kind.replace('/', '.')}.{name}")

    ns.exceptions = load("utils", "exceptions")
    ns.governance = load("utils", "governance")
    ns.envelope = load("models/pd", "task_envelope")
    ns.run_id = load("utils", "run_id")
    ns.pd_admission = load("models/pd", "admission")
    ns.caps = load("utils", "worker_capabilities")
    ns.credential_decision = load("utils", "credential_decision")
    ns.adm = load("utils", "admission")
    ns.binding_admin = load("utils", "binding_admin")
    ns.real_now = ns.adm._now  # pylint: disable=W0212

    class CountingSession(sa_orm.Session):
        def __init__(self, env, **kwargs):
            super().__init__(**kwargs)
            self.env = env
            self.commits = 0

        def commit(self):
            if self.env.fail_commit_once is not None:
                error, self.env.fail_commit_once = self.env.fail_commit_once, None
                raise error
            self.commits += 1
            return super().commit()

    ns.EliteATool, ns.EntityToolMapping = EliteATool, EntityToolMapping
    ns.sa, ns.sa_orm, ns.Base = sa, sa_orm, base
    ns.CountingSession = CountingSession
    ns.ApplicationVersion = ApplicationVersion
    ns.AdmissionRequest = ns.pd_admission.AdmissionRequest
    ns.postgresql = postgresql

    # --- the event model of the tracing plugin: audit events are validated against it, so it must exist ---
    event_model = find_tracing_event_model(plugin_root)
    if os.environ.get("ELITEA_TEST_NO_TRACING"):  # reproduce the single-plugin CI checkout locally
        event_model = None
    if event_model is None:
        # CI checks out this plugin alone: say so loudly (warning + the schema test skips) instead of passing silently
        warnings.warn("tracing model not found: no ancestor of elitea_core holds " + str(TRACING_EVENT_MODEL)
                      + "; audit events are not validated against SecurityAuditEventV1 in this run", stacklevel=1)
        ns.event_model = None
    else:
        ns.event_model = load_module_with_stubs(event_model, "tracing_security_audit_event_6756")

    # --- fakes ---------------------------------------------------------------------------------
    flask = types.ModuleType("flask")
    flask.g = types.SimpleNamespace()
    flask.has_request_context = lambda: False
    sys.modules["flask"] = flask
    ns.flask = flask

    def install_fakes(env):
        flask.has_request_context = lambda: env.in_request
        flask.g = env.g
        tools.db.get_session = env.get_session
        tools.auth.sio_users = env.sio_users

        def verify_identity(evidence, *, admitted_kinds):
            env.verify_calls.append((dict(evidence), frozenset(admitted_kinds)))
            if env.verify_raises:
                raise env.verify_raises
            return env.verify_result

        def read_identity_epochs(auth_type, auth_id):
            env.epoch_calls.append((auth_type, auth_id))
            if env.epochs_raise:
                raise RuntimeError("auth unreachable")
            return env.epochs.get(auth_id)

        tools.auth.verify_identity = verify_identity
        tools.auth.read_identity_epochs = read_identity_epochs
        tools.auth.auth_mode = env.auth_mode
        tools.auth.identity_verifier = types.SimpleNamespace(configured=lambda kind: env.verifier_configured)

        def validated(event):
            if ns.event_model is not None:
                ns.event_model.SecurityAuditEventV1.model_validate(event)
            return event

        def allowed_local():
            env.tracing_calls += 1
            if env.audit_allowed_raises:
                raise RuntimeError("outbox not available")
            return env.audit_allowed

        def record(session, event):
            env.tracing_calls += 1
            if env.record_raises:
                raise env.record_raises
            env.recorded.append((session, validated(event)))
            return {"event_id": event.get("event_id") or str(uuid.uuid4())}

        def record_autocommit(event):
            env.tracing_calls += 1
            if env.autocommit_raises:
                raise env.autocommit_raises
            env.denied.append(validated(event))
            return {"event_id": str(uuid.uuid4())}

        def tracing_module():
            method_names = {"security_audit_admission_allowed_local": allowed_local,
                            "security_audit_record": record,
                            "security_audit_record_autocommit": record_autocommit}
            if not env.has_audit_method:
                method_names.pop("security_audit_record")
            return types.SimpleNamespace(**method_names)

        def configuration_credential_facts(project_id, uuids):
            env.configuration_calls.append((project_id, list(uuids)))
            if env.configurations_raises:
                raise env.configurations_raises
            return {item: dict(env.configurations[(project_id, item)]) for item in uuids if (project_id, item) in env.configurations}

        def credential_use_facts(project_id, config_ids, grant_keys, ref_ids=None):
            env.secrets_calls.append((project_id, list(config_ids), list(grant_keys), None if ref_ids is None else list(ref_ids)))
            if env.secrets_raises:
                raise env.secrets_raises
            references, grants = {}, {}
            for row in env.references:
                if (row["project_id"] == project_id and row["state"] != "retired"
                        and (row["configuration_id"] in config_ids or row["ref_id"] in (ref_ids or []))):
                    references[row["configuration_id"]] = {
                        key: row[key] for key in ("ref_id", "state", "generation", "configuration_generation",
                                                  "store_kind", "ownership_class", "revocation_epoch")}
            for key in ("|".join(item) for item in grant_keys):
                grant = env.grants.get((project_id, key))
                if grant is not None and grant["state"] != "revoked":
                    grants[key] = dict(grant)
            return {"references": references, "grants": grants}

        def modules():
            return {
                "tracing": tracing_module(),
                "configurations": types.SimpleNamespace(configuration_credential_facts=configuration_credential_facts),
                "secrets": types.SimpleNamespace(credential_use_facts=credential_use_facts),
            }

        def for_module(name):
            if name in env.missing_modules:
                return types.SimpleNamespace(module=None)
            return types.SimpleNamespace(module=modules()[name])

        tools.this.for_module = for_module

        def get_user_permissions(user_id, mode, project_id):
            env.permission_calls.append((user_id, mode, project_id))
            if env.permissions_raise:
                raise env.permissions_raise
            return set(env.permissions.get(user_id, set()))

        tools.auth.get_user_permissions = get_user_permissions

        def find_schema(tool_type, project_id, user_id):
            env.schema_calls.append((tool_type, project_id, user_id))
            return env.schemas.get(tool_type), bool(env.schemas.get(tool_type))

        application_tools.find_toolkit_schema_by_type_everywhere = find_schema

        def digest_for_version(session, version_id):  # pylint: disable=W0613
            env.digest_calls += 1
            if version_id not in env.digests:
                raise ns.digest.DigestError("VERSION_NOT_FOUND", "no such version")
            return env.digests[version_id]

        ns.adm.digest_for_version = digest_for_version
        ns.binding_admin.digest_for_version = digest_for_version

    ns.install_fakes = install_fakes
    env = Env(ns, tmp_path)
    install_fakes(env)
    ns.env = env
    return ns
