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
import importlib
import sys
import types
import uuid
from collections import OrderedDict
from pathlib import Path

from fixtures import identity
from fixtures.helpers import load_module_with_stubs

PKG = identity.PKG
TRACING_EVENT_MODEL = Path("tracing") / "models" / "security_audit_event.py"


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

    for table in base.metadata.tables.values():  # PostgreSQL-only defaults do not exist on SQLite
        for column in table.columns:
            if column.server_default is not None and "jsonb" in str(getattr(column.server_default, "arg", "")):
                column.server_default = None

    for name in ("plugins", PKG, f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        package = sys.modules.get(name) or types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    models_all = types.ModuleType(f"{PKG}.models.all")
    models_all.ApplicationVersion = ApplicationVersion
    sys.modules[f"{PKG}.models.all"] = models_all
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

    ns.sa, ns.sa_orm, ns.Base = sa, sa_orm, base
    ns.CountingSession = CountingSession
    ns.ApplicationVersion = ApplicationVersion
    ns.AdmissionRequest = ns.pd_admission.AdmissionRequest
    ns.postgresql = postgresql

    # --- the event model of the tracing plugin, when it is checked out beside this one ---------
    event_model = plugin_root.parents[2] / TRACING_EVENT_MODEL
    ns.event_model = None
    if event_model.exists():
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
            return {"event_id": str(uuid.uuid4())}

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

        tools.this.for_module = lambda name: types.SimpleNamespace(module=tracing_module())

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
