"""The admission harness plus the autonomous mandate modules (#6947, C-1009).

Loads the real mandate state, principal, report and operator-task files next to the admission modules of
``admission_env`` and runs them against its SQLite files, so the conditional UPDATEs, the constraints and the
partial unique index are executed, not mocked. Only the Pylon runtime is faked (``auth.get_user`` here, the rest
in ``admission_env``). The caller saves ``sys.modules`` before ``install`` and restores it after.
"""
import types
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fixtures import admission_env
from fixtures.helpers import load_module_with_stubs

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
OWNER = 5


def install(plugin_root: Path, tmp_path: Path) -> types.SimpleNamespace:
    ns = admission_env.install(plugin_root, tmp_path)

    def load(name):
        return load_module_with_stubs(plugin_root / "utils" / f"{name}.py", f"{admission_env.PKG}.utils.{name}")

    ns.mandate_admin = load("mandate_admin")
    ns.trigger_report = load("trigger_report")
    ns.Mandate = ns.models.AutonomousMandate
    ns.users = {}
    ns.projects = [1]   # what ``project_list`` answers to the owner-event scan
    ns.tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
        timeout=lambda seconds: types.SimpleNamespace(
            project_list=lambda filter_=None: [{"id": project_id} for project_id in ns.projects]))))
    ns.mandate_owner = load("mandate_owner")
    return ns


def prepare(ns, env):
    """Per test: a clean harness, the clock pinned to ``NOW``, and the digest and user fakes in place."""
    env.reset()
    ns.users.clear()
    ns.users[OWNER] = {"id": OWNER, "idp_issuer": "https://issuer.example", "idp_tid": "tid-1", "idp_oid": "oid-5"}
    ns.mandate_admin._now = lambda: NOW  # pylint: disable=W0212
    ns.mandate_state._now = lambda: NOW  # pylint: disable=W0212
    ns.mandate_admin.digest_for_version = lambda session, version_id: env.digests[version_id]

    def get_user(user_id):
        if user_id not in ns.users:
            raise RuntimeError("No such user found")
        return dict(ns.users[user_id])

    ns.tools.auth.get_user = get_user
    env.epochs[OWNER] = {"active": True, "user": 1, "token": 0}
    # what admission of a mandate run asks of the identity service about the owner
    env.idp_links[("https://issuer.example", "tid-1", "oid-5")] = {"id": OWNER}
    env.permissions[OWNER] = {"models.applications.predict.post", "credentials.use"}
    ns.adm._now = lambda: NOW  # pylint: disable=W0212
    return env


def add_mandate(env, ns, project_id=1, *, version=70, application=7, state="draft", owner=OWNER, created_by=3,
                approved_by=None, binding_ref=None, operations=("run:schedule",), valid_until=None, revision=1,
                runtime=600, epoch=0, suspended_reason=None):
    """One mandate row; active and suspended rows get the approval the table demands. Returns its id."""
    if state in ("active", "suspended") and approved_by is None:
        approved_by = 1
    mandate_id = uuid.uuid4()
    row = ns.Mandate(
        id=mandate_id, project_id=project_id, application_id=application, agent_version=version,
        agent_binding_ref=binding_ref or uuid.uuid4(), owner_user_id=owner,
        owner_user_ref={"user_id": owner, "issuer": "https://issuer.example", "tid": "tid-1", "oid": f"oid-{owner}"},
        allowed_operations=list(operations), allowed_targets=["*"], max_runtime_s=runtime,
        valid_until=valid_until or NOW + timedelta(days=30), state=state, suspended_reason=suspended_reason,
        approval_ref="CHG-1" if approved_by else None, policy_revision="rev-1" if approved_by else None,
        created_by=created_by, approved_by=approved_by, revision=revision, revocation_epoch=epoch,
    )
    with ns.sa_orm.Session(env.engine(project_id)) as session:
        session.add(row)
        session.commit()
    return mandate_id


def mandate_row(env, ns, mandate_id, project_id=1):
    [row] = env.query(project_id, ns.Mandate, ns.Mandate.id == mandate_id)
    return row
