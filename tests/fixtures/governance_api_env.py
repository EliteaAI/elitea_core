"""The governance switch endpoints and state, run against the admission harness (#7120, C-1009).

``install`` adds to ``admission_env``: the settings models on SQLite (the public schema is an attached database), the
two API files loaded from disk with a faked HTTP layer, and a ``check_api`` that decides like the platform does (the
actor's roles in administration mode against the endpoint's ``recommended_roles``), so the authorization matrix is run
against the permissions the endpoints really declare.
"""
import sys
import types
from pathlib import Path

from fixtures import admission_env
from fixtures.helpers import load_module_with_stubs

PKG = admission_env.PKG
ADMINISTRATION = "administration"


class FakeRequest:
    def __init__(self):
        self.args, self.body, self.content_length = {}, None, None

    def get_json(self, silent=False):  # pylint: disable=W0613
        return self.body


class Actor:
    """Who is calling: ``roles`` in administration mode, ``extra`` permissions granted outside the role matrix (a
    matrix edit), ``scope`` the permissions a token is limited to (None: not a token)."""

    def __init__(self, user_id=1, roles=("super_admin",), authenticated=True, extra=(), scope=None):
        self.id, self.roles, self.authenticated = user_id, list(roles), authenticated
        self.extra, self.scope = set(extra), scope


def install(plugin_root: Path, tmp_path: Path) -> types.SimpleNamespace:
    ns = admission_env.install(plugin_root, tmp_path)
    tools, env = ns.tools, ns.env
    request = FakeRequest()
    sys.modules["flask"].request = request
    ns.request = request
    ns.actor = Actor()
    ns.users = {1: {"id": 1, "email": "root@example.test"}, 2: {"id": 2, "email": "ops@example.test"}}
    ns.roles = {1: ["super_admin"], 2: ["admin"]}
    ns.fired = []

    def check_api(config):
        permissions = config["permissions"]
        matrix = config["recommended_roles"][ADMINISTRATION]

        def decorator(func):
            def wrapper(*args, **kwargs):
                actor = ns.actor
                if not actor.authenticated:
                    return {"error": "unauthorized"}, 401
                if actor.scope is not None and not set(permissions) <= actor.scope:
                    return {"error": "forbidden"}, 403
                granted = set(permissions) <= actor.extra or any(matrix.get(role) for role in actor.roles)
                if not granted:
                    return {"error": "forbidden"}, 403
                return func(*args, **kwargs)
            wrapper.declared = config
            return wrapper
        return decorator

    class APIModeHandler:
        module = None

    tools.auth.decorators = types.SimpleNamespace(check_api=check_api)
    tools.auth.current_user = lambda: {"id": ns.actor.id}
    tools.auth.get_user_roles = lambda user_id, mode: list(ns.roles.get(user_id, []))
    tools.auth.list_users = lambda user_ids=None: [dict(u) for u in ns.users.values() if u["id"] in (user_ids or [])]
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=APIModeHandler, APIBase=object, endpoint_metrics=lambda func: func)
    tools.register_openapi = lambda **kwargs: (lambda func: func)
    tools.config.ADMINISTRATION_MODE = ADMINISTRATION
    for name in (f"{PKG}.api", f"{PKG}.api.v2"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    ns.api_settings = load_module_with_stubs(plugin_root / "api" / "v2" / "governance_settings.py",
                                             f"{PKG}.api.v2.governance_settings")
    ns.api_history = load_module_with_stubs(plugin_root / "api" / "v2" / "governance_settings_history.py",
                                            f"{PKG}.api.v2.governance_settings_history")
    ns.env = env
    return ns


def create_tables(ns):
    ns.settings_models.METADATA.create_all(ns.env.engine(None))


def state_for(ns, module, *, ceiling="enforce", source="default", env_environment=None):
    """A real ``GovernanceState`` that reads the harness database and runs the real preflight on ``module``."""
    gs = ns.governance_settings
    return gs.GovernanceState(
        ceiling=gs.Ceiling(ceiling, source), env_environment=env_environment, context_ttl_s=3600,
        preflight=lambda read: ns.adm.governance_preflight(module, table_read=read))


def api_module(ns, *, ceiling="enforce", env_environment=None):
    """The module the endpoints run on: the harness module plus a real state and a recording event manager."""
    module = ns.env.module(governed=False)
    module.governance_state = state_for(ns, module, ceiling=ceiling, env_environment=env_environment)
    module.context = types.SimpleNamespace(event_manager=types.SimpleNamespace(
        fire_event=lambda name, payload: ns.fired.append((name, payload))))
    ns.fired.clear()
    return module


def call(ns, module, method, body=None, handler="settings", **args):
    """The handler's answer ``(body, status)`` to a request from ``ns.actor``."""
    ns.request.args, ns.request.body, ns.request.content_length = args, body, None
    api = ns.api_settings if handler == "settings" else ns.api_history
    instance = api.AdminAPI()
    instance.module = module
    return getattr(instance, method)()
