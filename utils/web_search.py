""" Web Search toggle: toolkit resolution, availability and injection (#6982) """

from typing import Optional

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from tools import rpc_tools, this  # pylint: disable=E0401

from .tool_spend_features import feature_defaults, feature_map, toolkit_feature

WEB_SEARCH_FEATURE = "web_search"
WEB_SEARCH_INTERNAL_TOOL_KEY = "web_search"
WEB_SEARCH_TOOLKIT_NAME = "Web Search"
SETTINGS_TYPE = "web_search_settings"
_RPC_TIMEOUT = 3
_ADMIN_ROLE_NAMES = frozenset({"admin", "super_admin", "system"})


def _public_project_id() -> int:
    from .utils import get_public_project_id  # pylint: disable=C0415
    return int(get_public_project_id())


def _settings_toolkit_id(project_id: int) -> Optional[int]:
    config = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).configurations_get_first_filtered_project(
        project_id=project_id, filter_fields={"type": SETTINGS_TYPE, "elitea_title": SETTINGS_TYPE},
    )
    toolkit_id = ((config or {}).get("data") or {}).get("toolkit_id")
    return toolkit_id if isinstance(toolkit_id, int) and not isinstance(toolkit_id, bool) else None


def _load_toolkit_row(project_id: int, toolkit_id: int) -> Optional[dict]:
    from tools import db  # pylint: disable=C0415,E0401
    from ..models.elitea_tools import EliteATool  # pylint: disable=C0415
    with db.get_session(project_id) as session:
        row = session.query(EliteATool).filter(EliteATool.id == toolkit_id).first()
        if row is None:
            return None
        return {"id": row.id, "name": row.name, "type": row.type, "meta": dict(row.meta or {})}


def resolve_web_search_toolkit() -> Optional[dict]:
    """{owner_project_id, toolkit_id, name, type}: the public project's shared pick, else None."""
    public_id = _public_project_id()
    toolkit_id = _settings_toolkit_id(public_id)
    if toolkit_id is None:
        return None
    row = _load_toolkit_row(public_id, toolkit_id)
    if row is None:
        log.warning("web_search: platform toolkit %s missing", toolkit_id)
        return None
    if toolkit_feature({"type": row["type"]}, feature_map(this.module.toolkit_schemas)) != WEB_SEARCH_FEATURE:
        log.warning("web_search: platform toolkit %s is not a web-search toolkit", toolkit_id)
        return None
    if not row["meta"].get("shared"):
        log.warning("web_search: platform toolkit %s is not shared, ignored", toolkit_id)
        return None
    return {"owner_project_id": public_id, "toolkit_id": toolkit_id, "name": row["name"], "type": row["type"]}


def web_search_availability(project_id: Optional[int], user_id: Optional[int]) -> dict:
    """{available, reason, toolkit}; reason in disabled_platform, disabled_project, no_toolkit, not_ready."""
    if not feature_defaults(this.module.descriptor.config, WEB_SEARCH_FEATURE).get("enabled", False):
        return {"available": False, "reason": "disabled_platform", "toolkit": None}
    if WEB_SEARCH_FEATURE not in feature_map(this.module.toolkit_schemas):
        return {"available": False, "reason": "not_ready", "toolkit": None}
    if project_id is not None:
        limits = this.module.get_effective_feature_limits(project_id, user_id, WEB_SEARCH_FEATURE)
        if not limits.get("feature_enabled"):
            return {"available": False, "reason": "disabled_project", "toolkit": None}
    resolved = resolve_web_search_toolkit()
    if resolved is None:
        return {"available": False, "reason": "no_toolkit", "toolkit": None}
    if resolved["type"] not in (this.module.toolkit_schemas or {}):
        return {"available": False, "reason": "not_ready", "toolkit": None}
    return {"available": True, "reason": None, "toolkit": {"id": resolved["toolkit_id"], "name": resolved["name"]}}


def _chat_tools(toolkit_type: str) -> list:
    for backend in ((feature_map(this.module.toolkit_schemas).get(WEB_SEARCH_FEATURE) or {}).get("backends") or {}).values():
        if backend.get("toolkit_type") == toolkit_type:
            return list(backend.get("chat_tools") or [])
    return []


def inject_internal_web_search_tool(
        internal_tools: list, user_id: int, project_id: int, existing_tools: list,
) -> Optional[dict]:
    """A never-persisted copy of the resolved toolkit, expanded in its owner project; None when unavailable."""
    if WEB_SEARCH_INTERNAL_TOOL_KEY not in (internal_tools or []):
        return None
    try:
        features = feature_map(this.module.toolkit_schemas)
        if any(toolkit_feature(tool, features) == WEB_SEARCH_FEATURE for tool in existing_tools or []):
            return None
        availability = web_search_availability(project_id, user_id)
        if not availability["available"]:
            log.info("web_search: unavailable for project %s (%s)", project_id, availability["reason"])
            return None
        resolved = resolve_web_search_toolkit()
        if resolved is None:
            return None
        from .application_tools import ValidatorNotSupportedError  # pylint: disable=C0415
        from .predict_utils import get_toolkit_config  # pylint: disable=C0415
        try:
            toolkit = get_toolkit_config(resolved["owner_project_id"], user_id, resolved["toolkit_id"])
        except ValidatorNotSupportedError:
            log.warning("web_search: toolkit schema %s not registered yet, temporarily unavailable", resolved["type"])
            return None
        if "error" in toolkit:
            log.warning("web_search: toolkit %s could not be loaded", resolved["toolkit_id"])
            return None
        if not (toolkit.get("meta") or {}).get("shared"):
            return None
        chat_tools = _chat_tools(toolkit["type"])
        settings = dict(toolkit.get("settings") or {})
        template_tools = settings.get("selected_tools") or chat_tools
        selected = [name for name in template_tools if name in chat_tools]
        # An empty list means "all tools" to the SDK, which would expose crawl/map
        if not selected:
            log.warning("web_search: toolkit %s selects no chat tools, skipped", resolved["toolkit_id"])
            return None
        settings["selected_tools"] = selected
        return {
            "type": toolkit["type"], "name": WEB_SEARCH_TOOLKIT_NAME, "toolkit_name": WEB_SEARCH_TOOLKIT_NAME,
            "description": toolkit.get("description") or "", "settings": settings,
            "id": None, "agent_type": None, "variables": [],
        }
    except Exception as e:  # pylint: disable=W0703
        log.warning("web_search: injection failed for project %s: %s", project_id, e)
        return None


def is_public_admin(project_id: int, user_id: int) -> bool:
    try:
        roles = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).admin_get_user_roles(
            project_id=project_id, user_id=user_id,
        ) or []
        return any(role.get("name") in _ADMIN_ROLE_NAMES for role in roles)
    except Exception as e:  # pylint: disable=W0703
        log.warning("web_search: admin role lookup failed for user %s: %s", user_id, e)
        return False


def shared_toolkit_error(project_id: int, user_id: int, settings: dict, meta: dict,
                         was_shared: bool = False) -> Optional[str]:
    """Why this toolkit may not carry meta.shared, or None when it may."""
    shared = bool((meta or {}).get("shared"))
    if not shared and not was_shared:
        return None
    if project_id != _public_project_id():
        return "Only toolkits in the public project can be shared with all projects"
    if not is_public_admin(project_id, user_id):
        return "Only public project admins can change sharing"
    if shared:
        for value in (settings or {}).values():
            if isinstance(value, dict) and set(value.keys()) == {"elitea_title", "private"} and value.get("private"):
                return "A shared toolkit must use a project credential, not a private one"
    return None
