"""What the agent identity binding REST endpoints share (#7105, C-1009): permissions, request parsing,
readable refusals. Nothing here touches the database."""
from pydantic import ValidationError
from tools import auth, config as c

from ..models.agent_identity import BINDING_STATES, IDENTITY_MODES

VIEW = "governance.bindings.view"
MANAGE = "governance.bindings.manage"
APPROVE = "governance.bindings.approve"
PERMISSIONS = (VIEW, MANAGE, APPROVE)
OPENAPI_TAG = "elitea_core/governance"
MAX_BODY_BYTES = 8 * 1024
DEFAULT_LIMIT, MAX_LIMIT, MAX_SEARCH = 20, 50, 128

# The status of each code (design-operator-ui-slice1 §4): 403 permission and governance refusals, 400 validation,
# 404 unknown or foreign id, 409 a state that does not fit, 503 a dependency that is not there
STATUS = {
    "INVALID_REQUEST": 400, "APPROVAL_REQUIRED": 400, "DIGEST_REQUIRED": 400,
    "UNAUTHENTICATED": 401,
    "FORBIDDEN": 403, "SELF_APPROVAL": 403, "DENIED_BINDING": 403,
    "NOT_FOUND": 404, "VERSION_NOT_FOUND": 404,
    "PAYLOAD_TOO_LARGE": 413,
    "INVALID_STATE": 409, "CONCURRENT_CHANGE": 409, "BINDING_EXISTS": 409, "INVALID_TRANSITION": 409,
    "BINDING_DIGEST_MISMATCH": 409, "DIGEST_UNSTABLE": 409,
    "AUDIT_UNAVAILABLE": 503, "DEPENDENCY_UNAVAILABLE": 503,
}
MESSAGES = {
    "INVALID_REQUEST": "the request is not valid",
    "UNAUTHENTICATED": "sign in to continue",
    "FORBIDDEN": "you are not allowed to do this",
    "NOT_FOUND": "no such record",
    "PAYLOAD_TOO_LARGE": "the request body is too large",
    "DEPENDENCY_UNAVAILABLE": "the request could not be completed; nothing was changed",
}


def check_spec(*permissions):
    """The ``check_api`` descriptor: platform administration only, the permissions are any-of."""
    return {
        "permissions": list(permissions),
        "recommended_roles": {c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False}},
    }


def refusal(code, message=None):
    """``({error, code}, status)``: a fixed readable message, never an id, a value or a stack trace."""
    return {"error": message or MESSAGES.get(code, "the request was refused"), "code": code}, STATUS.get(code, 500)


def action_refusal(exc):
    """The response for a ``BindingActionError``."""
    return refusal(exc.code, exc.message)


def current_actor():
    """The signed-in user's id (the token's owner for a token), or None."""
    user = auth.current_user() or {}
    actor = user.get("id")
    return actor if isinstance(actor, int) and not isinstance(actor, bool) and actor > 0 else None


def has_permission(permission, project_id):
    return permission in auth.resolve_permissions(mode=c.ADMINISTRATION_MODE, project_id=project_id)


def parse_body(request, model):
    """``(model instance, None)`` or ``(None, response)``. The size is checked before the body is parsed."""
    length = request.content_length
    if length is None or length > MAX_BODY_BYTES:
        return None, refusal("PAYLOAD_TOO_LARGE")
    raw = request.get_json(silent=True)
    if not isinstance(raw, dict):
        return None, refusal("INVALID_REQUEST", "the request body must be a JSON object")
    try:
        return model.model_validate(raw), None
    except ValidationError as exc:   # only the model's own field names are echoed, never a value
        errors = exc.errors()
        fields = sorted({error["loc"][0] for error in errors
                         if error["loc"] and error["loc"][0] in model.model_fields and error["type"] != "extra_forbidden"})
        message = "the request body is not valid"
        if fields:
            message += ": invalid " + ", ".join(fields)
        if any(error["type"] == "extra_forbidden" for error in errors):
            message += "; unknown field"
        return None, refusal("INVALID_REQUEST", message)


def _int_arg(args, name, default, low, high):
    try:
        return max(low, min(int(args.get(name, default)), high))
    except (TypeError, ValueError):
        return default


def page_args(args, sorts, default_sort):
    """``(params, None)`` or ``(None, response)``: ``limit`` (default 20, at most 50), ``offset``, ``search`` (at most
    128 characters), ``sort_by`` (only the whitelisted names, else the default), ``sort_order``."""
    search = (args.get("search") or "").strip()
    if len(search) > MAX_SEARCH:
        return None, refusal("INVALID_REQUEST", f"search is longer than {MAX_SEARCH} characters")
    sort_by = args.get("sort_by")
    return {
        "limit": _int_arg(args, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT),
        "offset": _int_arg(args, "offset", 0, 0, 10 ** 9),
        "search": search or None,
        "sort_by": sort_by if sort_by in sorts else default_sort,
        "sort_order": "asc" if args.get("sort_order") == "asc" else "desc",
    }, None


def list_filters(args):
    """``(filters, None)`` or ``(None, response)`` for ``state``, ``identity_mode`` and ``application_id``."""
    filters = {}
    for name, allowed in (("state", BINDING_STATES), ("identity_mode", IDENTITY_MODES)):
        value = args.get(name)
        if value:
            if value not in allowed:
                return None, refusal("INVALID_REQUEST", f"{name} is not one of: {', '.join(allowed)}")
            filters[name] = value
    application_id = args.get("application_id")
    if application_id:
        if not application_id.isdigit() or int(application_id) > 2 ** 31 - 1:
            return None, refusal("INVALID_REQUEST", "application_id must be a positive integer")
        filters["application_id"] = int(application_id)
    return filters, None
