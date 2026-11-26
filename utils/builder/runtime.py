"""Builds a BuilderService for one request (#7383): catalog from the openapi registry, in-process dispatch,
the project changeset table and the caller's write permission. Every find / read carries its type's playbook; the
SDK BuilderMiddleware passes each playbook to the model once per conversation."""
from pylon.core.tools import log
from tools import auth, config as c, openapi_registry, this

from . import pipelines
from .catalog import BUILDER_TAGS, Catalog
from .dispatch import make_call
from .prompts import playbook
from .service import BuilderService
from .store import DbStore

WRITE_PERMISSIONS = frozenset({
    "models.applications.applications.create", "models.applications.version.update",
    "models.applications.skills.create", "models.project_context.edit",
})


def _can_write(project_id) -> bool:
    try:
        held = auth.resolve_permissions(mode=c.DEFAULT_MODE, project_id=project_id)
    except Exception:  # pylint: disable=W0703
        log.warning("[builder] permissions of the caller could not be read in project %s", project_id)
        return False
    return bool(WRITE_PERMISSIONS & set(held or ()))


def _max_starters():
    try:
        from ..conversation_starters_utils import get_max_conversation_starters  # pylint: disable=C0415
        return get_max_conversation_starters()
    except Exception:  # pylint: disable=W0703
        return 4


def _material_checks(project_id):
    """(#7228) Under a run token: ``(listed, material)`` from the internal OBO credential-material rules, so the Builder
    lists and proposes what the plain MCP listing and call would. Without a run token: ``(None, None)``. Under a run
    token without those rules every operation proposal is refused: the check fails closed."""
    from ..mcp_service import _run_token_claims  # pylint: disable=C0415
    from .. import internal_obo  # pylint: disable=C0415
    claims = _run_token_claims()
    if claims is None:
        return None, None
    if not hasattr(internal_obo, "material_of_call"):
        log.warning("[builder] no credential-material rules under a run token; operation proposals are refused")
        return None, lambda tool, args: True

    def listed(tool):
        return internal_obo.listed_for_run(tool.get("method"), tool.get("path"))

    def material(tool, args):
        path = tool.get("path") or ""
        path_params = {k: v for k, v in args.items() if "{" + k + "}" in path}
        path_params.setdefault("project_id", project_id)
        body = {k: v for k, v in args.items() if k not in path_params}
        try:
            return bool(internal_obo.material_of_call(tool.get("method"), path, path_params, body, claims.get("uid")))
        except Exception:  # pylint: disable=W0703
            log.warning("[builder] the credential-material check failed; refused")
            return True

    return listed, material


def _tool_schemas(toolkit_types) -> dict:
    """``{toolkit_type: {tool_name: json_schema}}`` for built-in toolkit types (#6388).

    Read from the schemas the module already holds, so the pipeline check costs no remote call. MCP and provider
    hub toolkits are left out: their tools are listed by a live connection, and their parameters are not checked.
    """
    held = getattr(this.module, "toolkit_schemas", None) or {}
    found = {}
    for kind in toolkit_types:
        selected = ((held.get(kind) or {}).get("properties") or {}).get("selected_tools") or {}
        schemas = selected.get("args_schemas") or {}
        if schemas:
            found[kind] = schemas
    return found


def build_service(project_id, user_id, seen_playbooks=None) -> BuilderService:
    listed, material = _material_checks(project_id)
    catalog = Catalog(openapi_registry.get_mcp_api_tools(filter_tags=list(BUILDER_TAGS), public_only=True),
                      listed=listed)
    return BuilderService(
        project_id=project_id, user_id=user_id, call=make_call(project_id, catalog), store=DbStore(project_id),
        catalog=catalog, can_write=_can_write(project_id), seen_playbooks=seen_playbooks,
        max_starters=_max_starters(), material=material, tool_schemas=_tool_schemas,
    )


def reference(topic: str) -> dict:
    if topic.startswith("playbook:"):
        text = playbook(topic.split(":", 1)[1])
        return {"status": "ok", "topic": topic, "text": text} if text else {
            "status": "invalid", "message": f"unknown topic '{topic}'"}
    if topic.startswith(pipelines.TOPIC_PREFIX):
        # The SDK knowledge pack (#6388): one entry per page, generated from the node registry. The SDK
        # BuilderMiddleware keeps each page only in its first result in the conversation.
        found = pipelines.reference_pages(topic)
        result = {"status": "ok", "topic": topic, "reference_pages": found["pages"]}
        if found["unknown"]:
            result["unknown_pages"] = found["unknown"]
            result["valid_pages"] = [t[len(pipelines.TOPIC_PREFIX):] for t in pipelines.topics()]
        if found["not_loaded"]:
            result["not_loaded"] = found["not_loaded"]
            result["message"] = f"At most {pipelines.MAX_TOPICS_PER_CALL} pages per call; ask again for the rest."
        return result
    return {"status": "invalid", "message": "topic must be playbook:<type> or pipeline:<topic>, e.g. "
                                            "pipeline:overview,node:llm",
            "topics": pipelines.topics()}


def handle(project_id, user_id, action, body) -> dict:
    if action == "reference":
        return reference(body["topic"])
    service = build_service(project_id, user_id)
    if action == "find":
        return service.find(body["type"], body.get("query") or "", body.get("limit") or 10)
    if action == "read":
        return service.read(body["type"], body.get("id"), body.get("version_id"))
    if action == "propose":
        return service.propose(body["type"], body["op"], body.get("changes"), entity_id=body.get("id"),
                               version_id=body.get("version_id"), entity_name=body.get("name"),
                               base_revision=body.get("base_revision"))
    if action == "apply":
        return service.apply(body["changeset_id"])
    raise ValueError(action)


def confirm_changeset(project_id, user_id, changeset_id) -> bool:
    """HITL resume (S3): the user clicked Apply on the Changes card."""
    return BuilderService(project_id=project_id, user_id=user_id, call=None, store=DbStore(project_id),
                          catalog=None, can_write=True).confirm(changeset_id, user_id)
