"""Builds a BuilderService for one request (#7383): catalog from the openapi registry, in-process dispatch,
the project changeset table and the caller's write permission. Every find / read carries its type's playbook; the
SDK BuilderMiddleware passes each playbook to the model once per conversation."""
from pylon.core.tools import log
from tools import auth, config as c, openapi_registry

from ..service_prompt_utils import get_service_prompt
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


def build_service(project_id, user_id, seen_playbooks=None) -> BuilderService:
    catalog = Catalog(openapi_registry.get_mcp_api_tools(filter_tags=list(BUILDER_TAGS), public_only=True))
    return BuilderService(
        project_id=project_id, user_id=user_id, call=make_call(project_id, catalog), store=DbStore(project_id),
        catalog=catalog, can_write=_can_write(project_id), seen_playbooks=seen_playbooks,
        max_starters=_max_starters(),
    )


def reference(topic: str) -> dict:
    if topic.startswith("playbook:"):
        text = playbook(topic.split(":", 1)[1])
        return {"status": "ok", "topic": topic, "text": text} if text else {
            "status": "invalid", "message": f"unknown topic '{topic}'"}
    if topic.startswith("pipeline:"):
        text = get_service_prompt("pipeline_authoring_reference")
        return {"status": "ok", "topic": topic, "text": text} if text else {
            "status": "error", "message": "The pipeline authoring reference is not configured."}
    return {"status": "invalid", "message": "topic must be playbook:<type> or pipeline:overview"}


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
