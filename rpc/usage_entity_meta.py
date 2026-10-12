import uuid

from pylon.core.tools import web, log
from sqlalchemy import func

from tools import db  # pylint: disable=E0401

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.skill import Skill, SkillVersion
from ..models.enums.all import AgentTypes
from ..utils.utils import get_public_project_id

KIND_AGENT = "agent"
KIND_PIPELINE = "pipeline"


def _kind(is_pipeline) -> str:
    return KIND_PIPELINE if is_pipeline else KIND_AGENT


def _applications(project_id: int, application_ids) -> dict:
    """{app_id: {'id', 'name', 'kind'}} for the ids found in project_id's schema."""
    if not application_ids:
        return {}
    with db.get_session(project_id) as session:
        rows = session.query(
            Application.id,
            Application.name,
            func.bool_or(ApplicationVersion.agent_type == AgentTypes.pipeline.value),
        ).outerjoin(
            ApplicationVersion, ApplicationVersion.application_id == Application.id,
        ).filter(
            Application.id.in_(sorted(application_ids)),
        ).group_by(Application.id, Application.name).all()
    return {
        app_id: {"id": app_id, "name": name, "kind": _kind(is_pipeline)}
        for app_id, name, is_pipeline in rows
    }


def pick_default_version(skill_meta: dict, versions: list):
    default_id = (skill_meta or {}).get("default_version_id")
    return next((v for v in versions if v[0] == default_id), None) or next(
        (v for v in versions if v[1] == "base"), None,
    )


def fetch_skills(project_id: int, skill_ids) -> dict:
    if not skill_ids:
        return {}
    with db.get_session(project_id) as session:
        skills = session.query(Skill.id, Skill.name, Skill.meta).filter(Skill.id.in_(sorted(skill_ids))).all()
        versions = session.query(
            SkillVersion.id, SkillVersion.skill_id, SkillVersion.name, SkillVersion.meta,
        ).filter(SkillVersion.skill_id.in_(sorted(skill_ids))).order_by(SkillVersion.id).all()
    versions_by_skill = {}
    for version_id, skill_id, name, meta in versions:
        versions_by_skill.setdefault(skill_id, []).append((version_id, name, meta or {}))
    result = {}
    for skill_id, name, meta in skills:
        skill_versions = versions_by_skill.get(skill_id, [])
        default = pick_default_version(meta, skill_versions)
        result[skill_id] = {
            "id": skill_id,
            "project_id": project_id,
            "name": name,
            "icon_meta": (default[2].get("icon_meta") if default else None) or {},
            "versions": [{"id": version_id, "name": version_name} for version_id, version_name, _ in skill_versions],
        }
    return result


def collect_skill_refs(skill_refs) -> set:
    return {
        (int(ref_project), int(skill_id))
        for ref_project, skill_id in skill_refs or []
        if ref_project is not None and skill_id is not None
    }


def fetch_scoped_skills(project_id: int, public_project_id: int, skill_refs: set) -> list:
    scoped = []
    for skill_project_id in sorted({p for p, _ in skill_refs} & {project_id, public_project_id}):
        found = fetch_skills(skill_project_id, {i for p, i in skill_refs if p == skill_project_id})
        scoped.extend({**meta, "is_public": skill_project_id == public_project_id} for meta in found.values())
    return scoped


def parse_uuids(values) -> set:
    parsed = set()
    for value in values or []:
        try:
            parsed.add(uuid.UUID(str(value)))
        except (TypeError, ValueError):
            continue
    return parsed


def resolve_conversation_ids(project_id: int, conversation_uuids) -> list:
    wanted = parse_uuids(conversation_uuids)
    if not wanted:
        return []
    with db.get_session(project_id) as session:
        rows = session.query(Conversation.id, Conversation.uuid).filter(Conversation.uuid.in_(wanted)).all()
    return [{"uuid": str(conversation_uuid), "id": conversation_id} for conversation_id, conversation_uuid in rows]


class RPC:
    @web.rpc("elitea_core_usage_entity_meta", "usage_entity_meta")
    def usage_entity_meta(
            self, project_id: int, application_ids: list | None = None, version_ids: list | None = None,
            application_refs: list | None = None, skill_refs: list | None = None,
            conversation_uuids: list | None = None,
    ) -> dict:
        """Names and agent/pipeline kind for the ids usage analytics grouped by (#6678).

        usage_event stores no root name and always writes root_entity_type='application', so the
        pipeline/agent split is only knowable from ApplicationVersion.agent_type. Deleted ids are
        simply absent from the result.

        application_refs are (schema project, app id) pairs from usage_event.root_entity_project_id
        (#6902) and come back under "scoped_applications" keyed by the requested pair. Only this
        project and the public project are read. application_ids carry no schema (legacy rows).
        Either way an id this project's schema lacks is retried in the public project's schema:
        a public agent run here predates the column, at the price of a same-id collision.
        """
        application_ids = {int(i) for i in application_ids or [] if i is not None}
        version_ids = sorted({int(i) for i in version_ids or [] if i is not None})
        refs = {
            (int(ref_project), int(app_id))
            for ref_project, app_id in application_refs or []
            if ref_project is not None and app_id is not None
        }
        wanted_skills = collect_skill_refs(skill_refs)
        wants_applications = bool(application_ids or version_ids or refs)
        result = {"applications": [], "versions": [], "scoped_applications": []}
        if skill_refs is not None:
            result["scoped_skills"] = []
        if conversation_uuids is not None:
            result["conversations"] = []
        if not wants_applications and not wanted_skills and not conversation_uuids:
            return result
        try:
            public_project_id = get_public_project_id()
            if wanted_skills:
                result["scoped_skills"] = fetch_scoped_skills(project_id, public_project_id, wanted_skills)
            if conversation_uuids:
                result["conversations"] = resolve_conversation_ids(project_id, conversation_uuids)
            if not wants_applications:
                return result
            #
            local_ids = application_ids | {i for p, i in refs if p == project_id}
            local = _applications(project_id, local_ids)
            if public_project_id == project_id:
                public = local
            else:
                public_ids = {i for p, i in refs if p == public_project_id} | (local_ids - local.keys())
                public = _applications(public_project_id, public_ids)
            #
            result["applications"] = [
                local.get(i) or public[i] for i in sorted(application_ids) if i in local or i in public
            ]
            for ref_project, app_id in sorted(refs):
                if ref_project == project_id:
                    meta = local.get(app_id) or public.get(app_id)
                elif ref_project == public_project_id:
                    meta = public.get(app_id)
                else:
                    meta = None
                if meta:
                    result["scoped_applications"].append({**meta, "project_id": ref_project})
            #
            if version_ids:
                with db.get_session(project_id) as session:
                    rows = session.query(
                        ApplicationVersion.id,
                        ApplicationVersion.name,
                        ApplicationVersion.agent_type,
                        Application.id,
                        Application.name,
                    ).join(
                        Application, ApplicationVersion.application_id == Application.id,
                    ).filter(
                        ApplicationVersion.id.in_(version_ids),
                    ).all()
                result["versions"] = [
                    {
                        "id": version_id,
                        "name": version_name,
                        "application_id": app_id,
                        "application_name": app_name,
                        "kind": _kind(agent_type == AgentTypes.pipeline.value),
                    }
                    for version_id, version_name, agent_type, app_id, app_name in rows
                ]
            return result
        except Exception:  # pylint: disable=broad-except
            log.exception("usage entity meta lookup failed: project %s", project_id)
            raise
