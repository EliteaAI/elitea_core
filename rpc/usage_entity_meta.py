from pylon.core.tools import web, log
from sqlalchemy import func

from tools import db  # pylint: disable=E0401

from ..models.all import Application, ApplicationVersion
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


class RPC:
    @web.rpc("elitea_core_usage_entity_meta", "usage_entity_meta")
    def usage_entity_meta(
            self, project_id: int, application_ids: list | None = None, version_ids: list | None = None,
            application_refs: list | None = None,
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
        result = {"applications": [], "versions": [], "scoped_applications": []}
        if not application_ids and not version_ids and not refs:
            return result
        try:
            public_project_id = get_public_project_id()
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
