from pylon.core.tools import web, log
from sqlalchemy import func

from tools import db  # pylint: disable=E0401

from ..models.all import Application, ApplicationVersion
from ..models.enums.all import AgentTypes

KIND_AGENT = "agent"
KIND_PIPELINE = "pipeline"


def _kind(is_pipeline) -> str:
    return KIND_PIPELINE if is_pipeline else KIND_AGENT


class RPC:
    @web.rpc("elitea_core_usage_entity_meta", "usage_entity_meta")
    def usage_entity_meta(
            self, project_id: int, application_ids: list | None = None, version_ids: list | None = None,
    ) -> dict:
        """Names and agent/pipeline kind for the ids usage analytics grouped by (#6678).

        usage_event stores no root name and always writes root_entity_type='application', so the
        pipeline/agent split is only knowable from ApplicationVersion.agent_type. Deleted ids are
        simply absent from the result.
        """
        application_ids = sorted({int(i) for i in application_ids or [] if i is not None})
        version_ids = sorted({int(i) for i in version_ids or [] if i is not None})
        result = {"applications": [], "versions": []}
        if not application_ids and not version_ids:
            return result
        try:
            with db.get_session(project_id) as session:
                if application_ids:
                    rows = session.query(
                        Application.id,
                        Application.name,
                        func.bool_or(ApplicationVersion.agent_type == AgentTypes.pipeline.value),
                    ).outerjoin(
                        ApplicationVersion, ApplicationVersion.application_id == Application.id,
                    ).filter(
                        Application.id.in_(application_ids),
                    ).group_by(Application.id, Application.name).all()
                    result["applications"] = [
                        {"id": app_id, "name": name, "kind": _kind(is_pipeline)}
                        for app_id, name, is_pipeline in rows
                    ]
                if version_ids:
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
