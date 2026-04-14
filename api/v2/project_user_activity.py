"""
Project user activity endpoint.

Returns distinct users with activity in a project based on audit_events.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


if _API_AVAILABLE:
    from flask import request
    from sqlalchemy import func

    class AdminAPI(api_tools.APIModeHandler):
        """Admin API for per-project user activity."""

        @auth.decorators.check_api({
            "permissions": ["models.admin.audit_trail.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            }
        })
        def get(self, **kwargs):
            """
            GET /api/v2/elitea_core/project_user_activity/administration?project_id=X&date_from=ISO&date_to=ISO

            Returns list of users with event counts for a given project,
            optionally filtered by date range.
            """
            from datetime import datetime
            from tools import db
            from ...models.audit_event import AuditEvent

            project_id = request.args.get("project_id")
            if not project_id:
                return {"error": "project_id is required"}, 400

            try:
                project_id = int(project_id)
            except (ValueError, TypeError):
                return {"error": "project_id must be an integer"}, 400

            date_from = request.args.get("date_from")
            date_to = request.args.get("date_to")

            try:
                with db.with_project_schema_session(None) as db_session:
                    query = db_session.query(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                        func.count().label("event_count"),
                    ).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.user_id.isnot(None),
                    )

                    if date_from:
                        query = query.filter(
                            AuditEvent.timestamp >= datetime.fromisoformat(date_from)
                        )
                    if date_to:
                        query = query.filter(
                            AuditEvent.timestamp <= datetime.fromisoformat(date_to)
                        )

                    rows = query.group_by(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                    ).all()

                    return {
                        "rows": [
                            {
                                "user_id": row.user_id,
                                "user_email": row.user_email,
                                "event_count": row.event_count,
                            }
                            for row in rows
                        ],
                    }, 200

            except Exception as e:
                log.error(f"Project user activity query failed: {e}")
                return {"error": "Failed to query project user activity"}, 500

    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '',
        ])
        mode_handlers = {
            'administration': AdminAPI,
        }
else:
    API = None
