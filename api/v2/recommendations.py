#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from datetime import datetime, timedelta, timezone
from traceback import format_exc

from flask import request
from sqlalchemy import func, Integer, String, cast

from tools import api_tools, config as c, db, auth
from pylon.core.tools import log

from ...models.all import Application
from ...models.pd.application import ApplicationListModel
from ...utils.constants import PROMPT_LIB_MODE


def _query_audit_recommendations(project_id, user_id=None, limit=5, days=30):
    """Query audit_events for most-used applications.

    Returns list of dicts with 'application_id' and 'usage_count', or None on failure.
    """
    try:
        from pylon.core.tools import db_support
        db_support.create_local_session()
        try:
            with db.with_project_schema_session(None) as session:
                from sqlalchemy import text
                schema = c.POSTGRES_SCHEMA

                filters = [
                    f"{schema}.audit_events.entity_type = 'application'",
                    f"{schema}.audit_events.project_id = :project_id",
                    f"{schema}.audit_events.timestamp >= :since",
                ]
                params = {
                    "project_id": project_id,
                    "since": datetime.now(timezone.utc) - timedelta(days=days),
                }

                if user_id is not None:
                    filters.append(f"{schema}.audit_events.user_id = :user_id")
                    params["user_id"] = user_id

                where_clause = " AND ".join(filters)
                query = text(f"""
                    SELECT entity_id, COUNT(*) as usage_count
                    FROM {schema}.audit_events
                    WHERE {where_clause}
                      AND entity_id IS NOT NULL
                    GROUP BY entity_id
                    ORDER BY usage_count DESC
                    LIMIT :limit
                """)
                params["limit"] = limit

                rows = session.execute(query, params).fetchall()
                if rows:
                    return [
                        {"application_id": int(row[0]), "usage_count": int(row[1])}
                        for row in rows
                    ]
        finally:
            db_support.close_local_session()
    except Exception as e:
        log.debug(f"audit_events query failed: {e}")
    return None


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.applications.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int | None = None, **kwargs):
        """
        Get recommended applications for a user based on usage analytics.

        Fallback logic:
        1. Most used applications by this user in this project (from audit_events)
        2. Most used applications in this project overall (from audit_events)
        3. First N applications sorted alphabetically

        Query parameters:
        - limit: Number of applications to return (default: 5, max: 20)
        - days: Number of days to look back (default: 30)
        """
        user_id = auth.current_user().get("id")
        if not user_id:
            return {
                "ok": False,
                "error": "User not authenticated"
            }, 401

        if not project_id:
            return {
                "ok": False,
                "error": "Project ID is required"
            }, 400

        limit = min(request.args.get("limit", default=5, type=int), 20)
        days = request.args.get("days", default=30, type=int)

        try:
            # Step 1: User-specific recommendations from audit_events
            user_recs = _query_audit_recommendations(project_id, user_id=user_id, limit=limit, days=days)
            if user_recs:
                result = self._get_applications_by_ids(project_id, user_recs, limit)
                if result:
                    return result

            # Step 2: Project-wide recommendations from audit_events
            project_recs = _query_audit_recommendations(project_id, user_id=None, limit=limit, days=days)
            if project_recs:
                result = self._get_applications_by_ids(project_id, project_recs, limit)
                if result:
                    return result

            # Step 3: Fallback to alphabetical order
            return self._get_applications_alphabetically(project_id, limit)

        except Exception as e:
            log.error(f'Recommendations API error: {format_exc()}')
            return {
                "ok": False,
                "error": str(e)
            }, 500

    def _get_applications_by_ids(self, project_id: int, recommendations: list, limit: int):
        """Get application details for recommended IDs. Returns response tuple or None."""
        application_ids = [rec['application_id'] for rec in recommendations if rec.get('application_id')]
        if not application_ids:
            return None

        with db.get_session(project_id) as session:
            applications = (
                session.query(Application)
                .filter(
                    Application.id.in_(application_ids),
                    Application.owner_id == project_id
                )
                .all()
            )

            if not applications:
                return None

            app_map = {app.id: ApplicationListModel.from_orm(app) for app in applications}
            rec_map = {rec['application_id']: rec['usage_count'] for rec in recommendations}

            result = []
            for app_id in application_ids:
                if app_id in app_map:
                    app = app_map[app_id]
                    result.append({
                        'id': app.id,
                        'name': app.name,
                        'description': app.description,
                        'agent_type': app.agent_type,
                        'icon_meta': app.icon_meta,
                        'created_at': app.created_at.isoformat() if app.created_at else None,
                        'usage_count': rec_map.get(app_id, 0),
                        'recommendation_source': 'usage_analytics'
                    })
                if len(result) >= limit:
                    break

            if not result:
                return None

            return {
                'applications': result,
                'total': len(result)
            }, 200

    def _get_applications_alphabetically(self, project_id: int, limit: int):
        """Fallback: Get applications sorted alphabetically"""
        with db.get_session(project_id) as session:
            applications = (
                session.query(Application)
                .filter(Application.owner_id == project_id)
                .order_by(func.lower(Application.name))
                .limit(limit)
                .all()
            )

            if not applications:
                return {
                    'applications': [],
                    'total': 0,
                    'message': 'No applications available in this project'
                }, 200

            result = []
            for app in applications:
                app = ApplicationListModel.from_orm(app)
                result.append({
                    'id': app.id,
                    'name': app.name,
                    'description': app.description,
                    'agent_type': app.agent_type,
                    'icon_meta': app.icon_meta,
                    'created_at': app.created_at.isoformat() if app.created_at else None,
                    'usage_count': 0,
                    'recommendation_source': 'alphabetical'
                })

            return {
                'applications': result,
                'total': len(result)
            }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "",
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
