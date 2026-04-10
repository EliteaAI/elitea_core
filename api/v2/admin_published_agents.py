"""Admin dashboard: list all published agents with adoption metrics."""

from flask import request
from sqlalchemy.orm import selectinload

from pylon.core.tools import log
from tools import api_tools, auth, db

from ...models.all import Application, ApplicationVersion
from ...models.enums.all import PublishStatus
from ...utils.utils import get_public_project_id


class AdminAPI(api_tools.APIModeHandler):
    """GET /api/v2/admin_published_agents/administration — list published agents."""

    @auth.decorators.check_api(["runtime.admin.published_agents"])
    @api_tools.endpoint_metrics
    def get(self):
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
        sort = request.args.get('sort', 'created_at')

        public_project_id = get_public_project_id()

        with db.get_session(public_project_id) as session:
            query = (
                session.query(Application)
                .options(selectinload(Application.versions))
                .filter(
                    Application.versions.any(
                        ApplicationVersion.status == PublishStatus.published,
                    )
                )
            )

            total = query.count()

            # Sorting
            order_col = getattr(Application, 'created_at', None)
            if sort == 'name':
                order_col = Application.name
            query = query.order_by(order_col.desc())

            applications = query.offset((page - 1) * page_size).limit(page_size).all()

            items = []
            for app in applications:
                published_versions = []
                for v in app.versions:
                    if v.status != PublishStatus.published:
                        continue
                    published_versions.append({
                        'version_id': v.id,
                        'version_name': v.name,
                        'published_at': v.created_at.isoformat() if v.created_at else None,
                        'published_by': (v.meta or {}).get('published_by'),
                    })

                adoption = (app.meta or {}).get('adoption', {})
                items.append({
                    'public_agent_id': app.id,
                    'name': app.name,
                    'description': app.description,
                    'author_project_id': app.shared_owner_id,
                    'published_versions': published_versions,
                    'total_published_versions': len(published_versions),
                    'adoption': {
                        'conversation_count': adoption.get('conversation_count', 0),
                        'project_count': adoption.get('project_count', 0),
                    },
                    'created_at': app.created_at.isoformat() if app.created_at else None,
                })

        return {
            'items': items,
            'total': total,
            'page': page,
            'page_size': page_size,
        }


class API(api_tools.APIBase):
    url_params = [
        "<string:mode>",
    ]

    mode_handlers = {
        'administration': AdminAPI,
    }
