import json

from flask import request

from tools import api_tools, config as c, db, auth

from ...models.pd.application import (
    MultiplePublishedApplicationListModel,
)

from ...utils.application_utils import list_applications_api
from ...models.enums.all import PublishStatus
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.utils import add_public_project_id


class PromptLibAPI(api_tools.APIModeHandler):
    @add_public_project_id
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.public_applications.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, *, project_id: int | None = None, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            result = list_applications_api(
                project_id=project_id,
                tags=request.args.get('tags'),
                author_id=request.args.get('author_id'),
                q=request.args.get('query'),
                limit=request.args.get("limit", default=10, type=int),
                offset=request.args.get("offset", default=0, type=int),
                sort_by=request.args.get("sort_by", default="created_at"),
                sort_order=request.args.get("sort_order", default='desc'),
                my_liked=request.args.get('my_liked', False),
                trend_start_period=request.args.get('trend_start_period'),
                trend_end_period=request.args.get('trend_end_period'),
                statuses=[PublishStatus.published],
                agents_type=request.args.get('agents_type'),
                without_tags=request.args.get('without_tags', False),
                session=session
            )

        try:
            parsed = MultiplePublishedApplicationListModel(applications=result['applications'])
            return {
                'total': result['total'],
                'rows': [
                    i.model_dump(mode='json')  # Direct dict conversion, no double serialization
                    for i in parsed.applications
                ]
            }, 200
        except Exception as e:
            return {
                "error": str(e)
            }, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
