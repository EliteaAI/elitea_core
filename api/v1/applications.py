from traceback import format_exc

import json

from flask import request
from pydantic import ValidationError

from sqlalchemy.orm import selectinload
from tools import api_tools, config as c, db, auth, serialize

from ...models.pd.application import (
    ApplicationCreateModel,
    ApplicationDetailModel,
    ApplicationVersionDetailModel,
    MultipleApplicationListModel,
)

from ...models.all import ApplicationVersion
from ...utils.create_utils import create_application
from ...utils.application_utils import list_applications_api
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


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
        collection = {
            "id": request.args.get('collection_id', type=int),
            "owner_id": request.args.get('collection_owner_id', type=int)
        }
        with db.get_session(project_id) as session:
            some_result = list_applications_api(
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
                statuses=request.args.get('statuses'),
                collection=collection,
                agents_type=request.args.get('agents_type'),
                without_tags=request.args.get('without_tags', False),
                session=session
            )
        try:
            parsed = MultipleApplicationListModel(applications=some_result['applications'])
            return {
                'total': some_result['total'],
                'rows': [
                    serialize(i)
                    for i in parsed.applications
                ]
            }, 200
        except Exception as e:
            log.error(f'application list exc\n{format_exc()}')
            return {
                "ok": False,
                "error": str(e)
            }, 400

    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.applications.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int | None = None, **kwargs):
        raw = dict(request.json)
        raw["owner_id"] = project_id
        author_id = auth.current_user().get("id")
        raw['project_id'] = project_id
        raw['user_id'] = author_id
        for version in raw.get("versions", []):
            version["author_id"] = author_id
        try:
            application_data = ApplicationCreateModel.model_validate(raw)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        with db.get_session(project_id) as session:
            application = create_application(application_data, session, project_id)
            session.commit()

            # Explicitly load relationships for the first version since they are now lazy
            first_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == application.versions[0].id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables)
            ).first()

            result = ApplicationDetailModel.from_orm(application)
            result.version_details = ApplicationVersionDetailModel.from_orm(first_version)

            return result.model_dump(mode='json'), 201


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes(
        [
            "",
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
