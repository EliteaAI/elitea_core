from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, db, config as c
from pylon.core.tools import log

from ...models.project_context import ProjectContext
from ...models.pd.project_context import ProjectContextDetail, ProjectContextUpdate
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.project_context.view"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        with db.get_session(project_id) as session:
            ctx = session.query(ProjectContext).first()
            if ctx is None:
                return {"id": None, "content": "", "enabled": True, "updated_at": None}, 200
            return ProjectContextDetail.model_validate(ctx).model_dump(mode='json'), 200

    @auth.decorators.check_api({
        "permissions": ["models.project_context.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, **kwargs):
        try:
            parsed = ProjectContextUpdate.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        with db.get_session(project_id) as session:
            ctx = session.query(ProjectContext).first()
            if ctx is None:
                ctx = ProjectContext(content=parsed.content, enabled=parsed.enabled)
                session.add(ctx)
            else:
                ctx.content = parsed.content
                ctx.enabled = parsed.enabled
            session.commit()
            session.refresh(ctx)
            return ProjectContextDetail.model_validate(ctx).model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/project-context',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
