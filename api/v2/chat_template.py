from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, config as c, db

from pylon.core.tools import log

from ...models.chat_template import ChatTemplate
from ...models.pd.chat_template import ChatTemplateRead, ChatTemplateUpdate
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.project_context.edit"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, template_id: int, **kwargs):
        try:
            payload = ChatTemplateUpdate.model_validate(request.json)
        except ValidationError as e:
            return e.errors(include_url=False), 400

        with db.get_session(project_id) as session:
            template = session.query(ChatTemplate).filter(ChatTemplate.id == template_id).first()
            if not template:
                return {'error': 'Template not found.'}, 404

            name_conflict = (
                session.query(ChatTemplate)
                .filter(ChatTemplate.name.ilike(payload.name), ChatTemplate.id != template_id)
                .first()
            )
            if name_conflict:
                return {'error': 'A template with this name already exists.'}, 400

            template.name = payload.name
            template.participants = [p.model_dump() for p in payload.participants]
            session.commit()
            result = ChatTemplateRead.model_validate(template).model_dump(mode='json')

        return result, 200

    @auth.decorators.check_api({
        "permissions": ["models.project_context.edit"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, template_id: int, **kwargs):
        with db.get_session(project_id) as session:
            template = session.query(ChatTemplate).filter(ChatTemplate.id == template_id).first()
            if not template:
                return {'error': 'Template not found.'}, 404

            if template.is_default:
                return {'error': 'Cannot delete the default template. Set another template as default first.'}, 400

            session.delete(template)

        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/templates/<int:template_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
