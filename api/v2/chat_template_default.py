from tools import api_tools, auth, config as c, db

from ...models.chat_template import ChatTemplate
from ...models.pd.chat_template import ChatTemplateRead
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.project_context.edit"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, template_id: int, **kwargs):
        with db.get_session(project_id) as session:
            target = session.query(ChatTemplate).filter(ChatTemplate.id == template_id).first()
            if not target:
                return {'error': 'Template not found.'}, 404

            if target.is_default:
                return ChatTemplateRead.model_validate(target).model_dump(mode='json'), 200

            # Clear current default, set new one
            session.query(ChatTemplate).filter(ChatTemplate.is_default == True).update(  # noqa: E712
                {'is_default': False}
            )
            target.is_default = True
            session.commit()
            result = ChatTemplateRead.model_validate(target).model_dump(mode='json')

        return result, 200

    @auth.decorators.check_api({
        "permissions": ["models.project_context.edit"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, template_id: int, **kwargs):
        # Unset default — a project may have no default template
        with db.get_session(project_id) as session:
            target = session.query(ChatTemplate).filter(ChatTemplate.id == template_id).first()
            if not target:
                return {'error': 'Template not found.'}, 404

            if target.is_default:
                target.is_default = False
                session.commit()
            result = ChatTemplateRead.model_validate(target).model_dump(mode='json')

        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/templates/<int:template_id>/set-default',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
