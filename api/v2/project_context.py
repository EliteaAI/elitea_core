from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, config as c, rpc_tools

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
        config = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_first_filtered_project(
            project_id=project_id,
            filter_fields={'type': 'project_context'},
        )
        return ProjectContextDetail.from_config(config).model_dump(mode='json'), 200

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

        config = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_first_filtered_project(
            project_id=project_id,
            filter_fields={'type': 'project_context'},
        )

        rpc = rpc_tools.RpcMixin().rpc.timeout(5)
        if config is None:
            result, _ = rpc.configurations_create_if_not_exists(payload={
                'project_id': project_id,
                'elitea_title': f'project_context_{project_id}',
                'label': 'Project Context',
                'type': 'project_context',
                'data': {'content': parsed.content, 'enabled': parsed.enabled},
            })
        else:
            result = rpc.configurations_update(
                project_id=project_id,
                config_id=config['id'],
                payload={'data': {'content': parsed.content, 'enabled': parsed.enabled}},
            )

        return ProjectContextDetail.from_config(result).model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/project-context',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
