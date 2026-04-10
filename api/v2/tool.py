import json

from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, config as c, db, register_openapi

from pylon.core.tools import log

from ...models.all import ApplicationVersion
from ...models.enums.events import ApplicationEvents
from ...models.elitea_tools import EliteATool, EntityToolMapping
from ...models.pd.tool import ToolDetails, ToolAPIUpdateModel

from ...utils.constants import PROMPT_LIB_MODE
from ...models.enums.all import ToolEntityTypes
from ...models.enums.all import AgentTypes

from ...utils.pipeline_utils import validate_yaml_from_str, from_str_to_yaml
from ...utils.application_tools import (
    toolkit_change_relation,
    ToolkitChangeRelationError,
    expand_toolkit_settings,
    ValidatorNotSupportedError,
    ConfigurationExpandError,
    wrap_provider_hub_secret_fields,
)


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, tool_id: int, **kwargs):
        # Check for expand parameter to return expanded credentials
        expand = request.args.get('expand', '').lower() == 'true'

        with db.with_project_schema_session(project_id) as session:
            result = session.query(EliteATool).filter(
                EliteATool.id == tool_id
            ).first()
            if not result:
                return {'error': f'No such tool with id {tool_id}'}, 400
            result = ToolDetails.from_orm(result)
            result.fix_name(project_id)
            result.set_online(project_id)
            result.set_agent_meta_and_fields(project_id)
            result.check_is_pinned(project_id)

            tool_data = result.model_dump(mode='json')

            # Expand credential configurations if requested
            if expand and tool_data.get('settings') and tool_data.get('type'):
                try:
                    user_id = auth.current_user()['id']
                    expanded_settings = expand_toolkit_settings(
                        tool_data['type'],
                        tool_data['settings'],
                        project_id=project_id,
                        user_id=user_id
                    )
                    tool_data['settings'] = expanded_settings
                    log.debug(f"Expanded toolkit settings for tool {tool_id}")
                except ValidatorNotSupportedError as e:
                    log.debug(f"Toolkit {tool_id} does not support settings expansion: {e}")
                except ConfigurationExpandError as e:
                    log.warning(f"Failed to expand credentials for tool {tool_id}: {e}")
                except Exception as e:
                    log.warning(f"Error expanding toolkit settings for tool {tool_id}: {e}")

            return tool_data, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id, tool_id):
        with db.with_project_schema_session(project_id) as session:
            if tool := session.query(EliteATool).get(tool_id):
                session.delete(tool)
                session.query(ApplicationVersion).filter(
                    ApplicationVersion.meta['attachment_toolkit_id'].astext == str(tool_id)
                ).update(
                    {
                        ApplicationVersion.meta: ApplicationVersion.meta.op('-')('attachment_toolkit_id')
                    }
                )
                session.commit()
                toolkit_data = ToolDetails.from_orm(tool)
                toolkit_data.fix_name(project_id)
                toolkit_data = toolkit_data.dict()
                toolkit_data['owner_id'] = project_id
                self.module.context.event_manager.fire_event(
                    ApplicationEvents.toolkit_deleted, toolkit_data
                )
                return '', 204
            return {"ok": False, "error": "Tool is not found"}, 400

    @register_openapi(
        name="Link Agent to Toolkit",
        description="Link an agent (application) to a toolkit.",
        mcp_tool=True
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.patch"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, tool_id: int):
        update_relation_data = dict(request.json)

        try:
            result = toolkit_change_relation(
                project_id=project_id,
                toolkit_id=tool_id,
                relation_data=update_relation_data
            )
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400
        except ToolkitChangeRelationError as e:
            return {'error': str(e)}, 400
        except Exception as e:
            log.exception(f"Error while changing toolkit relation: {str(e)}")
            return {'error': 'Can not change toolkit relation'}, 500

        return result, 201

    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.tool.update"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def put(self, project_id: int, tool_id: int):
        payload = dict(request.json)
        payload['project_id'] = project_id
        payload['user_id'] = auth.current_user()['id']

        try:
            update_data = ToolAPIUpdateModel.model_validate(payload)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        update_data.fix_name(project_id)

        with db.with_project_schema_session(project_id) as session:
            from tools import serialize, store_secrets
            from ...utils.toolkits_utils import get_mcp_schemas
            wrap_provider_hub_secret_fields(update_data.type, update_data.settings, project_id)
            store_secrets(update_data.dict(), project_id)

            user_id = auth.current_user()['id']
            if not update_data.meta.get('mcp') and update_data.type in get_mcp_schemas(project_id, user_id):
                update_data.meta['mcp'] = True

            old_tool = session.query(EliteATool).filter(
                EliteATool.id == tool_id
            ).first()

            if not old_tool:
                return {'error': f'No such tool with id {tool_id}'}, 400

            old_tool_parsed = ToolDetails.from_orm(old_tool)
            old_tool_parsed.fix_name(project_id)

            tool_query = session.query(EliteATool).filter(
                EliteATool.id == tool_id,
            )
            tool_query.update(
                serialize(update_data)
            )
            new_tool = tool_query.first()

            new_tool_parsed = ToolDetails.from_orm(new_tool)
            new_tool_parsed.fix_name(project_id)
            new_tool_parsed.set_online(project_id)
            new_tool_parsed.set_agent_meta_and_fields(project_id)

            toolkit_pipelines_query = session.query(ApplicationVersion).join(
                EntityToolMapping, ApplicationVersion.id == EntityToolMapping.entity_version_id
            ).filter(
                ApplicationVersion.agent_type == AgentTypes.pipeline.value,
                EntityToolMapping.tool_id == tool_id,
                EntityToolMapping.entity_type == ToolEntityTypes.agent.value
            )
            for pipeline in toolkit_pipelines_query.all():
                if not pipeline.instructions:
                    continue

                try:
                    instructions = validate_yaml_from_str(pipeline.instructions)
                except Exception:
                    return {
                        'error': f'Invalid pipeline instructions (pipeline ID: {pipeline.application.id}, version ID: {pipeline.id})'
                    }, 400

                for node in instructions.get('nodes', []):
                    tool_names = node.get('tool_names', {})
                    if old_tool_parsed.toolkit_name in tool_names:
                        tool_names[new_tool_parsed.toolkit_name] = tool_names.pop(old_tool_parsed.toolkit_name)
                new_instructions = from_str_to_yaml(instructions)
                session.query(ApplicationVersion).filter(
                    ApplicationVersion.id == pipeline.id,
                ).update(
                    {
                        'instructions': new_instructions,
                    }
                )
            session.commit()
            toolkit_data = new_tool_parsed.dict()
            toolkit_data['project_id'] = project_id
            self.module.context.event_manager.fire_event(
                ApplicationEvents.toolkit_updated,
                {
                    "id": tool_id,
                    "owner_id": project_id,
                    "data": toolkit_data
                }
            )
            return new_tool_parsed.model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:tool_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
