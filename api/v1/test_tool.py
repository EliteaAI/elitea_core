from yaml import dump as yaml_dump

from flask import request
from pydantic import ValidationError
from pydantic.v1 import ValidationError as ValidationErrorV1
from tools import api_tools, auth, config as c, this, db, serialize

from ...models.elitea_tools import EliteATool, EntityToolMapping
from ...models.enums.all import AgentTypes
from ...models.pd.application import ApplicationCreateModel, ApplicationDetailModel
from ...models.pd.tool import ToolDetails, TestToolInputModel
from ...models.pd.version import ApplicationVersionBaseCreateModel

from ...utils.create_utils import create_application
from ...utils.application_utils import  get_application_version_details_expanded

from ...models.pd.llm import LLMSettingsModel
from ...utils.constants import PROMPT_LIB_MODE
from ...models.enums.all import ToolEntityTypes
from ...utils.sio_utils import SioEvents, SioValidationError

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.test_tool.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, tool_id: str, **kwargs):
        raw = dict(request.json)
        raw['call_type'] = request.args.get('call_type', 'tool')
        await_response = request.args.get('await_response', 'true').lower() == 'true'

        try:
            test_tool_data = TestToolInputModel.parse_obj(raw)
        except (ValidationError, ValidationErrorV1) as e:
            return {"error": e.errors()}, 400

        log.debug(f'Test tool data: {test_tool_data=}')

        # SID is optional for async calls - it's only needed for Socket.IO streaming
        # If not provided, the caller will need to poll the task status
        if await_response:
            test_tool_data.sid = None

        with db.with_project_schema_session(project_id) as session:
            elitea_tool = session.query(EliteATool).filter(
                EliteATool.id == tool_id
            ).first()
            if not elitea_tool:
                return {'error': f'No such tool with id {tool_id}'}, 400
            elitea_tool = ToolDetails.from_orm(elitea_tool)
            elitea_tool.fix_name(project_id)
            elitea_tool.set_agent_type(project_id)

            raw = {
              "entry_point": test_tool_data.testing_name,
              "nodes": [
                {
                  "id": test_tool_data.testing_name,
                  "type": 'function',
                  "tool": f"{test_tool_data.tool}",
                  "toolkit_name": f"{elitea_tool.toolkit_name}",
                  "input": test_tool_data.input,
                  "output": test_tool_data.output,
                  "structured_output": test_tool_data.structured_output,
                  "input_mapping": test_tool_data.input_mapping,
                  "transition": test_tool_data.transition,
                  "input_variables": test_tool_data.input_variables,
                }
              ]
            }
            log.debug(f'Tool schema: {raw=}')

            yaml_string = yaml_dump(raw)
            log.debug(f'YAML string: {yaml_string=}')

            # if a test tool type is function, we should find the default integration to get llm settings
            # if test_tool_data.testing_type == TestingTypes.function.value and not test_tool_data.llm_settings:
            #     ai_integrations = self.module.context.rpc_manager.call.integrations_get_all_integrations_by_section(
            #         project_id, "ai"
            #     )
            #     ai_integrations = [
            #         integration.dict(
            #             exclude={'section'}
            #         ) for integration in ai_integrations
            #     ]
            #     if ai_integrations:
            #         ai_integration_settings = find_chat_completion_model_llm_settings(ai_integrations)
            #         if ai_integration_settings:
            #             test_tool_data.llm_settings = LLMSettingsModel(
            #                 **ai_integration_settings
            #             )
            #         else:
            #             return {'error': 'No integration with chat completion capability found'}, 400
            #     else:
            #         return {'error': 'You need to have at least one integration in AI section'}, 400

            try:
                user_id = auth.current_user().get("id")
                application_data = ApplicationCreateModel(
                    name='Temporary agent (for the tool testing purpose)',
                    description='Testing tool technical agent. It will be eliminated.',
                    owner_id=project_id,
                    project_id=project_id,
                    user_id=user_id,
                    versions=[
                        ApplicationVersionLatestCreateModel(
                            project_id=project_id,
                            user_id=user_id,
                            author_id=user_id,
                            agent_type=AgentTypes.pipeline.value,
                            instructions=yaml_string,
                            # llm_settings=test_tool_data.llm_settings,
                            meta={'is_temporary': True},
                            tools=[elitea_tool.dict()]
                        ).dict()
                    ]
                )
            except (ValidationError, ValidationErrorV1) as e:
                return e.errors(), 400

            application = create_application(application_data, session, project_id)
            session.flush()

            application_tool_to_application = EntityToolMapping(
                tool_id=elitea_tool.id,
                entity_version_id=application.get_latest_version().id,
                entity_id=application.id,
                entity_type=ToolEntityTypes.agent
            )
            session.add(application_tool_to_application)
            session.flush()

            temporary_agent = ApplicationDetailModel.from_orm(application)
            version_id = temporary_agent.versions[0].id

            try:
                temporary_version_details_expanded = get_application_version_details_expanded(
                    project_id=project_id,
                    application_id=temporary_agent.id,
                    version_id=version_id,
                    user_id=user_id,
                    unsecret=True,
                    session=session,

                )
            except Exception as e:
                log.error(f'Error while getting version details expanded for temporary agent: {e}')
                return {'error': 'Error while getting version details expanded for temporary agent'}, 500

            session.rollback()

        temporary_version_details_expanded.pop('application_id', None)
        temporary_version_details_expanded.pop('id', None)

        payload = {
            'version_details': temporary_version_details_expanded,
            'llm_settings': temporary_version_details_expanded.get('llm_settings'),
            'project_id': project_id,
            'user_input': test_tool_data.user_input,
            'ai_model': test_tool_data.user_input,
        }

        try:
            result = self.module.predict_sio(
                test_tool_data.sid, payload, SioEvents.test_tool.value,
                await_task_timeout=300 if await_response else -1
            )
        except SioValidationError as e:
            return {'error': str(e.error)}, 400

        task_id = result.get('task_id')
        if await_response:
            if not result.get('result'):
                self.task_node.stop_task(task_id)
                return {"error": "Timeout"}, 400
        return serialize(result), 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:tool_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
