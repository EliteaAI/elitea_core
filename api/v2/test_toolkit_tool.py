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

""" API for Testing Toolkit Tools """
from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, config as c, serialize

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioValidationError

from ...models.pd.test_toolkit_tool import TestToolkitToolInputModel

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.patch"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        """
        Test a toolkit tool with given parameters.
        
        Args:
            project_id: Project ID
            
        Returns:
            Dictionary with task ID or result
        """
        raw = dict(request.json)
        await_response = request.args.get('await_response', 'true').lower() == 'true'
        # Configurable timeout with sensible default (5 minutes for sync, no timeout for async)
        timeout = int(request.args.get('timeout', '300' if await_response else '-1'))
        
        # Add project_id to the request data from URL parameter
        raw['project_id'] = project_id
        
        try:
            test_data = TestToolkitToolInputModel.model_validate(raw)
        except ValidationError as e:
            return {"error": e.errors()}, 400
        
        log.debug(f'Test toolkit tool request: {test_data.model_dump()}')
        
        # SID is optional for async calls - it's only needed for Socket.IO streaming
        # If not provided, the caller will need to poll the task status
        
        if await_response:
            test_data.sid = None
        
        try:
            result = self.module.test_toolkit_tool_sio(
                test_data.sid, test_data.model_dump(), "test_toolkit_tool",
                await_task_timeout=timeout
            )
        except SioValidationError as e:
            return {'error': str(e.error)}, 400
        except Exception as e:
            log.error(f"Error in test_toolkit_tool API: {str(e)}")
            return {'error': str(e)}, 500
        
        task_id = result.get('task_id')
        if await_response:
            if not result.get('result'):
                # Stop the task if it didn't complete in time
                try:
                    self.module.task_node.stop_task(task_id)
                except:
                    pass
                return {"error": "Timeout"}, 400
        
        return serialize(result), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
