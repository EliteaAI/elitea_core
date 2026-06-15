#!/usr/bin/python3
# coding=utf-8
# pylint: disable=W0201

#   Copyright 2024-2025 EPAM Systems
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

""" API """

from flask import request  # pylint: disable=E0401

from pydantic import ValidationError  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0401,E0611
from tools import api_tools, auth, config as c, register_openapi  # pylint: disable=E0401

from ...models.pd.predict_llm import LLMPredictRequest  # pylint: disable=E0402
from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError


class PromptLibAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    """ API """

    @register_openapi(
        name="Send a message directly to an LLM model without invoking any agent or pipeline — stateless, no tools, no version ID required, uses project default model if none specified",
        description="Send a message directly to an LLM model without invoking any agent or pipeline — stateless, no tools, no version ID required, uses project default model if none specified.",
        mcp_description="""
        USE for raw, stateless LLM inference: testing prompts, simple Q&A, translation, formatting. Use when no agent tools, memory, or pipeline execution are needed.
        DO NOT USE when you need tools, memory, or pipeline execution → use execute_agent (POST /predict/{version_id}).
        
        Key difference from execute_agent: no version_id, no tools, no graph, stateless.
        
        Examples:
        1. Simple call (project default model):
        { 'user_input': 'What is the capital of France?' }
        
        2. Specific model + system prompt:
        { 'user_input': 'Translate: Hello', 'instructions': 'Return translation only.', 'llm_settings': { 'model_name': 'gpt-4o', 'temperature': 0.0 } }
        
        3. Multi-turn with history:
        { 'user_input': 'What was my last question?', 'chat_history': [{ 'role': 'user', 'content': 'What is 2+2?' }, { 'role': 'assistant', 'content': '4' }] }
        
        4. Async call: { 'user_input': 'Write a long essay...', 'await_task_timeout': 0 }""",
        mcp_tool=True,
        tags=["elitea_core/applications"],
        request_body=LLMPredictRequest,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.predict.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        """ 
        Execute LLM prediction with a chat message.
        
        Supports two payload formats:
        - Nested: llm_settings object with model_name, temperature, max_tokens, etc.
        - Flat: model_name, temperature, max_tokens at top level (auto-converted to llm_settings)
        
        If llm_settings.model_name is not provided, uses the project's default model.
        
        Args:
            project_id: Project ID
            
        Returns:
            - 200: Task started or completed
            - 400: Validation error or missing llm_settings without default
            - 500: Internal error
        """
        try:
            # Validate request against schema
            predict_request = LLMPredictRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        # Convert to dict for RPC call
        request_json = predict_request.model_dump(exclude_unset=False, exclude={"return_chat_history"})
        request_json['project_id'] = project_id

        # Extract for RPC call
        await_task_timeout = request_json.get("await_task_timeout", 30)
        sid = request_json.get("sid")

        #
        self.module.not_starting_task_event.clear()
        #
        try:
            result: dict = self.module.predict_sio_llm(
                sid=sid,
                data=request_json,
                await_task_timeout=await_task_timeout,
                user_id=auth.current_user().get("id"),
                return_chat_history=predict_request.return_chat_history,
            )
        except ValidationError as e:
            return e.errors(), 400
        except PredictPayloadError as e:
            return {"error": str(e)}, 400
        except PermissionError as e:
            return {"error": str(e)}, 403
        except PoolSaturationError as e:
            return {
                "error": "temporarily_unavailable",
                "message": "The service is busy processing other requests. Please try again in a few seconds.",
                "retry_after": e.retry_after,
            }, 503
        except BaseException as exc:  # pylint: disable=W0718
            log.exception("LLM Predict error: %s", exc)
            return {"error": "Can not do LLM predict"}, 500
        finally:
            self.module.not_starting_task_event.set()
        #
        if not isinstance(result, dict):
            log.error("predict_sio_llm returned unexpected result: %r", result)
            return {"error": "LLM predict failed: empty result"}, 500
        #
        response_code = 200
        if "error" in result and result["error"] is not None:
            response_code = 400
        #
        return result, response_code


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """

    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
