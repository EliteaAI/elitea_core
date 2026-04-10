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

from pydantic.v1 import ValidationError  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0401,E0611
from tools import api_tools, auth, config as c  # pylint: disable=E0401

from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.predict_utils import PredictPayloadError


class PromptLibAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    """ API """

    @auth.decorators.check_api({
        "permissions": ["models.applications.predict.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        """ LLM predict endpoint """
        request_json = dict(request.json)
        #
        await_task_timeout = request_json.pop("await_task_timeout", 30)  # Default 30 seconds
        sid = request_json.pop("sid", None)  # Extract sid for streaming support

        # Set project_id in payload
        request_json['project_id'] = project_id
        #
        self.module.not_starting_task_event.clear()
        #
        try:
            result: dict = self.module.predict_sio_llm(
                sid=sid,
                data=request_json,
                await_task_timeout=await_task_timeout,
                user_id=auth.current_user().get("id"),
            )
        except ValidationError as e:
            return e.errors(), 400
        except PredictPayloadError as e:
            return {"error": str(e)}, 400
        except BaseException as exc:  # pylint: disable=W0718
            log.exception("LLM Predict error: %s", exc)
            return {"error": "Can not do LLM predict"}, 500
        finally:
            self.module.not_starting_task_event.set()
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
