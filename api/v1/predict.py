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
    def post(self, project_id: int, version_id: int):
        """ Get task result """
        request_json = dict(request.json)
        #
        callback_url = request_json.pop("callback_url", None)
        callback_headers = request_json.pop("callback_headers", None)
        async_mode = request_json.pop("async_mode", False)
        #
        is_async = callback_url is not None or async_mode or \
            request.args.get("async", "no").lower().strip() in ["yes", "true"]
        #
        self.module.not_starting_task_event.clear()
        #
        try:
            result: dict = self.module.do_predict(
                project_id=project_id,
                user_id=auth.current_user()["id"],
                version_id=version_id,
                payload_in=request_json,
                raw=request.data,
                webhook_signature=None,
                predict_wait=not is_async,
            )
            #
            if callback_url is not None and "task_id" in result and result["task_id"]:
                self.module.callback_tasks[result["task_id"]] = {
                    "callback_url": callback_url,
                    "callback_headers": callback_headers,
                }
        except ValidationError as e:
            return e.errors(), 400
        except PredictPayloadError as e:
            return {"error": str(e)}, 400
        except BaseException as exc:  # pylint: disable=W0718
            log.exception("Predict error: %s", exc)
            return {"error": "Can not do predict"}, 500
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

    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
