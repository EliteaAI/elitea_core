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
from tools import api_tools, auth  # pylint: disable=E0401

from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.exceptions import VerifySignatureError  # pylint: disable=E0402


class WebHookAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    """ API """

    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, webhook_type: str):  # pylint: disable=R0911
        """ POST """
        payload_in = {
            "chat_history": [],
            "user_input": request.data.decode("utf-8"),
        }
        #
        if webhook_type == "github":
            webhook_signature = request.headers.get("x-hub-signature-256")
            if webhook_signature is None:
                return {"error": "Missing request header x-hub-signature-256"}, 400
        #
        elif webhook_type == "gitlab":
            webhook_signature = request.headers.get("x-gitlab-token")
            if webhook_signature is None:
                return {"error": "Missing request header x-gitlab-token"}, 400
        #
        elif webhook_type == "custom":
            webhook_signature = request.headers
        #
        else:
            return {"error": "Bad signature type"}, 400
        #
        try:
            result = self.module.do_predict(
                project_id=project_id,
                user_id=auth.current_user()["id"],
                version_id=version_id,
                payload_in=payload_in,
                raw=request.data,
                webhook_signature=webhook_signature,
                webhook_type=webhook_type,
            )
            #
            if "error" in result:
                return result, 400
        except ValidationError as e:
            return e.errors(), 400
        except VerifySignatureError as e:
            return e.value, 400
        except BaseException as exc:  # pylint: disable=W0718
            log.error(exc)
            return {"error": "Can not do predict"}, 500
        #
        return result, 200


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>/<webhook_type>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: WebHookAPI,
    }
