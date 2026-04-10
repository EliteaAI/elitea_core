#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
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

import flask  # pylint: disable=E0401,W0611

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611

from tools import auth  # pylint: disable=E0401
from tools import api_tools  # pylint: disable=E0401


class AdminAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    """ API """

    @auth.decorators.check_api(["runtime.airun.serviceproviders"])
    def get(self):  # pylint: disable=R0911,R0912
        """ Process GET """
        result = []
        #
        present_providers = self.module.present_providers
        #
        for project_id in present_providers:
            for provider_name in present_providers[project_id]:
                for service_location_url in present_providers[project_id][provider_name]:
                    result.append({
                        "project_id": project_id,
                        "provider_name": provider_name,
                        "service_location_url": service_location_url,
                        "healthy": True,
                    })
        #
        unhealthy_providers = self.module.unhealthy_providers
        #
        for project_id in unhealthy_providers:
            for provider_name in unhealthy_providers[project_id]:
                for service_location_url in unhealthy_providers[project_id][provider_name]:
                    result.append({
                        "project_id": project_id,
                        "provider_name": provider_name,
                        "service_location_url": service_location_url,
                        "healthy": False,
                    })
        #
        return {
            "total": len(result),
            "rows": result,
        }


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """

    module_name_override = "provider_hub"

    url_params = [
        "<string:mode>",
    ]

    mode_handlers = {
        'administration': AdminAPI,
    }
