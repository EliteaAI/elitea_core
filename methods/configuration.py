#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 EPAM Systems
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

""" Method """

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from tools import context, db, rpc_tools  # pylint: disable=E0401

from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import JSONB

from ..models.all import ApplicationVersion
from ..utils.utils import get_public_project_id


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def delete_configuration_settings(self, configuration):
        """ Method """
        configuration_section = configuration["section"]

        if configuration_section in ["llm"]:
            log.debug(f"Deleting configuration settings: {configuration}")

            deleted_model_name = configuration["data"]["name"]
            project_id = configuration["project_id"]

            log.debug(f"Deleted model name: {deleted_model_name}")

            project_ids = [project_id]

            if project_id == get_public_project_id():
                project_ids.extend([item["id"] for item in rpc_tools.RpcMixin().rpc.timeout(3).project_list()])

            log.debug(f"Project IDs: {project_ids}")

            for project_id in set(project_ids):
                with db.get_session(project_id) as session:
                    models_available = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_models(
                        project_id=project_id,
                        section="llm",
                        include_shared=True
                    )
                    model_names: set = {item['name'] for item in models_available['items']}
                    log.debug(f"Model names: {model_names}")

                    if deleted_model_name not in model_names:
                        session.query(ApplicationVersion).where(
                            ApplicationVersion.llm_settings.op("->>")("model_name") == deleted_model_name
                        ).update(
                            {
                                ApplicationVersion.llm_settings: cast(
                                    ApplicationVersion.llm_settings, JSONB
                                ).op("||")(
                                    cast({
                                        "model_name": models_available['default_model_name'],
                                        "model_project_id":  models_available['default_model_project_id']
                                    }, JSONB)
                                )
                            }
                        )
                        session.commit()
