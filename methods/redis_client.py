#!/usr/bin/python3
# coding=utf-8

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

""" Methods """

import redis  # pylint: disable=E0401

from pylon.core.tools import web  # pylint: disable=E0401,E0611
from tools import config as c  # pylint: disable=E0401


class Method:  # pylint: disable=R0903
    """ Method """

    @web.method()
    def get_redis_client(self):
        """ Create redis client """
        redis_config = self.descriptor.config.get("redis_config", None)  # pylint: disable=E1101
        #
        if not redis_config:
            redis_config = {
                "host": c.REDIS_HOST,
                "port": c.REDIS_PORT,
                "db": c.REDIS_CHAT_CANVAS_DB,
                "username": c.REDIS_USER,
                "password": c.REDIS_PASSWORD,
                "ssl": c.REDIS_USE_SSL,
                "decode_responses": True,
            }
        #
        redis_config = redis_config.copy()
        #
        if redis_config.get("use_managed_identity", False):
            redis_config.pop("use_managed_identity")
            redis_config.pop("password", None)
            #
            from redis_entraid.cred_provider import create_from_default_azure_credential  # pylint: disable=C0415,E0401,W0401
            #
            credential_provider = create_from_default_azure_credential(  # pylint: disable=E0602
                ("https://redis.azure.com/.default",),
            )
            #
            redis_config["credential_provider"] = credential_provider
        #
        return redis.Redis(**redis_config)
