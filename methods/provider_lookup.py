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

import random

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from tools import context  # pylint: disable=E0401

from ..utils.utils import get_public_project_id


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def lookup_provider(self, user_id, project_id, provider_name):  # pylint: disable=R
        """ Method """
        projects = self.expand_project_ids(user_id, project_id)
        #
        for project in projects:
            if project not in self.present_providers:
                continue
            #
            if provider_name not in self.present_providers[project]:
                continue
            #
            providers = self.present_providers[project][provider_name]
            #
            if not providers:
                continue
            #
            return random.choice(list(providers.values()))
        #
        return None

    @web.method()
    def expand_project_ids(self, user_id, project_id):  # pylint: disable=R
        """ Method """
        try:
            try:
                personal_project_id = context.rpc_manager.timeout(
                    15
                ).projects_get_personal_project_id(user_id)
            except:  # pylint: disable=W0702
                personal_project_id = None
            #
            try:
                public_project_id = get_public_project_id()
            except:  # pylint: disable=W0702
                public_project_id = None
            #
            projects = []
            #
            for target_id in [personal_project_id, project_id, public_project_id]:
                if target_id and target_id not in projects:
                    projects.append(target_id)
            #
            return projects
        except:  # pylint: disable=W0702
            log.exception("Failed to expand project IDs")
            return [project_id]
