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

import json

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from tools import this  # pylint: disable=E0401

from ..db.models.providers import ProviderDescriptor


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def load_providers(self):
        """ Method """
        #
        # Load from DB
        #
        with this.db.session as session:
            descriptor_objs = session.query(ProviderDescriptor).all()
            #
            for descriptor_obj in descriptor_objs:
                try:
                    descriptor_key = json.loads(
                        descriptor_obj.provider
                    )
                    #
                    descriptor = self.descriptor_model.model_validate_json(
                        descriptor_obj.descriptor
                    )
                    #
                    self.init_provider(descriptor_key["project_id"], descriptor)
                except:  # pylint: disable=W0702
                    log.exception("Error during provider registration")

    @web.method()
    def init_provider(self, project_id, descriptor):
        """ Method """
        service_location_url = str(descriptor.service_location_url)
        #
        log.info(
            "Initializing provider: %s:%s:%s",
            project_id, descriptor.name, service_location_url,
        )
        #
        # Check health
        #
        api_client = self.make_api_client(
            service_location_url=service_location_url,
            timeout=10,
        )
        #
        try:
            health_response = api_client.health_check()
            log.info("Health response: %s", health_response)
            #
            if project_id not in self.present_providers:
                self.present_providers[project_id] = {}
            #
            if descriptor.name not in self.present_providers[project_id]:
                self.present_providers[project_id][descriptor.name] = {}
            #
            self.present_providers[project_id][descriptor.name][service_location_url] = \
                descriptor
        except:  # pylint: disable=W0702
            log.exception("Provider not healthy")
            #
            self.deinit_provider(project_id, descriptor.name, service_location_url)
            #
            if project_id not in self.unhealthy_providers:
                self.unhealthy_providers[project_id] = {}
            #
            if descriptor.name not in self.unhealthy_providers[project_id]:
                self.unhealthy_providers[project_id][descriptor.name] = {}
            #
            self.unhealthy_providers[project_id][descriptor.name][service_location_url] = \
                descriptor

    @web.method()
    def deinit_provider(self, project_id, provider_name, service_location_url):
        """ Method """
        log.info(
            "De-initializing provider: %s:%s:%s",
            project_id, provider_name, service_location_url,
        )
        #
        if project_id in self.present_providers:
            if provider_name in self.present_providers[project_id]:
                self.present_providers[project_id][provider_name].pop(service_location_url, None)
        #
        if project_id in self.unhealthy_providers:
            if provider_name in self.unhealthy_providers[project_id]:
                self.unhealthy_providers[project_id][provider_name].pop(service_location_url, None)
