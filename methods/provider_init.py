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

import sys
import types
import logging

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.init()
    def generated_init(self):
        """ Method """
        logging.getLogger("blib2to3.pgen2.driver").setLevel(logging.INFO)
        #
        this_module = self.__module__.rsplit(".", 1)[0]
        self.generated_module_base = f"{this_module}.generated"
        #
        sys.modules[self.generated_module_base] = types.ModuleType(self.generated_module_base)
        sys.modules[self.generated_module_base].__path__ = []
        #
        self.descriptor_model = None
        self.load_provider_descriptor_model()
        #
        self.api_models = None
        self.api_schema = None
        self.api_schema_json = None
        self.load_api_client()
        #
        self.present_providers = {}  # project -> provider -> url -> descriptor
        self.unhealthy_providers = {}  # project -> provider -> url -> descriptor
