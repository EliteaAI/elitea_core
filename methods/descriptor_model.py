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

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from datamodel_code_generator import DataModelType, PythonVersion  # pylint: disable=E0401
from datamodel_code_generator.model import get_data_model_types  # pylint: disable=E0401
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser  # pylint: disable=E0401


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def load_provider_descriptor_model(self):
        """ Method """
        descriptor_schema = self.descriptor.loader.get_data(
            "data/ExternalServiceProviderDescriptor.json"
        ).decode()
        #
        data_model_types = get_data_model_types(
            DataModelType.PydanticV2BaseModel,
            target_python_version=PythonVersion.PY_312,
        )
        #
        parser = JsonSchemaParser(
           descriptor_schema,
           #
           data_model_type=data_model_types.data_model,
           data_model_root_type=data_model_types.root_model,
           data_model_field_type=data_model_types.field_model,
           data_type_manager_type=data_model_types.data_type_manager,
           dump_resolve_reference_action=data_model_types.dump_resolve_reference_action,
           #
           target_python_version=PythonVersion.PY_312,
           #
           remove_special_field_name_prefix=True,
           allow_population_by_field_name=True,
        )
        #
        parsed_schema = parser.parse()
        #
        module_name = f"{self.generated_module_base}.descriptor"
        #
        sys.modules[module_name] = types.ModuleType(module_name)
        sys.modules[module_name].__path__ = []
        #
        # Inject 'Any' from typing into the generated module's namespace BEFORE executing
        from typing import Any  # pylint: disable=C0415
        sys.modules[module_name].__dict__['Any'] = Any
        #
        module_code = compile(
            parsed_schema, "<generated>:ExternalServiceProviderDescriptor.json",
            mode="exec", dont_inherit=True,
        )
        exec(module_code, sys.modules[module_name].__dict__)  # pylint: disable=W0122
        #
        from ..generated.descriptor import ExternalServiceProviderDescriptor  # pylint: disable=C0415,E0401
        #
        # Rebuild the model to resolve forward references (e.g., 'Any')
        ExternalServiceProviderDescriptor.model_rebuild()
        #
        self.descriptor_model = ExternalServiceProviderDescriptor
