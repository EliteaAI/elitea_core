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
import json
import types
import functools

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from datamodel_code_generator import DataModelType, PythonVersion, OpenAPIScope  # pylint: disable=E0401
from datamodel_code_generator.model import get_data_model_types  # pylint: disable=E0401
from datamodel_code_generator.parser.openapi import OpenAPIParser  # pylint: disable=E0401
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser  # pylint: disable=E0401

import requests  # pylint: disable=E0401
import jsonref  # pylint: disable=E0401


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def load_api_client(self):
        """ Method """
        api_schema = json.loads(
            self.descriptor.loader.get_data(
                "data/epam_ai_run.spi.json"
            ).decode().replace(
                "epam_ai_run.spi.schema.json#/$defs", "#/$defs"
            )
        )
        #
        addon_schema = json.loads(
            self.descriptor.loader.get_data(
                "data/epam_ai_run.spi.schema.json"
            ).decode()
        )
        #
        api_schema["$defs"] = addon_schema["$defs"]
        #
        self.api_schema_json = json.dumps(api_schema)
        #
        data_model_types = get_data_model_types(
            DataModelType.PydanticV2BaseModel,
            target_python_version=PythonVersion.PY_312,
        )
        #
        parser = OpenAPIParser(
            self.api_schema_json,
            #
            data_model_type=data_model_types.data_model,
            data_model_root_type=data_model_types.root_model,
            data_model_field_type=data_model_types.field_model,
            data_type_manager_type=data_model_types.data_type_manager,
            dump_resolve_reference_action=data_model_types.dump_resolve_reference_action,
            #
            target_python_version=PythonVersion.PY_312,
            #
            openapi_scopes=[
                OpenAPIScope.Schemas,
                OpenAPIScope.Paths,
                OpenAPIScope.Tags,
                OpenAPIScope.Parameters,
            ],
            #
            remove_special_field_name_prefix=True,
            allow_population_by_field_name=True,
        )
        #
        parsed_schema = parser.parse()
        #
        module_name = f"{self.generated_module_base}.api_models"
        #
        sys.modules[module_name] = types.ModuleType(module_name)
        sys.modules[module_name].__path__ = []
        #
        module_code = compile(
            parsed_schema, "<generated>:epam_ai_run.spi.json",
            mode="exec", dont_inherit=True,
        )
        exec(module_code, sys.modules[module_name].__dict__)  # pylint: disable=W0122
        #
        from ..generated import api_models  # pylint: disable=C0415,E0401
        self.api_models = api_models
        #
        openapi_schema = jsonref.replace_refs(api_schema)
        self.api_schema = openapi_schema

    @web.method()
    def make_api_client(self, service_location_url, **kwargs):
        """ Method """
        return OpenAPIClient(
            base_url=service_location_url,
            api_schema=self.api_schema,
            api_models=self.api_models,
            **kwargs
        )


class OpenAPIClient:  # pylint: disable=R0903
    """ Client """

    def __init__(  # pylint: disable=R0913
            self, *, base_url,
            api_schema=None,
            api_models=None,
            headers=None,
            timeout=None,
            verify=None,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        #
        if headers is not None:
            self.session.headers.update(headers)
        #
        self.timeout = timeout
        self.verify = verify
        #
        self._populate_methods(api_schema, api_models)

    def _populate_methods(self, api_schema, api_models):
        if api_schema is None:
            return
        #
        api_models = api_models.__dict__ if api_models is not None else {}
        #
        for path_url, path in api_schema.get("paths", {}).items():
            for method, operation in path.items():
                operation_id = operation.get("operationId", "").strip()
                if not operation_id:
                    continue
                #
                parameters = operation.get("parameters", [])
                request_body = operation.get("requestBody", {})
                responses = operation.get("responses", {})
                #
                target_method = functools.partial(
                    self._make_method(
                        path_url, method, parameters, request_body, responses, api_models
                    ),
                    self
                )
                #
                for method_name in [operation_id, self._to_snake_case(operation_id)]:
                    setattr(self, method_name, target_method)

    def _to_snake_case(self, camel_case_name):
        result = "".join(
            item if item.islower() else f"_{item.lower()}" for item in camel_case_name
        )
        #
        result = result.replace("-", "_")
        while "__" in result:
            result = result.replace("__", "_")
        #
        return result.strip("_")

    def _request(self, method, path_url, *args, **kwargs):
        target_url = "/".join([
            self.base_url,
            path_url.lstrip("/"),
        ])
        #
        return self.session.request(method, target_url, *args, **kwargs)

    def _make_schema_model(self, schema_obj, models):
        try:
            ref = schema_obj["schema"].__reference__
            model_name = ref["$ref"].rsplit("/", 1)[1]
            #
            if model_name in models:
                return models[model_name]
        except:  # pylint: disable=W0702
            pass
        #
        param_schema = schema_obj.get("schema", {})
        if not param_schema:
            return None
        #
        data_model_types = get_data_model_types(
            DataModelType.PydanticV2BaseModel,
            target_python_version=PythonVersion.PY_312,
        )
        #
        parser = JsonSchemaParser(
           json.dumps(param_schema),
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
        code = compile(
            parsed_schema, "<generated>:<schema>",
            mode="exec", dont_inherit=True,
        )
        scope = {}
        exec(code, scope)  # pylint: disable=W0122
        #
        if "Model" in scope:
            return scope["Model"]
        #
        return None

    def _make_param_models(self, parameters, api_models):
        param_models = {}
        #
        for parameter in parameters:
            param_name = parameter.get("name", "").strip()
            if not param_name:
                continue
            #
            model = self._make_schema_model(parameter, api_models)
            if model is not None:
                param_models[param_name] = model
        #
        return param_models

    def _make_request_models(self, request_body, api_models):
        request_models = {}
        #
        if request_body and "content" in request_body:
            for content_type, schema_obj in request_body["content"].items():
                model = self._make_schema_model(schema_obj, api_models)
                if model is not None:
                    request_models[content_type] = model
        #
        return request_models

    def _make_response_models(self, responses, api_models):
        response_models = {}
        #
        for response_code, response_data in responses.items():
            if response_code not in response_models:
                response_models[response_code] = {}
            #
            if "content" not in response_data:
                continue
            #
            for content_type, schema_obj in response_data["content"].items():
                model = self._make_schema_model(schema_obj, api_models)
                if model is not None:
                    response_models[response_code][content_type] = model
        #
        return response_models

    def _make_method(self, path_url, http_method, parameters, request_body, responses, api_models):  # pylint: disable=R
        _path_url = path_url
        _http_method = http_method
        _parameters = parameters
        _request_body = request_body
        _responses = responses
        _api_models = api_models
        #
        param_models = self._make_param_models(_parameters, _api_models)
        request_models = self._make_request_models(_request_body, _api_models)
        response_models = self._make_response_models(_responses, _api_models)
        #
        def _method(self, **kwargs):  # pylint: disable=R
            url = _path_url
            params = {}
            data = None
            json_data = None
            headers = {}
            cookies = {}
            #
            # Prepare data
            #
            for parameter in _parameters:
                param_name = parameter.get("name", "").strip()
                if not param_name:
                    continue
                #
                param_aliases = [param_name, self._to_snake_case(param_name)]
                param_data = None
                param_found = False
                #
                for alias in param_aliases:
                    if alias in kwargs:
                        param_data = kwargs.pop(alias)
                        param_found = True
                        break
                #
                if parameter.get("required", False) and not param_found:
                    raise ValueError(f"Required parameter not set: {param_name}")
                #
                if not param_found:
                    continue
                #
                if param_name in param_models:
                    param_value = param_models[param_name].model_validate(param_data).model_dump(
                        by_alias=True,
                    )
                else:
                    param_value = param_data
                #
                param_loc = parameter.get("in", "").strip()
                if not param_loc:
                    continue
                #
                if param_loc == "header":
                    headers[param_name] = param_value
                elif param_loc == "path":
                    path_var = f"{{{param_name}}}"
                    url = url.replace(path_var, param_value)
                elif param_loc == "query":
                    params[param_name] = param_value
                elif param_loc == "cookie":
                    cookies[param_name] = param_value
            #
            if _request_body.get("required", False) and "request_body" not in kwargs:
                raise ValueError("Request body not set")
            #
            if "request_body" in kwargs:
                body_data = kwargs.pop("request_body")
                body_type = None
                #
                if not request_models:
                    if isinstance(body_data, dict):
                        json_data = body_data
                    else:
                        data = body_data
                else:
                    for content_type, body_model in request_models.items():
                        try:
                            body_obj = body_model.model_validate(body_data).model_dump(
                                by_alias=True,
                            )
                            body_type = content_type
                            break
                        except:  # pylint: disable=W0702
                            pass
                    #
                    if body_type is None:
                        raise ValueError("Invalid request body")
                    #
                    if body_type == "application/json":
                        json_data = body_obj
                    else:
                        headers["Content-Type"] = body_type
                        data = body_obj
            #
            # Make request
            #
            request_kwargs = {}
            request_kwargs.update(kwargs)
            #
            if params:
                request_kwargs["params"] = params
            #
            if data is not None:
                request_kwargs["data"] = data
            #
            if json_data is not None:
                request_kwargs["json"] = json_data
            #
            if headers:
                request_kwargs["headers"] = headers
            #
            if cookies:
                request_kwargs["cookies"] = cookies
            #
            if "timeout" not in request_kwargs and self.timeout is not None:
                request_kwargs["timeout"] = self.timeout
            #
            if "verify" not in request_kwargs and self.verify is not None:
                request_kwargs["verify"] = self.verify
            #
            response = self._request(_http_method, url, **request_kwargs)
            #
            # Validate response
            #
            if not response_models:
                return response
            #
            response_code = str(response.status_code)
            response_code_models = None
            #
            for code_alias in [response_code, f"{response_code[0]}XX"]:
                if code_alias in response_models:
                    response_code_models = response_models[code_alias]
                    break
            #
            if response_code_models is None:
                raise ValueError("Invalid response code")
            #
            response_type = response.headers.get("content-type", "")
            #
            if response_type not in response_code_models:
                raise ValueError("Invalid response type")
            #
            response_model = response_code_models[response_type]
            #
            try:
                if response_type == "application/json":
                    response_value = response_model.model_validate_json(response.content)
                else:
                    response_value = response_model.model_validate(response.content)
                #
                return response_value
            except Exception as exc:
                raise ValueError("Invalid response") from exc
            #
            return None
        #
        return _method
