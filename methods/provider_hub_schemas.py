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
import re

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from tools import auth  # pylint: disable=E0401

# Common abbreviations that should be uppercased in titles
_ABBREVIATIONS = {'api', 'url', 'id', 'uuid', 'http', 'https', 'uri', 'sql', 'html', 'css', 'json', 'xml', 'jwt',
                  'oauth', 'aws', 'gcp', 'ssh', 'ftp', 'smtp', 'ip', 'tcp', 'udp', 'dns', 'ssl', 'tls', 'llm', 'ai'}


def prettify_title(name: str) -> str:
    """
    Convert a variable/parameter name to a human-readable title.

    Handles:
    - snake_case: api_key -> "API Key"
    - camelCase: apiKey -> "API Key"
    - PascalCase: ApiKey -> "API Key"
    - kebab-case: api-key -> "API Key"
    - Common abbreviations: api, url, id, etc. -> uppercase

    Args:
        name: The variable/parameter name to prettify

    Returns:
        A human-readable title string
    """
    if not name:
        return name

    # First, split camelCase and PascalCase by inserting spaces before uppercase letters
    # But handle consecutive uppercase (like "APIKey" -> "API Key")
    result = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    result = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', result)

    # Replace underscores and hyphens with spaces
    result = result.replace('_', ' ').replace('-', ' ')

    # Split into words and process each
    words = result.split()
    processed_words = []

    for word in words:
        lower_word = word.lower()
        if lower_word in _ABBREVIATIONS:
            processed_words.append(word.upper())
        else:
            processed_words.append(word.capitalize())

    return ' '.join(processed_words)


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def get_tool_schemas_provider_hub(self, project_id, user_id, personal_project_id=None):  # pylint: disable=R
        # user_id should be provided directly as a parameter of this method to prevent out odf context execution errors when calling this method from other system plugins e.g. applications
        """ Method """
        try:
            #
            projects = self.expand_project_ids(user_id, project_id, personal_project_id=personal_project_id)
            result = {}
            #
            for project in projects:
                if project not in self.present_providers:
                    continue
                #
                for provider_name, providers in self.present_providers[project].items():
                    if not providers:
                        continue
                    #
                    provider = random.choice(list(providers.values()))
                    #
                    try:
                        self.prepare_provider_toolkits(result, provider_name, provider)
                    except:  # pylint: disable=W0702
                        log.exception("Failed to add provider toolkits: %s:%s", project, provider)
            #
            return result
        except:  # pylint: disable=W0702
            log.exception("Failed to get tool schemas")
            return {}

    @web.method()
    def get_configuration_schemas(self, project_id):
        """ Method """
        _ = project_id
        return {}

    @web.method()
    def prepare_provider_toolkits(self, result, provider_name, provider):  # pylint: disable=R
        """ Method """
        for toolkit in provider.provided_toolkits:
            toolkit_name = toolkit.name
            #
            # Allow toolkit to override the type name via toolkit_metadata.type_override
            toolkit_metadata = getattr(toolkit, "toolkit_metadata", None) or {}
            if isinstance(toolkit_metadata, dict):
                type_override = toolkit_metadata.get("type_override")
            else:
                type_override = getattr(toolkit_metadata, "type_override", None)
            #
            if type_override:
                name = type_override
            else:
                name = f"{provider_name}_{toolkit_name}"
            #
            if name in result:
                continue
            #
            toolkit_props = {}
            toolkit_props_required = []
            #
            # Get fields_order from toolkit config if available
            fields_order = getattr(toolkit.toolkit_config, "fields_order", None)
            parameters = toolkit.toolkit_config.parameters

            if fields_order:
                # Start with fields in fields_order
                ordered_keys = [key for key in fields_order if key in parameters]
                # Add any missing keys at the end
                missing_keys = [key for key in parameters if key not in ordered_keys]
                all_keys = ordered_keys + missing_keys
                items_iterator = [(key, parameters[key]) for key in all_keys]
            else:
                items_iterator = parameters.items()
            #
            for key, data in items_iterator:
                prop_obj = {}
                #
                data_type = data.type.value
                #
                if data_type in ["Text"]:
                    prop_obj["type"] = "string"
                    prop_obj["lines"] = 5
                elif data_type in ["String", "URL", "UUID", "Secret"]:
                    prop_obj["type"] = "string"
                    #
                    if data_type == "Secret":
                        prop_obj["format"] = "password"
                        prop_obj["secret"] = True
                        prop_obj["writeOnly"] = True
                elif data_type in ["Integer", "Float"]:  # FIXME: floats
                    prop_obj["type"] = "integer"
                elif data_type in ["Bool"]:
                    prop_obj["type"] = "boolean"
                else:
                    prop_obj["type"] = "object"
                prop_obj["title"] = prettify_title(key)
                #
                data_description = data.description
                #
                if data_description:
                    prop_obj["description"] = data_description
                # If the parameter object exposes a json_schema_extra (dict) merge its content
                # into the property schema (without overwriting existing generated keys).
                try:
                    schema_extra = getattr(data, "json_schema_extra", None)
                    if schema_extra and isinstance(schema_extra, dict):
                        for shema_key, shema_value in schema_extra.items():
                            # Do not override already set keys e.g. title, description etc. if json_schema_extra contains them
                            if shema_key in prop_obj:
                                continue
                            prop_obj[shema_key] = shema_value
                        # mark the individual property object as configuration to be able to handle it as other toolkits in the UI currently
                        prop_obj["type"] = "configuration"
                except Exception:  # pylint: disable=broad-except
                    log.exception("Failed to merge json_schema_extra for %s", key)
                #
                prop_key = f"toolkit_configuration_{key}"
                #
                if data.required:
                    toolkit_props_required.append(prop_key)
                #
                # Copy default value if present (handle both object and dict)
                if hasattr(data, 'default') and data.default is not None:
                    prop_obj["default"] = data.default
                elif isinstance(data, dict) and data.get("default") is not None:
                    prop_obj["default"] = data.get("default")
                #
                toolkit_props[prop_key] = prop_obj
            #
            toolkit_obj = {
                "type": "object",
                "title": name,
                "description": toolkit.description,
                "metadata": {
                    "icon_url": None,
                    "label": toolkit_name,
                    **(toolkit.toolkit_metadata if hasattr(toolkit, 'toolkit_metadata') and toolkit.toolkit_metadata else {}),
                },
                "properties": {
                    **toolkit_props,
                    #
                    "selected_tools": {
                        "type": "array",
                        "title": "Selected Tools",
                        "items": {
                            "type": "string",
                            "enum": [],
                        },
                        "default": [],
                        "args_schemas": {},
                    },
                    #
                    "module": {
                        "type": "string",
                        "title": "Module",
                        "description": "Toolkit module",
                        "default": "plugins.provider_worker.utils.tools",
                        "hidden": True,
                    },
                    "class": {
                        "type": "string",
                        "title": "Class",
                        "description": "Toolkit class",
                        "default": "Toolkit",
                        "hidden": True,
                    },
                    "provider": {
                        "type": "string",
                        "title": "Provider",
                        "description": "Toolkit provider",
                        "default": provider_name,
                        "hidden": True,
                    },
                    "toolkit": {
                        "type": "string",
                        "title": "Toolkit",
                        "description": "Toolkit",
                        "default": toolkit_name,
                        "hidden": True,
                    },
                },
                "required": toolkit_props_required,
            }
            #
            for tool in toolkit.provided_tools:
                toolkit_obj["properties"]["selected_tools"]["items"]["enum"].append(tool.name)
                toolkit_obj["properties"]["selected_tools"]["args_schemas"][tool.name] = \
                    self.convert_args_schema(tool.args_schema, tool.name, tool.description)
            #
            result[name] = toolkit_obj

    @web.method()
    def convert_args_schema(self, args_schema, title=None, description=None):
        """ Method """
        result = {
            "type": "object",
            "properties": {},
            "required": [],
        }
        #
        if title is not None:
            result["title"] = title
        #
        if description is not None:
            result["description"] = description
        #
        for param, schema in args_schema.items():
            prop_obj = {
                "title": prettify_title(param),
                "description": schema.get("description", param),
            }
            #
            # Handle different type names (both SDK and descriptor formats)
            schema_type = schema.get("type", "String")
            #
            if schema_type in ["String", "Text", "URL", "UUID", "Secret"]:
                prop_obj["type"] = "string"
            elif schema_type in ["Integer"]:
                prop_obj["type"] = "integer"
            elif schema_type in ["Float"]:
                prop_obj["type"] = "number"
            elif schema_type in ["Number"]:
                prop_obj["type"] = "number"
            elif schema_type in ["Boolean", "Bool"]:
                prop_obj["type"] = "boolean"
            elif schema_type == "List":
                prop_obj["type"] = "array"
                if "enum" in schema:
                    prop_obj["items"] = {
                        "type": "string",
                        "enum": schema["enum"],
                    }
            else:
                # Default to object for unknown types (JSON, YAML, etc.)
                prop_obj["type"] = "object"
            #
            # Copy default value if present
            if "default" in schema:
                prop_obj["default"] = schema["default"]
            #
            # set numeric validation rules only for (integers, numbers)
            if prop_obj.get("type") in {"integer", "number"}:
                # Map pydantic-style numeric constraints to JSON Schema format.
                # gt (greater than) -> exclusiveMinimum, ge (greater or equal) -> minimum
                # lt (less than) -> exclusiveMaximum, le (less or equal) -> maximum
                #
                # If both strict and non-strict bounds are provided for the same direction
                # (e.g., both "gt" and "ge"), prefer the strict bound to avoid emitting
                # potentially conflicting constraints in the JSON Schema output.
                if "gt" in schema and "ge" in schema:
                    prop_obj["exclusiveMinimum"] = schema["gt"]
                elif "gt" in schema:
                    prop_obj["exclusiveMinimum"] = schema["gt"]
                elif "ge" in schema:
                    prop_obj["minimum"] = schema["ge"]

                if "lt" in schema and "le" in schema:
                    prop_obj["exclusiveMaximum"] = schema["lt"]
                elif "lt" in schema:
                    prop_obj["exclusiveMaximum"] = schema["lt"]
                elif "le" in schema:
                    prop_obj["maximum"] = schema["le"]
            #
            result["properties"][param] = prop_obj
            #
            if schema.get("required", False):
                result["required"].append(param)
        #
        return result
