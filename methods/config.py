#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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

import flask  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def get_elitea_ui_config(self):
        """ Get config """
        vite_server_url = self.descriptor.config.get(
            "vite_server_url",
            flask.url_for(
                "elitea_core.route_elitea_ui",
                _external=True,
            ).rstrip("/").replace("/app", "/api/v2"),
        )
        #
        vite_base_uri = flask.url_for("elitea_core.route_elitea_ui").rstrip("/")
        #
        vite_public_project_id = self.descriptor.config.get("vite_public_project_id", 1)
        vite_socket_path = self.descriptor.config.get("vite_socket_path", "/socket.io/")
        vite_socket_server = self.descriptor.config.get("vite_socket_server", "/")
        #
        default_release = self.default_release
        #
        elitea_ui_config_data = {
            "vite_server_url": vite_server_url,
            "vite_base_uri": vite_base_uri,
            "vite_public_project_id": vite_public_project_id,
            "vite_socket_path": vite_socket_path,
            "vite_socket_server": vite_socket_server,
            "default_release": default_release,
        }
        #
        additional_config_keys = [
            "vite_gaid",
        ]
        #
        for key in additional_config_keys:
            if key in self.descriptor.config:
                elitea_ui_config_data[key] = self.descriptor.config.get(key)
        #
        # Add extra UI config
        #
        extra_ui_config_key = "extra_ui_config"
        #
        if extra_ui_config_key in self.descriptor.config:
            elitea_ui_config_data.update(self.descriptor.config.get(extra_ui_config_key, {}))
        #
        # Add LLM settings for UI enforcement
        #
        try:
            from tools import context as ctx  # pylint: disable=C0415,E0401
            descriptor = ctx.module_manager.descriptors.get("runtime_interface_litellm")
            if descriptor is not None:
                elitea_ui_config_data["allow_project_own_llms"] = descriptor.config.get(
                    "allow_project_own_llms", True,
                )
            else:
                elitea_ui_config_data["allow_project_own_llms"] = True
        except:  # pylint: disable=W0702
            elitea_ui_config_data["allow_project_own_llms"] = True
        #
        # Expose blocked toolkit types so the UI can show a named warning when a
        # configured toolkit has been blocked by org guardrails. Read live from
        # the descriptor (blocked toolkits are otherwise omitted from the toolkit
        # catalog, making them indistinguishable from deleted/renamed ones).
        #
        try:
            toolkit_security = self.descriptor.config.get("toolkit_security", {}) or {}
            elitea_ui_config_data["blocked_toolkits"] = list(
                toolkit_security.get("blocked_toolkits") or []
            )
        except Exception as e:  # pylint: disable=W0703
            log.warning("Failed to load blocked_toolkits from toolkit_security config: %s", e)
            elitea_ui_config_data["blocked_toolkits"] = []
        #
        return elitea_ui_config_data
