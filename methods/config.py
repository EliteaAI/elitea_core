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
        if self.standalone_mode:  # pylint: disable=R1705
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
        else:
            from tools import theme, VaultClient  # pylint: disable=E0611,E0401,W0611,C0415
            #
            secrets = VaultClient().get_all_secrets()
            #
            vite_server_url = flask.url_for(
                "api.v2.elitea_core.elitea_ui_ci", _external=True
            ).replace(
                "elitea_core/elitea_ui_ci/", ""
            ).replace(
                "/promptlib_shared/elitea_ui_ci/", ""
            )
            #
            vite_base_uri = flask.url_for("elitea_core.route_elitea_ui").rstrip("/")
            vite_public_project_id = int(self.descriptor.config.get("ai_project_id", 1))
            try:
                vite_socket_path = flask.url_for("theme.socketio")
            except:  # pylint: disable=W0702
                vite_socket_path = "/socket.io/"
            #
            default_release = self.default_release
            #
            elitea_ui_config_data = {
                "vite_server_url": vite_server_url,
                "vite_base_uri": vite_base_uri,
                "vite_public_project_id": vite_public_project_id,
                "vite_socket_path": vite_socket_path,
                "vite_socket_server": self.descriptor.config.get('vite_socket_server', '/'),
                "default_release": default_release,
            }
            #
            additional_config_keys = [
                "vite_gaid",
                #
                "vite_server_url",
            ]
            #
            for key in additional_config_keys:
                if key in secrets:
                    elitea_ui_config_data[key] = secrets.get(key)
                elif key in self.descriptor.config:
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
        return elitea_ui_config_data
