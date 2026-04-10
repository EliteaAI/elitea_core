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

""" Route """

import ssl
import socket
import threading
import urllib.parse

import flask  # pylint: disable=E0401
import requests  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from werkzeug.datastructures.headers import Headers  # pylint: disable=E0401

from tools import context, auth  # pylint: disable=E0401


class Route:  # pylint: disable=E1101,R0903
    """ Route """

    @web.route(
        "/ui_host/<provider_name>/<ui_name>/<project_id>/",
        defaults={"url": "/"},
        methods=["OPTIONS", "HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"],
        endpoint="ui_host_route_http",
    )
    @web.route(
        "/ui_host/<provider_name>/<ui_name>/<project_id>/<path:url>",
        methods=["OPTIONS", "HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"],
        endpoint="ui_host_route_http__url",
    )
    def ui_host_route_http(self, provider_name, ui_name, project_id, url):  # pylint: disable=R
        """ Handler """
        #
        # Initial checks
        #
        try:
            project_id = int(project_id)
        except:  # pylint: disable=W0702
            return "Bad project ID", 400
        #
        user = auth.current_user()
        user_id = user["id"]
        #
        user_projects = {
            project["id"]: project
            for project in context.rpc_manager.call.list_user_projects(user_id)
        }
        #
        if project_id not in user_projects:
            return "No project access", 403
        #
        project = user_projects[project_id]
        #
        provider = self.lookup_provider(user_id, project_id, provider_name)
        #
        if provider is None:
            return "Provider not found", 404
        #
        provider_uis = {}
        #
        log.debug("Provider configuration: %s", provider.configuration.dict())
        log.debug("Provider UIs: %s", provider_uis)
        #
        for ui_item in provider.configuration.dict().get("provided_ui", []):
            provider_uis[ui_item["name"]] = ui_item
        #
        log.debug("Provider UIs: %s", provider_uis)
        #
        if ui_name not in provider_uis:
            return "UI not found", 404
        #
        provider_ui = provider_uis[ui_name]
        provider_ui_headers = provider_ui.get("headers", {})
        provider_ui_options = provider_ui.get("options", {})
        #
        # Request method
        #
        method = flask.request.method
        #
        # Request params
        #
        params = flask.request.args
        #
        # Request headers
        #
        exclude_headers = {
            "Connection",
            "Keep-Alive",
            "Proxy-Authenticate",
            "Proxy-Authorization",
            "TE",
            "Trailers",
            "Transfer-Encoding",
            "Upgrade",
        }
        #
        if not provider_ui_options.get("pass_host_header", True):
            exclude_headers.add("Host")
        #
        request_headers = flask.request.headers
        headers = Headers(dict(request_headers))
        #
        for header in exclude_headers:
            headers.remove(header)
        #
        headers["Accept-Encoding"] = "identity"
        #
        for header_name, header_data in provider_ui_headers.items():
            if isinstance(header_data, str):
                headers[header_name] = header_data
            elif isinstance(header_data, dict):
                header_type = header_data.get("type", None)
                #
                if header_type == "user_id":
                    headers[header_name] = str(user_id)
                #
                elif header_type == "user_name":
                    headers[header_name] = str(user["name"])
                #
                elif header_type == "project_id":
                    headers[header_name] = str(project_id)
                #
                elif header_type == "project_name":
                    headers[header_name] = str(project["name"])
                #
                # Possible future types:
                # - base_url
                # - base_path
                # - base_host
                #
        #
        # Request URL
        # TBD: support passing full path in URL
        #
        if "url" in provider_ui:
            target_url = "/".join([
                provider_ui["url"].rstrip("/"),
                url.lstrip("/"),
            ])
        elif "path" in provider_ui:
            target_url = "/".join([
                str(provider.service_location_url).rstrip("/"),
                provider_ui["path"].strip("/"),
                url.lstrip("/"),
            ])
        else:
            target_url = "/".join([
                str(provider.service_location_url).rstrip("/"),
                url.lstrip("/"),
            ])
        #
        # Request data
        #
        data = None
        json = None
        files = None
        #
        if method in ["POST", "PUT", "PATCH"]:
            if flask.request.files:
                files = {
                    key: (file.filename, file.stream, file.content_type)
                    for key, file in flask.request.files.items()
                }
                data = flask.request.form
            elif flask.request.content_type == "application/json":
                json = flask.request.get_json(silent=True)
                if json is None:
                    data = flask.request.data
            elif flask.request.content_type == "application/x-www-form-urlencoded":
                data = flask.request.form
            else:
                data = flask.request.data
        #
        # Perform request
        #
        try:
            target_response = requests.request(
                method=method,
                url=target_url,
                params=params,
                headers=headers,
                data=data,
                json=json,
                files=files,
                stream=True,
                allow_redirects=False,
                verify=provider_ui_options.get("ssl_verify", False),
                timeout=(
                    provider_ui_options.get("connect_timeout", 100),
                    provider_ui_options.get("read_timeout", 600),
                ),
            )
            #
            response_headers = Headers(target_response.headers)
            #
            for header in exclude_headers:
                response_headers.remove(header)
            #
            if "Host" in request_headers:
                response_headers["Host"] = request_headers["Host"]
            else:
                response_headers.remove("Host")
            #
            if response_headers.get("Transfer-Encoding", "").lower() == "chunked":
                response_headers.remove("Content-Length")
            #
            def _response_generator():
                try:
                    for chunk in target_response.iter_content(
                            chunk_size=self.descriptor.config.get("proxy_chunk_size", 4096),
                    ):
                        if chunk:
                            yield chunk
                finally:
                    target_response.close()
            #
            return flask.Response(
                flask.stream_with_context(_response_generator()),
                status=target_response.status_code,
                headers=response_headers
            )
        #
        except requests.Timeout:
            return "Timeout", 504
        except:  # pylint: disable=W0702
            log.exception("Proxy exception")
            return "Error", 500

    @web.route(
        "/ui_host/<provider_name>/<ui_name>/<project_id>/",
        defaults={"url": "/"},
        websocket=True,
        endpoint="ui_host_route_ws",
    )
    @web.route(
        "/ui_host/<provider_name>/<ui_name>/<project_id>/<path:url>",
        websocket=True,
        endpoint="ui_host_route_ws__url",
    )
    def ui_host_route_ws(self, provider_name, ui_name, project_id, url):  # pylint: disable=R
        """ Handler """
        #
        # Initial checks
        #
        try:
            project_id = int(project_id)
        except:  # pylint: disable=W0702
            return "Bad project ID", 400
        #
        user = auth.current_user()
        user_id = user["id"]
        #
        user_projects = {
            project["id"]: project
            for project in context.rpc_manager.call.list_user_projects(user_id)
        }
        #
        if project_id not in user_projects:
            return "No project access", 403
        #
        project = user_projects[project_id]
        #
        provider = self.lookup_provider(user_id, project_id, provider_name)
        #
        if provider is None:
            return "Provider not found", 404
        #
        provider_uis = {}
        #
        for ui_item in provider.configuration.dict().get("provided_ui", []):
            provider_uis[ui_item["name"]] = ui_item
        #
        if ui_name not in provider_uis:
            return "UI not found", 404
        #
        provider_ui = provider_uis[ui_name]
        provider_ui_headers = provider_ui.get("headers", {})
        provider_ui_options = provider_ui.get("options", {})
        #
        # Request params
        #
        params = flask.request.args
        #
        # Request headers
        #
        request_headers = flask.request.headers
        headers = Headers(dict(request_headers))
        #
        exclude_headers = {
            "Keep-Alive",
            "Proxy-Authenticate",
            "Proxy-Authorization",
            "TE",
            "Trailers",
            "Transfer-Encoding",
        }
        #
        for header in exclude_headers:
            headers.remove(header)
        #
        headers["Accept-Encoding"] = "identity"
        #
        for header_name, header_data in provider_ui_headers.items():
            if isinstance(header_data, str):
                headers[header_name] = header_data
            elif isinstance(header_data, dict):
                header_type = header_data.get("type", None)
                #
                if header_type == "user_id":
                    headers[header_name] = str(user_id)
                #
                elif header_type == "user_name":
                    headers[header_name] = str(user["name"])
                #
                elif header_type == "project_id":
                    headers[header_name] = str(project_id)
                #
                elif header_type == "project_name":
                    headers[header_name] = str(project["name"])
                #
                # Possible future types:
                # - base_url
                # - base_path
                # - base_host
                #
        #
        # Request URL
        # TBD: support passing full path in URL
        #
        if "url" in provider_ui:
            target_url = "/".join([
                provider_ui["url"].rstrip("/"),
                url.lstrip("/"),
            ])
        elif "path" in provider_ui:
            target_url = "/".join([
                str(provider.service_location_url).rstrip("/"),
                provider_ui["path"].strip("/"),
                url.lstrip("/"),
            ])
        else:
            target_url = "/".join([
                str(provider.service_location_url).rstrip("/"),
                url.lstrip("/"),
            ])
        #
        if params:
            target_url += "?" + urllib.parse.urlencode(dict(params))
        #
        parsed_url = urllib.parse.urlparse(target_url)
        scheme = parsed_url.scheme
        netloc = parsed_url.netloc
        secure = scheme.lower() in ["wss", "https"]
        #
        if ":" in netloc:
            host, port_str = netloc.split(":", 1)
            port = int(port_str)
        else:
            host = netloc
            port = 443 if secure else 80
        #
        connect_timeout = provider_ui_options.get("connect_timeout", 100)
        read_timeout = provider_ui_options.get("read_timeout", 600)
        ssl_verify = provider_ui_options.get("ssl_verify", False)
        #
        target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        target_socket.setblocking(True)
        target_socket.settimeout(connect_timeout)
        target_socket.connect((host, port))
        #
        if secure:
            ssl_context = ssl.create_default_context()
            #
            if not ssl_verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            #
            target_socket = ssl_context.wrap_socket(target_socket, server_hostname=host)
        #
        path = parsed_url.path
        if parsed_url.query:
            path += "?" + parsed_url.query
        #
        headers["Host"] = netloc
        headers["Origin"] = f"{scheme}://{netloc}"
        #
        handshake_lines = [f"GET {path} HTTP/1.1"]
        for name, value in headers.items():
            handshake_lines.append(f'{name}: {value.strip()}')
        #
        handshake = "\r\n".join(handshake_lines) + "\r\n\r\n"
        handshake = handshake.encode()
        target_socket.sendall(handshake)
        #
        target_socket.settimeout(read_timeout)
        #
        handshake_response_data = b""
        while True:
            chunk = target_socket.recv(4096)
            if not chunk:
                break
            #
            handshake_response_data += chunk
            if b"\r\n\r\n" in handshake_response_data:
                break
        #
        handshake_data, stream_data = handshake_response_data.split(b"\r\n\r\n", 1)
        header_lines = handshake_data.decode("utf-8", errors="ignore").split("\r\n")
        status_line_parts = header_lines[0].split(" ", 2)
        status_code = int(status_line_parts[1])
        #
        if status_code != 101:
            return "Bad status", 400
        #
        client_socket = get_socket(flask.request.environ)
        client_socket.setblocking(True)
        client_socket.settimeout(read_timeout)
        #
        client_handshake = handshake_data + b"\r\n\r\n"
        client_socket.sendall(client_handshake)
        #
        if stream_data:
            client_socket.sendall(stream_data)
        #
        client_target_thread = SocketForwarder(client_socket, target_socket, tag="ClientToTarget")
        target_client_thread = SocketForwarder(target_socket, client_socket, tag="TargetToClient")
        #
        client_target_thread.start()
        target_client_thread.start()
        #
        client_target_thread.join()
        target_client_thread.join()
        #
        return "", 200


def get_socket(environ):
    """Get the socket from the WSGI environment"""
    wsgi_input = environ["wsgi.input"]
    #
    if hasattr(wsgi_input, "socket"):
        result = wsgi_input.socket
        return result
    #
    if hasattr(wsgi_input, "raw") and hasattr(wsgi_input.raw, "_sock"):
        result = wsgi_input.raw._sock  # pylint: disable=W0212
        wsgi_input.raw.close()
        return result
    #
    raise RuntimeError("No socket found in WSGI environment")


class SocketForwarder(threading.Thread):  # pylint: disable=R0903
    """ Forward data from socket to another socket """

    def __init__(self, socket_from, socket_to, tag=None):
        super().__init__(daemon=True)
        self.socket_from = socket_from
        self.socket_to = socket_to
        self.tag = tag or f"SocketForwarder-{id(self)}"

    def run(self):
        """ Run thread """
        try:
            while True:
                data = self.socket_from.recv(4096)
                if not data:
                    break
                self.socket_to.sendall(data)
        except:  # pylint: disable=W0702
            log.exception("[%s] Error during socket forwarding", self.tag)
        finally:
            try:
                self.socket_from.shutdown(socket.SHUT_WR)
            except:  # pylint: disable=W0702
                pass
