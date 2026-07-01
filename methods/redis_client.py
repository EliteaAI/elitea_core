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

import os
import ssl

import redis  # pylint: disable=E0401
from redis.sentinel import Sentinel  # pylint: disable=E0401

from pylon.core.tools import web, log  # pylint: disable=E0401,E0611
from tools import config as c  # pylint: disable=E0401


def _parse_sentinel_hosts(hosts_str):
    """Parse comma-separated sentinel host:port pairs into a list of tuples."""
    sentinels = []
    for entry in hosts_str.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            host, port = entry.rsplit(":", 1)
            sentinels.append((host.strip(), int(port.strip())))
        else:
            sentinels.append((entry.strip(), 26379))
    return sentinels


def _build_ssl_context():
    """Build an SSL context from environment variables for Redis TLS connections.

    Returns an ssl.SSLContext if TLS is configured, or None otherwise.
    Env vars:
      REDIS_TLS_CA_FILE   - path to CA certificate
      REDIS_TLS_CERT_FILE - path to client certificate (optional, for mTLS)
      REDIS_TLS_KEY_FILE  - path to client private key (optional, for mTLS)
    """
    ca_file = os.environ.get("REDIS_TLS_CA_FILE", "")
    cert_file = os.environ.get("REDIS_TLS_CERT_FILE", "")
    key_file = os.environ.get("REDIS_TLS_KEY_FILE", "")
    #
    if not ca_file or not os.path.isfile(ca_file):
        return None
    #
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca_file)
    #
    if cert_file and key_file and os.path.isfile(cert_file) and os.path.isfile(key_file):
        ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
    #
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


class Method:  # pylint: disable=R0903
    """ Method """

    @web.method()
    def get_redis_client(self):
        """ Return a cached redis client (built once, reused thereafter).

        Supports two connection modes:
        - Sentinel: if REDIS_SENTINEL_HOSTS env var is set, discovers master
          via Sentinel and returns a failover-aware client.
        - Direct: otherwise connects directly to REDIS_HOST:REDIS_PORT.

        When REDIS_TLS_ENABLED=true and TLS certificate files are present,
        connections use TLS with certificate verification.

        redis-py clients and their pools are greenlet/thread-safe and meant
        to be long-lived, so we cache one on the module instance.
        """
        client = getattr(self, "_redis_client", None)  # pylint: disable=E1101
        if client is not None:
            return client
        #
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
        tls_enabled = os.environ.get("REDIS_TLS_ENABLED", "false").lower() == "true"
        ssl_context = None
        if tls_enabled:
            ssl_context = _build_ssl_context()
            if ssl_context:
                redis_config["ssl"] = True
                redis_config["ssl_context"] = ssl_context
                redis_config.setdefault("port", 6380)
                if redis_config.get("port") == 6379:
                    redis_config["port"] = 6380
                log.info("Redis TLS enabled with certificate verification")
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
            client = redis.Redis(**redis_config)
        else:
            sentinel_hosts = os.environ.get("REDIS_SENTINEL_HOSTS", "")
            sentinel_master = os.environ.get("REDIS_SENTINEL_MASTER", "mymaster")
            #
            if sentinel_hosts:
                sentinels = _parse_sentinel_hosts(sentinel_hosts)
                sentinel_kwargs = {}
                if redis_config.get("password"):
                    sentinel_kwargs["password"] = redis_config["password"]
                if redis_config.get("ssl"):
                    sentinel_kwargs["ssl"] = True
                if ssl_context:
                    sentinel_kwargs["ssl_context"] = ssl_context
                #
                sentinel_obj = Sentinel(
                    sentinels,
                    socket_timeout=5.0,
                    sentinel_kwargs=sentinel_kwargs,
                )
                master_kwargs = {
                    "socket_timeout": 5.0,
                    "db": redis_config.get("db", 0),
                    "password": redis_config.get("password", ""),
                    "username": redis_config.get("username", ""),
                    "decode_responses": redis_config.get("decode_responses", True),
                }
                if ssl_context:
                    master_kwargs["ssl"] = True
                    master_kwargs["ssl_context"] = ssl_context
                #
                client = sentinel_obj.master_for(sentinel_master, **master_kwargs)
                log.info(
                    "Redis client connected via Sentinel (master=%s, sentinels=%d, tls=%s)",
                    sentinel_master, len(sentinels), tls_enabled,
                )
            else:
                client = redis.Redis(**redis_config)
        #
        self._redis_client = client  # pylint: disable=W0201
        return client

    @web.method()
    def get_sentinel_info(self):
        """Return Sentinel connection info for health checks.

        Returns a dict with sentinel status if Sentinel is configured,
        or None if using direct connection.
        """
        sentinel_hosts = os.environ.get("REDIS_SENTINEL_HOSTS", "")
        if not sentinel_hosts:
            return None
        #
        sentinel_master = os.environ.get("REDIS_SENTINEL_MASTER", "mymaster")
        sentinels = _parse_sentinel_hosts(sentinel_hosts)
        #
        redis_config = self.descriptor.config.get("redis_config", None)  # pylint: disable=E1101
        password = ""
        use_ssl = False
        if redis_config:
            password = redis_config.get("password", "")
            use_ssl = redis_config.get("ssl", False)
        else:
            password = c.REDIS_PASSWORD
            use_ssl = c.REDIS_USE_SSL
        #
        tls_enabled = os.environ.get("REDIS_TLS_ENABLED", "false").lower() == "true"
        ssl_context = None
        if tls_enabled:
            ssl_context = _build_ssl_context()
            if ssl_context:
                use_ssl = True
        #
        sentinel_kwargs = {}
        if password:
            sentinel_kwargs["password"] = password
        if use_ssl:
            sentinel_kwargs["ssl"] = True
        if ssl_context:
            sentinel_kwargs["ssl_context"] = ssl_context
        #
        result = {
            "enabled": True,
            "master_name": sentinel_master,
            "sentinels_configured": len(sentinels),
            "sentinels_reachable": 0,
            "master_address": None,
            "tls_enabled": tls_enabled,
        }
        #
        try:
            sentinel_obj = Sentinel(
                sentinels,
                socket_timeout=2.0,
                sentinel_kwargs=sentinel_kwargs,
            )
            master_addr = sentinel_obj.discover_master(sentinel_master)
            result["master_address"] = f"{master_addr[0]}:{master_addr[1]}"
            #
            reachable = 0
            for host, port in sentinels:
                try:
                    s = redis.Redis(host=host, port=port, socket_timeout=2.0, **sentinel_kwargs)
                    s.ping()
                    reachable += 1
                except Exception:  # pylint: disable=W0703
                    pass
            result["sentinels_reachable"] = reachable
        except Exception as e:  # pylint: disable=W0703
            result["error"] = str(e)
        #
        return result
