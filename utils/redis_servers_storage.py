"""Redis-backed MCP servers storage for horizontal scaling.

Replaces the in-memory ServersStorage with Redis hashes so that MCP server
state is shared across all pylon_main replicas. Any pod can serve any request
without sticky sessions.

Redis key layout:
  mcp_servers:{project_id}    — hash: server_name → JSON(McpServer)
  mcp_sid_to_project:{sid}    — string: project_id
"""

import json

from pylon.core.tools import log

from ..models.mcp import McpServer


DEFAULT_TTL = 3600  # 1 hour


class RedisServersStorage:
    def __init__(self, redis_client, ttl: int = DEFAULT_TTL):
        self._client = redis_client
        self._ttl = ttl

    def _servers_key(self, project_id) -> str:
        return f"mcp_servers:{project_id}"

    def _sid_key(self, sid: str) -> str:
        return f"mcp_sid_to_project:{sid}"

    def _serialize_server(self, server: McpServer) -> str:
        return server.model_dump_json()

    def _deserialize_server(self, data: str) -> McpServer:
        return McpServer.model_validate_json(data)

    def add_server(self, project_id: int, server: McpServer) -> bool:
        """Register a new server for the project. Returns True if registered, False if already exists."""
        servers_key = self._servers_key(project_id)
        sid_key = self._sid_key(server.sio_sid)
        serialized = self._serialize_server(server)

        # HSETNX returns 1 if field was set (new), 0 if it already existed
        was_set = self._client.hsetnx(servers_key, server.name, serialized)
        if was_set:
            self._client.expire(servers_key, self._ttl)
            self._client.set(sid_key, str(project_id), ex=self._ttl)
            return True
        return False

    def get_server(self, project_id: int, server_name: str):
        """Get a single server by project_id and name. Returns McpServer or None."""
        servers_key = self._servers_key(project_id)
        data = self._client.hget(servers_key, server_name)
        if data is None:
            return None
        return self._deserialize_server(data)

    def get_servers_dict(self, project_id: int) -> dict:
        """Get all servers for a project as {name: McpServer}."""
        servers_key = self._servers_key(project_id)
        all_data = self._client.hgetall(servers_key)
        if not all_data:
            return {}
        result = {}
        for name, data in all_data.items():
            # redis-py with decode_responses=True returns strings directly
            field_name = name if isinstance(name, str) else name.decode()
            field_data = data if isinstance(data, str) else data.decode()
            try:
                result[field_name] = self._deserialize_server(field_data)
            except Exception as e:
                log.warning("Failed to deserialize MCP server '%s': %s", field_name, e)
        return result

    def refresh_and_get_server(self, project_id: int, server_name: str, server_provider):
        """Refresh server state via server_provider callback.

        If the provider returns a new server version, update Redis.
        If it returns None, remove the server from storage.
        """
        servers_key = self._servers_key(project_id)
        data = self._client.hget(servers_key, server_name)
        if data is None:
            return None

        old_server = self._deserialize_server(data)
        new_server = server_provider(old_server)

        if new_server:
            self._client.hset(servers_key, server_name, self._serialize_server(new_server))
            self._client.expire(servers_key, self._ttl)
            return new_server

        # Server no longer available — remove it
        self._client.hdel(servers_key, server_name)
        return None

    def remove_servers(self, sid: str) -> list:
        """Remove all servers associated with a Socket.IO session ID.

        Returns list of dicts: [{'name': ..., 'project_id': ...}]
        """
        sid_key = self._sid_key(sid)
        project_id_raw = self._client.get(sid_key)
        if project_id_raw is None:
            return []

        project_id = project_id_raw if isinstance(project_id_raw, str) else project_id_raw.decode()
        servers_key = self._servers_key(project_id)
        all_data = self._client.hgetall(servers_key)
        if not all_data:
            self._client.delete(sid_key)
            return []

        removed_servers = []
        for name, data in all_data.items():
            field_name = name if isinstance(name, str) else name.decode()
            field_data = data if isinstance(data, str) else data.decode()
            try:
                server = self._deserialize_server(field_data)
                if server.sio_sid == sid:
                    self._client.hdel(servers_key, field_name)
                    removed_servers.append({'name': server.name, 'project_id': int(project_id)})
            except Exception as e:
                log.warning("Failed to deserialize server '%s' during removal: %s", field_name, e)

        # If no servers left for the project, clean up the key
        if not self._client.hlen(servers_key):
            self._client.delete(servers_key)

        self._client.delete(sid_key)
        return removed_servers

    def validate_all(self, sid_connected_state_provider):
        """Validate all tracked SIDs and remove disconnected ones.

        Scans all mcp_sid_to_project:* keys and checks connectivity.
        """
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match="mcp_sid_to_project:*", count=100)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                sid = key_str.split(":", 1)[1]
                if not sid_connected_state_provider(sid):
                    self.remove_servers(sid)
            if cursor == 0:
                break

    def list_active_servers(self, project_id: int) -> list:
        """List names of active servers for a project."""
        servers_key = self._servers_key(project_id)
        names = self._client.hkeys(servers_key)
        return [n if isinstance(n, str) else n.decode() for n in names]

    def status(self) -> str:
        """Return a human-readable status string of all tracked servers."""
        result = []
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match="mcp_servers:*", count=100)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                project_id = key_str.split(":", 1)[1]
                server_names = self._client.hkeys(key_str)
                names = [n if isinstance(n, str) else n.decode() for n in server_names]
                result.append(f"\nProject {project_id}:\n       {names}")
            if cursor == 0:
                break
        return "".join(result)
