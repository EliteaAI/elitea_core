import collections
import hashlib
import json
import threading
import time
from dataclasses import dataclass, field

from ..models.mcp import McpServer
from . import schema_registry_version

CONNECTS_PER_WINDOW = 30
CONNECT_WINDOW_SECONDS = 60


@dataclass
class ClientState:
    sid: str
    epoch: str
    generation: int
    names: set = field(default_factory=set)
    failed_names: set = field(default_factory=set)


@dataclass
class SnapshotResult:
    stale: bool = False
    registered: list = field(default_factory=list)
    declined: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    added: list = field(default_factory=list)
    updated: list = field(default_factory=list)
    removed: list = field(default_factory=list)


def hash_server(server: McpServer) -> str:
    """What the platform derives from a server: its tools and the timeouts it calls them with."""
    content = {
        "tools": [tool.model_dump() for tool in server.tools],
        "timeouts": [server.timeout_tools_list, server.timeout_tools_call],
    }
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


class ServersStorage:
    def __init__(self, clock=time.monotonic):
        self.project_id_to_server_name_to_server = {}
        self.sid_to_project_id = {}
        self.clients = {}
        self.failed_servers = {}
        self._clock = clock
        self._connects = {}
        self._lock = threading.RLock()

    def add_server(self, project_id: int, server: McpServer):
        with self._lock:
            if project_id not in self.project_id_to_server_name_to_server:
                self.project_id_to_server_name_to_server[project_id] = {}
            # Register new server for the project only if it does not exist
            if server.name not in self.project_id_to_server_name_to_server[project_id]:
                self.project_id_to_server_name_to_server[project_id][server.name] = server
                self.sid_to_project_id[server.sio_sid] = project_id
                schema_registry_version.bump()
                #
                return True
            #
            return False

    def apply_snapshot(self, project_id: int, client_id: str, user_id, sid: str, epoch: str, generation: int,
                       servers: list, failed: list) -> SnapshotResult:
        """Make the client's servers in the project exactly the snapshot, in one step with nothing yielding in between.

        A client is its durable id together with the user it authenticates as, so a new socket of the same client
        takes over its names while another user, or another client, is declined. Snapshots are ordered by
        ``(epoch, generation)``: an older one is answered as stale and changes nothing.
        """
        with self._lock:
            key = (project_id, client_id, user_id)
            state = self.clients.get(key)
            if state is not None and (epoch, generation) <= (state.epoch, state.generation):
                return SnapshotResult(stale=True)
            project_servers = self.project_id_to_server_name_to_server.setdefault(project_id, {})
            owned = self._find_owned_names(state, project_servers)
            result = SnapshotResult()
            for server in servers:
                existing = project_servers.get(server.name)
                if existing is not None and server.name not in owned:
                    result.declined.append({"name": server.name, "reason": "name_already_registered"})
                    continue
                if existing is None:
                    result.added.append(server.name)
                elif hash_server(existing) != hash_server(server):
                    result.updated.append(server.name)
                project_servers[server.name] = server
                result.registered.append(server.name)
            result.removed = sorted(owned - set(result.registered))
            for name in result.removed:
                project_servers.pop(name, None)
            result.failed = self._replace_failed(project_id, state, failed, project_servers)
            if not project_servers:
                self.project_id_to_server_name_to_server.pop(project_id, None)
            self.clients[key] = ClientState(sid, epoch, generation, set(result.registered), set(result.failed))
            self.sid_to_project_id[sid] = project_id
            if result.added or result.updated or result.removed:
                schema_registry_version.bump()
            return result

    @staticmethod
    def _find_owned_names(state, project_servers):
        """The client's names, including those it added without a snapshot while its socket did not get the answers."""
        if state is None:
            return set()
        return state.names | {name for name, server in project_servers.items() if server.sio_sid == state.sid}

    def _replace_failed(self, project_id, state, failed, project_servers):
        failed_store = self.failed_servers.setdefault(project_id, {})
        for name in state.failed_names if state else ():
            failed_store.pop(name, None)
        recorded = []
        for item in failed:
            if not item.name or item.name in project_servers or item.name in failed_store:
                continue
            failed_store[item.name] = item
            recorded.append(item.name)
        if not failed_store:
            self.failed_servers.pop(project_id, None)
        return recorded

    def get_server(self, project_id: int, server_name):
        return self.project_id_to_server_name_to_server.get(project_id, {}).get(server_name, None)

    def get_servers_dict(self, project_id: int):
        return self.project_id_to_server_name_to_server.get(project_id, {})

    def get_failed_server(self, project_id: int, server_name):
        """The failure a client reported for a server that is not online, or None."""
        return self.failed_servers.get(project_id, {}).get(server_name, None)

    def allow_connect(self, sid: str) -> bool:
        """Let one socket register at most CONNECTS_PER_WINDOW times per CONNECT_WINDOW_SECONDS."""
        with self._lock:
            now = self._clock()
            recent = self._connects.setdefault(sid, collections.deque())
            while recent and now - recent[0] >= CONNECT_WINDOW_SECONDS:
                recent.popleft()
            if len(recent) >= CONNECTS_PER_WINDOW:
                return False
            recent.append(now)
            return True

    def remove_servers(self, sid: str) -> list[dict]:
        with self._lock:
            self._connects.pop(sid, None)
            project_id = self.sid_to_project_id.pop(sid, None)
            if project_id is None:
                return []
            servers = self.project_id_to_server_name_to_server.get(project_id, {})
            to_remove = [name for name, server in servers.items() if server.sio_sid == sid]
            removed_servers = []
            for name in to_remove:
                server = servers.pop(name)
                removed_servers.append(
                    {'name': server.name, 'project_id': project_id}
                )
            if not servers:
                self.project_id_to_server_name_to_server.pop(project_id, None)
            self._forget_clients_of(sid)
            if removed_servers:
                schema_registry_version.bump()
            return removed_servers

    def _forget_clients_of(self, sid):
        for key, state in list(self.clients.items()):
            if state.sid != sid:
                continue
            failed_store = self.failed_servers.get(key[0], {})
            for name in state.failed_names:
                failed_store.pop(name, None)
            if not failed_store:
                self.failed_servers.pop(key[0], None)
            del self.clients[key]

    def validate_all(self, sid_connected_state_provider) -> list[dict]:
        """Drop the servers of every socket that no longer answers, and return them so they can be announced."""
        removed = []
        for sid in list(self.sid_to_project_id.keys()):
            if not sid_connected_state_provider(sid):
                removed.extend(self.remove_servers(sid))
        return removed

    def status(self):
        result = []
        for project_id, servers in self.project_id_to_server_name_to_server.items():
            server_names = list(servers.keys())
            result.append(f"\nProject {project_id}:\n       {server_names}")
        return "".join(result)
