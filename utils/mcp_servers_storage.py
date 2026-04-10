from ..models.mcp import McpServer


class ServersStorage:
    def __init__(self):
        self.project_id_to_server_name_to_server = {}
        self.sid_to_project_id = {}

    def add_server(self, project_id: int, server: McpServer):
        if project_id not in self.project_id_to_server_name_to_server:
            self.project_id_to_server_name_to_server[project_id] = {}
        # Register new server for the project only if it does not exist
        if server.name not in self.project_id_to_server_name_to_server[project_id]:
            self.project_id_to_server_name_to_server[project_id][server.name] = server
            self.sid_to_project_id[server.sio_sid] = project_id
            #
            return True
        #
        return False

    def get_server(self, project_id: int, server_name):
        return self.project_id_to_server_name_to_server.get(project_id, {}).get(server_name, None)

    def get_servers_dict(self, project_id: int):
        return self.project_id_to_server_name_to_server.get(project_id, {})

    def refresh_and_get_server(self, project_id: int, server_name, server_provider):
        old_server_version = self.project_id_to_server_name_to_server.get(project_id, {}).get(server_name, None)
        #
        if old_server_version:
            new_server_version = server_provider(old_server_version)
            #
            if new_server_version:
                # Update the server with the new version in the storage
                self.project_id_to_server_name_to_server[project_id][server_name] = new_server_version
                #
                return new_server_version
            #
            # If the server is no longer available, remove it from the storage
            self.project_id_to_server_name_to_server[project_id].pop(server_name, None)
        #
        return None

    def remove_servers(self, sid: str) -> list[dict]:
        if project_id := self.sid_to_project_id.get(sid, None):
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
            self.sid_to_project_id.pop(sid, None)
            return removed_servers
        return []

    def validate_all(self, sid_connected_state_provider):
        for sid in list(self.sid_to_project_id.keys()):
            if not sid_connected_state_provider(sid):
                self.remove_servers(sid)

    def status(self):
        result = []
        for project_id, servers in self.project_id_to_server_name_to_server.items():
            server_names = list(servers.keys())
            result.append(f"\nProject {project_id}:\n       {server_names}")
        return "".join(result)
