from queue import Empty

from pylon.core.tools import log, web
from tools import this


applications_roles = [
    "models.applications.applications.list",
    "models.applications.applications.create",
    "models.applications.version.details",
    "models.applications.version.update",
    "models.applications.version.delete",
    "models.applications.application.details",
    "models.applications.application.delete",
    "models.applications.application.update",
    "models.applications.predict.post",
    "models.applications.task.get",
    "models.applications.task.delete",
    "models.applications.tool.details",
    "models.applications.tool.delete",
    "models.applications.tool.update",
    "models.applications.tools.list",
    "models.applications.tools.create",
    "models.applications.toolkits.details",
    "models.applications.versions.get",
    "models.applications.versions.create"
]


class Method:
    @web.method()
    def handle_pylon_modules_initialized(self):
        #
        # event_pylon_id = payload
        # if self.context.id != event_pylon_id:
        #     return
        #
        try:
            autocreate_dbs_enabled = \
                this.for_module("bootstrap").descriptor.config.get("autocreate_dbs", {}).get("enabled", False)
        except:  # pylint: disable=W0702
            autocreate_dbs_enabled = False
        #
        if self.descriptor.config.get("auto_setup", False):
            log.info("Performing post-init setup checks")
            # Data
            # Custom public project roles removed — all permissions folded
            # into standard roles (viewer, editor, admin) in central config.
            setup_roles = {}
            #
            # Create public project if it doesn't exist yet
            system_user = "system@centry.user"
            try:
                system_user_id = self.context.rpc_manager.call.auth_get_user(
                    email=system_user,
                )["id"]
            except:  # pylint: disable=W0702
                system_user_id = None
            #
            # Check if public project already exists by trying to look it up
            ai_project_id = self.descriptor.config.get("ai_project_id", 1)
            public_project_exists = False
            try:
                existing = self.context.rpc_manager.call.project_get_by_id(int(ai_project_id))
                public_project_exists = existing is not None
            except:  # pylint: disable=W0702
                pass
            #
            public_project_id = None
            if not public_project_exists and system_user_id is not None:
                public_project_name = self.descriptor.config.get(
                    "public_project_name",
                    "promptlib_public",
                )
                #
                if autocreate_dbs_enabled:
                    log.info("Autocreate DBs enabled - waiting for toolkit configurations and runtime engine ready events")
                    self.toolkit_configurations_ready_event.wait()
                    worker_client = this.for_module("worker_client").module
                    worker_client.runtime_engine_ready_event.wait()
                #
                public_project_id = self.context.rpc_manager.call.projects_create_project(
                    project_name=public_project_name,
                    plugins=["configuration", "models"],
                    admin_email=system_user,
                    owner_id=system_user_id,
                    roles=["system"],
                )
                #
                if public_project_id is not None:
                    # Store project ID in plugin config for persistence
                    log.info("Public project created with ID: %s", public_project_id)
                    #
                    if autocreate_dbs_enabled:
                        from tools import config  # pylint: disable=C0415,E0401
                        #
                        vectors_db_url = 'postgresql://{username}:{password}@{host}:{port}/{database}'.format(  # pylint: disable=C0209
                            host=config.POSTGRES_HOST,
                            port=config.POSTGRES_PORT,
                            username=config.POSTGRES_USER,
                            password=config.POSTGRES_PASSWORD,
                            database="vectors",
                        )
                        # Create PGVector configuration
                        _, created = self.context.rpc_manager.call.configurations_create_if_not_exists(
                            payload={
                                'elitea_title': "elitea-pgvector",
                                'label': "elitea-pgvector",
                                'project_id': public_project_id,
                                'type': 'pgvector',
                                'source': 'system',
                                'section': 'vectorstorage',
                                'data': {
                                    'connection_string': vectors_db_url,
                                },
                            }
                        )
                        log.info("PGVector configuration %s created: %s", "elitea-pgvector", created)
                        # Sync PGVector credentials
                        from plugins.admin.tasks import project_tasks  # pylint: disable=C0415,E0401
                        project_tasks.sync_pgvector_credentials(param="force_recreate,save_connstr_to_secrets")
            # Apply/add correct permissions (keep extra manually added for now)
            if public_project_exists or public_project_id is not None:
                public_project_id = int(ai_project_id) if public_project_exists else int(public_project_id)
                #
                for role, permissions in setup_roles.items():
                    role_item = self.context.rpc_manager.call.admin_get_role(
                        project_id=public_project_id,
                        role_name=role,
                    )
                    #
                    if not role_item:
                        log.info("Adding role: %s", role)
                        #
                        self.context.rpc_manager.call.admin_add_role(
                            project_id=public_project_id,
                            role_names=[role],
                        )
                        #
                        self.context.rpc_manager.call.admin_set_permissions_for_role(
                            project_id=public_project_id,
                            role_name=role,
                            permissions=permissions,
                        )
                    else:
                        role_perms = self.context.rpc_manager.call.admin_get_permissions_for_role(
                            project_id=public_project_id,
                            role_name=role,
                        )
                        missing_permissions = list(set(permissions) - set(role_perms))
                        #
                        log.info("Adding new/missing permissions for role: %s -> %s", role, missing_permissions)
                        #
                        self.context.rpc_manager.call.admin_add_permissions_for_role(
                            project_id=public_project_id,
                            role_name=role,
                            permissions=missing_permissions,
                        )
            # Activate personal project schedule
            try:
                self.context.rpc_manager.timeout(5).scheduling_make_active(
                    "projects_create_personal_project",
                    True,
                )
            except Empty:
                log.warning('Scheduling module is not available')
