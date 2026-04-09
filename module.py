import os
import re

from json import dumps
from pathlib import Path
from queue import Empty
from threading import Thread, Event

from pylon.core.tools import module, log

from tools import db, theme, config as c, auth, context, this
import arbiter  # pylint: disable=E0401

from .utils.sio_utils import SioEvents
from .scripts.tool_icons import download_github_repo_zip, unzip_file
from .utils.prompt_eliminate_utils import prompt_2_agent_migration


LEGACY_CONFIG_MODULES = ("elitea_ui", "promptlib_shared", "applications")


class Module(module.ModuleModel):
    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor
        self.thread = None
        #
        self.bp = None
        #
        self.event_node = None
        self._setup_from_old_config()
        event_node_config = self.descriptor.config.get("event_node", None)
        #
        if event_node_config is not None:
            self.event_node = arbiter.make_event_node(config=event_node_config)
        else:
            module_manager = self.context.module_manager
            #
            for module_name in ["datasources"]:
                if module_name in module_manager.modules:
                    cfg_module = module_manager.modules[module_name].module
                    #
                    clone_config = cfg_module.event_node.clone_config
                    if clone_config is None:
                        continue
                    #
                    clone_config = clone_config.copy()
                    #
                    self.event_node = arbiter.make_event_node(config=clone_config)
                    break
        #
        if self.event_node is None:
            raise ValueError("No event_node config")
        #
        self.toolkit_configurations_ready_event = Event()
        #
        self.task_node = arbiter.TaskNode(
            self.event_node,
            #
            pool="indexer",
            ident_prefix="indexer_",
            multiprocessing_context="fork",
            #
            task_limit=0,
            #
            kill_on_stop=False,
            stop_node_task_wait=3,
            #
            housekeeping_interval=60,
            task_retention_period=3600,
            #
            start_attempts=5,
            start_max_wait=12,
            #
            query_wait=5,
            watcher_max_wait=3,
        )
        # logs in-memory cache
        self.task_logs = {}
        #
        config = self.descriptor.config
        base_path = Path(
            config.get("icons_base_path", "/data/static")
        )
        #
        self.application_icon_path = base_path.joinpath(
            config.get("application_icon_subpath", "application_icon")
        )
        #
        self.application_tool_icon_path = base_path.joinpath(
            config.get("application_tool_icon_subpath", "elitea_static-main/tool_icons")
        )
        #
        self.default_entity_icons_path = base_path.joinpath(
            config.get("default_entity_icons_subpath", "elitea_static-main/default_entity_icons")
        )
        #
        # EliteA UI attributes (migrated from elitea_ui plugin)
        self.elitea_base_path = Path('ui', 'dist')
        self.build_meta = {
            'release': None,
            'updated_at': None,
            'commit_sha': '',
            'commit_ref': '',
        }
        self.standalone_mode = False
        self.release_owner = None
        self.release_repo = None
        self.default_release = None
        self.release_verify = None
        self.auth_token = None
        #
        self._configure_elitea_ui()
        self._register_openapi()

    def _configure_elitea_ui(self):
        """Configure elitea_ui settings from descriptor config"""
        self.release_owner = self.descriptor.config.get("release_owner", "EliteaAI")
        self.release_repo = self.descriptor.config.get("release_repo", "EliteaUI")
        self.release_verify = self.descriptor.config.get("release_verify", True)
        #
        self.default_release = self.descriptor.config.get("default_release", "latest")
        #
        if self.default_release == "main" and \
                self.release_owner == "EliteaAI" and \
                self.release_repo == "EliteaUI":
            self.default_release = "latest"
        #
        self.auth_token = self.descriptor.config.get(
            "auth_token",
            os.environ.get(
                self.descriptor.config.get("auth_token_env", "LICENSE_PASSWORD"),
                None,
            ),
        )

    def _setup_from_old_config(self):
        """Merge legacy module configs with lower priority than current descriptor config.

        Uses direct descriptor access to avoid triggering DB initialization or other
        side effects that might occur with this.for_module() or module instance access.
        """
        res_config = {}
        descriptor_config = self.descriptor.config or {}

        # Use descriptors dict - it's populated before __init__() runs and doesn't trigger
        # any DB initialization or other side effects (unlike module_manager.modules)
        descriptors = self.context.module_manager.descriptors

        # Merge legacy configs (lowest priority)
        for module_name in LEGACY_CONFIG_MODULES:
            legacy_descriptor = descriptors.get(module_name)
            if legacy_descriptor is None:
                continue

            legacy_config = legacy_descriptor.config
            if legacy_config:
                log.warning(
                    "Using config from DEPRECATED legacy module '%s': %s keys found",
                    module_name,
                    len(legacy_config)
                )
                for key in legacy_config.keys():
                    if key not in descriptor_config:
                        log.warning("  - Using legacy config key: %s", key)
                res_config.update(legacy_config)

        # Merge current descriptor config (highest priority - overrides legacy)
        if res_config:
            res_config.update(descriptor_config)
            self.descriptor.config = res_config

    def _register_openapi(self):
        """Register API endpoints with OpenAPI registry."""
        from tools import openapi_registry # pylint: disable=E0401,C0415
        from .api import v2 as api_v2
        openapi_registry.register_plugin(
            plugin_name="elitea_core",
            version=self.descriptor.metadata.get("version", "1.0.0"),
            description="Elitea core API endpoints",
            api_module=api_v2,
        )

    def _init_publish_validation_secret(self):
        """Initialize HMAC signing key for publish validation tokens."""
        configured = self.descriptor.config.get('publish_validation_secret')
        if configured:
            self._publish_validation_secret = configured
        else:
            self._publish_validation_secret = os.urandom(32).hex()
            log.info("Auto-generated publish validation secret (valid until restart)")

    def _init_publishing_guardrail(self):
        """Cache publishing guardrail settings from config."""
        guardrail = self.descriptor.config.get('publishing_guardrail', {})
        self.is_publish_blocked = guardrail.get('is_publish_blocked', False)
        self.publish_whitelist_project_ids = set(
            int(x) for x in guardrail.get('whitelist_project_ids', [])
            if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit())
        )
        log.info("Publishing guardrail: blocked=%s, whitelist=%s",
                 self.is_publish_blocked, self.publish_whitelist_project_ids)

    def preload(self):
        """Preload handler - download UI bundle if needed"""
        log.info("Preloading UI bundle")
        #
        static_folder = os.path.join(self.descriptor.path, "static")
        idx_test_path = Path(static_folder).joinpath(self.elitea_base_path, "index.html")
        #
        try:
            from tools import this  # pylint: disable=E0401,C0415
            #
            def _install_needed(*_args, **_kwargs):
                try:
                    return not idx_test_path.exists()
                except:  # pylint: disable=W0702
                    return True
            #
            this.for_module("bootstrap").module.get_bundle(
                "EliteAUI.zip",
                install_needed=_install_needed,
                processing="zip_extract",
                extract_target=Path(static_folder).joinpath(self.elitea_base_path),
                extract_cleanup=True,
                extract_cleanup_skip_files=[".gitkeep"],
            )
            #
            log.info("Preloaded UI bundle")
        except:  # pylint: disable=W0702
            log.exception("Failed to preload UI bundle")

    def ready(self):
        try:
            from tools import this
            this.for_module("admin").module.register_admin_task(
                "eliminate_prompts", prompt_2_agent_migration
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_toolkit_selected_tools", self.migrate_toolkit_selected_tools
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_provider_hub_secrets", self.migrate_provider_hub_secrets
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_application_description_size", self.migrate_application_description_size
            )
        except Exception as e:
            log.exception("Failed to register admin tasks: %s", e)

        self.handle_pylon_modules_initialized()

        try:
            self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists({
                'rpc_func': 'applications_empty_state',
                'rpc_kwargs': {'days_to_retain': 1},
                'name': 'empty_agent_state',
                'cron': '0 0 * * *',
                'active': True
            })
            self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists({
                'rpc_func': 'applications_check_index_scheduling',
                'rpc_kwargs': {},
                'name': 'index_scheduling',
                'cron': '* * * * *',
                'active': True
            })
            self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists({
                'rpc_func': 'elitea_core_cleanup_stale_chunks',
                'rpc_kwargs': {'max_age_seconds': 43200},
                'name': 'cleanup_stale_chunks',
                'cron': '0 */12 * * *',
                'active': True
            })
        except Empty:
            log.warning('No scheduling plugin found')

        log.info("Starting chat thread")
        self.thread.start()
        try:
            self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists({
                'rpc_func': 'chat_canvas_save_versions',
                'rpc_kwargs': {},
                'name': 'save_canvas_versions',
                'cron': '* * * * *',
                'active': True
            })
        except Empty:
            log.warning('No scheduling plugin found')

        # Load providers and register RPC method
        self.load_providers()

        # Register provider RPC method on worker client
        try:
            worker_client = this.for_module("worker_client").module
            worker_client.rpc_node.register(
                self.get_provider_api_info,
                name="get_provider_api_info",
            )
        except Exception as e:
            log.warning('Failed to register provider RPC method: %s', e)

    def init(self):
        self.bp = self.descriptor.init_all(url_prefix="/app")

        # Expose elitea_core config as a shared tool for cross-plugin access
        # Usage: from tools import elitea_config; elitea_config.get("ai_project_id", 1)
        self.descriptor.register_tool('elitea_config', self.descriptor.config)

        # Initialize elitea_ui FIRST (it originally loaded before elitea_core)
        self.elitea_ui_init()

        theme.register_mode_section(
            "administration", "elitea", "EliteA",
            kind="holder",
            permissions={
                "permissions": ["runtime.elitea"],
                "recommended_roles": {
                    "administration": {"admin": True, "viewer": False, "editor": False},
                    "default": {"admin": True, "viewer": False, "editor": False},
                    "developer": {"admin": True, "viewer": False, "editor": False},
                }
            },
            location="left",
            icon_class="fas fa-info-circle fa-fw",
        )
        theme.register_mode_subsection(
            "administration", "elitea",
            "ui", "UI",
            title="UI",
            kind="slot",
            permissions={
                "permissions": ["runtime.elitea.ui"],
                "recommended_roles": {
                    "administration": {"admin": True, "viewer": False, "editor": False},
                    "default": {"admin": True, "viewer": False, "editor": False},
                    "developer": {"admin": True, "viewer": False, "editor": False},
                }
            },
            prefix="admin_elitea_ui_",
            icon_class="fas fa-server fa-fw",
        )

        # self.init_db()
        # TaskNode
        self.task_node.start()
        self.task_node.subscribe_to_task_statuses(self.task_status_changed)
        # Events
        self.event_node.subscribe("application_stream_response", self.stream_response)
        self.event_node.subscribe("application_full_response", self.conversation_message_proxy)
        self.event_node.subscribe("application_partial_response", self.conversation_partial_message_proxy)
        self.event_node.subscribe("application_child_message", self.child_message_proxy)
        # self.event_node.subscribe("log_data", self.log_data)
        # configurations
        self.event_node.subscribe("application_toolkit_configurations_collected", self.toolkit_configurations_collected)
        self.event_node.emit("application_toolkit_configurations_request", dict())
        # toolkits
        self.toolkit_schemas = {}
        self.event_node.subscribe("application_toolkits_collected", self.toolkits_collected)
        self.event_node.emit("application_toolkits_request", dict())
        # file loaders (documents + images)
        self.index_types = {}
        self.event_node.subscribe("application_file_loaders_collected", self.index_types_collected)
        self.event_node.emit("application_file_loaders_request", dict())
        # MCP prebuilt configurations
        self.mcp_prebuilt_configs = {}
        self.event_node.subscribe("application_mcp_prebuilt_config_collected", self.mcp_prebuilt_config_collected)
        self.event_node.emit("application_mcp_prebuilt_config_request", dict())
        #
        self.application_icon_path.mkdir(parents=True, exist_ok=True)
        self.application_tool_icon_path.mkdir(parents=True, exist_ok=True)
        self.default_entity_icons_path.mkdir(parents=True, exist_ok=True)
        #
        if not os.listdir(self.application_tool_icon_path) or \
                not os.listdir(self.default_entity_icons_path):
            #
            log.info("Preloading static icons")
            #
            zip_path = download_github_repo_zip(
                repo_owner=self.descriptor.config.get("icons_repo_owner", "EliteaAI"),
                repo_name=self.descriptor.config.get("icons_repo_name", "elitea_static"),
                local_dir=self.descriptor.config.get("icons_base_path", "/data/static"),
            )
            #
            if zip_path.get("ok"):
                unzip_file(
                    zip_path.get("path"),
                    self.descriptor.config.get("icons_base_path", "/data/static"),
                    self.descriptor.config.get("icons_zip_subfolder", None),
                )
        #
        try:
            this.for_module("admin").module.register_admin_task(
                "download_static_icons", self.download_static_icons
            )
        except:  # pylint: disable=W0702
            log.exception("Failed to register admin tasks")
        #
        self.create_scheduling()

        # MCP SSE initialization
        self.mcp_sse_init()

        # Provider Hub initialization
        self.provider_hub_init()

        # Publish validation secret (HMAC signing key)
        self._init_publish_validation_secret()

        # Publishing guardrail (environment-wide block)
        self._init_publishing_guardrail()

        from .models import all, folder, message_group, participants
        from .models.message_items import base, text, canvas

        self.thread = Thread(
            target=self.listen_in_memory_event
        )
        self.thread.daemon = True
        self.thread.name = 'chat_thread'
        log.info("------------------------elite_core init() OK-------------------------------------------")

    def listen_in_memory_event(self, *args, **kwargs):
        try:
            redis_client = self.get_redis_client()
            try:
                redis_client.config_set('notify-keyspace-events', 'Ex')
            except:  # pylint: disable=W0702
                log.exception("Failed to change redis config, continuing")

            r_pubsub = redis_client.pubsub()
            r_pubsub.subscribe(f'__keyevent@{c.REDIS_CHAT_CANVAS_DB}__:expired')

            for message in r_pubsub.listen():
                if not message:
                    continue

                if message['type'] == 'message':
                    self.canvas_save_expired_version(
                        message['data'],
                    )

        except Exception as e:
            log.error(f'Listen in memory events error: {e}')

    def conversation_message_proxy(self, event: str, payload: dict, *args):
        # log.debug(f'conversation_message_proxy {payload}')
        if payload['sio_event'] == SioEvents.chat_predict.value:
            self.context.event_manager.fire_event('chat_message_stream_end', payload)

    def conversation_partial_message_proxy(self, event: str, payload: dict, *args):
        # log.debug(f'conversation_partial_message_proxy {payload}')
        if payload['sio_event'] == SioEvents.chat_predict.value:
            self.context.event_manager.fire_event('chat_message_stream_partial_save', payload)

    def child_message_proxy(self, event: str, payload: dict, *args):
        # log.debug(f'child_message_proxy {payload}')
        if payload.get('sio_event') == SioEvents.chat_predict.value:
            self.context.event_manager.fire_event('chat_child_message_save', payload)

    def deinit(self):
        log.info('De-initializing')
        self.thread._stop()

        # MCP SSE deinitialization
        self.mcp_sse_deinit()

        # Provider Hub deinitialization
        self.provider_hub_deinit()

        # Events
        # self.event_node.unsubscribe("log_data", self.log_data)
        self.event_node.unsubscribe("application_stream_response", self.stream_response)
        self.event_node.unsubscribe("application_full_response", self.conversation_message_proxy)
        self.event_node.unsubscribe("application_partial_response", self.conversation_partial_message_proxy)
        self.event_node.unsubscribe("application_child_message", self.child_message_proxy)
        self.event_node.unsubscribe("application_toolkit_configurations_collected", self.toolkit_configurations_collected)
        self.event_node.unsubscribe("application_toolkits_collected", self.toolkits_collected)
        self.event_node.unsubscribe("application_file_loaders_collected", self.index_types_collected)
        self.event_node.unsubscribe("application_mcp_prebuilt_config_collected", self.mcp_prebuilt_config_collected)

        # TaskNode
        self.task_node.stop()
        try:
            from tools import this
            this.for_module("admin").module.unregister_admin_task(
                "eliminate_prompts", prompt_2_agent_migration
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_toolkit_selected_tools", self.migrate_toolkit_selected_tools
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_provider_hub_secrets", self.migrate_provider_hub_secrets
            )
        except Exception as e:
            log.exception("Failed to unregister admin tasks: %s", e)

        # De-init
        self.descriptor.deinit_all()

    def reconfig(self):
        """Re-config"""
        # Reconfigure elitea_ui settings
        self._configure_elitea_ui()

    def create_scheduling(self):
        schedule1_data = {
            'name': 'Check vectorstore creds for each project',
            'cron': '*/10 * * * *',
            'rpc_func': 'applications_create_pgvector_credentials',
            'active': False
        }
        self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists(schedule1_data)

    # MCP SSE Methods
    def mcp_sse_init(self):
        """Initialize MCP SSE specific functionality"""
        # Cache MCP exposure settings (read once at startup)
        mcp_config = self.descriptor.config.get('mcp_exposure', {})
        self.mcp_exposure_enabled = mcp_config.get('enabled', True)
        self.mcp_in_menu_enabled = mcp_config.get('in_menu', True)
        log.info(f"MCP exposure enabled: {self.mcp_exposure_enabled}, in_menu: {self.mcp_in_menu_enabled}")

        # Add public messages route (optional)
        if self.descriptor.config.get("public_messages_route", False):
            log.info("Making /messages public")
            auth.add_public_rule({
                "uri": f"{context.url_prefix}/{this.module_name}/[0-9]+/messages\\?session_id=.+",
            })

        # Initialize MCP servers storage
        from .utils.mcp_servers_storage import ServersStorage
        self.servers_storage = ServersStorage()

        # Register SIO disconnect handler
        self.context.sio.on("disconnect", handler=self.sio_disconnect)

        # Schedule MCP server handler
        try:
            self.schedule_mcp_servers_handler()
        except Empty:
            log.warning('Scheduling module is not available')

    def mcp_sse_deinit(self):
        """Deinitialize MCP SSE specific functionality"""
        # Remove public route if configured
        if self.descriptor.config.get("public_messages_route", False):
            log.info("Un-making /messages public")
            auth.remove_public_rule({
                "uri": f"{context.url_prefix}/{this.module_name}/[0-9]+/messages\\?session_id=.+",
            })

    @auth.decorators.sio_disconnect()
    def sio_disconnect(self, sid, *args, **kwargs):
        """Handle SocketIO disconnect for MCP servers"""
        removed_servers = self.servers_storage.remove_servers(sid)
        for server in removed_servers:
            log.debug(f"[MCP_CLIENT] Server {server['name']} disconnected")
            self.context.sio.emit(
                event=SioEvents.mcp_status,
                data={"connected": False, "project_id": server['project_id'], "type": server['name']},
            )

    def schedule_mcp_servers_handler(self):
        """Schedule periodic MCP server validation task"""
        schedule_data = {
            'name': 'mcp_servers_handler',
            'cron': '*/1 * * * *',
            'rpc_func': 'mcp_servers_handler'
        }
        self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists(schedule_data)

    # Provider Hub Methods
    def provider_hub_init(self):
        """Initialize Provider Hub functionality"""
        # Register admin sections
        theme.register_mode_section(
            "administration", "airun", "AI/Run",
            kind="holder",
            permissions={
                "permissions": ["runtime.airun"],
                "recommended_roles": {
                    "administration": {"admin": True, "viewer": False, "editor": False},
                    "default": {"admin": True, "viewer": False, "editor": False},
                    "developer": {"admin": True, "viewer": False, "editor": False},
                }
            },
            location="left",
            icon_class="fas fa-info-circle fa-fw",
        )
        theme.register_mode_subsection(
            "administration", "airun",
            "serviceproviders", "ServiceProviders",
            title="ServiceProviders",
            kind="slot",
            permissions={
                "permissions": ["runtime.airun.serviceproviders"],
                "recommended_roles": {
                    "administration": {"admin": True, "viewer": False, "editor": False},
                    "default": {"admin": True, "viewer": False, "editor": False},
                    "developer": {"admin": True, "viewer": False, "editor": False},
                }
            },
            prefix="admin_airun_serviceproviders_",
            icon_class="fas fa-server fa-fw",
        )

    def provider_hub_deinit(self):
        """Deinitialize Provider Hub functionality"""
        # Cleanup provider storage
        if hasattr(self, 'present_providers'):
            self.present_providers.clear()
        if hasattr(self, 'unhealthy_providers'):
            self.unhealthy_providers.clear()

    # EliteA UI Methods
    def elitea_ui_init(self):
        """Initialize elitea_ui functionality (migrated from elitea_ui plugin)"""
        log.info("Initializing elitea_ui")
        #
        # Determine mode
        #
        module_manager = self.context.module_manager
        self.standalone_mode = "theme" not in module_manager.modules
        #
        # Blueprint already initialized by descriptor.init_all()
        # Just ensure we have it
        if not hasattr(self, 'bp') or self.bp is None:
            log.warning("Blueprint not initialized, cannot complete elitea_ui_init")
            return
        #
        if self.standalone_mode:
            import flask as _flask  # pylint: disable=C0415
            from pylon.core.tools.context import Context as Holder  # pylint: disable=C0415
            #
            # Register "default" mode landing with the framework router
            # so "/" redirects to the EliteA UI at /app/
            # (the framework router already handles "/" in app_router)
            #
            from tools import router  # pylint: disable=E0401,C0415
            router.register_mode(
                kind="route",
                route="elitea_core.route_elitea_ui",
            )
            #
            # Set g.theme on ALL apps for auth compatibility
            # (uses app shim → register_app_hook → applies to every Flask app)
            #
            def _set_g_theme():
                _flask.g.theme = Holder()
                _flask.g.theme.active_section = None
                _flask.g.theme.active_subsection = None
                _flask.g.theme.active_mode = c.DEFAULT_MODE
                _flask.g.theme.active_parameter = None
            self.context.app.before_request(_set_g_theme)
            #
            # SocketIO connect handler (save auth data for SID)
            #
            def _standalone_sio_connect(sid, environ, *args, **kwargs):
                auth.sio_users[sid] = auth.sio_make_auth_data(environ)
                log.debug("SIO connect (standalone): %s", sid)
            self.context.sio.on("connect", handler=_standalone_sio_connect)
            #
            # Public auth rules (moved from theme)
            #
            auth.add_public_rule({"uri": f'{re.escape("/socket.io/")}.*'})
            auth.add_public_rule({"uri": re.escape("/robots.txt")})
            auth.add_public_rule({"uri": re.escape("/favicon.ico")})
            auth.add_public_rule({"uri": re.escape("/app/access_denied")})
            #
            # Set auth denied URL to styled access denied page
            #
            auth.descriptor.config["auth_denied_url"] = "/app/access_denied"
            #
            # Global error handler for styled error pages
            # (uses app shim → register_app_hook → applies to every Flask app)
            #
            import traceback as _tb  # pylint: disable=C0415
            from werkzeug.exceptions import HTTPException  # pylint: disable=C0415
            #
            _error_pages = {
                400: (
                    "Bad Request",
                    "The server couldn't understand your request.",
                    ["Check that the URL is correct", "Try refreshing the page"],
                ),
                403: (
                    "Access Denied",
                    "Sorry, you don't have permission to access this resource.",
                    ["Your session may have expired", "You may lack the required permissions"],
                ),
                404: (
                    "Page Not Found",
                    "The page you're looking for doesn't exist or has been moved.",
                    ["Check that the URL is correct", "The page may have been moved or deleted"],
                ),
                405: (
                    "Method Not Allowed",
                    "The request method is not supported for this resource.",
                    ["Check the API documentation for allowed methods"],
                ),
                500: (
                    "Internal Server Error",
                    "Something went wrong on our end.",
                    ["Try again in a few moments", "If the problem persists, contact your administrator"],
                ),
                502: (
                    "Bad Gateway",
                    "The server received an invalid response from an upstream service.",
                    ["Try again in a few moments", "The service may be temporarily unavailable"],
                ),
                503: (
                    "Service Unavailable",
                    "The service is temporarily unavailable.",
                    ["Try again in a few moments", "The service may be undergoing maintenance"],
                ),
            }
            _descriptor = self.descriptor
            #
            def _error_handler(error):
                code = error.code if isinstance(error, HTTPException) else 500
                log.error(
                    "Error: (%s) %s:\n%s",
                    type(error), error,
                    "".join(_tb.format_tb(error.__traceback__)),
                )
                # Return JSON for API requests
                if _flask.request.path.startswith("/api/"):
                    msg = str(error)
                    if isinstance(error, HTTPException):
                        msg = error.description
                    return _flask.jsonify({"error": msg}), code
                # Render styled HTML page for browser requests
                title, message, hints = _error_pages.get(
                    code,
                    ("Error", "An unexpected error occurred.", ["Try again or go back to the main page"]),
                )
                return _descriptor.render_template(
                    "error.html",
                    error_code=code,
                    error_title=title,
                    error_message=message,
                    error_hints=hints,
                ), code
            self.context.app.errorhandler(Exception)(_error_handler)
        else:
            #
            # Register a mode (for admin UI switch-back)
            #
            from tools import theme  # pylint: disable=E0401,C0415
            #
            try:
                theme.register_mode(
                    "elitea", "EliteA",
                    public=True,
                )
            except:  # pylint: disable=W0702
                log.warning("Failed to register EliteA mode, assuming present")
            #
            # IMPORTANT: Change route reference from elitea_ui to elitea_core
            theme.register_mode_landing(
                mode="elitea",
                kind="route",
                route="elitea_core.route_elitea_ui",  # Changed from elitea_ui.route_elitea_ui
            )
        #
        # Download UI if needed for first time
        #
        idx_test_path = Path(self.bp.static_folder).joinpath(self.elitea_base_path, "index.html")
        if not idx_test_path.exists():
            log.info("Downloading and installing initial release: %s", self.default_release)
            self.update_ui()

    # def init_db(self):
    #     log.info("DB init")
    #     from .models.all import (
    #         Tag
    #     )
    #     project_list = self.context.rpc_manager.call.project_list(filter_={'create_success': True})
    #     for i in project_list:
    #         log.info("Creating missing tables in project %s", i['id'])
    #         with db.with_project_schema_session(i['id']) as tenant_db:
    #             db.get_all_metadata().create_all(bind=tenant_db.connection())
    #             tenant_db.commit()
