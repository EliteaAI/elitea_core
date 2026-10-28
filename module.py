import functools
import os
import re

from collections import OrderedDict
from json import dumps
from pathlib import Path
from queue import Empty
from threading import Thread, Event, Lock, Timer

from pylon.core.tools import module, log

from tools import db, config as c, auth, context, this
import arbiter  # pylint: disable=E0401

from .utils.evaluation_run_utils import EVAL_RUN_TASK_NAME, execute_run_task
from .utils.admission import AdmissionSettings, admit_dispatch, track_context
from .utils.binding_api import PERMISSIONS as BINDING_PERMISSIONS, registration as binding_registration
from .utils.governance import init_governance
from .utils.governance_settings import has_live_contexts, record_boot_preflight
from .utils.mandate_admission import TicketStore
from .utils.run_id import stamp_predict_run_id
from .utils.schedule_access import register_schedule_permissions
from .utils.schedule_settings import register_schedule_settings_type
from .utils.worker_capabilities import (
    CAPABILITIES_EVENT,
    CAPABILITIES_REQUEST_EVENT,
    DEFAULT_CAPABILITY_TTL_S,
    WITHHOLD_EVENT,
    WorkerCapabilityRegistry,
    apply_envelope_gate,
)
from .utils.scheduler_bindings import (
    advertised_bindings,
    build_scheduler_sync_plan,
)
from pydantic import ValidationError

from .models.pd.sio import NextInputSuggestionPayload
from .utils.sio_utils import SioEvents
from .scripts.tool_icons import download_github_repo_zip, unzip_file
from .utils.prompt_eliminate_utils import prompt_2_agent_migration
from .sio.asr import on_whisper_call_done


LEGACY_CONFIG_MODULES = ("elitea_ui", "promptlib_shared", "applications")


class Module(module.ModuleModel):
    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor
        self.thread = None
        self._scheduler_bindings_active = True
        #
        self.bp = None
        #
        self.event_node = None
        self._setup_from_old_config()
        event_node_config = self.descriptor.config.get("event_node", None)
        #
        if event_node_config is not None:
            self.event_node = arbiter.make_event_node(config=event_node_config)
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
        #
        # Eval runs execute *locally* in pylon_main (they orchestrate agent predicts and judge
        # dispatches, both of which go out through `task_node` above), so this node serves its own
        # pool rather than dispatching to the indexer.
        #
        # Why a node at all instead of the bare daemon thread this replaces: `task_limit` bounds how
        # many runs execute at once, so a handful of users starting 50-case runs can no longer spawn
        # unbounded threads all hammering the judge model; and registering the node in the bootstrap
        # TASK_TARGETS makes graceful shutdown drain in-flight runs instead of killing them.
        #
        # It does NOT make a run survive the process dying — arbiter keeps task state in memory, so
        # a hard restart still orphans a `running` row. That is what the eval_run_reap cron is for.
        #
        # The limit is per node, and gunicorn runs several pylon_main workers each with their own
        # node, so the effective ceiling is limit x workers. It is a guard against a single user
        # flooding the pool, not a global quota.
        self.eval_task_node = arbiter.TaskNode(
            self.event_node,
            #
            pool="eval_runs",
            ident_prefix="eval_run_",
            multiprocessing_context="threading",
            #
            task_limit=self.descriptor.config.get("eval_run_task_limit", 4),
            #
            result_transport="memory",
            # A run's terminal state is written to the `eval_run` row by the task itself, so the
            # in-memory result is never read and does not need to be retained for long.
            task_retention_period=600,
            housekeeping_interval=60,
            thread_scan_interval=0.1,
            #
            kill_on_stop=False,
            stop_node_task_wait=3,
            #
            start_attempts=1,
            start_max_wait=3,
            query_wait=3,
            watcher_max_wait=3,
            result_max_wait=3,
        )
        # Task callback state (used by task_status_changed in methods/task_callbacks.py).
        # Must be initialized here in __init__ — the callback is registered in init() via
        # task_node.subscribe_to_task_statuses, which can fire before ready() runs.
        self.callback_tasks = {}
        # task_id -> (project_id, context_ref) of governed runs admitted by this process (#6756); bounded
        self._context_by_task = OrderedDict()
        self._context_lock = Lock()   # the map is read by the deadline sweep while dispatch threads change it
        # Credential governance (#7120): the snapshot of the switch (built in init), and which of the worker bus
        # handlers of a governed run (validate, release one slot, release a batch) are registered. They are registered on
        # the first move into enforce and stay until deinit, so runs admitted under enforce keep validating after a switch to off; booting in off registers none
        self.governance_state = None
        self.governed_handlers_registered = False
        self._governed_handlers = {"validate": False, "release": False, "release_batch": False}
        self._governed_handlers_lock = Lock()
        # single-use tickets of autonomous (mandate) runs, minted and taken in this process only; bounded (#6947)
        self._mandate_tickets = TicketStore()
        self.active_index_tasks = OrderedDict()
        self.active_index_tasks_max = 4096
        self.recently_stopped_index_tasks = OrderedDict()
        # Marked for EVERY stopped task (a late in_progress can't be known in advance), so
        # keep the cap generous to avoid churning out an index task's entry before its late
        # in_progress event lands on a busy platform.
        self.recently_stopped_index_tasks_max = 8192
        # Guards active_index_tasks + recently_stopped_index_tasks: register (in_progress
        # event) and mark+drain ('stopped' callback) run on different threads. Held only
        # around dict ops, never during the cancel's DB/vault I/O.
        self.active_index_tasks_lock = Lock()
        self.not_starting_task_event = Event()
        self.not_starting_task_event.set()
        # Live worker capabilities (C-1009, #6940); only consulted for governed dispatch
        self.worker_capabilities = WorkerCapabilityRegistry(
            ttl_s=self.descriptor.config.get("capability_ttl_s", DEFAULT_CAPABILITY_TTL_S),
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
        self.skill_icon_path = base_path.joinpath(
            config.get("skill_icon_subpath", "skill_icon")
        )
        self.project_icon_path = base_path.joinpath(
            config.get("project_icon_subpath", "project_icon")
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
        self.platform_logo_path = base_path.joinpath(
            config.get("platform_logo_subpath", "platform_logo")
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
            tags=[
                {
                    "name": "elitea_core/applications",
                    "description": "Create, manage, execute, export, import, fork, and publish agents/pipelines and their versions within a project.",
                },
                {
                    "name": "elitea_core/analytics",
                    "description": "Project-level analytics for AI adoption: LLM usage, agent/pipelines runs, tools/toolkits runs, user engagement, and daily trends.",
                },
                {
                    "name": "elitea_core/authors",
                    "description": "Author profile and aggregated contribution statistics (agents, pipelines, toolkits, conversations).",
                },
                {
                    "name": "elitea_core/chat",
                    "description": "Manage conversations, send messages, upload attachments, edit canvases, and manage conversation participants.",
                },
                {
                    "name": "elitea_core/discovery",
                    "description": "Discover available tags and search filter options across agents, pipelines, toolkits, and credentials.",
                },
                {
                    "name": "elitea_core/mcp",
                    "description": "Sync tools from remote MCP servers and proxy OAuth token exchange for MCP integrations.",
                },
                {
                    "name": "elitea_core/project_context",
                    "description": "Manage project context content and AI-assisted project context draft generation.",
                },
                {
                    "name": "elitea_core/schedules",
                    "description": "Run agents and pipelines on a cron schedule: create, edit, pause, resume, "
                                   "run now, change owner, list runs, export and import schedules, draft one "
                                   "with AI, set your notices, and allow \"Run without asking\" in the "
                                   "project. All under /schedules/{mode}/{project_id}.",
                },
                {
                    "name": "elitea_core/skills",
                    "description": "Create, update, version, list, and generate skills from natural-language requirements.",
                },
                {
                    "name": "elitea_core/runtime",
                    "description": "Runtime and infrastructure utility endpoints.",
                },
                {
                    "name": "elitea_core/toolkits",
                    "description": "Browse available toolkit types and manage installed toolkit instances within a project.",
                },
            ],
            api_module=api_v2,
        )
        if not hasattr(openapi_registry, "register_endpoint"):  # the preload stub registers nothing
            return
        # One handler class serves every Schedules URL, so the folder scan cannot list them (BUG-11)
        from .utils.constants import SCHEDULES_OPENAPI_TAG  # pylint: disable=C0415
        from .utils.schedule_openapi import OPENAPI_ENDPOINTS, add_refusal_schema  # pylint: disable=C0415
        add_refusal_schema(openapi_registry)
        for endpoint in OPENAPI_ENDPOINTS:
            openapi_registry.register_endpoint(
                plugin_name="elitea_core", tags=[SCHEDULES_OPENAPI_TAG], available_to_users=True, mcp_tool=False,
                **endpoint)

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
        self.publish_validation_rules = guardrail.get(
            'publish_validation_rules', '',
        )
        # Extra admin-added agent categories (on top of DEFAULT_AGENT_CATEGORIES).
        # The active list is resolved live by category_utils.get_active_categories();
        # this attribute is cached only for logging visibility at init / reload.
        self.extra_agent_categories = guardrail.get('agent_categories', [])
        log.info("Publishing guardrail: blocked=%s, whitelist=%s, custom_rules=%s",
                 self.is_publish_blocked, self.publish_whitelist_project_ids,
                 bool(self.publish_validation_rules))

    def _init_skill_publishing_guardrail(self):
        """Log skill publishing guardrail settings.

        The block toggle, whitelist and validation rules are read live from
        config at use time (see skill_publish_utils), so admin changes take
        effect without a reload. Skill categories reuse the shared agent
        category list (publishing_guardrail.agent_categories).
        """
        guardrail = self.descriptor.config.get('skill_publishing_guardrail', {})
        log.info("Skill publishing guardrail (live): blocked=%s, whitelist=%s, custom_rules=%s",
                 bool(guardrail.get('is_publish_blocked', False)),
                 guardrail.get('whitelist_project_ids', []),
                 bool(guardrail.get('publish_validation_rules', '')))

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
        self._register_managed_schedules()
        self._register_input_guardrail_redactor()
        self._bootstrap_platform_schedules()

        # ORM create_all() creates missing tenant tables but never expands an
        # existing one. Apply the transactionally committed compatibility guard
        # before any chat callback can write the normalized step rows.
        from .utils.trace_step_schema import ensure_trace_step_schema
        ensure_trace_step_schema(db.engine)

        try:
            from tools import this
            this.for_module("admin").module.register_admin_task(
                "eliminate_prompts", prompt_2_agent_migration
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_toolkit_selected_tools", self.migrate_toolkit_selected_tools
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_list_collections_to_list_indexes",
                self.migrate_list_collections_to_list_indexes,
                group="R-2.0.5"
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_provider_hub_secrets", self.migrate_provider_hub_secrets
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_application_description_size", self.migrate_application_description_size
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_toolkit_settings_alita_title", self.migrate_toolkit_settings_alita_title
            )
            this.for_module("admin").module.register_admin_task(
                "chat_cleanup_dup_msgs", self.chat_cleanup_dup_msgs
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_jira_confluence_hosting", self.migrate_jira_confluence_hosting
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_confluence_api_version", self.migrate_confluence_api_version
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_toolkit_settings_fields", self.migrate_toolkit_settings_fields
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_mcp_client_secrets", self.migrate_mcp_client_secrets
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_conversation_source_to_elitea", self.migrate_conversation_source_to_elitea
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_admin_shell_to_inplace", self.migrate_admin_shell_to_inplace
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_agent_version_null_instructions", self.migrate_agent_version_null_instructions
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_ado_project_to_toolkit", self.migrate_ado_project_to_toolkit
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_llm_model", self.migrate_llm_model
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_embedding_model", self.migrate_embedding_model
            )
            this.for_module("admin").module.register_admin_task(
                "collections_removal_migration", self.collections_removal_migration, group="R-2.0.4"
            )
            this.for_module("admin").module.register_admin_task(
                "datasource_removal_migration", self.datasource_removal_migration, group="R-2.0.4"
            )
            this.for_module("admin").module.register_admin_task(
                "cleanup_oversized_message_meta", self.cleanup_oversized_message_meta
            )
            this.for_module("admin").module.register_admin_task(
                "backfill_legacy_trace_steps", self.backfill_legacy_trace_steps, group="R-2.0.5"
            )
            this.for_module("admin").module.register_admin_task(
                "heal_llm_settings_family_conflicts", self.heal_llm_settings_family_conflicts, group="R-2.0.5"
            )
            this.for_module("admin").module.register_admin_task(
                "reassign_agent_category", self.reassign_agent_category,
            )
            this.for_module("admin").module.register_admin_task(
                "rename_agent_category", self.rename_agent_category,
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_skill_publish_columns", self.migrate_skill_publish_columns, group="R-2.0.5",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_empty_conversation_starters", self.migrate_empty_conversation_starters,
                group="R-2.0.5",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_budget_alert_columns", self.migrate_budget_alert_columns, group="R-2.0.5",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_project_member_default_limit", self.migrate_project_member_default_limit,
                group="R-2.0.5",
            )
            this.for_module("admin").module.register_admin_task(
                "rename_skill_category", self.rename_skill_category,
            )
            this.for_module("admin").module.register_admin_task(
                "reassign_skill_category", self.reassign_skill_category,
            )
            this.for_module("admin").module.register_admin_task(
                "set_index_schedule_expiration", self.set_index_schedule_expiration,
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_share_token_columns", self.migrate_share_token_columns, group="R-2.0.6",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_eval_binding_constraints", self.migrate_eval_binding_constraints,
                group="R-2.0.6",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_eval_platform_dimension_columns",
                self.migrate_eval_platform_dimension_columns, group="R-2.0.6",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_eval_dataset_columns", self.migrate_eval_dataset_columns, group="R-2.0.6",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_eval_dimension_columns", self.migrate_eval_dimension_columns, group="R-2.0.6",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_eval_suite_columns", self.migrate_eval_suite_columns, group="R-2.0.6",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_project_chat_config", self.migrate_project_chat_config, group="R-2.0.7",
            )
            this.for_module("admin").module.register_admin_task(
                "seed_eval_trajectory_dimensions", self.seed_eval_trajectory_dimensions,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "prune_orphaned_index_schedules", self.prune_orphaned_index_schedules,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_skill_run_settings_column", self.migrate_skill_run_settings_column,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_eval_case_usage_columns", self.migrate_eval_case_usage_columns,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "migrate_legacy_pipeline_schedules", self.migrate_legacy_pipeline_schedules,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "create_agent_identity_tables", self.create_agent_identity_tables,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "manage_agent_identity_binding", self.manage_agent_identity_binding,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "create_mandate_tables", self.create_mandate_tables,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "manage_autonomous_mandate", self.manage_autonomous_mandate,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "report_trigger_principals", self.report_trigger_principals,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "create_governance_settings_tables", self.create_governance_settings_tables,
                group="R-2.0.8",
            )
            this.for_module("admin").module.register_admin_task(
                "fix_platform_tick_row", self.fix_platform_tick_row,
                group="R-2.0.8",
            )
        except Exception as e:
            log.exception("Failed to register admin tasks: %s", e)

        self._ensure_skill_publish_schema()
        self._ensure_share_token_schema()
        self._ensure_agent_identity_schema()
        self._start_governance()
        self._start_internal_obo()

        self.handle_pylon_modules_initialized()

    def _ensure_skill_publish_schema(self):
        """Startup safety net: apply the skill-publishing columns to any project
        schema that predates them, so a deploy provisions the environment without
        an operator having to run the migration task. Runs off-thread; guarded by a
        catalog check so steady-state boots issue no DDL. New projects are already
        covered by create_all."""
        if not self.descriptor.config.get("skill_publish_auto_migrate", True):
            return

        def _run():
            try:
                from .utils.skill_publish_schema import (
                    project_ids_missing_skill_columns,
                    apply_skill_columns,
                )
                pending = project_ids_missing_skill_columns()
                if not pending:
                    return
                log.info("skill publish schema: auto-migrating %s project(s)", len(pending))
                migrated, failed = apply_skill_columns(pending)
                log.info(
                    "skill publish schema: auto-migration done (migrated=%s failed=%s)",
                    len(migrated), len(failed),
                )
            except Exception:  # pylint: disable=W0703
                log.exception("skill publish schema: auto-migration failed")

        Thread(target=_run, daemon=True).start()

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


    # --- credential governance switch (#7120, C-1009) ---------------------------------------------------------------

    @property
    def governance_effective(self):
        """What is in effect now (the snapshot; no I/O)."""
        return self.governance_state.effective

    @property
    def governance_profile(self):
        """``enabled`` is true under enforce only; audit only keeps the legacy profile."""
        return self.governance_state.effective.profile

    @property
    def admission_settings(self):
        effective = self.governance_state.effective
        return AdmissionSettings(
            allow_runtime_domain_binding=effective.allow_runtime_domain_binding,
            admit_legacy_pat=effective.admit_legacy_pat, allow_development_runs=effective.allow_development_runs,
            context_ttl_s=self.governance_state.context_ttl_s, environment=effective.environment,
        )

    def _start_governance(self):
        """Once every plugin is up: read the switch again (the first read ran in init, before the plugins it needs
        were loaded), keep serving the runs admitted before this restart, start the refresher, and say so if the mode
        is on and the platform cannot honour it."""
        state = self.governance_state
        state.refresh()
        if not self.governed_handlers_registered and has_live_contexts():
            # booting into off (or audit only) with governed runs still live: their workers still validate them and ask
            # for their credentials, so the handlers (and the sweep of their deadlines) stay until the next restart
            log.warning("Credential governance: governed runs admitted before this start are still live; "
                        "their worker handlers are registered")
            self._ensure_governed_handlers_registered()
        state.start()
        record_boot_preflight(self)

    def _ensure_governed_handlers_registered(self):
        """Register the worker bus handlers of a governed run: workers prove a run still holds its admission
        (#6756), and ask Main for the material of a credential slot, or of all the slots of a run in one call, sealed to
        the asking worker (#6946). Idempotent; a handler that could not be registered is tried again at the next
        refresh of the switch. True when the two required ones are (the batch release is optional)."""
        with self._governed_handlers_lock:
            for key, handler, name in (
                    ("validate", self.validate_execution_context, "restricted_validate_execution_context"),
                    ("release", self.release_credential, "restricted_release_credential"),
                    ("release_batch", self.release_credentials, "restricted_release_credentials")):
                if self._governed_handlers[key]:
                    continue
                try:
                    this.for_module("worker_client").module.rpc_node.register(handler, name=name)
                    self._governed_handlers[key] = True
                except Exception as exc:  # pylint: disable=W0703
                    if key == "release_batch":   # optional: workers then release slot by slot, and are not told otherwise
                        log.error("Failed to register %s (%s); batch release is not offered", name, type(exc).__name__)
                    else:
                        log.exception("Failed to register %s", name)
            self.governed_handlers_registered = any(self._governed_handlers.values())
            return self._governed_handlers["validate"] and self._governed_handlers["release"]

    def _unregister_governed_handlers(self):
        with self._governed_handlers_lock:
            for key, handler, name in (
                    ("validate", self.validate_execution_context, "restricted_validate_execution_context"),
                    ("release", self.release_credential, "restricted_release_credential"),
                    ("release_batch", self.release_credentials, "restricted_release_credentials")):
                if not self._governed_handlers[key]:
                    continue
                try:
                    this.for_module("worker_client").module.rpc_node.unregister(handler, name=name)
                except Exception:  # pylint: disable=W0703
                    log.exception("Failed to unregister %s", name)
                self._governed_handlers[key] = False
            self.governed_handlers_registered = False

    def _start_internal_obo(self):
        """The run token of governed runs (#7214): the auth plugin hands its bearers to ``verify_request``, and the
        binding column it scopes by is added to the project schemas that predate it (off-thread, catalog-guarded).
        Without the auth plugin's hook no run token is accepted (the internal MCP refuses it), which fails closed."""
        from .utils import internal_obo  # pylint: disable=C0415
        try:
            auth.register_bearer_verifier(
                internal_obo.TOKEN_PREFIX, lambda token, environ: internal_obo.verify_request(token, environ, module=self))
        except Exception as exc:  # pylint: disable=W0703
            log.error("internal OBO: the run token verifier could not be registered (%s)", type(exc).__name__)
        if internal_obo.signing_key() is None:
            log.warning("internal OBO: SECRETS_MASTER_KEY is not set; governed runs with built-in Elitea tools are "
                        "refused (INTERNAL_TOKEN_UNAVAILABLE)")

        def _run():
            try:
                from .utils.binding_operations_schema import (  # pylint: disable=C0415
                    apply_operations_column, project_ids_missing_operations_column,
                )
                pending = project_ids_missing_operations_column()
                if pending:
                    migrated, failed = apply_operations_column(pending)
                    log.info("binding operations schema: column added (migrated=%s failed=%s)",
                             len(migrated), len(failed))
            except Exception:  # pylint: disable=W0703
                log.exception("binding operations schema: auto-migration failed")

        Thread(target=_run, daemon=True).start()

        def _check_map():
            # every MCP-exposed write must be in the operation map, or agents with a run token are refused it
            try:
                from tools import openapi_registry  # pylint: disable=C0415
                missing = internal_obo.unmapped_writes(openapi_registry.get_mcp_api_tools())
                log.info("security_metric name=internal_obo_unmapped_writes value=%s", len(missing))
                if missing:
                    log.error("internal OBO: MCP-exposed writes missing from the operation map (refused for runs): %s",
                              ", ".join(missing))
            except Exception:  # pylint: disable=W0703
                log.exception("internal OBO: the operation map could not be checked")

        timer = Timer(60.0, _check_map)   # after every plugin registered its OpenAPI endpoints
        timer.daemon = True
        timer.start()

    def _ensure_share_token_schema(self):
        """Startup safety net: create share-token tables in any project schema (and
        the public schema) that predates them. Runs off-thread; guarded by catalog
        checks so steady-state boots issue no DDL."""
        def _run():
            try:
                from .utils.share_token_schema import apply_share_token_schema
                migrated, failed = apply_share_token_schema()
                if migrated or failed:
                    log.info(
                        "share token schema: auto-migration done (migrated=%s failed=%s)",
                        len(migrated), len(failed),
                    )
            except Exception:  # pylint: disable=W0703
                log.exception("share token schema: auto-migration failed")

        Thread(target=_run, daemon=True).start()

    def _ensure_agent_identity_schema(self):
        """Startup safety net: create the agent identity and mandate tables in any project schema that predates
        them (#6756, #6947), and let ``execution_security_context.agent_binding_ref`` (development runs, #7213) and
        ``application_id``/``version_id`` (toolkit runs, #7229) be NULL where they still cannot. Runs off-thread; guarded by catalog checks so steady-state boots issue no DDL.
        New projects are already covered by create_all."""
        def _run():
            try:
                from .utils.agent_identity_schema import (
                    IDENTITY_TABLES, MANDATE_TABLES,
                    project_ids_missing_tables, project_ids_with_not_null_binding_ref,
                    apply_tables, apply_nullable_binding_ref,
                )
                tables = IDENTITY_TABLES + MANDATE_TABLES
                pending = project_ids_missing_tables(tables)
                if pending:
                    log.info("agent identity schema: auto-migrating %s project(s)", len(pending))
                    migrated, failed = apply_tables(pending, tables)
                    log.info(
                        "agent identity schema: auto-migration done (migrated=%s failed=%s)",
                        len(migrated), len(failed),
                    )
                strict = project_ids_with_not_null_binding_ref()
                if strict:
                    log.info("agent identity schema: relaxing the nullable context columns in %s project(s)", len(strict))
                    migrated, failed = apply_nullable_binding_ref(strict)
                    log.info(
                        "agent identity schema: nullable context columns relaxed (migrated=%s failed=%s)",
                        len(migrated), len(failed),
                    )
            except Exception:  # pylint: disable=W0703
                log.exception("agent identity schema: auto-migration failed")

        Thread(target=_run, daemon=True).start()

    def init(self):
        # Fail fast on a misconfigured governance ceiling, before anything can consult the switch
        init_governance(self)
        #
        self.bp = self.descriptor.init_all(url_prefix="/app")

        # Expose elitea_core config as a shared tool for cross-plugin access
        # Usage: from tools import elitea_config; elitea_config.get("ai_project_id", 1)
        # Unregister first to handle hot-reload (pylon does not call deinit on old module)
        try:
            self.descriptor.unregister_tool('elitea_config')
        except RuntimeError:
            pass  # Not yet registered on first startup
        self.descriptor.register_tool('elitea_config', self.descriptor.config)

        # Initialize elitea_ui FIRST (it originally loaded before elitea_core)
        self.elitea_ui_init()

        auth.register_permissions({
            "permissions": ["runtime.elitea"],
            "recommended_roles": {
                "administration": {"admin": True, "viewer": False, "editor": False},
                "default": {"admin": True, "viewer": False, "editor": False},
            }
        })
        auth.register_permissions({
            "permissions": ["runtime.elitea.ui"],
            "recommended_roles": {
                "administration": {"admin": True, "viewer": False, "editor": False},
                "default": {"admin": True, "viewer": False, "editor": False},
            }
        })

        # Register schedule permissions for schedules permission family (EL-7011)
        register_schedule_permissions()
        register_schedule_settings_type()
        # Autonomous mandates (#6947, C-1009), in the governance.* family of the operator UI. Registered whatever
        # the governance flag says, so roles can be assigned before it is switched on; nothing evaluates them yet
        # (the operator tasks are the only writers).
        for permission, default_roles in (
                ("governance.mandates.manage", {"admin": True, "viewer": False, "editor": False}),
                ("governance.mandates.approve", {"admin": True, "viewer": False, "editor": False}),
                ("governance.mandates.view", {"admin": True, "viewer": False, "editor": False}),
        ):
            auth.register_permissions({
                "permissions": [permission],
                "recommended_roles": {
                    "administration": {"admin": default_roles["admin"], "viewer": False, "editor": False},
                    "default": default_roles,
                }
            })

        # The governance switch (#7120): anyone with `view` sees it; changing it needs `manage`, which the endpoint also
        # pins to the super_admin role, so a role matrix edit cannot widen it. Nothing here is reachable in default mode.
        for permission, roles in (
                ("governance.settings.view", {"super_admin": True, "admin": True}),
                ("governance.settings.manage", {"super_admin": True, "admin": False}),
        ):
            auth.register_permissions({
                "permissions": [permission],
                "recommended_roles": {
                    "administration": {**roles, "viewer": False, "editor": False, "system": False},
                    "default": {"super_admin": False, "admin": False, "viewer": False, "editor": False, "system": False},
                }
            })
        # Agent identity bindings (#7105, C-1009): the operator REST API. Platform administration only in this slice
        # (roles in binding_api.RECOMMENDED_ROLES). Registered whatever the governance flag says.
        for permission in BINDING_PERMISSIONS:
            auth.register_permissions(binding_registration(permission))

        # self.init_db()
        # TaskNode
        self.task_node.start()
        self.task_node.subscribe_to_task_statuses(self.task_status_changed)
        # Maintenance gate: one check at the shared dispatch point covers every
        # start_task callsite (predicts, index, TTS/ASR, pipeline, parallel
        # child dispatch). Structurally prevents a new predict entry point from
        # being missed. Callers that want entry-point-specific error shapes
        # catch MaintenanceInProgressError and translate; unhandled propagation
        # is intentional and preferable to silently returning None (which is
        # indistinguishable from pool saturation). Same seam also mints/stamps
        # the platform run id (stamp_predict_run_id) for the same reason: a
        # new dispatch path can't forget to correlate its LLM/tool usage.
        _original_start_task = self.task_node.start_task

        def _maintenance_gated_start_task(*args, **kwargs):
            admission = kwargs.pop("admission", None)  # built by the entry point from its trusted caller; None when ungoverned
            from .utils.maintenance_gate import is_maintenance_active
            from .utils.budget_door import (
                closed_budget_scope, dispatch_may_use_llm, dispatch_owner, dispatch_uses_own_model,
            )
            from .utils.exceptions import BudgetDoorClosedError, MaintenanceInProgressError
            from .utils.input_guardrail import apply_input_guardrail
            if is_maintenance_active():
                task_name = args[0] if args else kwargs.get("task_name") or "?"
                log.info(
                    "task_node.start_task: rejected — maintenance mode active (task=%s)",
                    task_name,
                )
                raise MaintenanceInProgressError(task_name=task_name)
            # Advisory door check: read-only, fails open, and reserves nothing — the
            # inference-plane gate remains the authority on what may be spent
            project_id, owner_id = dispatch_owner(kwargs)
            closed_scope = (
                closed_budget_scope(project_id, owner_id)
                if dispatch_may_use_llm(kwargs) and not dispatch_uses_own_model(kwargs, project_id)
                else None
            )
            if closed_scope is not None:
                log.info(
                    "task_node.start_task: rejected — %s budget exhausted (project=%s)",
                    closed_scope, project_id,
                )
                raise BudgetDoorClosedError(scope=closed_scope, project_id=project_id)
            # Versioned worker envelope; a governed task with no capable worker is refused here
            # the state is read once, so a switch in the middle of a dispatch cannot give it a governed envelope and an
            # ungoverned admission (or the other way round)
            effective = self.governance_effective
            apply_envelope_gate(
                args[0] if args else kwargs.get("name"),
                kwargs, effective.profile, self.worker_capabilities,
            )
            # Input guardrail: raises InputGuardrailBlockedError on block, rewrites the
            # payload on redact; inert while input_guardrail.enabled is false
            apply_input_guardrail(kwargs)
            stamp_predict_run_id(kwargs)
            after_dispatch = []   # audit only: its evaluation waits for nothing, so it runs once the task is started
            try:
                name = args[0] if args else kwargs.get("name")
                context_ref = admit_dispatch(self, name, kwargs, admission, effective, after_dispatch)
                # a mandate run carries its time limit here (admit_dispatch); it is for Main only, the worker never sees it
                deadline = (kwargs.get("meta") or {}).pop("context_deadline", None) if context_ref else None
                task_id = _original_start_task(*args, **kwargs)
                if context_ref:
                    track_context(self, task_id, admission.project_id, context_ref, deadline=deadline)
                return task_id
            finally:
                for evaluate in after_dispatch:
                    try:
                        evaluate()
                    except BaseException as exc:  # pylint: disable=W0703
                        # the task is started: nothing the evaluation meets (a gevent Timeout included) may replace
                        # that result, or the caller retries a run that is already going. Only the greenlet being
                        # killed is let through.
                        log.error("admission: the audit-only evaluation was interrupted (%s)", type(exc).__name__)
                        if "GreenletExit" in {kind.__name__ for kind in type(exc).__mro__}:
                            raise

        self.task_node.start_task = _maintenance_gated_start_task

        # Eval run pool. Registered after the gate above so a run submitted during maintenance is
        # rejected before it starts: its own code-validation dispatches go through the gated
        # `task_node` and would fail closed, producing a scorecard of errors rather than an
        # evaluation. `launch_run` translates the rejection into a 503.
        self.eval_task_node.start()
        self.eval_task_node.register_task(
            functools.partial(execute_run_task, self), EVAL_RUN_TASK_NAME)

        _original_eval_start_task = self.eval_task_node.start_task

        def _maintenance_gated_eval_start_task(*args, **kwargs):
            from .utils.maintenance_gate import is_maintenance_active
            from .utils.budget_door import closed_budget_scope, dispatch_owner
            from .utils.exceptions import BudgetDoorClosedError, MaintenanceInProgressError
            from .utils.input_guardrail import apply_input_guardrail
            if is_maintenance_active():
                raise MaintenanceInProgressError(task_name=EVAL_RUN_TASK_NAME)
            project_id, owner_id = dispatch_owner(kwargs)
            closed_scope = closed_budget_scope(project_id, owner_id)
            if closed_scope is not None:
                log.info(
                    "eval start_task: rejected — %s budget exhausted (project=%s)",
                    closed_scope, project_id,
                )
                raise BudgetDoorClosedError(scope=closed_scope, project_id=project_id)
            apply_input_guardrail(kwargs)
            stamp_predict_run_id(kwargs)
            return _original_eval_start_task(*args, **kwargs)

        self.eval_task_node.start_task = _maintenance_gated_eval_start_task
        # Events
        self.event_node.subscribe("application_stream_response", self.stream_response)
        self.event_node.subscribe("application_full_response", self.conversation_message_proxy)
        self.event_node.subscribe("application_partial_response", self.conversation_partial_message_proxy)
        self.event_node.subscribe("application_child_message", self.child_message_proxy)
        # Eval run progress (eval worker → every pylon_main replica → browser)
        self.event_node.subscribe("elitea_core_eval_run_progress", self.eval_run_progress_event)
        # Voice TTS events (indexer → pylon_main → browser)
        self.event_node.subscribe("voice_tts_audio_chunk", self.voice_tts_audio_chunk)
        self.event_node.subscribe("voice_tts_done", self.voice_tts_done)
        self.event_node.subscribe("voice_tts_error", self.voice_tts_error)
        # Voice ASR events (indexer → pylon_main → browser)
        self.event_node.subscribe("voice_asr_transcript_delta", self.voice_asr_transcript_delta)
        self.event_node.subscribe("voice_asr_transcript_done", self.voice_asr_transcript_done)
        self.event_node.subscribe("voice_asr_error", self.voice_asr_error)
        self.event_node.subscribe("voice_asr_speech_started", self.voice_asr_speech_started)
        self.event_node.subscribe("voice_asr_vad_flush", self.voice_asr_vad_flush)
        # Next-input suggestion (indexer → pylon_main → browser)
        self.event_node.subscribe("next_input_suggestion_ready", self.next_input_suggestion_ready)
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
        # Worker capabilities: listen first, then ask workers that are already running
        self.event_node.subscribe(CAPABILITIES_EVENT, self.worker_capabilities.on_capabilities)
        self.event_node.subscribe(WITHHOLD_EVENT, self.worker_capabilities.on_withhold)
        self.event_node.emit(CAPABILITIES_REQUEST_EVENT, dict())
        #
        self.application_icon_path.mkdir(parents=True, exist_ok=True)
        self.skill_icon_path.mkdir(parents=True, exist_ok=True)
        self.application_tool_icon_path.mkdir(parents=True, exist_ok=True)
        self.default_entity_icons_path.mkdir(parents=True, exist_ok=True)
        self.platform_logo_path.mkdir(parents=True, exist_ok=True)
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

        # Webhook public URL registration
        # URL pattern: /api/v2/{module_name}/webhook/prompt_lib/{project_id}/{version_id}/{webhook_type}
        self.webhook_api_url_re = \
            f"/api/v2/{this.module_name}/webhook/prompt_lib/[0-9]+/[0-9]+/(github|gitlab|custom)"
        log.info(f"Making webhook API url public: {self.webhook_api_url_re}")
        auth.add_public_rule({"uri": self.webhook_api_url_re})

        # Shared conversation view/unlock public URL registration (unauthenticated access)
        # URL pattern: /api/v2/{module_name}/shared_chat_view/prompt_lib/<token>
        self.shared_view_url_re = f"/api/v2/{this.module_name}/shared_chat_view/.*"
        log.info(f"Making shared chat view API url public: {self.shared_view_url_re}")
        auth.add_public_rule({"uri": self.shared_view_url_re})
        # URL pattern: /api/v2/{module_name}/shared_chat_view_unlock/prompt_lib/<token>/unlock
        self.shared_view_unlock_url_re = f"/api/v2/{this.module_name}/shared_chat_view_unlock/.*"
        log.info(f"Making shared chat view unlock API url public: {self.shared_view_unlock_url_re}")
        auth.add_public_rule({"uri": self.shared_view_unlock_url_re})
        # URL pattern: /api/v2/{module_name}/shared_chat_attachment/prompt_lib/<token>/<group_id>/<filename>
        self.shared_attachment_url_re = f"/api/v2/{this.module_name}/shared_chat_attachment/.*"
        log.info(f"Making shared chat attachment API url public: {self.shared_attachment_url_re}")
        auth.add_public_rule({"uri": self.shared_attachment_url_re})

        # Provider Hub initialization
        self.provider_hub_init()

        # Publish validation secret (HMAC signing key)
        self._init_publish_validation_secret()

        # Publishing guardrail (environment-wide block)
        self._init_publishing_guardrail()
        self._init_skill_publishing_guardrail()

        from .models import all, folder, message_group, message_trace_step, participants, project_budget, user_budget, feature_budget, user_feature_budget
        from .models import evaluation, eval_platform_dimension
        from .models.message_items import base, text, canvas, context
        from .models.all import ConversationShareToken, ConversationShareTokenIndex  # noqa: F401 — ensure tables are created
        from .models import chat_template  # noqa: F401 — register ChatTemplate with db.Base.metadata
        from .models import entity_schedule  # noqa: F401 — register the schedule tables with db.Base.metadata

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

    # ------------------------------------------------------------------
    # Voice TTS/ASR event_node handlers (indexer → pylon_main → browser)
    # ------------------------------------------------------------------

    def voice_tts_audio_chunk(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        audio = payload.get("audio")
        sample_rate = payload.get("sample_rate", 24000)
        if not sid or not audio:
            return
        self.context.sio.emit(
            SioEvents.tts_audio_chunk,
            {"audio": audio, "sample_rate": sample_rate},
            to=sid,
        )

    def voice_tts_done(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        if sid:
            data = {}
            char_end = payload.get("char_end")
            if char_end is not None:
                data["char_end"] = char_end
            self.context.sio.emit(SioEvents.tts_done, data, to=sid)

    def voice_tts_error(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        error = payload.get("error", "TTS error")
        if sid:
            self.context.sio.emit(SioEvents.tts_error, {"error": error}, to=sid)

    def voice_asr_transcript_delta(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        delta = payload.get("delta", "")
        if sid:
            self.context.sio.emit(SioEvents.asr_transcript_delta, {"delta": delta}, to=sid)

    def voice_asr_transcript_done(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        transcript = payload.get("transcript", "")
        if sid:
            self.context.sio.emit(SioEvents.asr_transcript_done, {"transcript": transcript}, to=sid)
            on_whisper_call_done(sid)

    def voice_asr_error(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        error = payload.get("error", "ASR error")
        if sid:
            self.context.sio.emit(SioEvents.asr_error, {"error": error}, to=sid)

    def voice_asr_speech_started(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        if sid:
            self.context.sio.emit(SioEvents.asr_speech_started, {}, to=sid)

    def voice_asr_vad_flush(self, event: str, payload: dict, *args):
        sid = payload.get("sid")
        if sid:
            self.context.sio.emit(SioEvents.asr_vad_flush, {}, to=sid)

    def next_input_suggestion_ready(self, event: str, payload: dict, *args):
        try:
            p = NextInputSuggestionPayload.model_validate(payload)
        except ValidationError:
            log.warning("next_input_suggestion_ready: invalid payload %s", payload)
            return
        self.context.sio.emit(
            SioEvents.next_input_suggestion_ready,
            {"stream_id": p.stream_id, "message_id": p.message_id, "suggestions": p.suggestions},
            to=p.sid,
        )

    def _register_input_guardrail_redactor(self):
        """Tracing span payloads carry the redacted text, not the detected value (#6886).

        The redactor reads the live policy, so it is inert while the guardrail is off. It
        returns None for a value it could not fully inspect: tracing masks the whole value.
        """
        try:
            from tools import this  # pylint: disable=E0401,C0415
            from .utils.input_guardrail import redact_value
            this.for_module("tracing").module.register_value_redactor(redact_value)
        except Exception as e:  # pylint: disable=W0703
            log.debug(f"Input guardrail: tracing redactor not registered: {e}")

    def deinit(self):
        log.info('De-initializing')
        try:   # the run token verifier goes with the module, first: what follows can raise (#7214)
            from .utils.internal_obo import TOKEN_PREFIX  # pylint: disable=C0415
            auth.unregister_bearer_verifier(TOKEN_PREFIX)
        except Exception as exc:  # pylint: disable=W0703
            log.warning("internal OBO: the run token verifier was not unregistered (%s)", type(exc).__name__)
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
        self.event_node.unsubscribe("elitea_core_eval_run_progress", self.eval_run_progress_event)
        self.event_node.unsubscribe("application_toolkit_configurations_collected", self.toolkit_configurations_collected)
        self.event_node.unsubscribe("application_toolkits_collected", self.toolkits_collected)
        self.event_node.unsubscribe("application_file_loaders_collected", self.index_types_collected)
        self.event_node.unsubscribe("application_mcp_prebuilt_config_collected", self.mcp_prebuilt_config_collected)
        self.event_node.unsubscribe(CAPABILITIES_EVENT, self.worker_capabilities.on_capabilities)
        self.event_node.unsubscribe(WITHHOLD_EVENT, self.worker_capabilities.on_withhold)
        # Voice TTS/ASR events
        self.event_node.unsubscribe("voice_tts_audio_chunk", self.voice_tts_audio_chunk)
        self.event_node.unsubscribe("voice_tts_done", self.voice_tts_done)
        self.event_node.unsubscribe("voice_tts_error", self.voice_tts_error)
        self.event_node.unsubscribe("next_input_suggestion_ready", self.next_input_suggestion_ready)
        self.event_node.unsubscribe("voice_asr_transcript_delta", self.voice_asr_transcript_delta)
        self.event_node.unsubscribe("voice_asr_transcript_done", self.voice_asr_transcript_done)
        self.event_node.unsubscribe("voice_asr_error", self.voice_asr_error)
        self.event_node.unsubscribe("voice_asr_speech_started", self.voice_asr_speech_started)
        self.event_node.unsubscribe("voice_asr_vad_flush", self.voice_asr_vad_flush)

        # TaskNode
        if self.governance_state is not None:
            self.governance_state.stop()
        self.eval_task_node.stop()
        self.task_node.stop()
        try:
            from tools import this
            self._unregister_governed_handlers()
            this.for_module("admin").module.unregister_admin_task(
                "eliminate_prompts", prompt_2_agent_migration
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_toolkit_selected_tools", self.migrate_toolkit_selected_tools
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_provider_hub_secrets", self.migrate_provider_hub_secrets
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_toolkit_settings_alita_title", self.migrate_toolkit_settings_alita_title
            )
            this.for_module("admin").module.unregister_admin_task(
                "chat_cleanup_dup_msgs", self.chat_cleanup_dup_msgs
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_jira_confluence_hosting", self.migrate_jira_confluence_hosting
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_confluence_api_version", self.migrate_confluence_api_version
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_toolkit_settings_fields", self.migrate_toolkit_settings_fields
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_mcp_client_secrets", self.migrate_mcp_client_secrets
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_conversation_source_to_elitea", self.migrate_conversation_source_to_elitea
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_admin_shell_to_inplace", self.migrate_admin_shell_to_inplace
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_agent_version_null_instructions", self.migrate_agent_version_null_instructions
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_ado_project_to_toolkit", self.migrate_ado_project_to_toolkit
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_llm_model", self.migrate_llm_model
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_embedding_model", self.migrate_embedding_model
            )
            this.for_module("admin").module.unregister_admin_task(
                "collections_removal_migration", self.collections_removal_migration
            )
            this.for_module("admin").module.unregister_admin_task(
                "datasource_removal_migration", self.datasource_removal_migration
            )
            this.for_module("admin").module.unregister_admin_task(
                "cleanup_oversized_message_meta", self.cleanup_oversized_message_meta
            )
            this.for_module("admin").module.unregister_admin_task(
                "backfill_legacy_trace_steps", self.backfill_legacy_trace_steps
            )
            this.for_module("admin").module.unregister_admin_task(
                "heal_llm_settings_family_conflicts", self.heal_llm_settings_family_conflicts
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_empty_conversation_starters", self.migrate_empty_conversation_starters
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_project_chat_config", self.migrate_project_chat_config
            )
            this.for_module("admin").module.unregister_admin_task(
                "set_index_schedule_expiration", self.set_index_schedule_expiration
            )
            this.for_module("admin").module.unregister_admin_task(
                "prune_orphaned_index_schedules", self.prune_orphaned_index_schedules
            )
            this.for_module("admin").module.unregister_admin_task(
                "migrate_legacy_pipeline_schedules", self.migrate_legacy_pipeline_schedules
            )
            this.for_module("admin").module.unregister_admin_task(
                "create_agent_identity_tables", self.create_agent_identity_tables
            )
            this.for_module("admin").module.unregister_admin_task(
                "manage_agent_identity_binding", self.manage_agent_identity_binding
            )
            this.for_module("admin").module.unregister_admin_task(
                "create_mandate_tables", self.create_mandate_tables
            )
            this.for_module("admin").module.unregister_admin_task(
                "manage_autonomous_mandate", self.manage_autonomous_mandate
            )
            this.for_module("admin").module.unregister_admin_task(
                "report_trigger_principals", self.report_trigger_principals
            )
            this.for_module("admin").module.unregister_admin_task(
                "create_governance_settings_tables", self.create_governance_settings_tables
            )
        except Exception as e:
            log.exception("Failed to unregister admin tasks: %s", e)

        # De-init
        self.descriptor.deinit_all()

    def reconfig(self):
        """Re-config"""
        self._configure_elitea_ui()
        try:
            self._apply_scheduler_runtime_config()
        except Exception:  # pylint: disable=W0703
            log.exception('Failed to apply scheduler runtime config')
        # Refresh cached MCP exposure settings so admin toggle changes take
        # effect immediately without requiring a container restart.
        mcp_config = self.descriptor.config.get('mcp_exposure', {})
        self.mcp_exposure_enabled = mcp_config.get('enabled', True)
        self.mcp_in_menu_enabled = mcp_config.get('in_menu', True)
        self.mcp_category_name = mcp_config.get('category_name') or 'MCP'
        log.info(f"MCP config reloaded: exposure={self.mcp_exposure_enabled}, in_menu={self.mcp_in_menu_enabled}")
        self._init_publishing_guardrail()

    def _bootstrap_platform_schedules(self):
        """Create the platform schedule rows and reconcile them with config."""
        payloads = []
        try:
            scheduler_cfg = self.descriptor.config.get('scheduler')
            managed = {
                target['name']: target
                for target in build_scheduler_sync_plan(scheduler_cfg)
            }
            payloads = [
                {
                    'rpc_func': 'applications_empty_state',
                    'rpc_kwargs': {'days_to_retain': 1},
                    'name': 'empty_agent_state',
                    'cron': '0 0 * * *',
                    'active': True,
                },
                {
                    'rpc_func': managed['index_scheduling']['rpc_func'],
                    'rpc_kwargs': {},
                    'name': 'index_scheduling',
                    'cron': managed['index_scheduling']['cron'],
                    'active': managed['index_scheduling']['active'],
                    'match_handler': True,
                },
                {
                    'rpc_func': 'elitea_core_cleanup_stale_chunks',
                    'rpc_kwargs': {'max_age_seconds': 43200},
                    'name': 'cleanup_stale_chunks',
                    'cron': '0 */12 * * *',
                    'active': True,
                },
                {
                    'rpc_func': 'elitea_core_reap_pgvector_engines',
                    'rpc_kwargs': {},
                    'name': 'pgvector_engine_reap',
                    'cron': '*/5 * * * *',
                    'active': True,
                },
                {
                    'rpc_func': 'elitea_core_reap_orphaned_eval_runs',
                    'rpc_kwargs': {},
                    'name': 'eval_run_reap',
                    'cron': '*/10 * * * *',
                    'active': True,
                },
                {
                    'rpc_func': 'elitea_core_check_pat_expiration',
                    'rpc_kwargs': {},
                    'name': 'pat_expiration_check',
                    'cron': '0 * * * *',
                    'active': True,
                },
                {
                    'rpc_func': managed['pipeline_scheduling']['rpc_func'],
                    'rpc_kwargs': {},
                    'name': 'pipeline_scheduling',
                    'cron': managed['pipeline_scheduling']['cron'],
                    'active': managed['pipeline_scheduling']['active'],
                    'match_handler': True,
                },
            ]
        except Exception:  # pylint: disable=W0703
            log.exception('Failed to plan platform schedules')
            self._release_managed_schedules()
            return

        for payload in payloads:
            try:
                self.context.rpc_manager.timeout(5).scheduling_create_if_not_exists(payload)
            except Empty:
                log.warning(
                    'No scheduling plugin responded: name=%s (skipping the rest)',
                    payload['name'],
                )
                break
            except Exception:  # pylint: disable=W0703
                log.exception(
                    'Failed to create schedule: name=%s', payload['name'],
                )

        try:
            self._apply_scheduler_runtime_config(plan=list(managed.values()))
        except Exception:  # pylint: disable=W0703
            log.exception('Failed to reconcile platform schedules')

    def get_managed_schedules(self):
        """Bindings the scheduling plugin pulls when it rebuilds its registry."""
        return advertised_bindings(self._scheduler_bindings_active)

    def _release_managed_schedules(self):
        """Give the rows back when this plugin cannot drive them."""
        self._scheduler_bindings_active = False
        try:
            scheduling = this.for_module("scheduling").module
            register = getattr(scheduling, "register_managed_schedules", None)
            if register is not None:
                register("elitea_core", {})
                log.warning(
                    "Released managed schedules: nothing here can push their "
                    "cadence, so they must not be left read-only"
                )
        except Exception as e:
            log.error("Failed to release managed schedules: %r", e)

    def _register_managed_schedules(self):
        """Tell the scheduling plugin these rows are edited in admin config."""
        try:
            scheduling = this.for_module("scheduling").module
            register = getattr(scheduling, "register_managed_schedules", None)
            if register is None:
                log.warning(
                    "scheduling plugin has no managed-schedule support; "
                    "index/pipeline scheduling rows stay editable in Admin Portal"
                )
                return
            self._scheduler_bindings_active = True
            register("elitea_core", self.get_managed_schedules())
        except Exception as e:
            log.error("Failed to register managed schedules: %r", e)

    def _apply_scheduler_runtime_config(self, plan=None):
        """Push admin-configured scheduler cron/enabled flags into DB rows."""
        self._register_managed_schedules()
        if plan is None:
            plan = build_scheduler_sync_plan(self.descriptor.config.get('scheduler'))
        for target in plan:
            name = target['name']
            try:
                self.context.rpc_manager.timeout(5).scheduling_update_schedule(
                    name=name,
                    cron=target['cron'],
                    active=target['active'],
                )
            except Exception as e:
                log.error(
                    "scheduling_update_schedule failed: name=%s error=%r",
                    name, e,
                )

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
        # Cache MCP exposure settings at startup; refreshed on reconfig so
        # admin toggle changes apply without a restart.
        mcp_config = self.descriptor.config.get('mcp_exposure', {})
        self.mcp_exposure_enabled = mcp_config.get('enabled', True)
        self.mcp_in_menu_enabled = mcp_config.get('in_menu', True)
        self.mcp_category_name = mcp_config.get('category_name') or 'MCP'
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
        """Handle SocketIO disconnect for MCP servers, ASR and TTS"""
        removed_servers = self.servers_storage.remove_servers(sid)
        for server in removed_servers:
            log.debug(f"[MCP_CLIENT] Server {server['name']} disconnected")
            self.context.sio.emit(
                event=SioEvents.mcp_status,
                data={"connected": False, "project_id": server['project_id'], "type": server['name']},
            )
        # Cancel any in-progress ASR / TTS tasks for the disconnected sid
        from .sio.asr import _close_session as _asr_close_session
        _asr_close_session(self, sid)
        self.event_node.emit("voice_events", {"type": "tts_cancel", "sid": sid})

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
        auth.register_permissions({
            "permissions": ["runtime.airun"],
            "recommended_roles": {
                "administration": {"admin": True, "viewer": False, "editor": False},
                "default": {"admin": True, "viewer": False, "editor": False},
            }
        })
        auth.register_permissions({
            "permissions": ["runtime.airun.serviceproviders"],
            "recommended_roles": {
                "administration": {"admin": True, "viewer": False, "editor": False},
                "default": {"admin": True, "viewer": False, "editor": False},
            }
        })

    def provider_hub_deinit(self):
        """Deinitialize Provider Hub functionality"""
        # Cleanup provider storage
        if hasattr(self, 'present_providers'):
            self.present_providers.clear()
            from .utils import schema_registry_version  # pylint: disable=C0415
            schema_registry_version.bump()
        if hasattr(self, 'unhealthy_providers'):
            self.unhealthy_providers.clear()

    # EliteA UI Methods
    def elitea_ui_init(self):
        """Initialize elitea_ui functionality (migrated from elitea_ui plugin)"""
        log.info("Initializing elitea_ui")
        #
        # Blueprint already initialized by descriptor.init_all()
        # Just ensure we have it
        if not hasattr(self, 'bp') or self.bp is None:
            log.warning("Blueprint not initialized, cannot complete elitea_ui_init")
            return
        #
        import flask as _flask  # pylint: disable=C0415
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
        auth.add_public_rule({"uri": "/app/shared/chat/[^/]+/?"})
        auth.add_public_rule({"uri": r"/app/assets/[^/]+\.(js|css|png|svg|woff2?)(\?.*)?"})
        auth.add_public_rule({"uri": r"/app/favicon\.svg"})
        auth.add_public_rule({"uri": r"/app/platform_logo/.*"})  # Custom branding logo
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
