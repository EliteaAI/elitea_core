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

""" Method """

import shutil
import time
from functools import partial

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from ..scripts.tool_icons import download_github_repo_zip, unzip_file
from ..utils.toolkit_migration import run_selected_tools_migration
from ..utils.llm_migration_utils import (
    parse_migration_params,
    resolve_target_project_ids,
    validate_target_model,
    lookup_source_model_capabilities,
    build_new_llm_settings,
    migrate_application_versions,
    migrate_participant_mappings,
)
from ..utils.utils import get_public_project_id


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    # pylint: disable=R,W0613
    @web.method()
    def download_static_icons(self, *args, **kwargs):
        """Re-download all static icons from the GitHub icons repository. No params. Deletes and recreates icon directories."""
        log.info("Deleting stale icons")
        #
        shutil.rmtree(self.application_tool_icon_path)
        shutil.rmtree(self.default_entity_icons_path)
        #
        log.info("Recreating icon dirs")
        #
        self.application_tool_icon_path.mkdir(parents=True, exist_ok=True)
        self.default_entity_icons_path.mkdir(parents=True, exist_ok=True)
        #
        log.info("Loading static icons")
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

    # pylint: disable=R,W0613
    @web.method()
    def migrate_toolkit_selected_tools(self, *args, **kwargs):
        """Admin task: migrate selected_tools in EliteATool.settings and EntityToolMapping.

        Supports removing and renaming tool entries across all or specific projects,
        with an optional dry-run mode.

        Param format:
            "<toolkit_type>;<operations>;project_id=<all|N>[;dry_run]"

        Operations (comma-separated):
            tool_name           - remove tool from selected_tools
            old_name>new_name   - rename tool (remove old, append new)

        Examples:
            "github;index_data>indexData,search_index;project_id=all"
                All projects, github toolkits: rename index_data->indexData, remove search_index

            "artifact;read_file_chunk;project_id=all;dry_run"
                All projects, artifact toolkits: remove read_file_chunk (dry run only)

            "gitlab;list_repos;project_id=34"
                Project 34 only, gitlab toolkits: remove list_repos

            "github;old_tool>new_tool;project_id=34;dry_run"
                Project 34 only, github toolkits: rename old_tool->new_tool (dry run)

        Idempotent: safe to run multiple times with the same arguments.
        Always run with dry_run first to verify expected changes.
        """
        log.info("Starting migrate_toolkit_selected_tools")
        start_ts = time.time()
        #
        try:
            param = kwargs.get("param", "")
            log.info("Param: %s", repr(param))
            result = run_selected_tools_migration(param)
            log.info("Result: %s", result)
        except:  # pylint: disable=W0702
            log.exception("Got exception during migrate_toolkit_selected_tools")
        #
        end_ts = time.time()
        log.info("Exiting migrate_toolkit_selected_tools (duration = %s)", end_ts - start_ts)

    @web.method()
    def migrate_provider_hub_secrets(self, *args, **kwargs):
        """Admin task: move plain-text secrets in provider hub toolkit settings to Vault.

        Iterates all projects, finds provider-hub-type EliteATool records whose
        toolkit_configuration_* fields still hold plain-text secret values, wraps
        them as SecretString, and calls store_secrets() so the Vault reference
        replaces the plain text in the DB.

        Idempotent: safe to run multiple times — skips fields already stored as
        {{secret.xxx}} Vault references.

        Param format (optional):
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all"            - migrate all projects
            "project_id=all;dry_run"    - dry run across all projects (no DB writes)
            "project_id=34"             - migrate project 34 only
            "project_id=34;dry_run"     - dry run for project 34

        Always run with dry_run first to verify expected changes.
        """
        from tools import db, serialize, store_secrets  # pylint: disable=C0415
        from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
        from ..models.all import EliteATool  # pylint: disable=C0415
        from ..utils.application_tools import wrap_provider_hub_secret_fields  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.warning("migrate_provider_hub_secrets: invalid project_id '%s', scanning all", value)
            elif seg_lower == "dry_run":
                dry_run = True

        prefix = "[DRY RUN] " if dry_run else ""
        log.info("Starting migrate_provider_hub_secrets (dry_run=%s, project_id_filter=%s)", dry_run, project_id_filter)
        start_ts = time.time()
        total_migrated = 0

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("migrate_provider_hub_secrets: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']
            log.info("%smigrate_provider_hub_secrets: scanning project %s", prefix, project_id)

            try:
                with db.with_project_schema_session(project_id) as session:
                    toolkits = session.query(EliteATool).all()

                    for toolkit in toolkits:
                        settings = dict(toolkit.settings or {})

                        # wrap_provider_hub_secret_fields mutates settings in-place;
                        # it returns without changes if no secret fields are found.
                        settings_before = dict(settings)
                        wrap_provider_hub_secret_fields(toolkit.type, settings, project_id)
                        needs_update = settings != settings_before

                        if needs_update:
                            log.info(
                                "%smigrate_provider_hub_secrets: %stoolkit "
                                "id=%s type=%s in project %s",
                                prefix, "would migrate " if dry_run else "migrated ",
                                toolkit.id, toolkit.type, project_id
                            )
                            if not dry_run:
                                store_secrets(settings, project_id)
                                toolkit.settings = serialize(settings)
                                flag_modified(toolkit, 'settings')
                            total_migrated += 1

                    if not dry_run:
                        session.commit()

            except Exception:  # pylint: disable=W0703
                log.exception(
                    "%smigrate_provider_hub_secrets: error in project %s", prefix, project_id
                )

        end_ts = time.time()
        log.info(
            "%sExiting migrate_provider_hub_secrets — %s %s toolkit(s) (duration = %s)",
            prefix, "would migrate" if dry_run else "migrated", total_migrated, end_ts - start_ts
        )
        return {"migrated": total_migrated, "dry_run": dry_run}

    @web.method()
    def migrate_application_description_size(self, *args, **kwargs):
        """Admin task: increase description column size in applications table from VARCHAR(1024) to VARCHAR(2304).

        Iterates all projects and runs ALTER TABLE on each project schema.
        Idempotent: safe to run multiple times — PostgreSQL no-ops if the column is already wider.

        Param format (optional):
            "project_id=<all|N>"

        Examples:
            "project_id=all"  - migrate all projects
            "project_id=34"   - migrate project 34 only
        """
        from sqlalchemy import text  # pylint: disable=C0415
        from tools import db  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        project_id_filter = None

        for seg in [s.strip() for s in param.split(";")]:
            if seg.lower().startswith("project_id="):
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.warning("migrate_application_description_size: invalid project_id '%s', scanning all", value)

        log.info("Starting migrate_application_description_size (project_id_filter=%s)", project_id_filter)
        start_ts = time.time()
        total_migrated = 0

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("migrate_application_description_size: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']
            log.info("migrate_application_description_size: processing project %s", project_id)

            try:
                with db.with_project_schema_session(project_id) as session:
                    session.execute(
                        text(f"ALTER TABLE p_{project_id}.applications ALTER COLUMN description TYPE VARCHAR(2304)")
                    )
                    session.commit()
                total_migrated += 1
            except Exception:  # pylint: disable=W0703
                log.exception("migrate_application_description_size: error in project %s", project_id)

        end_ts = time.time()
        log.info(
            "Exiting migrate_application_description_size — migrated %s project(s) (duration = %s)",
            total_migrated, end_ts - start_ts
        )
        return {"migrated": total_migrated}

    @web.method()
    def migrate_toolkit_settings_alita_title(self, *args, **kwargs):
        """Admin task: rename 'alita_title' to 'elitea_title' inside toolkit settings JSON.

        Credential reference objects in EliteATool.settings (e.g. gitlab_configuration,
        pgvector_configuration) may still contain the legacy 'alita_title' key from
        before the EliteaAI debranding. This migration renames them to 'elitea_title'.

        Idempotent: safe to run multiple times — skips objects that already use
        'elitea_title' or don't contain 'alita_title'.

        Param format (optional):
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - dry run across all projects
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only
        """
        from copy import deepcopy  # pylint: disable=C0415
        from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
        from tools import db  # pylint: disable=C0415
        from ..models.all import EliteATool  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error("migrate_toolkit_settings_alita_title: invalid project_id '%s'", value)
                        return {"migrated": 0, "error": f"invalid project_id: '{value}'"}
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error("migrate_toolkit_settings_alita_title: project_id= is required. Format: project_id=<all|N>[;dry_run]")
            return {"migrated": 0, "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info("Starting migrate_toolkit_settings_alita_title (dry_run=%s, project_id_filter=%s)", dry_run, project_id_filter)
        start_ts = time.time()
        total_migrated = 0

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("migrate_toolkit_settings_alita_title: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']

            try:
                with db.with_project_schema_session(project_id) as session:
                    toolkits = session.query(EliteATool).all()

                    for toolkit in toolkits:
                        settings = toolkit.settings
                        if not settings:
                            continue

                        needs_update = False
                        updated_settings = deepcopy(settings)

                        for key, val in settings.items():
                            if not isinstance(val, dict) or 'alita_title' not in val:
                                continue

                            new_val = dict(val)
                            alita_value = new_val.pop('alita_title')
                            if 'elitea_title' not in new_val:
                                new_val['elitea_title'] = alita_value
                            updated_settings[key] = new_val
                            needs_update = True

                            log.info(
                                "%sproject %s, toolkit id=%s (%s): %s.alita_title -> elitea_title",
                                prefix, project_id, toolkit.id, toolkit.type, key
                            )

                        if needs_update:
                            total_migrated += 1
                            if not dry_run:
                                toolkit.settings = updated_settings
                                flag_modified(toolkit, 'settings')

                    if not dry_run:
                        session.commit()

            except Exception:  # pylint: disable=W0703
                log.exception(
                    "%smigrate_toolkit_settings_alita_title: error in project %s", prefix, project_id
                )

        end_ts = time.time()
        log.info(
            "%sExiting migrate_toolkit_settings_alita_title — %s %s toolkit(s) (duration = %ss)",
            prefix, "would migrate" if dry_run else "migrated", total_migrated, round(end_ts - start_ts, 2)
        )
        return {"migrated": total_migrated, "dry_run": dry_run}

    @web.method()
    def migrate_jira_confluence_hosting(self, *args, **kwargs):
        """Admin task: migrate 'cloud' boolean from Jira/Confluence toolkit settings to 'hosting' field.

        This migration:
        1. Finds Jira/Confluence toolkits with 'cloud' field in settings
        2. Maps cloud boolean to hosting string: true -> "Cloud", false -> "Server"
        3. Updates the corresponding Configuration's data.hosting field
        4. Removes the 'cloud' field from toolkit settings (only after config is updated)

        Idempotent: safe to run multiple times — skips toolkits without 'cloud' field.

        Param format:
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - dry run across all projects
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only
        """
        from copy import deepcopy  # pylint: disable=C0415
        from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
        from tools import db  # pylint: disable=C0415
        from ..models.all import EliteATool  # pylint: disable=C0415

        # Import Configuration model
        try:
            from plugins.configurations.models.configuration import Configuration  # pylint: disable=C0415
        except ImportError:
            log.error("migrate_jira_confluence_hosting: configurations plugin not available")
            return {"migrated": 0, "error": "configurations plugin not available"}

        # Mapping of cloud boolean to hosting string
        CLOUD_TO_HOSTING = {True: "Cloud", False: "Server"}

        # Toolkit types and their corresponding configuration keys
        TOOLKIT_CONFIG_MAPPING = {
            "jira": "jira_configuration",
            "confluence": "confluence_configuration",
        }

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error("migrate_jira_confluence_hosting: invalid project_id '%s'", value)
                        return {"migrated": 0, "error": f"invalid project_id: '{value}'"}
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error("migrate_jira_confluence_hosting: project_id= is required. Format: project_id=<all|N>[;dry_run]")
            return {"migrated": 0, "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info("Starting migrate_jira_confluence_hosting (dry_run=%s, project_id_filter=%s)", dry_run, project_id_filter)
        start_ts = time.time()

        # Track migration results
        results = {
            "migrated_toolkits": 0,
            "migrated_configurations": 0,
            "skipped_toolkits": 0,
            "failed_projects": 0,
            "errors": []
        }

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("migrate_jira_confluence_hosting: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']

            try:
                with db.with_project_schema_session(project_id) as session:
                    # Find all Jira/Confluence toolkits
                    toolkits = session.query(EliteATool).filter(
                        EliteATool.type.in_(TOOLKIT_CONFIG_MAPPING.keys())
                    ).all()

                    for toolkit in toolkits:
                        settings = toolkit.settings or {}

                        # Check if toolkit has 'cloud' field
                        if 'cloud' not in settings:
                            results["skipped_toolkits"] += 1
                            continue

                        cloud_value = settings.get('cloud')
                        hosting_value = CLOUD_TO_HOSTING.get(cloud_value)

                        if hosting_value is None:
                            log.warning(
                                "%sproject %s, toolkit id=%s (%s): invalid 'cloud' value %s, skipping",
                                prefix, project_id, toolkit.id, toolkit.type, cloud_value
                            )
                            results["skipped_toolkits"] += 1
                            continue

                        # Get configuration key for this toolkit type
                        config_key = TOOLKIT_CONFIG_MAPPING.get(toolkit.type)
                        if not config_key:
                            results["skipped_toolkits"] += 1
                            continue

                        # Get configuration reference from toolkit settings
                        config_ref = settings.get(config_key)
                        if not config_ref:
                            results["skipped_toolkits"] += 1
                            continue

                        elitea_title = config_ref.get('elitea_title')
                        is_private = config_ref.get('private', True)

                        if not elitea_title:
                            results["skipped_toolkits"] += 1
                            continue

                        # Determine which project's configuration to update
                        config_project_id = project_id if is_private else project_id

                        # Find and update the configuration
                        config_updated = False
                        try:
                            with db.with_project_schema_session(config_project_id) as config_session:
                                configuration = config_session.query(Configuration).filter(
                                    Configuration.elitea_title == elitea_title
                                ).first()

                                if not configuration:
                                    log.warning(
                                        "%sproject %s, toolkit id=%s: configuration '%s' not found",
                                        prefix, project_id, toolkit.id, elitea_title
                                    )
                                    results["skipped_toolkits"] += 1
                                    continue

                                config_data = configuration.data or {}
                                old_hosting = config_data.get('hosting')

                                log.info(
                                    "%sproject %s, toolkit id=%s (%s): cloud=%s -> hosting=%s (config '%s': %s -> %s)",
                                    prefix, project_id, toolkit.id, toolkit.type, cloud_value, hosting_value,
                                    elitea_title, old_hosting, hosting_value
                                )

                                if not dry_run:
                                    # Update configuration hosting
                                    new_config_data = dict(config_data)
                                    new_config_data['hosting'] = hosting_value
                                    configuration.data = new_config_data
                                    flag_modified(configuration, 'data')
                                    config_session.commit()

                                config_updated = True
                                results["migrated_configurations"] += 1

                        except Exception as e:  # pylint: disable=W0703
                            log.exception(
                                "%smigrate_jira_confluence_hosting: error updating config for toolkit %s",
                                prefix, toolkit.id
                            )
                            results["errors"].append({
                                "project_id": project_id,
                                "toolkit_id": toolkit.id,
                                "error": str(e)
                            })
                            continue

                        if config_updated:
                            if not dry_run:
                                # Remove 'cloud' field from toolkit settings
                                new_settings = deepcopy(settings)
                                del new_settings['cloud']
                                toolkit.settings = new_settings
                                flag_modified(toolkit, 'settings')

                            results["migrated_toolkits"] += 1

                    if not dry_run:
                        session.commit()

            except Exception:  # pylint: disable=W0703
                log.exception(
                    "%smigrate_jira_confluence_hosting: error in project %s", prefix, project_id
                )
                results["failed_projects"] += 1

        end_ts = time.time()
        log.info(
            "%sExiting migrate_jira_confluence_hosting — %s %s toolkit(s), %s config(s) (duration = %ss)",
            prefix, "would migrate" if dry_run else "migrated",
            results["migrated_toolkits"], results["migrated_configurations"],
            round(end_ts - start_ts, 2)
        )

        return {
            "migrated_toolkits": results["migrated_toolkits"],
            "migrated_configurations": results["migrated_configurations"],
            "skipped_toolkits": results["skipped_toolkits"],
            "failed_projects": results["failed_projects"],
            "errors": results["errors"],
            "dry_run": dry_run
        }

    @web.method()
    def migrate_mcp_client_secrets(self, *args, **kwargs):
        """Admin task: vault-wrap plain-text client_secret in MCP toolkit settings.

        Finds all MCP toolkits (type == 'mcp' or type starting with 'mcp_') whose
        settings contain a plain-text client_secret value (not yet wrapped as a
        {{secret.xxx}} Vault reference) and stores the value in Vault, replacing the
        plain text with the reference.

        Idempotent: safe to run multiple times — skips toolkits whose client_secret
        is already a Vault reference or is absent.

        Param format:
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - preview what would be migrated (no changes)
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only

        Always run with dry_run first to verify expected changes.
        """
        from copy import deepcopy  # pylint: disable=C0415
        from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
        from tools import db, SecretString, VaultClient  # pylint: disable=C0415
        from ..models.all import EliteATool  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error("migrate_mcp_client_secrets: invalid project_id '%s'", value)
                        return {"migrated": 0, "error": f"invalid project_id: '{value}'"}
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error(
                "migrate_mcp_client_secrets: project_id= is required. "
                "Format: project_id=<all|N>[;dry_run]"
            )
            return {"migrated": 0, "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info(
            "Starting migrate_mcp_client_secrets (dry_run=%s, project_id_filter=%s)",
            dry_run, project_id_filter,
        )
        start_ts = time.time()
        total_migrated = 0

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("migrate_mcp_client_secrets: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']
            log.info("%smigrate_mcp_client_secrets: scanning project %s", prefix, project_id)

            try:
                with db.with_project_schema_session(project_id) as session:
                    toolkits = session.query(EliteATool).filter(
                        EliteATool.type.like('mcp%')
                    ).all()

                    if not toolkits:
                        continue

                    vault_client = None
                    any_changed = False

                    for toolkit in toolkits:
                        # Only process types == 'mcp' or starting with 'mcp_'
                        if toolkit.type != 'mcp' and not toolkit.type.startswith('mcp_'):
                            continue

                        settings = toolkit.settings or {}
                        client_secret = settings.get('client_secret')

                        if not client_secret or not isinstance(client_secret, str):
                            continue

                        if SecretString._secret_pattern.match(client_secret):
                            log.info(
                                "%smigrate_mcp_client_secrets: project %s, toolkit id=%s (%s): "
                                "client_secret already vaulted, skipping",
                                prefix, project_id, toolkit.id, toolkit.type,
                            )
                            continue

                        log.info(
                            "%smigrate_mcp_client_secrets: project %s, toolkit id=%s (%s) "
                            "name='%s': wrapping plain-text client_secret into Vault",
                            prefix, project_id, toolkit.id, toolkit.type, toolkit.name,
                        )

                        total_migrated += 1

                        if not dry_run:
                            if vault_client is None:
                                vault_client = VaultClient(project=project_id)
                            s = SecretString(client_secret)
                            s.vault_client = vault_client
                            vault_ref = s.store_secret()

                            new_settings = deepcopy(settings)
                            new_settings['client_secret'] = vault_ref
                            toolkit.settings = new_settings
                            flag_modified(toolkit, 'settings')
                            any_changed = True

                    if any_changed and not dry_run:
                        session.commit()

            except Exception:  # pylint: disable=W0703
                log.exception(
                    "%smigrate_mcp_client_secrets: error in project %s", prefix, project_id
                )

        end_ts = time.time()
        log.info(
            "%sExiting migrate_mcp_client_secrets — %s %s MCP toolkit(s) (duration = %ss)",
            prefix, "would migrate" if dry_run else "migrated",
            total_migrated, round(end_ts - start_ts, 2),
        )
        return {"migrated": total_migrated, "dry_run": dry_run}

    @web.method()
    def chat_cleanup_dup_msgs(self, *args, **kwargs):
        """Admin task: remove duplicate message groups from a single conversation.

        Detects adjacent messages with identical role and content, keeps the
        highest-ID copy and removes the rest.  Remaps reply_to_id on surviving
        messages before deletion, then resets context_analytics.

        Dry-run is ON by default — pass dry_run=false to actually mutate.

        Param format:
            "project_id=<N>;conversation_id=<id_or_uuid>[;dry_run=false]"

        Examples:
            "project_id=27;conversation_id=43"                - dry run (default)
            "project_id=27;conversation_id=43;dry_run=false"  - live run
            "project_id=27;conversation_id=a1b2c3d4-..."      - lookup by UUID
        """
        from tools import db  # pylint: disable=C0415
        from ..models.message_group import ConversationMessageGroup  # pylint: disable=C0415
        from ..models.message_items.text import TextMessageItem  # pylint: disable=C0415
        from ..models.conversation import Conversation  # pylint: disable=C0415
        from ..utils.context_analytics import update_conversation_meta  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        dry_run = True
        project_id = None
        conversation_arg = None

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                try:
                    project_id = int(seg[len("project_id="):].strip())
                except ValueError:
                    log.error("chat_cleanup_dup_msgs: invalid project_id in param: %s", param)
                    return {"error": "invalid project_id"}
            elif seg_lower.startswith("conversation_id="):
                conversation_arg = seg[len("conversation_id="):].strip()
            elif seg_lower.startswith("dry_run="):
                dry_run = seg_lower[len("dry_run="):].strip() != "false"

        if project_id is None or not conversation_arg:
            log.error(
                "chat_cleanup_dup_msgs: missing required params. "
                "Format: project_id=<N>;conversation_id=<id_or_uuid>[;dry_run=false]"
            )
            return {"error": "missing project_id or conversation_id"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info(
            "%sStarting chat_cleanup_dup_msgs (project_id=%s, conversation=%s, dry_run=%s)",
            prefix, project_id, conversation_arg, dry_run,
        )
        start_ts = time.time()

        try:
            result = _run_chat_cleanup_dup_msgs(
                project_id, conversation_arg, dry_run, prefix,
                db, ConversationMessageGroup, TextMessageItem,
                Conversation, update_conversation_meta,
            )
        except Exception:  # pylint: disable=W0703
            log.exception("%schat_cleanup_dup_msgs: unhandled exception", prefix)
            result = {"error": "unhandled exception"}

        end_ts = time.time()
        log.info("%sExiting chat_cleanup_dup_msgs (duration = %ss)", prefix, round(end_ts - start_ts, 2))
        return result

    @web.method()
    def cleanup_oversized_message_meta(self, *args, **kwargs):
        """Admin task: find and prune oversized chat_message_group.meta JSONB blobs.

        A tool result containing large content (e.g. a binary file dumped into an
        error message) can be persisted into a message group's meta, producing
        multi-MB rows. Loading such a conversation serializes the whole blob and
        stalls the gevent hub, freezing the platform. This task detects oversized
        rows and prunes the offending top-level keys, preserving the rest of meta.

        SAFETY: the oversized meta value is NEVER loaded into memory. Detection and
        pruning happen entirely SQL-side (length(meta::text), jsonb_each, meta-key
        removal), so running this task cannot itself trigger the freeze.

        Dry-run is ON by default — pass dry_run=false to actually prune.

        Param format:
            "project_id=<all|N>;threshold_bytes=<N>;per_key_bytes=<N>;dry_run=false"

        Defaults: project_id=all, threshold_bytes=1000000 (1 MB row total),
        per_key_bytes=262144 (256 KB per key), dry_run=true.

        Examples:
            ""                                          - dry-run census, all projects
            "project_id=3"                              - dry-run census, project 3 only
            "project_id=3;dry_run=false"                - prune oversized rows in project 3
            "project_id=all;threshold_bytes=500000;dry_run=false"  - prune across all projects
        """
        import datetime  # pylint: disable=C0415
        import json  # pylint: disable=C0415
        from sqlalchemy import text  # pylint: disable=C0415
        from tools import db  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        dry_run = True
        project_id_filter = None
        threshold_bytes = 1_000_000
        per_key_bytes = 262_144

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.warning(
                            "cleanup_oversized_message_meta: invalid project_id '%s', scanning all",
                            value,
                        )
            elif seg_lower.startswith("threshold_bytes="):
                try:
                    threshold_bytes = int(seg[len("threshold_bytes="):].strip())
                except ValueError:
                    log.warning("cleanup_oversized_message_meta: invalid threshold_bytes, using default")
            elif seg_lower.startswith("per_key_bytes="):
                try:
                    per_key_bytes = int(seg[len("per_key_bytes="):].strip())
                except ValueError:
                    log.warning("cleanup_oversized_message_meta: invalid per_key_bytes, using default")
            elif seg_lower.startswith("dry_run="):
                dry_run = seg_lower[len("dry_run="):].strip() != "false"

        prefix = "[DRY RUN] " if dry_run else ""
        log.info(
            "%sStarting cleanup_oversized_message_meta (project_id=%s, threshold_bytes=%s, "
            "per_key_bytes=%s, dry_run=%s)",
            prefix, project_id_filter if project_id_filter is not None else "all",
            threshold_bytes, per_key_bytes, dry_run,
        )
        start_ts = time.time()

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("cleanup_oversized_message_meta: failed to list projects")
            return {"error": "failed to list projects"}

        projects_scanned = 0
        groups_flagged = 0
        groups_pruned = 0
        by_project = {}

        for project in projects:
            project_id = project["id"]
            table = f"p_{project_id}.chat_message_group"
            proj_flagged = 0
            proj_pruned = 0
            details = []

            try:
                with db.with_project_schema_session(project_id) as session:
                    # Step A — detect. Reads only the SIZE of meta, never the value.
                    flagged_rows = session.execute(
                        text(
                            f"SELECT id, conversation_id, length(meta::text) AS meta_size "  # nosec B608
                            f"FROM {table} "
                            f"WHERE length(meta::text) > :threshold "
                            f"ORDER BY meta_size DESC"
                        ),
                        {"threshold": threshold_bytes},
                    ).fetchall()

                    projects_scanned += 1

                    for row in flagged_rows:
                        group_id = row.id
                        conversation_id = row.conversation_id
                        meta_size = row.meta_size

                        # Per-key sizes — jsonb_each + length evaluated server-side;
                        # only (key, size) pairs return, never the values.
                        key_rows = session.execute(
                            text(
                                f"SELECT key, length(value::text) AS key_size "  # nosec B608
                                f"FROM {table}, jsonb_each(meta) "
                                f"WHERE id = :row_id AND length(value::text) > :per_key"
                            ),
                            {"row_id": group_id, "per_key": per_key_bytes},
                        ).fetchall()

                        oversized_keys = [k.key for k in key_rows]
                        if not oversized_keys:
                            # Row is large but no single key exceeds per_key_bytes;
                            # report it but do not prune (no clear offender).
                            log.warning(
                                "%scleanup_oversized_message_meta: project %s group %s "
                                "(conversation %s) meta_size=%s but no key over per_key_bytes=%s",
                                prefix, project_id, group_id, conversation_id, meta_size, per_key_bytes,
                            )
                            continue

                        proj_flagged += 1
                        groups_flagged += 1
                        sizes = {k.key: k.key_size for k in key_rows}
                        details.append({
                            "conversation_id": conversation_id,
                            "group_id": group_id,
                            "meta_size": meta_size,
                            "oversized_keys": sizes,
                        })
                        log.info(
                            "%scleanup_oversized_message_meta: project %s group %s "
                            "(conversation %s) meta_size=%s oversized_keys=%s",
                            prefix, project_id, group_id, conversation_id, meta_size, sizes,
                        )

                        # Step B — prune (only when not dry-run). Built server-side,
                        # blob never enters Python.
                        if not dry_run:
                            marker = json.dumps({
                                "keys": oversized_keys,
                                "sizes_bytes": sizes,
                                "reason": "oversized meta pruned by cleanup_oversized_message_meta",
                                "pruned_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            })
                            session.execute(
                                text(
                                    f"UPDATE {table} "  # nosec B608
                                    f"SET meta = (meta - CAST(:keys AS text[])) "
                                    f"           || jsonb_build_object('_pruned_keys', CAST(:marker AS jsonb)) "
                                    f"WHERE id = :row_id"
                                ),
                                {
                                    "keys": oversized_keys,
                                    "marker": marker,
                                    "row_id": group_id,
                                },
                            )
                            proj_pruned += 1
                            groups_pruned += 1

                    if not dry_run and proj_pruned:
                        session.commit()

            except Exception:  # pylint: disable=W0703
                log.exception(
                    "%scleanup_oversized_message_meta: error in project %s", prefix, project_id
                )
                continue

            if proj_flagged:
                by_project[project_id] = {
                    "flagged": proj_flagged,
                    "pruned": proj_pruned,
                    "details": details,
                }

        end_ts = time.time()
        log.info(
            "%sExiting cleanup_oversized_message_meta — scanned=%s flagged=%s pruned=%s "
            "(duration = %ss)",
            prefix, projects_scanned, groups_flagged, groups_pruned, round(end_ts - start_ts, 2),
        )
        return {
            "dry_run": dry_run,
            "projects_scanned": projects_scanned,
            "groups_flagged": groups_flagged,
            "groups_pruned": groups_pruned,
            "by_project": by_project,
        }

    @web.method()
    def migrate_conversation_source_to_elitea(self, *args, **kwargs):
        """Admin task: rename legacy conversation source values to 'elitea'.

        Conversations created before the rebranding may have source='alita'.
        This migration updates them to 'elitea'.

        Idempotent: safe to run multiple times — only updates rows where source='alita'.

        Param format (optional):
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - dry run across all projects
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only
        """
        from tools import db
        from ..models.conversation import Conversation

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error("migrate_conversation_source_to_elitea: invalid project_id '%s'", value)
                        return {"migrated": 0, "error": f"invalid project_id: '{value}'"}
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error("migrate_conversation_source_to_elitea: project_id= is required. Format: project_id=<all|N>[;dry_run]")
            return {"migrated": 0, "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info("Starting migrate_conversation_source_to_elitea (dry_run=%s, project_id_filter=%s)", dry_run, project_id_filter)
        start_ts = time.time()
        total_migrated = 0

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:
            log.exception("migrate_conversation_source_to_elitea: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']
            try:
                with db.with_project_schema_session(project_id) as session:
                    count = session.query(Conversation).filter(
                        Conversation.source == 'alita'
                    ).count()

                    if count > 0:
                        log.info(
                            "%sproject %s: %d conversation(s) with source='alita'",
                            prefix, project_id, count
                        )
                        if not dry_run:
                            session.query(Conversation).filter(
                                Conversation.source == 'alita'
                            ).update({'source': 'elitea'}, synchronize_session=False)
                            session.commit()
                        total_migrated += count

            except Exception:
                log.exception(
                    "%smigrate_conversation_source_to_elitea: error in project %s", prefix, project_id
                )

        end_ts = time.time()
        log.info(
            "%sExiting migrate_conversation_source_to_elitea — %s %s conversation(s) (duration = %ss)",
            prefix, "would migrate" if dry_run else "migrated", total_migrated, round(end_ts - start_ts, 2)
        )
        return {"migrated": total_migrated, "dry_run": dry_run}

    @web.method()
    def migrate_admin_shell_to_inplace(self, *args, **kwargs):
        """Admin task (Bug #4643): merge legacy admin-publish "shell" applications
        back into their original applications.

        Background:
            Before the in-place admin publish flow was introduced, publishing
            an agent from inside the public project created a separate "shell"
            Application (shared_id=original.id) that hosted the published
            version.  This produced duplicate cards in the Admin agent listing.

        What this task does (per shell):
            1. Reassigns each ``published`` version's ``application_id`` from
               the shell to the original application.  Version IDs are
               preserved, so all references (sub-agent meta, AlitaTool
               settings, EntityToolMapping, conversation participants by
               version_id) keep working without changes.
            2. Updates ``version.meta.source_application_id`` to the original
               application id (was the shell id).
            3. Repoints embedded sub-agent ``meta.parent_published_app_id``
               from the shell to the original app id.
               (``parent_published_version_id`` stays unchanged.)
            4. Repoints ``Participant.entity_meta`` JSONB rows **across all
               project schemas** that reference the shell ``application_id`` to
               the original app id, so existing conversations in every project
               keep their proper agent identity.
            5. Merges ``application.meta.adoption`` from the shell into the
               original (sum ``conversation_count``, union ``project_ids``,
               recompute ``project_count``).
            6. Deletes the shell application (cascades base version).

        Version name collision handling:
            If a published version's name already exists on the original app,
            the migrating version is auto-renamed by appending
            ``_migrated_<unix_ts>`` to avoid the UniqueConstraint.

        Idempotent: re-running on already-migrated data finds zero shells
        matching the detection criteria.  Safe to run multiple times.

        Param format:
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - dry run across all projects
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only
        """
        from copy import deepcopy  # pylint: disable=C0415
        from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
        from tools import db  # pylint: disable=C0415
        from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415
        from ..models.participants import Participant  # pylint: disable=C0415
        from ..models.enums.all import PublishStatus, ParticipantTypes  # pylint: disable=C0415
        from ..utils.utils import get_public_project_id  # pylint: disable=C0415

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error(
                            "migrate_admin_shell_to_inplace: invalid project_id '%s'", value
                        )
                        return {
                            "migrated": 0,
                            "error": f"invalid project_id: '{value}'",
                        }
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error(
                "migrate_admin_shell_to_inplace: project_id= is required. "
                "Format: project_id=<all|N>[;dry_run]"
            )
            return {
                "migrated": 0,
                "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]",
            }

        prefix = "[DRY RUN] " if dry_run else ""
        public_project_id = get_public_project_id()
        log.info(
            "%sStarting migrate_admin_shell_to_inplace "
            "(dry_run=%s, project_id_filter=%s, public_project_id=%s)",
            prefix, dry_run, project_id_filter, public_project_id,
        )
        start_ts = time.time()

        # Admin shells live ONLY in the public project.
        if project_id_filter is not None and project_id_filter != public_project_id:
            log.info(
                "migrate_admin_shell_to_inplace: project_id_filter=%s != public_project_id=%s, "
                "nothing to migrate",
                project_id_filter, public_project_id,
            )
            return {"migrated": 0, "skipped": 0, "dry_run": dry_run}

        migrated_shells = 0
        skipped_shells = 0
        migrated_versions = 0

        try:
            with db.with_project_schema_session(public_project_id) as session:
                # Detect admin shells: same-project owner+shared, shared_id set
                shells = session.query(Application).filter(
                    Application.owner_id == public_project_id,
                    Application.shared_owner_id == public_project_id,
                    Application.shared_id.isnot(None),
                ).all()

                log.info(
                    "%sFound %d admin shell application(s) in public project %s",
                    prefix, len(shells), public_project_id,
                )

                for shell in shells:
                    shell_id = shell.id
                    original_id = shell.shared_id
                    shell_name = shell.name

                    # Original may have been deleted manually — guard
                    original = session.query(Application).get(original_id)
                    if original is None:
                        log.warning(
                            "%sshell id=%s (%s): original app %s not found, skipping",
                            prefix, shell_id, shell_name, original_id,
                        )
                        skipped_shells += 1
                        continue

                    # Find published versions on the shell
                    pub_versions = session.query(ApplicationVersion).filter(
                        ApplicationVersion.application_id == shell_id,
                        ApplicationVersion.status == PublishStatus.published,
                    ).all()

                    if not pub_versions:
                        log.info(
                            "%sshell id=%s (%s): no published versions, deleting empty shell",
                            prefix, shell_id, shell_name,
                        )
                        if not dry_run:
                            session.delete(shell)
                            session.flush()
                        migrated_shells += 1
                        continue

                    # Collision check: auto-rename colliding version names
                    VERSION_NAME_MAX = ApplicationVersion.__table__.c.name.type.length
                    original_names = {
                        v.name for v in session.query(ApplicationVersion).filter(
                            ApplicationVersion.application_id == original_id,
                        ).all()
                    }
                    ts_suffix = f"_migrated_{int(start_ts)}"
                    rename_failed = False
                    for v in pub_versions:
                        if v.name in original_names:
                            new_name = f"{v.name}{ts_suffix}"
                            if len(new_name) > VERSION_NAME_MAX:
                                log.warning(
                                    "%sshell id=%s ver id=%s: SKIP — renamed '%s' "
                                    "would be %d chars (max %d)",
                                    prefix, shell_id, v.id, v.name,
                                    len(new_name), VERSION_NAME_MAX,
                                )
                                rename_failed = True
                                break
                            if new_name in original_names:
                                log.warning(
                                    "%sshell id=%s ver id=%s: SKIP — renamed '%s' "
                                    "also collides on original app id=%s",
                                    prefix, shell_id, v.id, new_name, original_id,
                                )
                                rename_failed = True
                                break
                            log.info(
                                "%sshell id=%s ver id=%s: rename '%s' -> '%s' "
                                "(collision with original app id=%s)",
                                prefix, shell_id, v.id, v.name, new_name, original_id,
                            )
                            if not dry_run:
                                v.name = new_name
                    if rename_failed:
                        skipped_shells += 1
                        continue

                    # 1+2. Reassign FK + fix source meta
                    for v in pub_versions:
                        log.info(
                            "%sshell id=%s ver id=%s '%s': reassign application_id %s -> %s",
                            prefix, shell_id, v.id, v.name, shell_id, original_id,
                        )
                        if not dry_run:
                            v.application_id = original_id
                            new_meta = deepcopy(v.meta or {})
                            new_meta['source_application_id'] = original_id
                            v.meta = new_meta
                            flag_modified(v, 'meta')
                        migrated_versions += 1

                    # 3. Repoint embedded sub-agents (parent_published_app_id)
                    embedded_versions = session.query(ApplicationVersion).filter(
                        ApplicationVersion.status == PublishStatus.embedded,
                        ApplicationVersion.meta['parent_published_app_id'].astext
                        == str(shell_id),
                    ).all()
                    for ev in embedded_versions:
                        log.info(
                            "%sshell id=%s: embedded sub-agent ver id=%s parent_app %s -> %s",
                            prefix, shell_id, ev.id, shell_id, original_id,
                        )
                        if not dry_run:
                            new_meta = deepcopy(ev.meta or {})
                            new_meta['parent_published_app_id'] = original_id
                            ev.meta = new_meta
                            flag_modified(ev, 'meta')

                    # 4. Repoint Participant.entity_meta across ALL projects
                    try:
                        all_projects = self.context.rpc_manager.call.project_list() or []
                    except Exception:  # pylint: disable=W0703
                        log.exception(
                            "%sshell id=%s: failed to list projects for participant fix",
                            prefix, shell_id,
                        )
                        all_projects = []

                    for proj in all_projects:
                        pid = proj['id']
                        try:
                            with db.with_project_schema_session(pid) as p_session:
                                participants = p_session.query(Participant).filter(
                                    Participant.entity_name == ParticipantTypes.application,
                                    Participant.entity_meta['id'].astext == str(shell_id),
                                ).all()
                                for p in participants:
                                    log.info(
                                        "%sshell id=%s: project %s participant id=%s "
                                        "entity_meta.id %s -> %s",
                                        prefix, shell_id, pid, p.id, shell_id, original_id,
                                    )
                                    if not dry_run:
                                        new_em = deepcopy(p.entity_meta or {})
                                        new_em['id'] = original_id
                                        p.entity_meta = new_em
                                        flag_modified(p, 'entity_meta')
                                if not dry_run:
                                    p_session.commit()
                        except Exception:  # pylint: disable=W0703
                            log.exception(
                                "%sshell id=%s: error fixing participants in project %s",
                                prefix, shell_id, pid,
                            )

                    # 5. Merge adoption counter
                    shell_adoption = (shell.meta or {}).get('adoption') or {}
                    if shell_adoption:
                        orig_meta = deepcopy(original.meta or {})
                        orig_adoption = orig_meta.get('adoption') or {
                            'conversation_count': 0,
                            'project_count': 0,
                            'project_ids': [],
                        }
                        merged_ids = list({
                            *(orig_adoption.get('project_ids') or []),
                            *(shell_adoption.get('project_ids') or []),
                        })
                        merged = {
                            'conversation_count': int(orig_adoption.get('conversation_count', 0))
                            + int(shell_adoption.get('conversation_count', 0)),
                            'project_count': len(merged_ids),
                            'project_ids': merged_ids,
                        }
                        log.info(
                            "%sshell id=%s: merge adoption %s + %s = %s into original id=%s",
                            prefix, shell_id, orig_adoption, shell_adoption, merged, original_id,
                        )
                        if not dry_run:
                            orig_meta['adoption'] = merged
                            original.meta = orig_meta
                            flag_modified(original, 'meta')

                    # 6. Delete the shell (cascades base version row)
                    log.info(
                        "%sshell id=%s (%s): deleting shell application",
                        prefix, shell_id, shell_name,
                    )
                    if not dry_run:
                        session.delete(shell)
                        session.flush()

                    migrated_shells += 1

                if not dry_run:
                    session.commit()

        except Exception:  # pylint: disable=W0703
            log.exception(
                "%smigrate_admin_shell_to_inplace: error in public project %s",
                prefix, public_project_id,
            )

        end_ts = time.time()
        log.info(
            "%sExiting migrate_admin_shell_to_inplace — "
            "%s %s shell(s), %s version(s); skipped %s shell(s) (duration = %ss)",
            prefix,
            "would migrate" if dry_run else "migrated",
            migrated_shells,
            migrated_versions,
            skipped_shells,
            round(end_ts - start_ts, 2),
        )
        return {
            "migrated_shells": migrated_shells,
            "migrated_versions": migrated_versions,
            "skipped_shells": skipped_shells,
            "dry_run": dry_run,
        }

    @web.method()
    def migrate_ado_project_to_toolkit(self, *args, **kwargs):
        """Admin task: migrate 'project' from ADO configuration to toolkit settings.

        Migration #3620: Move ADO project from configuration (credentials) to toolkit level.

        This migration:
        1. Finds ADO toolkits (ado_boards, ado_repos, ado_wiki, ado_plans)
        2. For each toolkit without 'project' in settings:
           - Looks up the referenced ADO configuration by elitea_title
           - Copies 'project' from configuration.data to toolkit.settings
        3. After ALL toolkits are updated, removes 'project' from ADO configurations

        Transactional: if any toolkit fails to update, the entire project rolls back
        and configurations are not modified (prevents data loss).

        Idempotent: safe to run multiple times — skips toolkits that already have 'project'.

        Param format:
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - dry run across all projects
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only

        Always run with dry_run first to verify expected changes.
        """
        from tools import db  # pylint: disable=C0415
        from ..models.all import EliteATool  # pylint: disable=C0415

        # Import Configuration model
        try:
            from plugins.configurations.models.configuration import Configuration  # pylint: disable=C0415
        except ImportError:
            log.error("migrate_ado_project_to_toolkit: configurations plugin not available")
            return {"migrated": 0, "error": "configurations plugin not available"}

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error("migrate_ado_project_to_toolkit: invalid project_id '%s'", value)
                        return {"migrated": 0, "error": f"invalid project_id: '{value}'"}
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error("migrate_ado_project_to_toolkit: project_id= is required. Format: project_id=<all|N>[;dry_run]")
            return {"migrated": 0, "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info("Starting migrate_ado_project_to_toolkit (dry_run=%s, project_id_filter=%s)", dry_run, project_id_filter)
        start_ts = time.time()

        # Track migration results
        total_results = {
            "toolkits_migrated": 0,
            "toolkits_skipped": 0,
            "configurations_cleaned": 0,
            "failed_projects": 0,
            "errors": []
        }

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:  # pylint: disable=W0703
            log.exception("migrate_ado_project_to_toolkit: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']

            try:
                results = _run_ado_project_migration(
                    project_id, dry_run, prefix,
                    db, EliteATool, Configuration,
                )
                total_results["toolkits_migrated"] += results["toolkits_migrated"]
                total_results["toolkits_skipped"] += results["toolkits_skipped"]
                total_results["configurations_cleaned"] += results["configurations_cleaned"]
                total_results["errors"].extend(results.get("errors", []))

            except Exception as e:  # pylint: disable=W0703
                log.exception(
                    "%smigrate_ado_project_to_toolkit: error in project %s", prefix, project_id
                )
                total_results["failed_projects"] += 1
                total_results["errors"].append({
                    "project_id": project_id,
                    "error": str(e)
                })

        end_ts = time.time()
        log.info(
            "%sExiting migrate_ado_project_to_toolkit — %s %s toolkit(s), cleaned %s config(s) (duration = %ss)",
            prefix, "would migrate" if dry_run else "migrated",
            total_results["toolkits_migrated"], total_results["configurations_cleaned"],
            round(end_ts - start_ts, 2)
        )

        return {
            "toolkits_migrated": total_results["toolkits_migrated"],
            "toolkits_skipped": total_results["toolkits_skipped"],
            "configurations_cleaned": total_results["configurations_cleaned"],
            "failed_projects": total_results["failed_projects"],
            "errors": total_results["errors"],
            "dry_run": dry_run
        }

    @web.method()
    def migrate_agent_version_null_instructions(self, *args, **kwargs):
        """Admin task: fix agent versions where instructions is NULL instead of empty string.

        Some agent versions have instructions=NULL which causes Pydantic validation errors
        when the field expects a string type. This migration updates NULL to empty string ''.

        Idempotent: safe to run multiple times — only updates rows where instructions IS NULL.

        Param format:
            "project_id=<all|N>[;dry_run]"

        Examples:
            "project_id=all;dry_run"  - dry run across all projects
            "project_id=all"          - migrate all projects
            "project_id=3"            - migrate project 3 only
        """
        from tools import db
        from ..models.all import ApplicationVersion

        param = kwargs.get("param", "") or ""
        dry_run = False
        project_id_filter = None
        project_id_found = False

        for seg in [s.strip() for s in param.split(";")]:
            seg_lower = seg.lower()
            if seg_lower.startswith("project_id="):
                project_id_found = True
                value = seg[len("project_id="):].strip()
                if value.lower() != "all":
                    try:
                        project_id_filter = int(value)
                    except ValueError:
                        log.error("migrate_agent_version_null_instructions: invalid project_id '%s'", value)
                        return {"migrated": 0, "error": f"invalid project_id: '{value}'"}
            elif seg_lower == "dry_run":
                dry_run = True

        if not project_id_found:
            log.error("migrate_agent_version_null_instructions: project_id= is required. Format: project_id=<all|N>[;dry_run]")
            return {"migrated": 0, "error": "project_id= is required. Format: project_id=<all|N>[;dry_run]"}

        prefix = "[DRY RUN] " if dry_run else ""
        log.info("Starting migrate_agent_version_null_instructions (dry_run=%s, project_id_filter=%s)", dry_run, project_id_filter)
        start_ts = time.time()
        total_migrated = 0

        try:
            if project_id_filter is not None:
                projects = [{"id": project_id_filter}]
            else:
                projects = self.context.rpc_manager.call.project_list() or []
        except Exception:
            log.exception("migrate_agent_version_null_instructions: failed to list projects")
            return {"migrated": 0, "error": "failed to list projects"}

        for project in projects:
            project_id = project['id']
            try:
                with db.with_project_schema_session(project_id) as session:
                    count = session.query(ApplicationVersion).filter(
                        ApplicationVersion.instructions.is_(None)
                    ).count()

                    if count > 0:
                        log.info(
                            "%sproject %s: %d agent version(s) with instructions=NULL",
                            prefix, project_id, count
                        )
                        if not dry_run:
                            session.query(ApplicationVersion).filter(
                                ApplicationVersion.instructions.is_(None)
                            ).update({'instructions': ''}, synchronize_session=False)
                            session.commit()
                        total_migrated += count

            except Exception:
                log.exception(
                    "%smigrate_agent_version_null_instructions: error in project %s", prefix, project_id
                )

        end_ts = time.time()
        log.info(
            "%sExiting migrate_agent_version_null_instructions — %s %s version(s) (duration = %ss)",
            prefix, "would migrate" if dry_run else "migrated", total_migrated, round(end_ts - start_ts, 2)
        )
        return {"migrated": total_migrated, "dry_run": dry_run}

    # pylint: disable=R,W0613
    @web.method()
    def migrate_llm_model(self, *args, **kwargs):
        """Migrate stale LLM model_name references in ApplicationVersions and ParticipantMappings.

        LLM section only. Idempotent — safe to re-run after interruption.
        Dry-run is OFF by default — pass dry_run=true to preview changes without mutating.

        Params (semicolon-separated):
            from=<deprecated_model_name>  (required)
            to=<new_model_name>           (required, must exist as shared LLM in public project)
            project_id=<int|X-Y|all>      (optional, default: all)
            dry_run=<true|false>           (optional, default: false)

        Examples:
            from=gpt-4;to=gpt-4o                   - live run (all projects)
            from=gpt-4;to=gpt-4o;dry_run=true      - dry run to preview changes
            from=claude-2;to=claude-3-5-sonnet;project_id=1-1000;dry_run=true  - dry run on projects 1-1000
            from=o1-preview;to=gpt-4o;project_id=42  - live run on project 42 only
        """
        from tools import db  # pylint: disable=C0415

        start_ts = time.time()

        # ── 1. Parse and validate params ──────────────────────────────────
        raw_param = kwargs.get("param", "")
        try:
            params = parse_migration_params(raw_param)
        except ValueError as exc:
            log.error("Invalid params: %s", exc)
            return

        from_model = params['from_model']
        to_model = params['to_model']
        dry_run = params['dry_run']

        log.info(
            "Params: from=%s  to=%s  project_id=%s  dry_run=%s",
            from_model, to_model, params['project_id_spec'], dry_run,
        )

        # ── 2. Pre-flight: validate target model exists ───────────────────
        try:
            target_config = validate_target_model(to_model)
        except ValueError as exc:
            log.error("Pre-flight failed: %s", exc)
            return

        target_supports_reasoning = bool(target_config.get('supports_reasoning', False))
        public_project_id = get_public_project_id()

        # ── 3. Look up source model capabilities (best-effort) ────────────
        source_supports_reasoning = lookup_source_model_capabilities(from_model)
        if source_supports_reasoning is None:
            log.info(
                "Source model %s not found in available models (likely deleted). "
                "Cross-family defaults will be applied.",
                from_model,
            )
        else:
            log.info(
                "Source model %s: supports_reasoning=%s",
                from_model, source_supports_reasoning,
            )

        # ── 4. Build settings factory closure ─────────────────────────────
        settings_factory = partial(
            build_new_llm_settings,
            new_model_name=to_model,
            new_model_project_id=public_project_id,
            target_supports_reasoning=target_supports_reasoning,
            source_supports_reasoning=source_supports_reasoning,
        )

        # ── 5. Resolve target projects ────────────────────────────────────
        try:
            project_ids = resolve_target_project_ids(params['project_id_spec'])
        except ValueError as exc:
            log.error("Project resolution failed: %s", exc)
            return

        # ── 6. Per-project migration ──────────────────────────────────────
        total_projects = len(project_ids)
        total_versions = 0
        total_mappings = 0
        projects_touched = 0

        for idx, project_id in enumerate(project_ids, 1):
            try:
                with db.get_session(project_id) as session:
                    v_count = migrate_application_versions(
                        session, from_model, settings_factory, dry_run,
                    )
                    m_count = migrate_participant_mappings(
                        session, from_model, settings_factory, dry_run,
                    )

                    if v_count or m_count:
                        if not dry_run:
                            session.commit()
                        projects_touched += 1
                        total_versions += v_count
                        total_mappings += m_count
                        log.info(
                            "Project %s (%s/%s): %s versions, %s mappings %s",
                            project_id, idx, total_projects,
                            v_count, m_count,
                            "[dry_run]" if dry_run else "[committed]",
                        )
            except Exception:  # pylint: disable=W0702
                log.exception("Error processing project %s, skipping", project_id)

        # ── 7. Summary ───────────────────────────────────────────────────
        duration = time.time() - start_ts
        log.info(
            "Done%s. Projects scanned: %s, touched: %s. "
            "ApplicationVersions: %s, ParticipantMappings: %s. Duration: %.1fs",
            " [DRY RUN]" if dry_run else "",
            total_projects, projects_touched,
            total_versions, total_mappings,
            duration,
        )

    @web.method("collections_removal_migration")
    def collections_removal_migration(self, *args, **kwargs):
        """Drop prompt_collections table and applications.collections column for all or specific projects.

        Param / payload fields:
            project_ids (list[int], optional): projects to migrate; empty/omitted runs all projects.
            dry_run (bool, optional): inspect without making changes. Default false.

        Admin UI usage (param string):
            {"dry_run": true}
            {"project_ids": [1, 2], "dry_run": true}
        """
        import json
        from typing import Optional, List
        from pydantic import BaseModel
        from plugins.admin.tasks.logs import make_logger
        from sqlalchemy import text
        from tools import db, rpc_tools, serialize

        class CollectionsRemovalPayload(BaseModel):
            project_ids: Optional[List[int]] = []
            dry_run: Optional[bool] = False

        with make_logger() as log:
            raw_param = kwargs.get("param", "") or ""
            payload = None
            if raw_param.strip():
                try:
                    payload = json.loads(raw_param)
                except (json.JSONDecodeError, ValueError):
                    log.warning(
                        "[collections_removal_migration] Could not parse param as JSON: %r",
                        raw_param,
                    )

            if payload is None:
                migration_payload = CollectionsRemovalPayload.model_construct()
            else:
                migration_payload = CollectionsRemovalPayload.model_validate(payload)

            dry_run = migration_payload.dry_run
            mode = "DRY RUN" if dry_run else "LIVE"
            log.info("[collections_removal_migration] [%s] Starting. Payload: %s", mode, migration_payload)

            rpc = rpc_tools.RpcMixin().rpc.call

            def get_all_project_ids():
                return [i['id'] for i in rpc.project_list(filter_={'create_success': True})]

            project_ids = migration_payload.project_ids or get_all_project_ids()
            project_ids = sorted(set(project_ids))

            results = []
            errors = []

            for pid in project_ids:
                try:
                    with db.get_session(pid) as session:
                        has_collections_table = session.execute(text(
                            "SELECT EXISTS ("
                            "  SELECT 1 FROM information_schema.tables"
                            f"  WHERE table_schema = 'p_{pid}'"
                            "  AND table_name = 'prompt_collections'"
                            ")"
                        )).scalar()

                        has_collections_column = session.execute(text(
                            "SELECT EXISTS ("
                            "  SELECT 1 FROM information_schema.columns"
                            f"  WHERE table_schema = 'p_{pid}'"
                            "  AND table_name = 'applications'"
                            "  AND column_name = 'collections'"
                            ")"
                        )).scalar()

                        entry = {
                            "project_id": pid,
                            "prompt_collections_table_exists": has_collections_table,
                            "applications_collections_column_exists": has_collections_column,
                        }

                        if not dry_run:
                            if has_collections_table:
                                session.execute(text(
                                    f"DROP TABLE p_{pid}.prompt_collections"
                                ))
                            if has_collections_column:
                                session.execute(text(
                                    f"ALTER TABLE p_{pid}.applications DROP COLUMN collections"
                                ))
                            session.commit()
                            entry["status"] = "migrated"
                        else:
                            entry["status"] = "dry_run"

                    log.info("[collections_removal_migration] [%s] project_id=%s: %s", mode, pid, entry)
                    results.append(entry)

                except Exception as e:
                    log.error("[collections_removal_migration] [%s] project_id=%s failed: %s", mode, pid, e)
                    errors.append({"project_id": pid, "error": str(e)})

            log.info(
                "[collections_removal_migration] [%s] Finished: results=%s, errors=%s",
                mode, results, errors,
            )

            return serialize({
                "dry_run": dry_run,
                "results": results,
                "errors": errors,
            })


def _run_chat_cleanup_dup_msgs(  # pylint: disable=R0913,R0914
    project_id, conversation_arg, dry_run, prefix,
    db, ConversationMessageGroup, TextMessageItem,
    Conversation, update_conversation_meta,
):
    """Core logic for chat_cleanup_dup_msgs, separated for readability."""
    with db.get_session(project_id) as session:
        # --- Resolve conversation ---
        try:
            conv_id = int(conversation_arg)
            conversation = session.query(Conversation).filter(
                Conversation.id == conv_id
            ).first()
        except ValueError:
            conversation = session.query(Conversation).filter(
                Conversation.uuid == conversation_arg
            ).first()

        if not conversation:
            log.error("%sConversation not found: %s", prefix, conversation_arg)
            return {"error": "conversation not found"}

        conversation_id = conversation.id
        log.info("%sConversation: id=%s, name='%s'", prefix, conversation_id, conversation.name)

        # --- Fetch all message groups with text content ---
        groups = (
            session.query(ConversationMessageGroup)
            .filter(ConversationMessageGroup.conversation_id == conversation_id)
            .order_by(ConversationMessageGroup.created_at)
            .all()
        )

        # Build fingerprint map: group_id -> (item_count, item_types, text_content)
        # This ensures multimodal messages (text + attachments) only match if
        # they have the exact same structure, not just identical text.
        from ..models.message_items.base import MessageItem  # pylint: disable=C0415
        fingerprint_map = {}
        for g in groups:
            items = (
                session.query(MessageItem)
                .filter(MessageItem.message_group_id == g.id)
                .order_by(MessageItem.order_index)
                .all()
            )
            item_types = tuple(it.item_type for it in items)
            text_item = (
                session.query(TextMessageItem)
                .filter(TextMessageItem.message_group_id == g.id)
                .first()
            )
            text_content = text_item.content if text_item else ""
            fingerprint_map[g.id] = (len(items), item_types, text_content)

        log.info("%sTotal message groups: %d", prefix, len(groups))

        # --- Detect duplicate clusters ---
        # Only strictly adjacent messages with same author, content, AND created_at
        # are duplicates. [a,a,b,b] = 2 dup pairs; [a,b,c,a,b] = no dups.
        # The created_at check prevents false positives when a user legitimately
        # sends the same message twice at different times.
        remove_ids = set()
        keep_map = {}  # removed_id -> kept_id (for reply_to remapping)

        i = 0
        while i < len(groups):
            # Collect a run of adjacent messages with identical author+timestamp+fingerprint
            run = [groups[i]]
            while i + 1 < len(groups) \
                    and groups[i + 1].author_participant_id == run[0].author_participant_id \
                    and groups[i + 1].created_at == run[0].created_at \
                    and fingerprint_map[groups[i + 1].id] == fingerprint_map[run[0].id]:
                i += 1
                run.append(groups[i])

            if len(run) > 1:
                max_id = max(g.id for g in run)
                for g in run:
                    if g.id != max_id:
                        remove_ids.add(g.id)
                        keep_map[g.id] = max_id

            i += 1

        if not remove_ids:
            log.info("%sNo duplicates found.", prefix)
            return {"duplicates_found": 0}

        log.info(
            "%sDuplicates detected: %d messages to remove (keeping %d)",
            prefix, len(remove_ids), len(groups) - len(remove_ids),
        )

        # --- Identify reply_to remaps needed ---
        remaps = {}  # surviving_msg_id -> (old_reply_to, new_reply_to)
        for g in groups:
            if g.id in remove_ids:
                continue
            if g.reply_to_id and g.reply_to_id in remove_ids:
                new_target = keep_map.get(g.reply_to_id)
                if new_target:
                    remaps[g.id] = (g.reply_to_id, new_target)

        # --- Log details ---
        for rid in sorted(remove_ids):
            kept = keep_map[rid]
            _item_count, _item_types, text = fingerprint_map[rid]
            log.info(
                "%s  REMOVE id=%d (kept duplicate id=%d, items=%s, content='%s')",
                prefix, rid, kept, list(_item_types), text[:60],
            )

        for msg_id, (old_target, new_target) in remaps.items():
            log.info(
                "%s  REMAP id=%d: reply_to %d -> %d",
                prefix, msg_id, old_target, new_target,
            )

        log.info(
            "%sSummary: %d duplicates to remove, %d reply_to remaps, "
            "%d messages before, %d messages after",
            prefix, len(remove_ids), len(remaps),
            len(groups), len(groups) - len(remove_ids),
        )

        if dry_run:
            return {
                "dry_run": True,
                "duplicates_found": len(remove_ids),
                "reply_to_remaps": len(remaps),
                "messages_before": len(groups),
                "messages_after": len(groups) - len(remove_ids),
            }

        # --- Live run: apply changes ---
        # Step 1: Remap reply_to_id on surviving messages
        for msg_id, (_old, new_target) in remaps.items():
            session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.id == msg_id
            ).update(
                {ConversationMessageGroup.reply_to_id: new_target},
                synchronize_session=False,
            )
        if remaps:
            session.flush()
            log.info("Remapped %d reply_to_id pointers", len(remaps))

        # Step 2: Delete message items first, then message groups
        # (ORM bulk delete doesn't trigger FK cascade)
        from ..models.message_items.base import MessageItem  # pylint: disable=C0415
        session.query(MessageItem).filter(
            MessageItem.message_group_id.in_(remove_ids)
        ).delete(synchronize_session=False)
        deleted = session.query(ConversationMessageGroup).filter(
            ConversationMessageGroup.id.in_(remove_ids)
        ).delete(synchronize_session=False)
        session.commit()
        log.info("Deleted %d duplicate message groups", deleted)

        # Step 3: Reset context_analytics
        try:
            update_conversation_meta(project_id, conversation_id, {'context_analytics': None})
            log.info("Reset context_analytics for conversation %d", conversation_id)
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to reset context_analytics")

        return {
            "dry_run": False,
            "duplicates_removed": deleted,
            "reply_to_remaps": len(remaps),
            "messages_before": len(groups),
            "messages_after": len(groups) - len(remove_ids),
        }


def _run_ado_project_migration(  # pylint: disable=R0913,R0914
    project_id, dry_run, prefix,
    db, EliteATool, Configuration,
):
    """Core logic for migrate_ado_project_to_toolkit, separated for readability.

    Migration #3620: Move 'project' from ADO configuration to toolkit level.

    This migration:
    1. Finds ADO toolkits (ado_boards, ado_repos, ado_wiki, ado_plans, etc.)
    2. For each toolkit without 'project' in settings:
       - Looks up the referenced ADO configuration by elitea_title
       - Copies 'project' from configuration.data to toolkit.settings
    3. After ALL toolkits are updated, removes 'project' from ADO configurations

    The migration is transactional per-project: if any toolkit fails to update,
    the entire project is rolled back and configurations are not modified.
    """
    from copy import deepcopy  # pylint: disable=C0415
    from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415

    # ADO toolkit types that need project at toolkit level
    ADO_TOOLKIT_TYPES = (
        'ado_boards', 'ado_repos', 'ado_wiki', 'ado_plans',
        'azure_devops_plans', 'azure_devops_wiki'
    )

    results = {
        "toolkits_migrated": 0,
        "toolkits_skipped": 0,
        "configurations_cleaned": 0,
        "errors": []
    }

    with db.with_project_schema_session(project_id) as session:
        # Step 1: Find all ADO toolkits
        toolkits = session.query(EliteATool).filter(
            EliteATool.type.in_(ADO_TOOLKIT_TYPES)
        ).all()

        if not toolkits:
            return results

        # Track which configurations need cleanup (only after all toolkits succeed)
        configs_to_clean = set()

        # Step 2: Copy project from configuration to toolkit
        for toolkit in toolkits:
            settings = toolkit.settings or {}

            # Skip if toolkit already has project
            if settings.get('project'):
                results["toolkits_skipped"] += 1
                continue

            # Get ADO configuration reference
            ado_config_ref = settings.get('ado_configuration')
            if not ado_config_ref:
                log.warning(
                    "%sproject %s, toolkit id=%s (%s): no ado_configuration reference, skipping",
                    prefix, project_id, toolkit.id, toolkit.type
                )
                results["toolkits_skipped"] += 1
                continue

            elitea_title = ado_config_ref.get('elitea_title')
            if not elitea_title:
                log.warning(
                    "%sproject %s, toolkit id=%s (%s): no elitea_title in ado_configuration, skipping",
                    prefix, project_id, toolkit.id, toolkit.type
                )
                results["toolkits_skipped"] += 1
                continue

            # Find the configuration
            configuration = session.query(Configuration).filter(
                Configuration.elitea_title == elitea_title
            ).first()

            if not configuration:
                log.warning(
                    "%sproject %s, toolkit id=%s (%s): configuration '%s' not found, skipping",
                    prefix, project_id, toolkit.id, toolkit.type, elitea_title
                )
                results["toolkits_skipped"] += 1
                continue

            config_data = configuration.data or {}
            project_value = config_data.get('project')

            if not project_value:
                log.warning(
                    "%sproject %s, toolkit id=%s (%s): configuration '%s' has no 'project' field, skipping",
                    prefix, project_id, toolkit.id, toolkit.type, elitea_title
                )
                results["toolkits_skipped"] += 1
                continue

            log.info(
                "%sproject %s, toolkit id=%s (%s): copying project='%s' from config '%s'",
                prefix, project_id, toolkit.id, toolkit.type, project_value, elitea_title
            )

            if not dry_run:
                new_settings = deepcopy(settings)
                new_settings['project'] = project_value
                toolkit.settings = new_settings
                flag_modified(toolkit, 'settings')

            results["toolkits_migrated"] += 1
            configs_to_clean.add(configuration.id)

        # Step 3: Remove 'project' from ADO configurations (only if all toolkits succeeded)
        if configs_to_clean:
            for config_id in configs_to_clean:
                configuration = session.query(Configuration).filter(
                    Configuration.id == config_id
                ).first()

                if configuration and configuration.data and 'project' in configuration.data:
                    log.info(
                        "%sproject %s, config id=%s ('%s'): removing 'project' field",
                        prefix, project_id, configuration.id, configuration.elitea_title
                    )

                    if not dry_run:
                        new_data = deepcopy(configuration.data)
                        del new_data['project']
                        configuration.data = new_data
                        flag_modified(configuration, 'data')

                    results["configurations_cleaned"] += 1

        if not dry_run:
            session.commit()

    return results
