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

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from ..scripts.tool_icons import download_github_repo_zip, unzip_file
from ..utils.toolkit_migration import run_selected_tools_migration


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
