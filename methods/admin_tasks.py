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
