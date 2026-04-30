#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
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

"""Utilities for migrating EliteATool.settings['selected_tools'], EntityToolMapping.selected_tools,
and pipeline YAML instructions tool name references."""

import re
from copy import deepcopy

from sqlalchemy.orm.attributes import flag_modified

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import db, context  # pylint: disable=E0401

from ..models.all import ApplicationVersion
from ..models.enums.all import AgentTypes
from ..models.elitea_tools import EliteATool, EntityToolMapping


USAGE_HINT = (
    'Format: "<toolkit_type>;<operations>;project_id=<all|N>[;dry_run]"\n'
    'Operations (comma-separated): tool_name (remove) or old>new (rename)\n'
    'Examples:\n'
    '  "github;index_data>indexData,search_index;project_id=all"\n'
    '  "artifact;read_file_chunk;project_id=all;dry_run"\n'
    '  "gitlab;list_repos;project_id=34"\n'
    '  "github;old_tool>new_tool;project_id=34;dry_run"'
)


def parse_migration_args(param):
    """Parse semicolon-delimited migration param string into a structured dict.

    Returns dict with keys: toolkit_type, operations, project_id, dry_run.
    Raises ValueError on invalid input.
    """
    if not param or not param.strip():
        raise ValueError(f"Empty param string.\n{USAGE_HINT}")
    #
    segments = [s.strip() for s in param.split(";")]
    if len(segments) < 3:
        raise ValueError(
            f"Expected at least 3 semicolon-separated segments, got {len(segments)}.\n{USAGE_HINT}"
        )
    #
    # Segment 0: toolkit_type
    #
    toolkit_type = segments[0]
    if not toolkit_type:
        raise ValueError(f"toolkit_type (segment 1) cannot be empty.\n{USAGE_HINT}")
    if toolkit_type[0].isdigit():
        raise ValueError(
            f"toolkit_type must not start with a digit, got '{toolkit_type}'.\n{USAGE_HINT}"
        )
    #
    # Segment 1: operations
    #
    operations_raw = segments[1]
    if not operations_raw:
        raise ValueError(f"operations (segment 2) cannot be empty.\n{USAGE_HINT}")
    #
    operations = []
    for op_str in operations_raw.split(","):
        op_str = op_str.strip()
        if not op_str:
            continue
        if ">" in op_str:
            parts = op_str.split(">", 1)
            old_name = parts[0].strip()
            new_name = parts[1].strip()
            if not old_name or not new_name:
                raise ValueError(
                    f"Rename operation has empty name: '{op_str}'.\n{USAGE_HINT}"
                )
            operations.append({"action": "rename", "old_name": old_name, "new_name": new_name})
        else:
            operations.append({"action": "remove", "tool_name": op_str})
    #
    if not operations:
        raise ValueError(f"No valid operations found in '{operations_raw}'.\n{USAGE_HINT}")
    #
    # Remaining segments: project_id= and dry_run
    #
    project_id = None
    dry_run = False
    project_id_found = False
    #
    for seg in segments[2:]:
        seg_lower = seg.lower()
        if seg_lower.startswith("project_id="):
            project_id_found = True
            value = seg[len("project_id="):].strip()
            if value.lower() == "all":
                project_id = "all"
            else:
                try:
                    project_id = int(value)
                    if project_id <= 0:
                        raise ValueError(
                            f"project_id must be a positive integer, got {project_id}.\n{USAGE_HINT}"
                        )
                except ValueError as exc:
                    if "positive integer" in str(exc):
                        raise
                    raise ValueError(
                        f"project_id must be 'all' or a positive integer, got '{value}'.\n{USAGE_HINT}"
                    ) from exc
        elif seg_lower == "dry_run":
            dry_run = True
    #
    if not project_id_found:
        raise ValueError(f"project_id= segment is required.\n{USAGE_HINT}")
    #
    return {
        "toolkit_type": toolkit_type,
        "operations": operations,
        "project_id": project_id,
        "dry_run": dry_run,
    }


def apply_operations_to_selected_tools(selected_tools, operations):
    """Apply remove/rename operations to a selected_tools list.

    Pure function - no DB access.
    Returns (new_selected_tools, changes_log, tools_removed, tools_renamed).
    Idempotent: running twice with same input produces same result.
    """
    result = list(selected_tools)
    changes = []
    tools_removed = 0
    tools_renamed = 0
    #
    for op in operations:
        if op["action"] == "remove":
            tool_name = op["tool_name"]
            if tool_name in result:
                result.remove(tool_name)
                changes.append(f"removed '{tool_name}'")
                tools_removed += 1
            # else: already absent, no-op (not logged as change)
        #
        elif op["action"] == "rename":
            old_name = op["old_name"]
            new_name = op["new_name"]
            if old_name not in result:
                # Source not found, no-op
                pass
            elif new_name in result:
                # Target already present, just remove old to avoid duplicates
                result.remove(old_name)
                changes.append(
                    f"renamed '{old_name}' -> '{new_name}' (target already present, removed old)"
                )
                tools_renamed += 1
            else:
                result.remove(old_name)
                result.append(new_name)
                changes.append(f"renamed '{old_name}' -> '{new_name}'")
                tools_renamed += 1
    #
    return result, changes, tools_removed, tools_renamed


def apply_rename_to_instructions(instructions_text, operations):
    """Apply rename operations to pipeline YAML instructions text using word-boundary regex.

    Returns (new_text, changes_log, skipped_log).
    Per-operation guard: if an operation's new name is already present, that
    individual operation is skipped but remaining operations still apply.
    """
    text = instructions_text
    changes = []
    skipped = []
    #
    for op in operations:
        if op["action"] != "rename":
            continue
        old_name = op["old_name"]
        new_name = op["new_name"]
        #
        # Guard: skip this operation if new name already present
        #
        pattern_new = re.compile(r'\b' + re.escape(new_name) + r'\b')
        if pattern_new.search(text):
            skipped.append(
                f"SKIP '{old_name}' -> '{new_name}': new name already present"
            )
            continue
        #
        # Apply rename
        #
        pattern_old = re.compile(r'\b' + re.escape(old_name) + r'\b')
        new_text = pattern_old.sub(new_name, text)
        if new_text != text:
            count = len(pattern_old.findall(text))
            changes.append(f"renamed '{old_name}' -> '{new_name}' ({count} occurrences)")
            text = new_text
    #
    return text, changes, skipped


def migrate_project_pipeline_instructions(project_id, operations, dry_run):
    """Migrate tool names in pipeline YAML instructions for a project.

    Scans all pipeline ApplicationVersions and applies word-boundary rename.
    Returns summary dict.
    """
    prefix = "[DRY RUN] " if dry_run else ""
    rename_ops = [op for op in operations if op["action"] == "rename"]
    if not rename_ops:
        return {"pipeline_versions_scanned": 0, "pipeline_versions_affected": 0}
    #
    summary = {
        "pipeline_versions_scanned": 0,
        "pipeline_versions_affected": 0,
        "pipeline_details": [],
    }
    #
    with db.get_session(project_id) as session:
        pipeline_versions = session.query(ApplicationVersion).filter(
            ApplicationVersion.agent_type == AgentTypes.pipeline,
            ApplicationVersion.instructions.isnot(None),
            ApplicationVersion.instructions != "",
        ).all()
        #
        summary["pipeline_versions_scanned"] = len(pipeline_versions)
        #
        if not pipeline_versions:
            return summary
        #
        any_changed = False
        #
        for version in pipeline_versions:
            new_text, changes, skipped = apply_rename_to_instructions(
                version.instructions, rename_ops
            )
            #
            for skip_msg in skipped:
                log.warning(
                    "%sPipeline version id=%s (app_id=%s): %s",
                    prefix, version.id, version.application_id, skip_msg
                )
            #
            if changes:
                changes_str = ", ".join(changes)
                log.info(
                    "%sPipeline version id=%s (app_id=%s): %s",
                    prefix, version.id, version.application_id, changes_str
                )
                summary["pipeline_versions_affected"] += 1
                summary["pipeline_details"].append({
                    "version_id": version.id,
                    "application_id": version.application_id,
                    "changes": changes,
                    "skipped": skipped,
                })
                #
                if not dry_run:
                    version.instructions = new_text
                    any_changed = True
        #
        if any_changed and not dry_run:
            session.commit()
    #
    return summary


def migrate_project_toolkits(project_id, toolkit_type, operations, dry_run):
    """Migrate selected_tools for all toolkits of given type in a project.

    Returns summary dict with project_id, toolkits_found, toolkits_affected,
    entity_mappings_affected, tools_removed, tools_renamed,
    tool_mappings_removed, tool_mappings_renamed, details.
    """
    prefix = "[DRY RUN] " if dry_run else ""
    summary = {
        "project_id": project_id,
        "toolkits_found": 0,
        "toolkits_affected": 0,
        "entity_mappings_affected": 0,
        "tools_removed": 0,
        "tools_renamed": 0,
        "tool_mappings_removed": 0,
        "tool_mappings_renamed": 0,
        "pipeline_versions_scanned": 0,
        "pipeline_versions_affected": 0,
        "details": [],
    }
    #
    with db.get_session(project_id) as session:
        toolkits = session.query(EliteATool).filter(
            EliteATool.type == toolkit_type
        ).all()
        #
        summary["toolkits_found"] = len(toolkits)
        #
        if not toolkits:
            return summary
        #
        any_changed = False
        #
        for tool in toolkits:
            settings = tool.settings
            if not settings:
                log.warning(
                    "%sEliteATool id=%s '%s': settings is empty, skipping",
                    prefix, tool.id, tool.name
                )
                continue
            #
            current_selected = settings.get("selected_tools")
            if not current_selected:
                continue
            #
            # Apply operations to EliteATool.settings['selected_tools']
            #
            new_selected, tool_changes, removed_count, renamed_count = apply_operations_to_selected_tools(
                current_selected, operations
            )
            #
            if tool_changes:
                changes_str = ", ".join(tool_changes)
                log.info(
                    "%sEliteATool id=%s '%s': %s",
                    prefix, tool.id, tool.name, changes_str
                )
                summary["details"].append({
                    "tool_id": tool.id,
                    "tool_name": tool.name,
                    "changes": tool_changes,
                })
                summary["toolkits_affected"] += 1
                summary["tools_removed"] += removed_count
                summary["tools_renamed"] += renamed_count
                #
                if not dry_run:
                    updated_settings = deepcopy(settings)
                    updated_settings["selected_tools"] = new_selected
                    tool.settings = updated_settings
                    flag_modified(tool, "settings")
                    any_changed = True
            #
            # Apply operations to EntityToolMapping.selected_tools
            #
            mappings = session.query(EntityToolMapping).filter(
                EntityToolMapping.tool_id == tool.id
            ).all()
            #
            for mapping in mappings:
                if not mapping.selected_tools:
                    continue
                #
                new_mapping_selected, mapping_changes, mapping_removed_count, mapping_renamed_count = apply_operations_to_selected_tools(
                    mapping.selected_tools, operations
                )
                #
                if mapping_changes:
                    mapping_changes_str = ", ".join(mapping_changes)
                    log.info(
                        "%s  EntityToolMapping id=%s (entity_version_id=%s): %s",
                        prefix, mapping.id, mapping.entity_version_id, mapping_changes_str
                    )
                    summary["entity_mappings_affected"] += 1
                    summary["tool_mappings_removed"] += mapping_removed_count
                    summary["tool_mappings_renamed"] += mapping_renamed_count
                    #
                    if not dry_run:
                        mapping.selected_tools = new_mapping_selected
                        flag_modified(mapping, "selected_tools")
                        any_changed = True
        #
        if any_changed and not dry_run:
            session.commit()
    #
    # Step 3: Migrate pipeline YAML instructions
    #
    pipeline_result = migrate_project_pipeline_instructions(project_id, operations, dry_run)
    summary["pipeline_versions_scanned"] = pipeline_result["pipeline_versions_scanned"]
    summary["pipeline_versions_affected"] = pipeline_result["pipeline_versions_affected"]
    #
    return summary


def run_selected_tools_migration(param):
    """Top-level orchestrator for the selected_tools migration admin task.

    Parses args, iterates projects, applies operations, returns aggregated summary.
    """
    parsed = parse_migration_args(param)
    toolkit_type = parsed["toolkit_type"]
    operations = parsed["operations"]
    project_id = parsed["project_id"]
    dry_run = parsed["dry_run"]
    #
    prefix = "[DRY RUN] " if dry_run else ""
    #
    ops_desc = ", ".join(
        f"rename {op['old_name']}->{op['new_name']}" if op["action"] == "rename"
        else f"remove {op['tool_name']}"
        for op in operations
    )
    log.info(
        "Parsed: toolkit_type=%s, operations=[%s], project_id=%s, dry_run=%s",
        toolkit_type, ops_desc, project_id, dry_run
    )
    #
    # Get project list
    #
    if project_id == "all":
        project_list = context.rpc_manager.timeout(120).project_list(
            filter_={"create_success": True},
        )
    else:
        project_list = [{"id": project_id}]
    #
    log.info("%sProjects to process: %s", prefix, len(project_list))
    #
    total_summary = {
        "projects_scanned": 0,
        "toolkits_affected": 0,
        "entity_mappings_affected": 0,
        "tools_removed": 0,
        "tools_renamed": 0,
        "tool_mappings_removed": 0,
        "tool_mappings_renamed": 0,
        "pipeline_versions_scanned": 0,
        "pipeline_versions_affected": 0,
        "errors": 0,
    }
    #
    for project in project_list:
        pid = int(project["id"])
        total_summary["projects_scanned"] += 1
        log.info("%sProcessing project %s...", prefix, pid)
        #
        try:
            result = migrate_project_toolkits(pid, toolkit_type, operations, dry_run)
            #
            if result["toolkits_found"] == 0 and result["pipeline_versions_scanned"] == 0:
                log.info("%sProject %s: no %s toolkits found, no pipelines, skipping", prefix, pid, toolkit_type)
                continue
            #
            log.info(
                "%sProject %s summary: %s toolkits found, %s affected, %s entity_mappings affected, "
                "%s pipeline versions scanned, %s affected",
                prefix, pid, result["toolkits_found"],
                result["toolkits_affected"], result["entity_mappings_affected"],
                result["pipeline_versions_scanned"], result["pipeline_versions_affected"]
            )
            total_summary["toolkits_affected"] += result["toolkits_affected"]
            total_summary["entity_mappings_affected"] += result["entity_mappings_affected"]
            total_summary["tools_removed"] += result["tools_removed"]
            total_summary["tools_renamed"] += result["tools_renamed"]
            total_summary["tool_mappings_removed"] += result["tool_mappings_removed"]
            total_summary["tool_mappings_renamed"] += result["tool_mappings_renamed"]
            total_summary["pipeline_versions_scanned"] += result["pipeline_versions_scanned"]
            total_summary["pipeline_versions_affected"] += result["pipeline_versions_affected"]
        except Exception:  # pylint: disable=W0703
            log.exception("%sError processing project %s, continuing", prefix, pid)
            total_summary["errors"] += 1
    #
    log.info("%s=== MIGRATION SUMMARY ===", prefix)
    log.info(
        "%sProjects scanned: %s, Toolkits affected: %s, Entity mappings affected: %s",
        prefix, total_summary["projects_scanned"], total_summary["toolkits_affected"],
        total_summary["entity_mappings_affected"]
    )
    log.info(
        "%sTools removed: %s, Tools renamed: %s",
        prefix, total_summary["tools_removed"], total_summary["tools_renamed"]
    )
    log.info(
        "%sTool mappings removed: %s, Tool mappings renamed: %s",
        prefix, total_summary["tool_mappings_removed"], total_summary["tool_mappings_renamed"],
    )
    log.info(
        "%sPipeline versions scanned: %s, Pipeline versions affected: %s",
        prefix, total_summary["pipeline_versions_scanned"], total_summary["pipeline_versions_affected"]
    )
    log.info("%sErrors: %s", prefix, total_summary["errors"])
    #
    return total_summary
