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

"""Prompt assembly utilities for pipeline AI endpoints.

Each function receives a service-prompt template (fetched via
``get_service_prompt``), the pipeline authoring reference, and context
data, and returns a ready-to-use system prompt string.

Templates use ``str.format()`` placeholders; all values are injected
server-side so project-level overrides are always respected.
"""

from __future__ import annotations

import json


# ---------------------------------------------------------------------------
# Formatting helpers (shared with generate_application_utils patterns)
# ---------------------------------------------------------------------------

def _fmt_toolkits(items: list) -> str:
    if not items:
        return "(none)"
    return "\n".join(
        f'- id={t["id"]}  type="{t["type"]}"  name="{t["name"]}"  {(t.get("description") or "")[:120]}'
        for t in items
    )


def _fmt_mcp(items: list) -> str:
    if not items:
        return "(none)"
    return "\n".join(
        f'- id={t["id"]}  name="{t["name"]}"  {(t.get("description") or "")[:120]}'
        for t in items
    )


def _fmt_apps(items: list) -> str:
    if not items:
        return "(none)"
    return "\n".join(
        f'- application_id={a["application_id"]}  name="{a["name"]}"  {(a.get("description") or "")[:100]}'
        for a in items
    )


def _fmt_skills(items: list) -> str:
    if not items:
        return "(none)"
    return "\n".join(
        f'- id={s["id"]}  name="{s["name"]}"  {(s.get("description") or "")[:120]}'
        for s in items
    )


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def build_pipeline_system_prompt(
    template: str,
    reference: str,
    toolkits: list,
    mcp: list,
    agents: list,
    pipelines: list,
    skills: list,
) -> str:
    """Assemble the system prompt for Build Pipeline with AI (create mode).

    ``template`` is the ``generate_pipeline_draft`` service prompt (may
    contain ``{toolkits}``, ``{mcp}``, ``{agents}``, ``{pipelines}``,
    ``{skills}`` placeholders).  ``reference`` is prepended so the LLM
    always has the full schema context.
    """
    filled = template.format(
        toolkits=_fmt_toolkits(toolkits),
        mcp=_fmt_mcp(mcp),
        agents=_fmt_apps(agents),
        pipelines=_fmt_apps(pipelines),
        skills=_fmt_skills(skills),
    )
    return f"{reference}\n\n---\n\n{filled}"


def build_edit_pipeline_system_prompt(
    template: str,
    reference: str,
    current_config: dict,
    toolkits: list,
    mcp: list,
    agents: list,
    pipelines: list,
    skills: list,
) -> str:
    """Assemble the system prompt for Edit Pipeline with AI (edit mode).

    ``template`` is the ``edit_pipeline_draft`` service prompt (may
    contain ``{current_config}``, ``{toolkits}``, ``{mcp}``, ``{agents}``,
    ``{pipelines}``, ``{skills}`` placeholders).
    """
    filled = template.format(
        current_config=json.dumps(current_config, indent=2),
        toolkits=_fmt_toolkits(toolkits),
        mcp=_fmt_mcp(mcp),
        agents=_fmt_apps(agents),
        pipelines=_fmt_apps(pipelines),
        skills=_fmt_skills(skills),
    )
    return f"{reference}\n\n---\n\n{filled}"


def build_node_configure_system_prompt(
    template: str,
    reference: str,
    node_type: str,
    state_variables: list[str],
    available_nodes: list[str],
) -> str:
    """Assemble the system prompt for Configure Node with AI.

    ``template`` is the ``configure_node_with_ai`` service prompt (may
    contain ``{node_type}``, ``{state_variables}``, ``{available_nodes}``
    placeholders).
    """
    state_var_text = "\n".join(f"- {v}" for v in state_variables) if state_variables else "(none)"
    available_nodes_text = "\n".join(f"- {n}" for n in available_nodes) if available_nodes else "(none)"

    filled = template.format(
        node_type=node_type,
        state_variables=state_var_text,
        available_nodes=available_nodes_text,
    )
    return f"{reference}\n\n---\n\n{filled}"
