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

""" Predict-door budget pre-check

Advisory only: a project already over its limit fails here instead of burning a task node,
an arbiter hop and an indexer worker to be refused on its first LLM call. The inference-plane
gate in the usage plugin stays the authority.
"""

import yaml

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import context  # pylint: disable=E0401

# Pipeline node types that never reach an LLM. Anything else (llm, decision, agent, toolkit,
# subgraph, custom, ...) or an unknown type keeps the door check.
LLM_FREE_NODE_TYPES = frozenset({"code", "router", "state_modifier", "printer", "hitl"})


def dispatch_may_use_llm(call_kwargs: dict) -> bool:
    """False only for a pipeline whose every node is provably LLM-free; True when in doubt."""
    payload = call_kwargs.get("kwargs") or {}
    if (payload.get("next_input_suggestion") or {}).get("enabled"):
        return True
    #
    version_details = (payload.get("application") or {}).get("version_details") or {}
    if version_details:
        return schema_may_use_llm(version_details.get("agent_type"), version_details.get("instructions"))
    # REST predicts ship no version_details (the SDK refetches the version), so the call
    # site computes this from the stored version and stamps it into the task meta
    return (call_kwargs.get("meta") or {}).get("llm_free") is not True


def schema_may_use_llm(agent_type, instructions) -> bool:
    """False only for a pipeline schema whose every node is provably LLM-free."""
    if agent_type != "pipeline":
        return True
    #
    try:
        schema = yaml.safe_load(instructions or "")
    except Exception:  # pylint: disable=W0703
        return True
    nodes = schema.get("nodes") if isinstance(schema, dict) else None
    if not isinstance(nodes, list) or not nodes:
        return True
    #
    return any(
        not isinstance(node, dict)
        or node.get("type") not in LLM_FREE_NODE_TYPES
        or node.get("decision")  # a decision edge on any node is an LLM call
        for node in nodes
    )


def dispatch_uses_own_model(call_kwargs: dict, project_id) -> bool:
    """True only when the main model is explicitly the caller project's own (BYO, never budgeted).

    Unset model_project_id may still resolve to a public model, so it keeps the door.
    """
    if project_id is None:
        return False
    payload = call_kwargs.get("kwargs") or {}
    version_details = (payload.get("application") or {}).get("version_details") or {}
    llm_settings = dict(version_details.get("llm_settings") or {})
    if not version_details:
        # Plain LLM chat predicts carry the model on the LLM client kwargs instead
        llm_kwargs = (payload.get("llm") or {}).get("kwargs") or {}
        llm_settings = {
            "model_name": llm_kwargs.get("model"),
            "model_project_id": llm_kwargs.get("model_project_id"),
        }
    if not llm_settings.get("model_name"):
        return False
    if (llm_settings.get("selection") or {}).get("mode") == "auto":
        return False
    try:
        return int(llm_settings.get("model_project_id")) == int(project_id)
    except (TypeError, ValueError):
        return False


def dispatch_owner(call_kwargs: dict):
    """(project_id, user_id) of a start_task call — meta first, then the task payload."""
    meta = call_kwargs.get("meta") or {}
    payload = call_kwargs.get("kwargs") or {}
    #
    return (
        meta.get("project_id") or payload.get("project_id"),
        meta.get("user_id") or payload.get("user_id"),
    )


def closed_budget_scope(project_id, user_id=None):
    """Which budget is already full ('project'/'member'), or None. Fails open."""
    if project_id is None:
        return None
    #
    try:
        verdict = context.rpc_manager.timeout(5).usage_gate_check(
            project_id=project_id, user_id=user_id,
        ) or {}
        #
        return verdict.get("scope") or None if verdict.get("closed") else None
    except:  # pylint: disable=W0702
        log.debug("budget_door: check failed for project %s", project_id, exc_info=True)
        return None
