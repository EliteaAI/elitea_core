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

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import context  # pylint: disable=E0401


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
