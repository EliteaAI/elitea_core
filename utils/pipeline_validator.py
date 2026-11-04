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

"""Structural validator for pipeline YAML.

Validates node types, required fields, transitions, and END reachability.
Used by all AI paths (generate, edit, configure-node, Builder) and as
warnings on manual save.

Rules (V-01 … V-10):
  V-01  top-level 'nodes' key is a non-empty list
  V-02  every node has a non-empty 'id' string
  V-03  every node has a 'type' in KNOWN_NODE_TYPES
  V-04  entrypoint identified via top-level entry_point key, or legacy entrypoint: true, or node[0]
  V-05  at least one node with transition: END is reachable from the entrypoint
  V-06  every transition target references an existing node id (END is a reserved sentinel, not a node)
  V-07  LLM nodes have llm_settings.model_name
  V-08  agent/toolkit/mcp nodes have a non-empty 'name'
  V-09  code nodes have a non-empty 'code' field
  V-10  router nodes have at least one condition
"""

from __future__ import annotations

from collections import deque
from typing import Literal, Optional

from pydantic import BaseModel
from yaml import YAMLError, safe_dump, safe_load


# ---------------------------------------------------------------------------
# Public models
# ---------------------------------------------------------------------------

class PipelineValidationError(BaseModel):
    code: str
    node_id: Optional[str] = None
    field: Optional[str] = None
    message: str
    severity: Literal["error", "warning"]


class PipelineValidationResult(BaseModel):
    valid: bool
    errors: list[PipelineValidationError] = []
    warnings: list[PipelineValidationError] = []


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KNOWN_NODE_TYPES: frozenset[str] = frozenset({
    "llm", "code", "agent", "toolkit", "mcp", "router", "decision",
    "state_modifier", "printer", "hitl", "custom",
})

# END is a reserved transition sentinel value, not a node type.
_END_SENTINEL = "END"

# Node types that require a non-empty `name` field (V-08)
_NAME_REQUIRED_TYPES: frozenset[str] = frozenset({"agent", "toolkit", "mcp"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _err(code: str, node_id: str | None, field: str | None, message: str) -> PipelineValidationError:
    return PipelineValidationError(
        code=code, node_id=node_id, field=field, message=message, severity="error"
    )


def _warn(code: str, node_id: str | None, field: str | None, message: str) -> PipelineValidationError:
    return PipelineValidationError(
        code=code, node_id=node_id, field=field, message=message, severity="warning"
    )


def _is_terminal_node(node: dict) -> bool:
    """Return True if this node terminates the pipeline via ``transition: END``.

    Also accepts the legacy ``type: end`` marker for backward compatibility.
    """
    t = node.get("transition")
    if isinstance(t, str) and t.strip().upper() == _END_SENTINEL:
        return True
    # Legacy: node type 'end' (lowercase or uppercase)
    ntype = node.get("type")
    if isinstance(ntype, str) and ntype.strip().lower() == "end":
        return True
    return False


def _collect_transition_targets(node: dict) -> list[str]:
    """Return all node-id strings referenced in a node's transition fields.

    The reserved sentinel ``END`` is excluded — it is not a node id.

    Handles both the current schema and the legacy schema:

    Current schema:
      - ``transition: <node_id>``          (scalar, simple nodes; ``END`` is excluded)
      - ``routes: [node_id, ...]``          (list, router nodes)
      - ``nodes: [node_id, ...]``           (list, decision nodes)
      - ``routes: {action: node_id, ...}``  (dict values, HITL nodes)
      - ``default_output: <node_id>``       (router/decision fallback)

    Legacy schema (still accepted):
      - ``transitions: [{target: "id"}, ...]``
      - ``output: <node_id>``
      - ``conditions: [{output: "id"}, ...]``
    """
    targets: list[str] = []

    def _add(val: str) -> None:
        v = val.strip()
        if v and v.upper() != _END_SENTINEL:
            targets.append(v)

    # Current schema — scalar transition (skip END sentinel)
    t = node.get("transition")
    if isinstance(t, str):
        _add(t)

    # Current schema — routes (list for router, dict for HITL)
    routes = node.get("routes")
    if isinstance(routes, list):
        for r in routes:
            if isinstance(r, str):
                _add(r)
    elif isinstance(routes, dict):
        for v in routes.values():
            if isinstance(v, str):
                _add(v)

    # Current schema — nodes list (decision)
    for nid in node.get("nodes", []) or []:
        if isinstance(nid, str):
            _add(nid)

    # Current schema — default_output (router/decision fallback)
    default_output = node.get("default_output")
    if isinstance(default_output, str):
        _add(default_output)

    # Legacy schema — transitions list
    for t in node.get("transitions", []) or []:
        if isinstance(t, dict) and isinstance(t.get("target"), str):
            _add(t["target"])

    # Legacy schema — output
    output = node.get("output")
    if isinstance(output, str):
        _add(output)

    # Legacy schema — conditions list
    for cond in node.get("conditions", []) or []:
        if isinstance(cond, dict) and isinstance(cond.get("output"), str):
            _add(cond["output"])

    return targets


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def fix_deprecated_end_node(yaml_str: str) -> str:
    """Rewrite a pipeline YAML that uses the deprecated ``type: END`` node pattern.

    The old pattern adds a standalone ``{ id: end_node, type: END }`` node and
    has other nodes transition to it by id.  The correct schema uses the reserved
    sentinel ``transition: END`` directly on the last real node — no separate end
    node in the list.

    This function:
    1. Finds all nodes whose ``type`` is the string ``END`` (case-insensitive).
    2. For every other node that transitions to one of those ids, rewrites the
       transition to ``END`` (the sentinel).
    3. Removes the ``type: END`` nodes from the ``nodes`` list.

    If parsing fails or no deprecated nodes are found the original string is
    returned unchanged.
    """
    try:
        data = safe_load(yaml_str)
    except YAMLError:
        return yaml_str

    if not isinstance(data, dict):
        return yaml_str

    nodes_raw = data.get("nodes")
    if not isinstance(nodes_raw, list):
        return yaml_str

    end_ids: set[str] = {
        n["id"].strip()
        for n in nodes_raw
        if isinstance(n, dict)
        and isinstance(n.get("id"), str)
        and isinstance(n.get("type"), str)
        and n["type"].strip().lower() == "end"
    }
    if not end_ids:
        return yaml_str

    def _rewrite_target(val: str) -> str:
        return _END_SENTINEL if val.strip() in end_ids else val

    keep_nodes = []
    for node in nodes_raw:
        if not isinstance(node, dict):
            keep_nodes.append(node)
            continue
        ntype = node.get("type")
        if isinstance(ntype, str) and ntype.strip().lower() == "end":
            continue  # drop the end node

        node = dict(node)

        # scalar transition
        t = node.get("transition")
        if isinstance(t, str) and t.strip() in end_ids:
            node["transition"] = _END_SENTINEL

        # routes list (router)
        routes = node.get("routes")
        if isinstance(routes, list):
            node["routes"] = [_rewrite_target(r) if isinstance(r, str) else r for r in routes]
        elif isinstance(routes, dict):
            node["routes"] = {k: (_rewrite_target(v) if isinstance(v, str) else v) for k, v in routes.items()}

        # nodes list (decision)
        if isinstance(node.get("nodes"), list):
            node["nodes"] = [_rewrite_target(r) if isinstance(r, str) else r for r in node["nodes"]]

        # default_output
        do = node.get("default_output")
        if isinstance(do, str) and do.strip() in end_ids:
            node["default_output"] = _END_SENTINEL

        # legacy transitions list
        if isinstance(node.get("transitions"), list):
            node["transitions"] = [
                {**t, "target": _rewrite_target(t["target"])} if isinstance(t, dict) and isinstance(t.get("target"), str) else t
                for t in node["transitions"]
            ]

        # legacy output
        out = node.get("output")
        if isinstance(out, str) and out.strip() in end_ids:
            node["output"] = _END_SENTINEL

        # legacy conditions
        if isinstance(node.get("conditions"), list):
            node["conditions"] = [
                {**c, "output": _rewrite_target(c["output"])} if isinstance(c, dict) and isinstance(c.get("output"), str) else c
                for c in node["conditions"]
            ]

        keep_nodes.append(node)

    data["nodes"] = keep_nodes
    return safe_dump(data, allow_unicode=True, sort_keys=False)


def validate_pipeline_yaml(
    yaml_str: str,
    *,
    project_id: int | None = None,
    tool_names: list[str] | None = None,
) -> PipelineValidationResult:
    """Validate the structure of a pipeline YAML string.

    Returns a :class:`PipelineValidationResult` whose ``valid`` flag is
    ``True`` only when there are zero error-severity findings.

    ``project_id`` and ``tool_names`` are reserved for future cross-project
    or tool-reference checks; they are accepted but not yet used.
    """
    errors: list[PipelineValidationError] = []
    warnings: list[PipelineValidationError] = []

    # --- Parse YAML --------------------------------------------------------
    try:
        data = safe_load(yaml_str)
    except YAMLError as exc:
        errors.append(_err("yaml_parse_error", None, None, f"YAML parse error: {exc}"))
        return PipelineValidationResult(valid=False, errors=errors, warnings=warnings)

    if not isinstance(data, dict):
        errors.append(_err(
            "not_a_dict", None, None,
            "Pipeline YAML must be a YAML mapping (dict), not a list or scalar."
        ))
        return PipelineValidationResult(valid=False, errors=errors, warnings=warnings)

    # --- V-01: nodes key ---------------------------------------------------
    nodes_raw = data.get("nodes")
    if nodes_raw is not None and not isinstance(nodes_raw, list):
        # nodes key is present but is not a list — genuinely malformed
        errors.append(_err(
            "missing_nodes", None, "nodes",
            "The 'nodes' key must be a list. Found a non-list value."
        ))
        return PipelineValidationResult(valid=False, errors=errors, warnings=warnings)
    if not nodes_raw:
        # nodes key absent or empty list — incomplete/draft pipeline, warn only
        warnings.append(_warn(
            "missing_nodes", None, "nodes",
            "Pipeline has no nodes yet. Add at least one node before running."
        ))
        nodes_raw = []

    # --- V-02: every node has a non-empty id --------------------------------
    node_ids: list[str] = []
    id_seen: set[str] = set()
    for i, node in enumerate(nodes_raw):
        if not isinstance(node, dict):
            errors.append(_err(
                "node_not_a_dict", None, f"nodes[{i}]",
                f"Node at index {i} is not a mapping. Each node must be a YAML mapping."
            ))
            continue
        nid = node.get("id")
        if not nid or not isinstance(nid, str) or not nid.strip():
            errors.append(_err(
                "missing_node_id", None, f"nodes[{i}].id",
                f"Node at index {i} has no 'id'. Add a unique string id to each node."
            ))
        else:
            nid = nid.strip()
            if nid in id_seen:
                errors.append(_err(
                    "duplicate_node_id", nid, "id",
                    f"Node id '{nid}' is used more than once. Node ids must be unique."
                ))
            else:
                id_seen.add(nid)
                node_ids.append(nid)

    # Collect valid nodes for further checks
    valid_nodes = [
        n for n in nodes_raw
        if isinstance(n, dict) and isinstance(n.get("id"), str) and n.get("id", "").strip()
    ]
    node_id_set = {n["id"].strip() for n in valid_nodes}

    # --- V-03: known node types --------------------------------------------
    for node in valid_nodes:
        ntype = node.get("type")
        nid = node["id"].strip()
        if not ntype or not isinstance(ntype, str):
            errors.append(_err(
                "missing_node_type", nid, "type",
                f"Node '{nid}' has no 'type'. Set type to one of: "
                f"{', '.join(sorted(KNOWN_NODE_TYPES))}."
            ))
        elif ntype.lower() == "end":
            # Legacy end-node pattern: accepted with a deprecation warning.
            # Correct schema: set `transition: END` on the last real node instead.
            warnings.append(_warn(
                "deprecated_end_node_type", nid, "type",
                f"Node '{nid}' uses the deprecated `type: END` pattern. "
                f"Remove this node and set `transition: END` on the node that transitions to it."
            ))
        elif ntype.lower() not in KNOWN_NODE_TYPES:
            warnings.append(_warn(
                "unknown_node_type", nid, "type",
                f"Node '{nid}' has unknown type '{ntype}'. "
                f"Valid types: {', '.join(sorted(KNOWN_NODE_TYPES))}."
            ))

    # --- V-04: exactly one entrypoint --------------------------------------
    # Current schema: top-level `entry_point: <node_id>` key.
    # Legacy schema: a node with `entrypoint: true`.
    entrypoint_id: str | None = None
    top_level_entry = data.get("entry_point")
    if isinstance(top_level_entry, str) and top_level_entry.strip():
        entrypoint_id = top_level_entry.strip()
        if node_id_set and entrypoint_id not in node_id_set:
            errors.append(_err(
                "unknown_entrypoint", None, "entry_point",
                f"entry_point '{entrypoint_id}' does not match any node id. "
                f"Set entry_point to the id of the first node to execute."
            ))
            entrypoint_id = None
    else:
        # Fall back to legacy `entrypoint: true` on a node
        entrypoint_nodes = [n for n in valid_nodes if n.get("entrypoint") is True]
        if len(entrypoint_nodes) == 0:
            if valid_nodes:
                entrypoint_id = valid_nodes[0]["id"].strip()
                warnings.append(_warn(
                    "implicit_entrypoint", entrypoint_id, "entry_point",
                    f"No 'entry_point' key found at the top level and no node has "
                    f"'entrypoint: true'. Treating node '{entrypoint_id}' (index 0) as the "
                    f"entrypoint. Add 'entry_point: {entrypoint_id}' at the top level."
                ))
        elif len(entrypoint_nodes) > 1:
            ids = [n["id"].strip() for n in entrypoint_nodes]
            errors.append(_err(
                "multiple_entrypoints", None, "entrypoint",
                f"Multiple nodes have 'entrypoint: true': {ids}. "
                f"Exactly one node must be the entrypoint."
            ))
            entrypoint_id = ids[0]
        else:
            entrypoint_id = entrypoint_nodes[0]["id"].strip()

    # --- V-06: transition targets reference existing nodes -----------------
    # END is a reserved sentinel — it is NOT a node id and must never appear in node_id_set.
    adjacency: dict[str, list[str]] = {n["id"].strip(): [] for n in valid_nodes}
    for node in valid_nodes:
        nid = node["id"].strip()
        for target in _collect_transition_targets(node):
            if target not in node_id_set:
                errors.append(_err(
                    "unknown_transition_target", nid, "transitions",
                    f"Node '{nid}' has a transition to '{target}' which does not exist. "
                    f"Check the node id spelling."
                ))
            else:
                adjacency[nid].append(target)

    # --- V-05: at least one terminal node (transition: END) is reachable from entrypoint ------
    # Current schema: a node is terminal when it has `transition: END`.
    # Legacy schema: a node with `type: end` is also treated as terminal.
    terminal_node_ids = {
        n["id"].strip() for n in valid_nodes if _is_terminal_node(n)
    }
    if valid_nodes and not terminal_node_ids:
        warnings.append(_warn(
            "no_end_node", None, "nodes",
            "The pipeline has no terminal node yet. "
            "Set `transition: END` on the last node of each execution path before running."
        ))
    elif valid_nodes and entrypoint_id is not None:
        # BFS from entrypoint through the adjacency graph (END excluded from graph nodes)
        visited: set[str] = set()
        queue: deque[str] = deque([entrypoint_id])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)
        reachable_terminals = terminal_node_ids & visited
        if not reachable_terminals:
            errors.append(_err(
                "end_node_unreachable", None, "transitions",
                "No terminal node (transition: END) is reachable from the entrypoint. "
                "Wire the pipeline so that all paths eventually reach a node with "
                "`transition: END`."
            ))

    # --- V-07: LLM nodes have llm_settings.model_name ----------------------
    for node in valid_nodes:
        ntype = node.get("type")
        if not isinstance(ntype, str) or ntype.lower() != "llm":
            continue
        nid = node["id"].strip()
        llm_settings = node.get("llm_settings") or {}
        if not isinstance(llm_settings, dict) or not llm_settings.get("model_name"):
            warnings.append(_warn(
                "llm_no_model", nid, "llm_settings.model_name",
                f"LLM node '{nid}' has no model_name in llm_settings. "
                f"The pipeline's LLM settings will be used as a fallback."
            ))

    # --- V-08: agent/toolkit/mcp nodes have a non-empty 'name' -------------
    for node in valid_nodes:
        ntype = node.get("type")
        if not isinstance(ntype, str):
            continue
        if ntype.lower() not in _NAME_REQUIRED_TYPES:
            continue
        nid = node["id"].strip()
        name = node.get("name") or node.get("tool_name")
        if not name or not isinstance(name, str) or not name.strip():
            errors.append(_err(
                "missing_tool_name", nid, "name",
                f"{ntype.capitalize()} node '{nid}' requires a 'name' (or 'tool_name') field "
                f"identifying the tool to call."
            ))

    # --- V-09: code nodes have a non-empty 'code' field --------------------
    for node in valid_nodes:
        ntype = node.get("type")
        if not isinstance(ntype, str) or ntype.lower() != "code":
            continue
        nid = node["id"].strip()
        code_field = node.get("code")
        if code_field is None:
            errors.append(_err(
                "missing_code_field", nid, "code",
                f"Code node '{nid}' requires a 'code' field containing the Python code to execute."
            ))
        elif isinstance(code_field, dict):
            val = code_field.get("value")
            if not val or (isinstance(val, str) and not val.strip()):
                errors.append(_err(
                    "empty_code_value", nid, "code.value",
                    f"Code node '{nid}' has an empty code value. Provide the Python code to run."
                ))
        elif isinstance(code_field, str) and not code_field.strip():
            errors.append(_err(
                "empty_code_field", nid, "code",
                f"Code node '{nid}' has an empty 'code' field. Provide the Python code to run."
            ))

    # --- V-10: router nodes have at least one condition --------------------
    for node in valid_nodes:
        ntype = node.get("type")
        if not isinstance(ntype, str) or ntype.lower() != "router":
            continue
        nid = node["id"].strip()
        conditions = node.get("conditions") or node.get("routes") or []
        if not conditions:
            warnings.append(_warn(
                "router_no_conditions", nid, "conditions",
                f"Router node '{nid}' has no conditions. "
                f"Add at least one condition with an output target."
            ))

    valid = len(errors) == 0
    return PipelineValidationResult(valid=valid, errors=errors, warnings=warnings)
