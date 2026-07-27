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

"""Utilities for the migrate_llm_model admin task.

Migrates stale LLM model_name references in ApplicationVersion.llm_settings
and ParticipantMapping.entity_settings->llm_settings from a deprecated model
to a new shared model. LLM section only.
"""

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import context, rpc_tools  # pylint: disable=E0401

from ..models.all import ApplicationVersion
from ..models.participants import ParticipantMapping
from ..models.pd.llm import decide_family_heal
from .utils import get_public_project_id

# Defaults matching UI behavior (llmSettings.constants.js / application_utils.py)
DEFAULT_TEMPERATURE = 0.7
DEFAULT_REASONING_EFFORT = 'medium'
DEFAULT_MAX_TOKENS = -1  # auto mode, same as UI's DEFAULT_MAX_TOKENS


def _parse_bool(raw: str, param_name: str) -> bool:
    """Parse a "true"/"false"-ish flag, raising ``ValueError`` (naming ``param_name``) otherwise."""
    value = raw.strip().lower()
    if value in ('true', '1', 'yes'):
        return True
    if value in ('false', '0', 'no'):
        return False
    raise ValueError(f"Invalid {param_name} value: {value!r} (expected true/false)")


def parse_project_id_spec(raw: str, param_name: str = 'project_id') -> tuple:
    """Parse a "<all|N|lo-hi>" project-id spec into ('all', None) / ('range', (lo, hi)) / ('single', int).

    Shared by parse_migration_params and trace_step_backfill_utils.parse_backfill_params.
    Raises ``ValueError`` (using ``param_name`` in the message) on bad input.
    """
    raw = raw.strip()
    if raw == 'all':
        return ('all', None)
    if '-' in raw:
        parts = raw.split('-', 1)
        try:
            lo, hi = int(parts[0]), int(parts[1])
        except ValueError:
            raise ValueError(f"Invalid {param_name} range: {raw!r}")  # pylint: disable=W0707
        if lo > hi:
            raise ValueError(f"Invalid {param_name} range: {lo} > {hi}")
        return ('range', (lo, hi))
    try:
        return ('single', int(raw))
    except ValueError:
        raise ValueError(f"Invalid {param_name}: {raw!r}")  # pylint: disable=W0707


def parse_migration_params(raw_param: str) -> dict:
    """Parse semicolon-separated key=value migration params into a validated dict.

    Required: ``from``, ``to``.
    Optional: ``project_id`` (int | "X-Y" | "all", default "all"),
              ``dry_run`` ("true"/"false", default "true").
    Raises ``ValueError`` on bad input.
    """
    tokens = [t.strip() for t in raw_param.split(';') if t.strip()]
    params = {}
    for token in tokens:
        if '=' not in token:
            raise ValueError(f"Invalid param token (expected key=value): {token!r}")
        key, _, value = token.partition('=')
        params[key.strip()] = value.strip()

    # --- required ----------------------------------------------------------
    from_model = params.get('from')
    if not from_model:
        raise ValueError("Missing required param: from=<deprecated_model_name>")
    to_model = params.get('to')
    if not to_model:
        raise ValueError("Missing required param: to=<new_model_name>")
    if from_model == to_model:
        raise ValueError(f"from and to must be different (both are {from_model!r})")

    # --- project_id --------------------------------------------------------
    project_id_spec = parse_project_id_spec(params.get('project_id', 'all'), 'project_id')

    # --- dry_run -----------------------------------------------------------
    dry_run = _parse_bool(params.get('dry_run', 'false'), 'dry_run')

    return {
        'from_model': from_model,
        'to_model': to_model,
        'project_id_spec': project_id_spec,
        'dry_run': dry_run,
    }


def resolve_target_project_ids(project_id_spec: tuple) -> list:
    """Expand project_id spec into a sorted list of existing project ids."""
    kind, value = project_id_spec
    all_projects = context.rpc_manager.timeout(120).project_list()
    all_ids = sorted(int(p['id']) for p in all_projects)
    log.info("Total projects in platform: %s", len(all_ids))

    if kind == 'all':
        return all_ids

    if kind == 'single':
        if value not in all_ids:
            raise ValueError(f"Project {value} not found")
        return [value]

    # kind == 'range'
    lo, hi = value
    filtered = [pid for pid in all_ids if lo <= pid <= hi]
    if not filtered:
        raise ValueError(f"No projects found in range {lo}-{hi}")
    log.info("Projects in range %s-%s: %s", lo, hi, len(filtered))
    return filtered


def validate_target_model(to_model_name: str) -> dict:
    """Pre-flight: verify target model exists as a shared LLM in the public project.

    Returns the model config dict (contains ``supports_reasoning``, etc.).
    Raises ``ValueError`` if not found or not shared.
    """
    public_project_id = get_public_project_id()
    available = rpc_tools.RpcMixin().rpc.timeout(10).configurations_get_available_models(
        project_id=public_project_id, section='llm', include_shared=True
    )
    model_config = available.get((public_project_id, to_model_name))
    if not model_config:
        available_names = sorted({name for (_, name) in available})
        raise ValueError(
            f"Target model {to_model_name!r} not found as shared LLM "
            f"in public project (id={public_project_id}). "
            f"Available shared LLM models: {available_names}"
        )
    if not model_config.get('shared', False):
        raise ValueError(
            f"Target model {to_model_name!r} is not shared. "
            f"Migration target must be a shared model."
        )
    log.info(
        "Target model validated: %s (shared=True, supports_reasoning=%s)",
        to_model_name, model_config.get('supports_reasoning', False),
    )
    return model_config


def lookup_source_model_capabilities(from_model_name: str) -> bool | None:
    """Look up whether the source model supports reasoning.

    Returns ``True``/``False`` if found, ``None`` if deleted/unavailable.
    """
    public_project_id = get_public_project_id()
    try:
        available = rpc_tools.RpcMixin().rpc.timeout(10).configurations_get_available_models(
            project_id=public_project_id, section='llm', include_shared=True
        )
    except Exception:  # pylint: disable=W0703
        return None
    for (_, name), config in available.items():
        if name == from_model_name:
            return bool(config.get('supports_reasoning', False))
    return None


def build_new_llm_settings(
    existing: dict | None,
    new_model_name: str,
    new_model_project_id: int,
    target_supports_reasoning: bool,
    source_supports_reasoning: bool | None,
) -> dict:
    """Build updated llm_settings for a model migration.

    Same-family (both reasoning or both non-reasoning): swaps only
    ``model_name`` and ``model_project_id``, preserving user-tuned params.
    Cross-family: resets type-specific params to defaults.

    When ``source_supports_reasoning`` is ``None`` (source model deleted),
    the family is inferred from the existing llm_settings: presence of a
    non-None ``reasoning_effort`` indicates a reasoning model.
    """
    base = dict(existing) if existing else {}
    base['model_name'] = new_model_name
    base['model_project_id'] = new_model_project_id

    # If source config is gone, infer family from the stored llm_settings
    effective_source_reasoning = source_supports_reasoning
    if effective_source_reasoning is None and existing:
        effective_source_reasoning = existing.get('reasoning_effort') is not None

    same_family = (
        effective_source_reasoning is not None
        and effective_source_reasoning == target_supports_reasoning
    )
    if same_family:
        return base

    # Cross-family: reset to target family defaults
    if target_supports_reasoning:
        base['temperature'] = None
        base['reasoning_effort'] = DEFAULT_REASONING_EFFORT
    else:
        base['reasoning_effort'] = None
        base['temperature'] = DEFAULT_TEMPERATURE
    base['max_tokens'] = DEFAULT_MAX_TOKENS

    return base


def migrate_application_versions(session, from_model, settings_factory, dry_run) -> int:
    """Migrate ApplicationVersion rows whose llm_settings reference ``from_model``.

    Returns the count of matched rows.
    """
    rows = session.query(ApplicationVersion).filter(
        ApplicationVersion.llm_settings.op("->>")("model_name") == from_model
    ).all()

    if not rows:
        return 0

    for i, version in enumerate(rows):
        new_settings = settings_factory(version.llm_settings)
        if dry_run and i < 5:
            log.info(
                "  [dry_run] ApplicationVersion id=%s: %r -> %r",
                version.id, version.llm_settings, new_settings,
            )
        if not dry_run:
            version.llm_settings = new_settings

    return len(rows)


def migrate_participant_mappings(session, from_model, settings_factory, dry_run) -> int:
    """Migrate ParticipantMapping rows whose entity_settings.llm_settings reference ``from_model``.

    Returns the count of matched rows.
    """
    rows = session.query(ParticipantMapping).filter(
        ParticipantMapping.entity_settings.op("->")("llm_settings").op("->>")("model_name") == from_model
    ).all()

    if not rows:
        return 0

    for i, mapping in enumerate(rows):
        entity_settings = dict(mapping.entity_settings) if mapping.entity_settings else {}
        old_llm = entity_settings.get('llm_settings')
        if not old_llm:
            continue
        new_llm = settings_factory(old_llm)
        if dry_run and i < 5:
            log.info(
                "  [dry_run] ParticipantMapping id=%s: %r -> %r",
                mapping.id, old_llm, new_llm,
            )
        if not dry_run:
            entity_settings['llm_settings'] = new_llm
            mapping.entity_settings = entity_settings

    return len(rows)


def parse_heal_params(raw_param: str) -> dict:
    """Parse semicolon-separated key=value params for heal_llm_settings_family_conflicts.

    Optional: ``project_id`` (int | "X-Y" | "all", default "all"),
              ``dry_run`` ("true"/"false", default "true" -- conservative default since this
              task scans ALL projects with no from/to model boundary).
    Raises ``ValueError`` on bad input.
    """
    tokens = [t.strip() for t in raw_param.split(';') if t.strip()]
    params = {}
    for token in tokens:
        if '=' not in token:
            raise ValueError(f"Invalid param token (expected key=value): {token!r}")
        key, _, value = token.partition('=')
        params[key.strip()] = value.strip()

    project_id_spec = parse_project_id_spec(params.get('project_id', 'all'), 'project_id')
    dry_run = _parse_bool(params.get('dry_run', 'true'), 'dry_run')

    return {'project_id_spec': project_id_spec, 'dry_run': dry_run}


def _candidate_filter(col):
    """SQL pre-filter of rows to hand to decide_family_heal, given the llm_settings JSON column.

    Any row carrying a model_name is a candidate: the reasoning family (hence whether a null
    reasoning_effort is a defect) is only known after the per-project RPC capability lookup, so
    null-effort rows can't be narrowed in SQL. decide_family_heal makes the per-row decision.
    """
    return col.op("->>")("model_name").isnot(None)


def heal_family_conflict_versions(session, project_id: int, dry_run: bool) -> int:
    """Normalize family-misaligned ApplicationVersion.llm_settings rows against each row's own
    model's real supports_reasoning (issues #5821 + #5858). Does NOT change
    model_name/model_project_id — only the temperature/reasoning_effort pair is aligned.

    Returns the count of rows actually healed (idempotent: aligned rows are skipped).
    """
    rows = session.query(ApplicationVersion).filter(
        _candidate_filter(ApplicationVersion.llm_settings)
    ).all()

    if not rows:
        return 0

    available = rpc_tools.RpcMixin().rpc.timeout(10).configurations_get_available_models(
        project_id=project_id, section='llm', include_shared=True
    )

    healed = 0
    for version in rows:
        llm_settings = version.llm_settings or {}
        model_name = llm_settings.get('model_name')
        model_project_id = llm_settings.get('model_project_id')
        config = available.get((model_project_id, model_name), {}) if model_name else {}
        supports_reasoning = bool(config.get('supports_reasoning', False))
        new_settings = decide_family_heal(llm_settings, supports_reasoning)
        if new_settings is None:
            continue
        if dry_run and healed < 5:
            log.info(
                "  [dry_run] ApplicationVersion id=%s (supports_reasoning=%s): %r -> %r",
                version.id, supports_reasoning, llm_settings, new_settings,
            )
        if not dry_run:
            version.llm_settings = new_settings
        healed += 1

    return healed


def heal_family_conflict_mappings(session, project_id: int, dry_run: bool) -> int:
    """Normalize family-misaligned ParticipantMapping.entity_settings->llm_settings rows
    (issues #5821 + #5858). Same non-destructive, idempotent semantics as
    heal_family_conflict_versions — only the temperature/reasoning_effort pair is aligned.

    Returns the count of rows actually healed.
    """
    rows = session.query(ParticipantMapping).filter(
        _candidate_filter(ParticipantMapping.entity_settings.op("->")("llm_settings"))
    ).all()

    if not rows:
        return 0

    available = rpc_tools.RpcMixin().rpc.timeout(10).configurations_get_available_models(
        project_id=project_id, section='llm', include_shared=True
    )

    healed = 0
    for mapping in rows:
        entity_settings = dict(mapping.entity_settings) if mapping.entity_settings else {}
        old_llm = entity_settings.get('llm_settings')
        if not old_llm:
            continue
        model_name = old_llm.get('model_name')
        model_project_id = old_llm.get('model_project_id')
        config = available.get((model_project_id, model_name), {}) if model_name else {}
        supports_reasoning = bool(config.get('supports_reasoning', False))
        new_llm = decide_family_heal(old_llm, supports_reasoning)
        if new_llm is None:
            continue
        if dry_run and healed < 5:
            log.info(
                "  [dry_run] ParticipantMapping id=%s (supports_reasoning=%s): %r -> %r",
                mapping.id, supports_reasoning, old_llm, new_llm,
            )
        if not dry_run:
            entity_settings['llm_settings'] = new_llm
            mapping.entity_settings = entity_settings
        healed += 1

    return healed
