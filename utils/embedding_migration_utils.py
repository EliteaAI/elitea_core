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

"""Utilities for the migrate_embedding_model admin task.

Migrates stale embedding model references stored as plain strings in
EliteATool.settings (toolkit configuration). Embedding section only.

Unlike migrate_llm_model, embedding model names live solely in toolkit
settings (not in ApplicationVersion.llm_settings / ParticipantMapping),
they are plain strings (not config objects), and no mapping/family logic
is required -- the migration is a straight string swap.
"""

from sqlalchemy import or_

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import rpc_tools  # pylint: disable=E0401

from ..models.all import EliteATool
from .utils import get_public_project_id

# Toolkit settings keys that hold an embedding model name (plain string).
# ``embedding_model`` is the common field; ``toolkit_configuration_embedding_model``
# is the provider-hub prefixed variant. The credential-reference object field
# ``embedding_configuration`` is intentionally NOT included here.
EMBEDDING_MODEL_FIELDS = ('embedding_model', 'toolkit_configuration_embedding_model')


def validate_target_embedding_model(to_model_name: str) -> None:
    """Pre-flight: verify target model exists as a shared embedding in the public project.

    Raises ``ValueError`` if not found or not shared.
    """
    public_project_id = get_public_project_id()
    available = rpc_tools.RpcMixin().rpc.timeout(10).configurations_get_available_models(
        project_id=public_project_id, section='embedding', include_shared=True
    )
    model_config = available.get((public_project_id, to_model_name))
    if not model_config:
        available_names = sorted({name for (_, name) in available})
        raise ValueError(
            f"Target model {to_model_name!r} not found as shared embedding "
            f"in public project (id={public_project_id}). "
            f"Available shared embedding models: {available_names}"
        )
    if not model_config.get('shared', False):
        raise ValueError(
            f"Target model {to_model_name!r} is not shared. "
            f"Migration target must be a shared model."
        )
    log.info("Target embedding model validated: %s (shared=True)", to_model_name)


def migrate_toolkit_embedding_models(session, from_model, to_model, dry_run) -> int:
    """Migrate EliteATool rows whose settings reference ``from_model`` in any embedding field.

    Performs a plain string swap of ``from_model`` -> ``to_model`` across the
    fields in ``EMBEDDING_MODEL_FIELDS``. Returns the count of matched rows.
    """
    rows = session.query(EliteATool).filter(
        or_(*[
            EliteATool.settings.op("->>")(field) == from_model
            for field in EMBEDDING_MODEL_FIELDS
        ])
    ).all()

    if not rows:
        return 0

    for i, toolkit in enumerate(rows):
        settings = dict(toolkit.settings) if toolkit.settings else {}
        changed_fields = [
            field for field in EMBEDDING_MODEL_FIELDS
            if settings.get(field) == from_model
        ]
        if not changed_fields:
            continue
        for field in changed_fields:
            settings[field] = to_model
        if dry_run and i < 5:
            log.info(
                "  [dry_run] EliteATool id=%s (%s): %s %r -> %r",
                toolkit.id, toolkit.type, changed_fields, from_model, to_model,
            )
        if not dry_run:
            # JSONB columns require reassignment to be flagged dirty by SQLAlchemy.
            toolkit.settings = settings

    return len(rows)
