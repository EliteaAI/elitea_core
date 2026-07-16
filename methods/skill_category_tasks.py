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

""" Admin tasks for managing skill categories (predefined tag names). """

import time

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import web  # pylint: disable=E0611,E0401

from sqlalchemy.orm import selectinload

from tools import db  # pylint: disable=E0611,E0401

from .category_tasks import _parse_kv_params
from ..models.all import Tag
from ..models.skill import SkillVersion
from ..models.enums.all import PublishStatus
from ..utils.skill_category_utils import (
    get_active_skill_categories,
    resolve_skill_category,
    validate_skill_category,
)
from ..utils.utils import get_public_project_id


class Method:  # pylint: disable=E1101,R0903,W0201
    """Method Resource. ``self`` points to the current Module instance."""

    # pylint: disable=R,W0613
    @web.method()
    def rename_skill_category(self, *args, **kwargs):
        """Admin task: rename a skill category across all published skill versions.

        Param format: ``old=<old name>;new=<new name>``

        Retags every skill version (all statuses) tagged with the old category
        so it carries the new category instead. The new name must be one of the
        active categories (add it via guardrails first). This task only moves
        associations; it never renames or deletes the underlying tag row.

        Example (names may contain spaces — no quotes needed)::

            old=Documentation;new=Testing & QA
        """
        # NOTE: the ``tags`` table is SHARED with application_versions
        # (models/skill.py FKs into the same tags table), so this task must
        # NEVER rename or delete a Tag row — that would corrupt agent
        # categories. It only re-points per-version associations.
        log.info("Starting rename_skill_category")
        start_ts = time.time()
        #
        try:
            params = _parse_kv_params(kwargs.get("param", ""))
            old_name = params.get("old")
            new_name = params.get("new")
            #
            if not old_name or not new_name:
                log.error(
                    "Missing required params. Expected 'old=<name>;new=<name>', got: %s",
                    repr(kwargs.get("param", "")),
                )
                return
            if old_name == new_name:
                log.info("old and new category names are identical; nothing to do")
                return
            if not validate_skill_category(new_name):
                log.error(
                    "Target category '%s' is not active. Add it via guardrails first. "
                    "Active categories: %s",
                    new_name, get_active_skill_categories(),
                )
                return
            new_name = resolve_skill_category(new_name)
            #
            public_project_id = get_public_project_id()
            with db.get_session(public_project_id) as session:
                old_tag = session.query(Tag).filter(Tag.name == old_name).first()
                if old_tag is None:
                    log.info("No tag named '%s' found; nothing to rename", old_name)
                    return
                #
                new_tag = session.query(Tag).filter(Tag.name == new_name).first()
                if new_tag is None:
                    new_tag = Tag(name=new_name)
                    session.add(new_tag)
                #
                versions = (
                    session.query(SkillVersion)
                    .filter(SkillVersion.tags.any(Tag.id == old_tag.id))
                    .options(selectinload(SkillVersion.tags))
                    .all()
                )
                moved = 0
                for version in versions:
                    names = {t.name for t in version.tags}
                    version.tags = [t for t in version.tags if t.id != old_tag.id]
                    if new_tag.name not in names:
                        version.tags.append(new_tag)
                    session.add(version)
                    moved += 1
                session.commit()
            #
            log.info(
                "Renamed skill category '%s' -> '%s' across %d version(s)",
                old_name, new_name, moved,
            )
        except:  # pylint: disable=W0702
            log.exception("Got exception during rename_skill_category")
        #
        end_ts = time.time()
        log.info("Exiting rename_skill_category (duration = %s)", end_ts - start_ts)

    # pylint: disable=R,W0613
    @web.method()
    def reassign_skill_category(self, *args, **kwargs):
        """Admin task: reassign all published skills in one category to another.

        Param format: ``from=<source category>;to=<target category>``

        Moves every published skill version tagged with the source category so
        it carries the target category instead. The target must be one of the
        active categories. This task only moves associations; it never renames
        or deletes the underlying tag row.

        Example (names may contain spaces — no quotes needed)::

            from=Documentation;to=Testing & QA
        """
        # NOTE: the ``tags`` table is SHARED with application_versions
        # (models/skill.py FKs into the same tags table), so this task must
        # NEVER rename or delete a Tag row — that would corrupt agent
        # categories. It only re-points per-version associations.
        log.info("Starting reassign_skill_category")
        start_ts = time.time()
        #
        try:
            params = _parse_kv_params(kwargs.get("param", ""))
            from_name = params.get("from")
            to_name = params.get("to")
            #
            if not from_name or not to_name:
                log.error(
                    "Missing required params. Expected 'from=<name>;to=<name>', got: %s",
                    repr(kwargs.get("param", "")),
                )
                return
            if from_name == to_name:
                log.info("from and to category names are identical; nothing to do")
                return
            if not validate_skill_category(to_name):
                log.error(
                    "Target category '%s' is not active. Active categories: %s",
                    to_name, get_active_skill_categories(),
                )
                return
            to_name = resolve_skill_category(to_name)
            #
            public_project_id = get_public_project_id()
            with db.get_session(public_project_id) as session:
                from_tag = session.query(Tag).filter(Tag.name == from_name).first()
                if from_tag is None:
                    log.info("No tag named '%s' found; nothing to reassign", from_name)
                    return
                #
                to_tag = session.query(Tag).filter(Tag.name == to_name).first()
                if to_tag is None:
                    to_tag = Tag(name=to_name)
                    session.add(to_tag)
                #
                versions = (
                    session.query(SkillVersion)
                    .filter(SkillVersion.tags.any(Tag.id == from_tag.id))
                    .filter(SkillVersion.status == PublishStatus.published)
                    .options(selectinload(SkillVersion.tags))
                    .all()
                )
                moved = 0
                for version in versions:
                    names = {t.name for t in version.tags}
                    version.tags = [t for t in version.tags if t.id != from_tag.id]
                    if to_tag.name not in names:
                        version.tags.append(to_tag)
                    session.add(version)
                    moved += 1
                session.commit()
            #
            log.info(
                "Reassigned skill category '%s' -> '%s' across %d published version(s)",
                from_name, to_name, moved,
            )
        except:  # pylint: disable=W0702
            log.exception("Got exception during reassign_skill_category")
        #
        end_ts = time.time()
        log.info("Exiting reassign_skill_category (duration = %s)", end_ts - start_ts)
