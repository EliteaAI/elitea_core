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

""" Admin tasks for managing agent categories (predefined tag names). """

import time

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import web  # pylint: disable=E0611,E0401

from tools import db  # pylint: disable=E0611,E0401

from ..models.all import ApplicationVersion, Tag
from ..models.enums.all import PublishStatus
from ..utils.category_utils import (
    get_active_categories,
    is_valid_category,
    resolve_category,
    set_version_category,
)
from ..utils.utils import get_public_project_id


def _parse_kv_params(param: str) -> dict:
    """Parse a ``key=value;key=value`` admin-task param string into a dict."""
    result = {}
    for part in (param or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, _, value = part.partition("=")
        result[key.strip()] = value.strip()
    return result


class Method:  # pylint: disable=E1101,R0903,W0201
    """Method Resource. ``self`` points to the current Module instance."""

    # pylint: disable=R,W0613
    @web.method()
    def reassign_agent_category(self, *args, **kwargs):
        """Admin task: assign/reassign the category of a published agent version.

        Param format: ``version_id=<N>;category=<name>``

        Replaces any existing category tag on the published version with the
        chosen category (existing non-category tags are preserved). The category
        must be one of the active categories. Operates in the public project.

        Example (category name contains a space — no quotes needed)::

            version_id=42;category=Business Analyst
        """
        log.info("Starting reassign_agent_category")
        start_ts = time.time()
        #
        try:
            params = _parse_kv_params(kwargs.get("param", ""))
            version_id = params.get("version_id")
            category = params.get("category")
            #
            if not version_id or not category:
                log.error(
                    "Missing required params. Expected 'version_id=<N>;category=<name>', got: %s",
                    repr(kwargs.get("param", "")),
                )
                return
            try:
                version_id = int(version_id)
            except (TypeError, ValueError):
                log.error("version_id must be an integer, got: %s", repr(version_id))
                return
            #
            if not is_valid_category(category):
                log.error(
                    "Category '%s' is not active. Active categories: %s",
                    category, get_active_categories(),
                )
                return
            #
            public_project_id = get_public_project_id()
            with db.get_session(public_project_id) as session:
                version = session.query(ApplicationVersion).get(version_id)
                if version is None:
                    log.error("Published version %s not found in public project", version_id)
                    return
                if version.status != PublishStatus.published:
                    log.warning(
                        "Version %s is not published (status=%s); reassigning anyway",
                        version_id, version.status,
                    )
                set_version_category(session, version, category)
                session.commit()
            #
            log.info(
                "Reassigned version %s to category '%s'",
                version_id, resolve_category(category),
            )
        except:  # pylint: disable=W0702
            log.exception("Got exception during reassign_agent_category")
        #
        end_ts = time.time()
        log.info("Exiting reassign_agent_category (duration = %s)", end_ts - start_ts)

    # pylint: disable=R,W0613
    @web.method()
    def rename_agent_category(self, *args, **kwargs):
        """Admin task: rename an agent category across all published versions.

        Param format: ``old=<old name>;new=<new name>``

        Renames the underlying category tag so every published version tagged
        with the old name is retagged with the new name. If a tag with the new
        name already exists, associations are merged into it. The new name must
        be one of the active categories (add it via guardrails first). This task
        only renames; removing a category is done via the guardrails config.

        Example (names may contain spaces — no quotes needed)::

            old=Business Analyst;new=BA & Strategy
        """
        log.info("Starting rename_agent_category")
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
            if not is_valid_category(new_name):
                log.error(
                    "Target category '%s' is not active. Add it via guardrails first. "
                    "Active categories: %s",
                    new_name, get_active_categories(),
                )
                return
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
                    # Simple rename — no conflicting target tag.
                    old_tag.name = new_name
                    session.add(old_tag)
                    log.info("Renamed tag '%s' -> '%s'", old_name, new_name)
                else:
                    # Merge: move version associations from old_tag to new_tag.
                    versions = (
                        session.query(ApplicationVersion)
                        .filter(ApplicationVersion.tags.any(Tag.id == old_tag.id))
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
                    session.delete(old_tag)
                    log.info(
                        "Merged tag '%s' into '%s' across %d version(s)",
                        old_name, new_name, moved,
                    )
                session.commit()
        except:  # pylint: disable=W0702
            log.exception("Got exception during rename_agent_category")
        #
        end_ts = time.time()
        log.info("Exiting rename_agent_category (duration = %s)", end_ts - start_ts)
