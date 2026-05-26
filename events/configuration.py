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

""" Event """

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from tools import db  # pylint: disable=E0401

from ..models.elitea_tools import EliteATool


def _replace_elitea_title_in_dict(obj, old_title: str, new_title: str):
    """Recursively replace credential elitea_title references in a settings dict.

    A credential reference is identified by being a dict that contains both
    'elitea_title' and 'private' keys. Only those dicts are updated.

    Returns (new_obj, changed: bool).
    """
    if not isinstance(obj, dict):
        return obj, False

    if obj.get('elitea_title') == old_title and 'private' in obj:
        return {**obj, 'elitea_title': new_title}, True

    changed = False
    new_obj = {}
    for k, v in obj.items():
        if isinstance(v, dict):
            new_v, sub_changed = _replace_elitea_title_in_dict(v, old_title, new_title)
            new_obj[k] = new_v
            changed = changed or sub_changed
        elif isinstance(v, list):
            new_list = []
            for item in v:
                if isinstance(item, dict):
                    new_item, item_changed = _replace_elitea_title_in_dict(item, old_title, new_title)
                    new_list.append(new_item)
                    changed = changed or item_changed
                else:
                    new_list.append(item)
            new_obj[k] = new_list
        else:
            new_obj[k] = v
    return new_obj, changed


class Event:  # pylint: disable=E1101,R0903,W0201
    """
        Event Resource

        self is pointing to current Module instance

        Note: web.event decorator must be the last decorator (at top)
    """

    @web.event("configuration_deleted")
    def on_configuration_deleted(self, _context, _event, configuration, *_args, **_kwargs):  # pylint: disable=R0914
        """ Event """
        log.info("Got configuration_deleted: %s", configuration)
        #
        self.delete_configuration_settings(configuration)

    @web.event("configuration_renamed")
    def on_configuration_renamed(self, _context, _event, payload, *_args, **_kwargs):
        """ Event """
        log.info("Got configuration_renamed: %s", payload)
        project_id = payload.get('project_id')
        old_title = payload.get('old_elitea_title')
        new_title = payload.get('new_elitea_title')

        if not project_id or not old_title or not new_title:
            log.warning("configuration_renamed event missing required fields: %s", payload)
            return

        updated_count = 0
        with db.get_session(project_id) as session:
            tools = session.query(EliteATool).all()
            for tool in tools:
                if not tool.settings:
                    continue
                new_settings, changed = _replace_elitea_title_in_dict(
                    tool.settings, old_title, new_title
                )
                if changed:
                    tool.settings = new_settings
                    updated_count += 1
            if updated_count:
                session.commit()
                log.info(
                    "configuration_renamed: updated %d toolkit(s) in project %d (%s -> %s)",
                    updated_count, project_id, old_title, new_title
                )