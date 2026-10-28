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

""" Admin task for the platform tick row. """

import time

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import web  # pylint: disable=E0611,E0401

from tools import db  # pylint: disable=E0611,E0401

from ..utils import platform_tick_row


class Method:  # pylint: disable=E1101,R0903,W0201
    """Method Resource. ``self`` points to the current Module instance."""

    # pylint: disable=R,W0613
    @web.method()
    def fix_platform_tick_row(self, *args, **kwargs):
        """Admin task: keep one global ``pipeline_scheduling`` row driving the schedule tick.

        Only a stack that ran an interim build of the generic tick needs it: that build stored the row with
        handler ``entity_schedules_check``, and the boot call then added a second row with
        ``pipelines_check_scheduling``. Both rows call the same guarded tick, so the extra row only costs
        one wasted call per minute.

        The oldest ``entity_schedules_check`` row is renamed to ``pipelines_check_scheduling`` when no such
        row exists; it keeps its cron and active flag. Every other ``entity_schedules_check`` row is switched
        off, not deleted. Project rows and other names are not touched. Running it again changes nothing.

        Param format: ``dry_run=false``. Dry run by default.

        Examples::

            ""                - dry run: log the row ids it would rename and switch off
            "dry_run=false"   - write the changes
        """
        log.info("Starting fix_platform_tick_row")
        start_ts = time.time()
        #
        try:
            dry_run = platform_tick_row.parse_params(kwargs.get("param", ""))
            # The scheduling plugin's own model, so a column change there cannot drift from this task
            from plugins.scheduling.models.schedule import Schedule  # pylint: disable=C0415,E0401
            with db.get_session(None) as session:
                plan = platform_tick_row.repair(session, Schedule, dry_run=dry_run)
            log.info(
                "fix_platform_tick_row%s: found=%s renamed=%s switched_off=%s",
                " (dry run)" if dry_run else "", plan["found"], plan["rename"], plan["switch_off"],
            )
        except:  # pylint: disable=W0702
            log.exception("Got exception during fix_platform_tick_row")
        #
        end_ts = time.time()
        log.info("Exiting fix_platform_tick_row (duration = %s)", end_ts - start_ts)
