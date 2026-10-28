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

""" Admin tasks for index schedules. """

import time

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import web  # pylint: disable=E0611,E0401

from tools import db, rpc_tools  # pylint: disable=E0611,E0401

from .category_tasks import _parse_kv_params
from ..models.elitea_tools import EliteATool
from ..utils.application_tools import _get_pgvector_engine
from ..utils.index_scheduling import override_schedule_expiration
from ..utils import orphaned_index_schedules as orphans

_REQUIRED_PARAMS = ("project_id", "toolkit_id", "index", "minutes")


class Method:  # pylint: disable=E1101,R0903,W0201
    """Method Resource. ``self`` points to the current Module instance."""

    # pylint: disable=R,W0613
    @web.method()
    def set_index_schedule_expiration(self, *args, **kwargs):
        """Admin task: make index schedules expire ``minutes`` from now (#4997 testing).

        Param format: ``project_id=<N>;toolkit_id=<N>;index=<index name>;minutes=<N>[;user_id=<N>]``

        ``user_id`` is the schedule key — the author's id, or ``-1`` for a team schedule.
        Omit it to move every schedule of the index. Only the deadline changes: both expiry
        warnings are re-armed and the next scheduler tick notifies and retires the schedule
        exactly as it would at the end of a real 90/180-day window. Disabled schedules are
        skipped; switch them on first (that renews them), then run this again.

        The tick sends only the tightest warning already due, so to see each one::

            minutes=10079   ->  7-day warning on the next tick   (just under 7 days)
            minutes=1439    ->  24-hour warning on the next tick (just under 24 hours)
            minutes=5       ->  24-hour warning now, retirement ~5 minutes later
            minutes=0       ->  retirement on the next tick

        Example::

            project_id=12;toolkit_id=345;index=docs;minutes=5
        """
        log.info("Starting set_index_schedule_expiration")
        start_ts = time.time()
        #
        try:
            raw_param = kwargs.get("param", "")
            params = _parse_kv_params(raw_param)
            missing = [name for name in _REQUIRED_PARAMS if not params.get(name)]
            if missing:
                log.error(
                    "Missing required params %s. Expected "
                    "'project_id=<N>;toolkit_id=<N>;index=<name>;minutes=<N>[;user_id=<N>]', got: %s",
                    missing, repr(raw_param),
                )
                return
            try:
                project_id = int(params["project_id"])
                toolkit_id = int(params["toolkit_id"])
                minutes = int(params["minutes"])
                user_id = int(params["user_id"]) if params.get("user_id") else None
            except ValueError:
                log.error("project_id, toolkit_id, minutes and user_id must be integers, got: %s",
                          repr(raw_param))
                return
            index_name = params["index"]
            #
            with db.get_session(project_id) as session:
                toolkit = session.query(EliteATool).filter(EliteATool.id == toolkit_id).first()
                if toolkit is None:
                    log.error("Toolkit %s not found in project %s", toolkit_id, project_id)
                    return
                try:
                    result = override_schedule_expiration(
                        session, toolkit, index_name, minutes, user_id=user_id)
                except ValueError as exc:
                    log.error("Cannot set schedule expiration: %s", exc)
                    return
            #
            for key, expires_at in result["updated"].items():
                log.info(
                    "project=%s toolkit=%s index=%s user=%s: schedule now expires at %s; "
                    "warnings re-armed, the next scheduler tick applies them",
                    project_id, toolkit_id, index_name, key, expires_at,
                )
            for key, reason in result["skipped"].items():
                log.warning(
                    "project=%s toolkit=%s index=%s user=%s: skipped, %s",
                    project_id, toolkit_id, index_name, key, reason,
                )
            if not result["updated"] and not result["skipped"]:
                log.warning("Index %s has no schedules; nothing to do", index_name)
        except:  # pylint: disable=W0702
            log.exception("Got exception during set_index_schedule_expiration")
        #
        end_ts = time.time()
        log.info("Exiting set_index_schedule_expiration (duration = %s)", end_ts - start_ts)

    @web.method()
    def prune_orphaned_index_schedules(self, *args, **kwargs):
        """Admin task: remove index schedules whose index no longer exists (#7169).

        DISCLAIMER: removal is not reversible from the platform. There is no backup table:
        every removed ``indexes_meta.<index>`` entry is logged in full by this task, and that
        log line is the only way to restore it. Run the dry run first and keep the task log.

        An index's schedules are orphaned when the toolkit's own pgvector store has no
        ``index_meta`` row for it, e.g. after EL-7023 / EL-6356 deleted the index but left the
        schedules, which the scheduler then reports as "not found in database". Enabled and
        disabled schedules alike. Only the ``indexes_meta.<index>`` key is removed: the toolkit,
        its other indexes and any vector data are left as they are. A toolkit whose store
        cannot be checked is skipped, never pruned.

        Param format: ``project_id=<all|N>;toolkit_id=<N>;dry_run=false``

        Dry run by default. ``toolkit_id`` needs ``project_id``.

        Examples::

            ""                                      - dry run, all projects
            "project_id=12"                         - dry run, project 12
            "project_id=12;dry_run=false"           - remove orphans in project 12
            "project_id=12;toolkit_id=345;dry_run=false"
            "dry_run=false"                         - remove orphans in every project
        """
        log.info("Starting prune_orphaned_index_schedules")
        start_ts = time.time()
        #
        try:
            try:
                project_id, toolkit_id, dry_run = orphans.parse_prune_params(kwargs.get("param", ""))
            except ValueError as exc:
                log.error("Invalid params %r: %s. Expected "
                          "'project_id=<all|N>;toolkit_id=<N>;dry_run=false'",
                          kwargs.get("param", ""), exc)
                return
            #
            if project_id is None:
                project_ids = [
                    p["id"] for p in self.context.rpc_manager.call.project_list(
                        filter_={"create_success": True}
                    )
                ]
            else:
                project_ids = [project_id]
            #
            def list_toolkits(session, only_toolkit_id):
                query = session.query(EliteATool).filter(EliteATool.meta["indexes_meta"].isnot(None))
                if only_toolkit_id is not None:
                    query = query.filter(EliteATool.id == only_toolkit_id)
                return query.all()
            #
            def resolve_connection_string(pid, toolkit):
                # Per-schedule credentials only fill the toolkit's own slot, never
                # pgvector_configuration, so the toolkit's own settings point at the store.
                expanded = rpc_tools.RpcMixin().rpc.timeout(5).configurations_expand(
                    project_id=pid, settings=orphans.pgvector_settings(toolkit.settings),
                    user_id=toolkit.author_id, unsecret=True,
                )
                return (expanded.get("pgvector_configuration") or {}).get("connection_string")
            #
            orphans.prune_orphaned_index_schedules(
                project_ids=project_ids,
                open_project_session=db.get_session,
                list_toolkits=list_toolkits,
                resolve_connection_string=resolve_connection_string,
                present_index_names=lambda conn, schema: orphans.present_index_names(
                    _get_pgvector_engine(conn), schema),
                toolkit_id=toolkit_id,
                dry_run=dry_run,
            )
        except:  # pylint: disable=W0702
            log.exception("Got exception during prune_orphaned_index_schedules")
        #
        end_ts = time.time()
        log.info("Exiting prune_orphaned_index_schedules (duration = %s)", end_ts - start_ts)
