#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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

""" Method """
from pylon.core.tools import web, log  # pylint: disable=E0611,E0401

from ..utils.sio_utils import get_eval_run_room, SioEvents


class Method:
    @web.method()
    def eval_run_progress_event(self, event, payload):
        """Re-emit an eval run progress frame to the local replica's socket clients.

        The frame arrives over the event node because the run executes wherever the task pool
        put it, while the watching browser holds a socket on whichever replica served it. Every
        replica subscribes and emits into its own copy of the room; the ones with nobody joined
        return early.
        """
        run_id = payload.get('run_id')
        if run_id is None:
            log.warning('eval_run_progress_event without run_id: %s', payload)
            return
        room = get_eval_run_room(run_id)
        if not any(self.context.sio.manager.get_participants('/', room)):
            return
        self.context.sio.emit(
            event=SioEvents.eval_run_progress.value,
            data=payload,
            room=room,
        )
