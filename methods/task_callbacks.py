#!/usr/bin/python3
# coding=utf-8
# pylint: disable=W0201

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

""" Method """

import threading

import requests  # pylint: disable=E0401

from pylon.core.tools import web, log  # pylint: disable=E0401,E0611,W0611
from tools import context, this, auth  # pylint: disable=E0401


class Method:
    """ Method """

    @web.init()
    def task_status_init(self):
        """ Init """
        self.callback_tasks = {}
        #
        self.not_starting_task_event = threading.Event()
        self.not_starting_task_event.set()
        #
        re_prefix = context.url_prefix
        re_name = this.module_name
        self.webhook_api_url_re = \
            f"{re_prefix}/api/v2/{re_name}/webhook/prompt_lib/[0-9]+/[0-9]+/[a-z]+"
        #
        log.info("Making webhook API url public")
        auth.add_public_rule({"uri": self.webhook_api_url_re})

    @web.deinit()
    def task_status_deinit(self):
        """ De-init """
        log.info("Un-making webhook API url public")
        auth.remove_public_rule({"uri": self.webhook_api_url_re})

    @web.method()
    def task_status_changed(self, _, payload):
        """ Handler """
        task_id = payload.get("task_id", None)
        status = payload.get("status", None)
        #
        if status != "stopped":
            return
        #
        callback_data = self.callback_tasks.pop(task_id, None)
        #
        if not callback_data and not self.not_starting_task_event.is_set():
            self.not_starting_task_event.wait(self.task_node.start_max_wait)  # pylint: disable=E1101
            callback_data = self.callback_tasks.pop(task_id, None)
        #
        if not callback_data:
            return
        #
        try:
            task_result = self.task_node.get_task_result(task_id)  # pylint: disable=E1101
            #
            callback_payload = {
                "task_id": task_id,
                "task_result": task_result,
            }
        except:  # pylint: disable=W0702
            callback_payload = {
                "task_id": task_id,
                "task_error": "Exception",
            }
        #
        try:
            requests_result = requests.post(
                callback_data.get("callback_url"),
                headers=callback_data.get("callback_headers", None),
                json=callback_payload,
                timeout=120.0,
                verify=False,
            )
            #
            log.info("Callback POST result: %s", requests_result)
        except:  # pylint: disable=W0702
            log.exception("Error in callback sender (task_id=%s)", task_id)
