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

""" Notification routing for various events """
from pylon.core.tools import web, log  # pylint: disable=E0611,E0401

from ..models.enums.all import NotificationEventTypes, IndexDataStatus


class Method:
    @web.method()
    def notify_index_data_status(self, index_data_status):
        """
        Handle index operation status notifications
        Routes to different mechanisms based on initiator:
        - schedule: send notification (cron job - user needs to know)
        - user: send notification (user-initiated - user needs confirmation)
        - llm: log only (agent-initiated - user sees in chat already)
        
        Creates a notification in the notifications plugin
        """
        log.info(f'Index data status: {index_data_status}')
        
        # Only process terminal statuses that require notification
        state = index_data_status.get('state')
        if state not in (IndexDataStatus.completed, IndexDataStatus.failed, IndexDataStatus.created):
            return
        
        initiator = index_data_status.get('initiator')
        project_id = index_data_status.get('project_id')
        
        # For LLM-initiated operations, just log (user sees in chat already)
        if initiator == 'llm':
            log.info(f"LLM-initiated index operation completed: {index_data_status.get('index_name')}")
            return
        
        # For user and schedule initiated operations, create notification
        # Note: We need user_id to create notification
        # This might come from the index metadata or need to be added to the event payload
        user_id = index_data_status.get('user_id')
        
        if not user_id or not project_id:
            log.warning(f"Cannot create notification: missing user_id or project_id in {index_data_status}")
            return
        
        # Fire notification event for schedule and user initiators
        self.context.event_manager.fire_event(
            'notifications_stream', {
                'project_id': project_id,
                'user_id': user_id,
                'meta': {
                    'id': index_data_status.get('id'),
                    'index_name': index_data_status.get('index_name'),
                    'state': index_data_status.get('state'),
                    'error': index_data_status.get('error'),
                    'reindex': index_data_status.get('reindex'),
                    'indexed': index_data_status.get('indexed'),
                    'updated': index_data_status.get('updated'),
                    'toolkit_id': index_data_status.get('toolkit_id'),
                    'initiator': initiator,
                },
                'event_type': NotificationEventTypes.index_data_changed
            }
        )
