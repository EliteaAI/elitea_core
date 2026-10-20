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

""" Methods: governed credential release and test of a saved credential (#6946, C-1009) """

from pylon.core.tools import web  # pylint: disable=E0611,E0401


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def release_credential(  # pylint: disable=R0913
        self, project_id=None, context_ref=None, probe_ref=None, caller=None, toolkit_instance_id=None, field=None,
        slot=None, recipient_key=None, **kwargs,
    ):
        """ Worker bus (``restricted_release_credential``): the material of one credential slot, sealed to the
        requester's key. Registered only when governed. Never raises. """
        from ..utils.credential_release import release_credential  # pylint: disable=C0415
        #
        return release_credential(
            self, project_id=project_id, context_ref=context_ref, probe_ref=probe_ref, caller=caller,
            toolkit_instance_id=toolkit_instance_id, field=field, slot=slot, recipient_key=recipient_key, **kwargs)

    @web.method()
    def governed_check_connection(self, project_id, configuration_uuid, config_type, user_id):
        """ Same process (the check-connection routes of ``configurations``): test a saved credential through the
        governed path. Returns ``{"success": True, ...}``, a provider failure ``{"success": False, "message"}``, an
        authorization request, or a refusal ``{"success": False, "code", "message"}``. Never raises. """
        from ..utils.credential_probe import governed_check_connection  # pylint: disable=C0415
        #
        return governed_check_connection(self, project_id, configuration_uuid, config_type, user_id)
