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
    def release_credential(self, **kwargs):
        """ Worker bus (``restricted_release_credential``): the material of one credential slot, sealed to the
        requester's key. Registered only when governed. Never raises; only the request keys are read from ``kwargs``
        (the bus is not trusted to send nothing else). """
        from ..utils.credential_release import REQUEST_KEYS, release_credential  # pylint: disable=C0415
        #
        try:
            return release_credential(self, **{key: kwargs.get(key) for key in REQUEST_KEYS})
        except Exception:  # pylint: disable=W0703
            return {"ok": False, "reason_code": "DEPENDENCY_UNAVAILABLE",
                    "message": "A required service is unavailable; the run was not started. Try again."}

    @web.method()
    def release_credentials(self, **kwargs):
        """ Worker bus (``restricted_release_credentials``): the material of every credential slot of a run in one
        call, each slot sealed to the requester's key under its own binding. Registered only when governed. Never
        raises; only the request keys are read from ``kwargs``. """
        from ..utils.credential_release import BATCH_REQUEST_KEYS, release_credentials  # pylint: disable=C0415
        #
        try:
            return release_credentials(self, **{key: kwargs.get(key) for key in BATCH_REQUEST_KEYS})
        except Exception:  # pylint: disable=W0703
            return {"ok": False, "reason_code": "DEPENDENCY_UNAVAILABLE",
                    "message": "A required service is unavailable; the run was not started. Try again."}

    @web.method()
    def governed_check_connection(  # pylint: disable=R0913
        self, project_id, configuration_uuid, config_type, user_id, actor_user_id=None, onboarding=False, reuse=False,
    ):
        """ Same process (the check-connection routes of ``configurations``, and the credential onboarding of
        ``secrets``): test a saved credential through the governed path. ``user_id`` is whose personal reference is
        preferred, ``actor_user_id`` (default ``user_id``) who holds ``credentials.probe`` (an operator testing a
        personal reference for its custodian), ``onboarding`` lets a ``pending`` reference be probed, ``reuse`` (the
        validators, #7281) answers from an outcome kept a short while for the same tester and reference state. Returns
        ``{"success": True, ...}``, a provider failure ``{"success": False, "message"}``, an authorization request,
        or a refusal ``{"success": False, "code", "message"}``. Never raises. """
        from ..utils.credential_probe import governed_check_connection  # pylint: disable=C0415
        #
        return governed_check_connection(
            self, project_id, configuration_uuid, config_type, user_id, actor_user_id, onboarding, reuse=reuse)

    @web.method()
    def governed_check_connection_many(  # pylint: disable=R0913
        self, project_id, items, user_id, actor_user_id=None, onboarding=False, reuse=False,
    ):
        """ Same process (the batch check-connection route of ``configurations``): test several saved credentials of a
        project through the governed path, the probes running at the same time (at most a few at once). ``items``:
        ``[{"configuration_uuid", "config_type"}]``; the other arguments are those of ``governed_check_connection`` and
        are the same for every item (``reuse``: the validators, #7294). Returns one answer per item, in order, each as
        ``governed_check_connection`` answers. Never raises. """
        from ..utils.credential_probe import governed_check_connection_many  # pylint: disable=C0415
        #
        return governed_check_connection_many(self, project_id, items, user_id, actor_user_id, onboarding, reuse=reuse)

    @web.method()
    def entra_delegated_probe(self, project_id, reference_id, delegation, person):  # pylint: disable=R0913
        """ Same process (the credential onboarding of ``secrets``, #6756 step 4): can ``person`` (``{issuer, tid,
        oid}``, which ``secrets`` resolved from the identity service, never from a caller) obtain a token through the
        Entra-delegated reference ``reference_id`` of ``project_id`` now? ``delegation``: its descriptor with
        ``ownership_class`` and ``custodian_subject_ref``. ``None`` when yes, else the refusal code. One exchange (or a
        cached token); the outcome is kept a minute, the token never leaves this process. Never raises. """
        from ..utils import entra_obo  # pylint: disable=C0415
        #
        try:
            return entra_obo.probe(self, reference_id, dict(person), dict(delegation), project_id)
        except Exception:  # pylint: disable=W0703
            return "DEPENDENCY_UNAVAILABLE"

    @web.method()
    def entra_delegated_app_available(self, project_id, delegation):
        """ Same process (the credential onboarding of ``secrets``, #6756 step 4): can the app this delegated
        descriptor names be resolved now, for project ``project_id``? A boolean; the app's secret is never returned.
        Never raises. """
        from ..utils import entra_obo  # pylint: disable=C0415
        #
        try:
            return entra_obo.app_available(self, dict(delegation), project_id)
        except Exception:  # pylint: disable=W0703
            return False
