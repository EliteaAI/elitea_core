#!/usr/bin/python3
# coding=utf-8

""" Method """

from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method("autonomous_trigger_report")
    def trigger_report(self, project_ids, principal=None):
        """Triggers that run on the system user, per project, with the outcome governance would give them
        (#6947, C-1009). Read-only; rows hold ids, states and outcomes only. Projects that cannot be read are
        logged and left out."""
        from ..utils.trigger_report import build_trigger_report  # pylint: disable=C0415
        return build_trigger_report(project_ids, principal)[0]
