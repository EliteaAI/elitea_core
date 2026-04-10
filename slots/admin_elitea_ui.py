#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems

""" Slot """

from pylon.core.tools import web  # pylint: disable=E0611,E0401

from tools import auth  # pylint: disable=E0401
from tools import theme  # pylint: disable=E0401


class Slot:  # pylint: disable=E1101,R0903
    """
        Slot Resource

        self is pointing to current Module instance

        web.slot decorator takes one argument: slot name
        Note: web.slot decorator must be the last decorator (at top)

        Slot resources use check_slot auth decorator
        auth.decorators.check_slot takes the following arguments:
        - permissions
        - scope_id=1
        - access_denied_reply=None -> can be set to content to return in case of 'access denied'

    """

    @web.slot("admin_elitea_ui_scripts")
    @auth.decorators.check_slot(["runtime.elitea"], access_denied_reply=theme.access_denied_part)
    def _scripts(self, context, slot, payload):
        _ = slot, payload
        #
        with context.app.app_context():
            return self.descriptor.render_template(
                "admin_elitea_ui/scripts.html",
            )
