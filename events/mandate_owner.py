from pylon.core.tools import web

from ..utils.mandate_owner import on_auth_invalidate


class Event:
    @web.event("auth_invalidate")
    def mandate_owner_auth_invalidated(self, context, event, payload):
        """A user was revoked or changed: a governed platform suspends the mandates of an owner who is no longer
        active (#6947). Returns at once when governance is off."""
        on_auth_invalidate(self, payload)
