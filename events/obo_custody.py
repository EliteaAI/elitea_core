""" Event: a person signed out, so their Microsoft sign-in is no longer lent to runs (C-1009 #6756 step 4) """

from pylon.core.tools import web  # pylint: disable=E0611,E0401


class Event:  # pylint: disable=R0903
    @web.event("auth_oidc_obo_custody_dropped")
    def entra_obo_custody_dropped(self, context, event, payload):  # pylint: disable=W0613
        """auth_oidc dropped a person's custody at logout: drop the Graph tokens this process minted for them, so
        signing out stops delegated access at once rather than at the token's own expiry. ``payload``: ``{tid, oid}``
        only. A forged event can only make the next release exchange again."""
        from ..utils import entra_obo  # pylint: disable=C0415
        #
        payload = payload if isinstance(payload, dict) else {}
        entra_obo.forget_subject(payload.get("tid"), payload.get("oid"))
