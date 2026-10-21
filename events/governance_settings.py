from pylon.core.tools import web

from ..utils.governance_settings import on_settings_changed


class Event:
    @web.event("credential_governance_settings_changed")
    def governance_settings_changed(self, context, event, payload):
        """The governance switch was changed on some replica: read the new row now instead of at the next refresh
        (#7120). If this event is lost, the snapshot is at most ten seconds old."""
        on_settings_changed(self, payload)
