from pylon.core.tools import log, web


class Event:
    @web.event('conversation_shared')
    def handle_conversation_shared(self, context, event, payload: dict):
        log.info(
            "[SHARE] Conversation %s in project %s shared (token %s...) by user %s",
            payload.get('conversation_id'),
            payload.get('project_id'),
            str(payload.get('token', ''))[:8],
            payload.get('created_by'),
        )

    @web.event('conversation_share_revoked')
    def handle_conversation_share_revoked(self, context, event, payload: dict):
        log.info(
            "[SHARE] Token %s... in project %s revoked by user %s",
            str(payload.get('token', ''))[:8],
            payload.get('project_id'),
            payload.get('revoked_by'),
        )
