from pylon.core.tools import web, log

from ..models.enums.all import NotificationEventTypes


class RPC:
    @web.rpc("elitea_core_check_pat_expiration", "check_pat_expiration")
    def check_pat_expiration(self, **kwargs):
        """
        Scheduled task: checks all PATs expiring in ~24 hours and fires
        a notification for each. Runs hourly; the [23h, 24h] window ensures
        each token is notified exactly once.
        """
        log.info("[PAT_EXPIRY] Starting PAT expiration check")

        try:
            expiring_tokens = self.context.rpc_manager.timeout(4).auth_list_tokens_expiring_soon()
        except Exception as e:
            log.warning("[PAT_EXPIRY] Failed to fetch expiring tokens: %s", e)
            return

        log.info("[PAT_EXPIRY] Found %d candidate token(s)", len(expiring_tokens))

        for token in expiring_tokens:
            user_id = token['user_id']
            token_uuid = str(token['uuid'])
            token_name = token.get('name') or 'unnamed'

            # Get user's private project ID for notification routing
            try:
                private_project_id = self.context.rpc_manager.timeout(4).projects_get_personal_project_id(user_id)
            except Exception as e:
                log.warning("[PAT_EXPIRY] Cannot get private project for user %d: %s", user_id, e)
                continue

            if not private_project_id:
                log.warning("[PAT_EXPIRY] No private project for user %d, skipping", user_id)
                continue

            try:
                self.context.event_manager.fire_event(
                    'notifications_stream',
                    {
                        'project_id': private_project_id,
                        'user_id': user_id,
                        'event_type': NotificationEventTypes.personal_access_token_expiring,
                        'meta': {
                            'token_name': token_name,
                            'token_uuid': token_uuid,
                            'message': (
                                f'Your personal access token {token_name} will expire in 24 hours. '
                                f'After expiration, it will no longer work. '
                                f'You can delete and recreate a new token if needed. '
                                f'[Manage Personal Access Tokens]()'
                            ),
                        },
                    }
                )
                log.info(
                    "[PAT_EXPIRY] Notification fired for token '%s' (uuid=%s), user=%d",
                    token_name, token_uuid, user_id,
                )
            except Exception as e:
                log.warning(
                    "[PAT_EXPIRY] Failed to fire notification for token %s: %s",
                    token_uuid, e,
                )
