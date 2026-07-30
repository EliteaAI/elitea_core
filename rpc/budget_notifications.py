"""Budget threshold and limit notifications.

Called from the LLM proxy when spend crosses a configured threshold or a budget blocks a
call. Recipients differ by scope: a project's budget concerns its admins, a member's own
budget concerns only that member.
"""

from pylon.core.tools import web, log

from ..models.enums.all import NotificationEventTypes

PROJECT_USAGE_LINK = '[View project usage]()'
MEMBER_USAGE_LINK = '[View my usage]()'

# Budgets are monthly only, so the period reads as a constant rather than a placeholder
BUDGET_PERIOD = 'monthly'


def _is_admin_role(role):
    """Mirror of the platform's own admin test, which matches on the role name."""
    return 'admin' in str(role).lower()


class RPC:
    @web.rpc("elitea_core_get_project_admin_ids", "get_project_admin_ids")
    def get_project_admin_ids(self, project_id: int, **kwargs):
        """User ids of a project's admins.

        One call returning every member's roles, rather than an is-admin check per member.
        """
        try:
            roles_by_user = self.context.rpc_manager.timeout(15).admin_get_users_roles_in_project(
                project_id, filter_system_user=True,
            ) or {}
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to list roles for project %s", project_id)
            return []
        #
        return [
            int(user_id)
            for user_id, roles in roles_by_user.items()
            if any(_is_admin_role(role) for role in (roles or []))
        ]

    @web.rpc("elitea_core_notify_budget_event", "notify_budget_event")
    def notify_budget_event(  # pylint: disable=R0913
            self, project_id: int, kind: str, pct: int = None, user_id: int = None, **kwargs
    ):
        """Send one budget notification for a scope.

        ``kind`` is 'threshold' or 'limit'. A user_id makes it a member alert, delivered to
        that member alone; otherwise it is a project alert, delivered to every project
        admin. Never raises: the callers are on the request path.
        """
        try:
            project = self.context.rpc_manager.timeout(5).project_get_by_id(project_id) or {}
            project_name = project.get('name') or f'project {project_id}'
            #
            is_member = user_id is not None
            #
            if is_member:
                event_type = (
                    NotificationEventTypes.member_budget_threshold_reached if kind == 'threshold'
                    else NotificationEventTypes.member_budget_limit_reached
                )
                #
                message = (
                    f'Budget warning: You have reached {pct}% of your {BUDGET_PERIOD} budget '
                    f'in {project_name}. {MEMBER_USAGE_LINK}'
                ) if kind == 'threshold' else (
                    f'Budget limit reached: You have reached your {BUDGET_PERIOD} budget limit '
                    f'in {project_name}. {MEMBER_USAGE_LINK}'
                )
                #
                recipients = [user_id]
            else:
                event_type = (
                    NotificationEventTypes.budget_threshold_reached if kind == 'threshold'
                    else NotificationEventTypes.budget_limit_reached
                )
                #
                message = (
                    f'Budget warning: {project_name} has reached {pct}% of its '
                    f'{BUDGET_PERIOD} budget. {PROJECT_USAGE_LINK}'
                ) if kind == 'threshold' else (
                    f'Budget limit reached: {project_name} has reached its {BUDGET_PERIOD} '
                    f'budget limit. {PROJECT_USAGE_LINK}'
                )
                #
                recipients = self.get_project_admin_ids(project_id)
                if not recipients and project.get('owner_id') is not None:
                    # Auto-created personal projects give the owner editor/viewer/monitor,
                    # never admin (rpc/poc.py create_personal_project) — fall back to owner
                    # or the alert is silently dropped for every such project.
                    recipients = [int(project['owner_id'])]
            #
            if not recipients:
                log.warning(
                    "No recipients for %s budget %s alert on project %s",
                    'member' if is_member else 'project', kind, project_id,
                )
                return 0
            #
            # One row per recipient: the notification model has a single user_id
            for recipient in recipients:
                self.context.event_manager.fire_event(
                    'notifications_stream',
                    {
                        'project_id': project_id,
                        'user_id': recipient,
                        'event_type': event_type,
                        'meta': {
                            'project_name': project_name,
                            'budget_period': BUDGET_PERIOD,
                            'threshold_pct': pct,
                            'message': message,
                        },
                    },
                )
            #
            log.info(
                "Budget %s alert sent for project %s%s to %s recipient(s)",
                kind, project_id,
                f' user {user_id}' if is_member else '', len(recipients),
            )
            #
            return len(recipients)
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to send budget %s alert for project %s", kind, project_id)
            return 0
