from pylon.core.tools import log, web


class Event:
    @web.event("new_ai_user")
    def handle_new_ai_user(self, context, event, payload: dict):
        # payload == {user_id: int, user_email: str}
        allowed_domains_str = self.descriptor.config.get('ai_project_allowed_domains', '')
        allowed_domains = {i.strip().strip('@') for i in allowed_domains_str.split(',')}
        user_email_domain = payload.get('user_email', '').split('@')[-1]
        #
        user_allowed = "*" in allowed_domains or user_email_domain in allowed_domains
        #
        log.info(
            'Checking if user eligible to join special project. %s with domain |%s| in allowed domains |%s| and result is |%s|',
            payload.get('user_email'),
            user_email_domain,
            allowed_domains,
            user_allowed,
        )
        #
        if user_allowed:
            log.info('Adding AI user to project %s', payload)
            ai_project_id = self.descriptor.config.get('ai_project_id')
            if not ai_project_id:
                log.critical('"ai_project_id" is not set in config')
                return
            #
            ai_project_roles = ["viewer"]
            #
            global_admin_roles = {"admin", "super_admin"}
            global_user_roles = context.rpc_manager.call.auth_get_user_roles(
                payload['user_id']
            )
            #
            user_roles = context.rpc_manager.call.admin_get_user_roles(
                ai_project_id, payload['user_id']
            )
            user_role_names = [item["name"] for item in user_roles]
            target_roles = []
            #
            for target_role in ai_project_roles:
                if target_role not in user_role_names and target_role not in target_roles:
                    target_roles.append(target_role)
            #
            if global_admin_roles & global_user_roles:
                additional_roles = self.descriptor.config.get("add_admin_roles", [])
                for target_role in additional_roles:
                    if target_role not in user_role_names and target_role not in target_roles:
                        target_roles.append(target_role)
            #
            log.info(
                'Adding AI user %s to project %s with new roles %s',
                payload, ai_project_id, target_roles,
            )
            #
            if target_roles:
                context.rpc_manager.call.admin_add_user_to_project(
                    ai_project_id, payload['user_id'], target_roles
                )
        else:
            log.warning('User with non-AI email registered %s', payload)
