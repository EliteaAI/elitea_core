from tools import context


def get_accessible_project(project_id: int, user_id: int) -> dict | None:
    """ Project row if the user is a member and it is not suspended, else None """
    # Single indexed lookup; list_user_projects would load every project on a cache miss
    if not context.rpc_manager.call.admin_check_user_in_project(project_id, user_id):
        return None
    project = context.rpc_manager.call.project_get_by_id(project_id)
    if not project or project.get("suspended"):
        return None
    return project
