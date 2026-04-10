from tools import rpc_tools


def get_all_project_ids():
    """
    :return: list of all project ids
    """
    return [
        i['id'] for i in rpc_tools.RpcMixin().rpc.timeout(2).project_list(
            filter_={'create_success': True}
        )
    ]
