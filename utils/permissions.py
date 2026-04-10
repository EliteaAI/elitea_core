from tools import auth

from typing import List, TypeAlias, Tuple, Dict

STATUS_CODE: TypeAlias = int


class ProjectPermissionChecker:
    def __init__(self, owner_id: int):
        self._owner_id = owner_id

    def check_permissions(self, permissions: List[str]) -> Tuple[Dict, STATUS_CODE]:
        return auth.decorators.check_api({
            "permissions": permissions
        }, project_id=self._owner_id)(lambda: ({'ok': True}, 200))()
