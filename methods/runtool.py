import json
from pylon.core.tools import log, web
from sqlalchemy.orm import joinedload

from ..utils.predict_utils import generate_test_tool_payload
from ..utils.exceptions import PoolSaturationError


class Method:

    @web.method()
    def do_runtool(
        self,
        project_id: int,
        user_id: int,
        toolkit_id: int,
        tool_name: str,
        tool_params: dict,
        webhook_signature=None,
        predict_wait=True,
        predict_timeout=float(60*60),  # 1 hour
    ):
        payload = generate_test_tool_payload(project_id, user_id=user_id, toolkit_id=toolkit_id, tool_name=tool_name, tool_params=tool_params)
        #
        task_id = self.task_node.start_task(
            "indexer_test_toolkit_tool",
            args=[None, None],
            kwargs=payload,
            pool="agents",
            meta={},
        )

        # Handle pool saturation: start_task returns None when no workers available
        if task_id is None:
            log.warning(
                "Pool 'agents' saturated - no workers available for project_id=%s",
                project_id
            )
            raise PoolSaturationError(pool="agents", retry_after=5)

        if webhook_signature is not None or not predict_wait:
            result = {
                "message": "Task started",
                "task_id": task_id,
            }
        else:
            result = self.task_node.join_task(task_id, timeout=predict_timeout)
            if result is ...:
                self.task_node.stop_task(task_id)
                return {"error": "Timeout"}
        #
        return result