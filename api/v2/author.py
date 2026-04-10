from queue import Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

from pylon.core.tools import log

from tools import api_tools, auth, config as c

from ...utils.utils import add_public_project_id
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.authors import get_stats, get_author_data


class PromptLibAPI(api_tools.APIModeHandler):
    @add_public_project_id
    @auth.decorators.check_api(
        {
            "permissions": ["models.promptlib_shared.author.detail"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, author_id: int, *, project_id: int):
        author: dict = get_author_data(author_id=author_id)
        if not author:
            return {'error': f'author with id {author_id} not found'}
        try:
            author_project_id = self.module.context.rpc_manager.timeout(1).projects_get_personal_project_id(
                author['id'])
            stats = get_stats(author_project_id, author['id'])
            author.update(stats)

            # Parallel stats fetching to reduce latency
            module = self.module

            def fetch_toolkits_stats():
                try:
                    return module.get_toolkits_stats(author_project_id, author_id)
                except Empty:
                    log.warning("application plugin is not available, toolkit related stats will be empty")
                    return {}

            def fetch_applications_stats():
                try:
                    return module.get_stats(author_project_id, author_id)
                except Empty:
                    log.warning("applications plugin is not available, related stats will be empty")
                    return {}

            def fetch_pipelines_stats():
                try:
                    return module.get_pipeline_stats(author_project_id, author_id)
                except Empty:
                    log.warning("applications plugin is not available, pipeline related stats will be empty")
                    return {}

            def fetch_chat_stats():
                try:
                    return module.chat_get_stats(author_project_id, author_id)
                except Empty:
                    log.warning("chat plugin is not available, related stats will be empty")
                    return {}

            # Execute all stats calls in parallel
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(fetch_toolkits_stats): 'toolkits',
                    executor.submit(fetch_applications_stats): 'applications',
                    executor.submit(fetch_pipelines_stats): 'pipelines',
                    executor.submit(fetch_chat_stats): 'chat',
                }
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            author.update(result)
                    except Exception as e:
                        log.warning(f"Failed to fetch {futures[future]} stats: {e}")

        except Empty:
            ...
        return author, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:author_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
