from queue import Empty

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

            module = self.module

            try:
                res = module.get_toolkits_stats(author_project_id, author_id)
            except Empty:
                log.warning("application plugin is not available, toolkit related stats will be empty")
            finally:
                author.update(res)

            try:
                res = module.get_stats(author_project_id, author_id)
            except Empty:
                log.warning("applications plugin is not available, related stats will be empty")
            finally:
                author.update(res)

            try:
                res = module.get_pipeline_stats(author_project_id, author_id)
            except Empty:
                log.warning("applications plugin is not available, pipeline related stats will be empty")
            finally:
                author.update(res)

            try:
                res = module.chat_get_stats(author_project_id, author_id)
            except Empty:
                log.warning("chat plugin is not available, related stats will be empty")
            finally:
                author.update(res)

        except Empty:
            ...
        log.debug(f"Author data retrieved: {author}")
        return author, 200


class API(api_tools.APIBase):
    module_name_override = "promptlib_shared"

    url_params = api_tools.with_modes([
        '<int:author_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
