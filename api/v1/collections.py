import json
from typing import List

from flask import request
from pydantic import ValidationError

from ...models.pd.collections import CollectionListModel
from ...utils.collections import (
    get_collection_tags,
    list_collections,
    create_collection,
    get_detail_collection,
    get_include_entity_flag,
    check_addability_for_entity
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.authors import get_authors_data
from ...utils.exceptions import (
    EntityInaccessableError,
    EntityNotAvailableCollectionError,
)

from tools import api_tools, config as c, auth



class PromptLibAPI(api_tools.APIModeHandler):
    # def _get_project_id(self, project_id: int | None) -> int:
    #     if not project_id:
    #         project_id = 0  # todo: get user personal project id here
    #     return project_id

    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.collections.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int | None = None, **kwargs):
        # project_id = self._get_project_id(project_id)
        # list prompts
        # prompt_id = request.args.get('prompt_id')
        # prompt_owner_id = request.args.get('prompt_owner_id')
        datasource_id = request.args.get('datasource_id')
        datasource_owner_id = request.args.get('datasource_owner_id')
        application_id = request.args.get('application_id')
        application_owner_id = request.args.get('application_owner_id')
        need_tags = 'no_tags' not in request.args
        my_liked = request.args.get('my_liked', default=False, type=bool)
        total, collections = list_collections(project_id, request.args, with_likes=True, my_liked=my_liked)
        # parsing
        parsed: List[CollectionListModel] = []
        users = get_authors_data([i.author_id for i in collections])
        user_map = {i['id']: i for i in users}
        for col in collections:
            col_model = CollectionListModel.from_orm(col)
            col_model.author = user_map.get(col_model.author_id)
            if need_tags:
                col_model.tags = get_collection_tags(col)

            if datasource_id and datasource_owner_id:
                col_model.includes_datasource = get_include_entity_flag(
                    'datasource', col_model, int(datasource_id), int(datasource_owner_id)
                )
                col_model.datasource_addability = check_addability_for_entity(
                    project_id=project_id,
                    collection_id=col_model.id,
                    entity_name='datasource',
                    entity_id=int(datasource_id),
                    entity_owner_id=int(datasource_owner_id)
                )

            if application_id and application_owner_id:
                col_model.includes_application = get_include_entity_flag(
                    'application', col_model, int(application_id), int(application_owner_id)
                )
                col_model.application_addability = check_addability_for_entity(
                    project_id=project_id,
                    collection_id=col_model.id,
                    entity_name='application',
                    entity_id=int(application_id),
                    entity_owner_id=int(application_owner_id)
                )

            parsed.append(col_model)

        return {
            "rows": [json.loads(i.json(exclude={"author_id"})) for i in parsed],
            "total": total,
        }, 200

    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.collections.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int | None = None, **kwargs):
        # project_id = self._get_project_id(project_id)
        data = request.get_json()

        author_id = auth.current_user().get("id")
        data["owner_id"], data["author_id"] = project_id, author_id

        try:
            new_collection = create_collection(project_id, data)
            result = get_detail_collection(new_collection)
            return result, 201
        except ValidationError as e:
            return e.errors(), 400
        except EntityInaccessableError as e:
            return {"error": e.message}, 403
        except EntityNotAvailableCollectionError as e:
            return {"error": e.message}, 404
        except Exception as e:
            return {"error": str(e)}, 400


class API(api_tools.APIBase):
    module_name_override = "promptlib_shared"

    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }