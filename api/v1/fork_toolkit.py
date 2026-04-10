import copy
import uuid
from itertools import chain
from typing import Tuple

from flask import request, send_file
from pydantic.v1 import ValidationError

from tools import api_tools, rpc_tools, db, auth, config as c

from ...models.pd.fork import ForkToolInput
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.permissions import ProjectPermissionChecker


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.fork.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs) -> Tuple[dict, int]:
        fork_data = request.json
        author_id = auth.current_user().get("id")
        results, errors = {'toolkits': []}, {'toolkits': []}
        already_exists = {'toolkits': []}

        try:
            fork_input = ForkToolInput.parse_obj(fork_data)
        except ValidationError as e:
            errors['toolkits'].append(f'Validation error on item: {e}')
            return {'result': results, 'errors': errors}, 400

        new_idxs = []

        for idx, fork_input_toolkit in enumerate(fork_input.toolkits):
            permission_checker = ProjectPermissionChecker(fork_input_toolkit['owner_id'])
            check_owner_permission, status_code = permission_checker.check_permissions(
                ["models.applications.fork.post"]
            )
            if status_code != 200:
                return check_owner_permission, status_code

            if fork_input_toolkit.get('meta'):
                parent_entity_id = fork_input_toolkit['meta'].get('parent_entity_id', fork_input_toolkit['id'])
                parent_project_id = fork_input_toolkit['meta'].get('parent_project_id', fork_input_toolkit['owner_id'])

                forked_toolkit_id = self.module.find_existing_toolkit_fork(
                    target_project_id=project_id,
                    parent_entity_id=parent_entity_id,
                    parent_project_id=parent_project_id
                )
                if forked_toolkit_id:
                    forked_toolkit_details = self.module.get_toolkit_by_id(
                        project_id, forked_toolkit_id
                    )
                    forked_toolkit_details['import_uuid'] = fork_input_toolkit['import_uuid']
                    forked_toolkit_details['index'] = idx
                    already_exists['toolkits'].append(forked_toolkit_details)
                    continue
            try:
                new_toolkit = copy.deepcopy(fork_input_toolkit)
                new_toolkit['entity'] = 'toolkits'
                hash_ = hash((new_toolkit['id'], new_toolkit['owner_id'], new_toolkit['name']))
                new_toolkit['import_uuid'] = str(uuid.UUID(int=abs(hash_)))
                meta = new_toolkit.get('meta', {}) or {}
                if meta.get('icon_meta'):
                    meta['icon_meta'] = {}

                if 'parent_entity_id' not in meta:
                    shared_id = new_toolkit.get('shared_id')
                    shared_owner_id = new_toolkit.get('shared_owner_id')

                    if shared_id and shared_owner_id:
                        parent_entity_id = shared_id
                        parent_project_id = shared_owner_id
                    else:
                        parent_entity_id = fork_input_toolkit['id']
                        parent_project_id = fork_input_toolkit['owner_id']

                    meta.update({
                        'parent_entity_id': parent_entity_id,
                        'parent_project_id': parent_project_id,
                        'parent_author_id': fork_input_toolkit['author_id'],
                    })
                    new_toolkit['meta'] = meta
                new_toolkit.pop('id')
                new_toolkit['index'] = idx
                new_idxs.append(idx)
                results['toolkits'].append(new_toolkit)
            except KeyError as e:
                errors['toolkits'].append({
                    'index': idx,
                    'msg': f'{e}'
                })
                return {'result': results, 'errors': errors}, 404

        if results['toolkits']:
            import_wizard_result, errors = self.module.context.rpc_manager.call.prompt_lib_import_wizard(
                results['toolkits'], project_id, author_id
            )
        else:
            import_wizard_result = results

        has_results = any(import_wizard_result[key] for key in import_wizard_result if import_wizard_result[key])
        has_errors = any(errors[key] for key in errors if errors[key])

        if not has_errors and has_results:
            status_code = 201
        elif has_errors and has_results:
            status_code = 207
        elif not has_errors and not has_errors:
            status_code = 200
        else:
            status_code = 400

        for entity in import_wizard_result:
            for i in chain(import_wizard_result[entity], errors[entity]):
                i['index'] = new_idxs[i['index']]

        return {'result': import_wizard_result, 'already_exists': already_exists, 'errors': errors}, status_code


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
