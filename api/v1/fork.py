from itertools import chain

from flask import request

from pydantic.v1 import ValidationError
from tools import api_tools, db, auth, rpc_tools, config as c

from ...models.pd.fork import ForkApplicationInput
from ...utils.fork import ApplicationForkPayloadProcessor
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


RPC_TIMEOUT=5


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.fork.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        fork_data = request.json
        author_id = auth.current_user().get("id")
        results, errors = {"agents": []}, {"agents": []}
        already_exists = {"agents": [], "datasources": [], "prompts": [], "toolkits": []}

        ENTITY_DETAILS_RPC_MAPPER = {
            'agents': 'applications_get_application_by_id',
            'toolkits': 'applications_get_toolkit_by_id',
            'prompts': 'prompt_lib_get_by_id',
            'datasources': 'datasources_get_datasource_by_id',
        }
        ENTITY_EXISTING_FORK_RPC_MAPPER = {
            'agents': 'applications_find_existing_fork',
            'toolkits': 'applications_find_existing_toolkit_fork',
            'prompts': 'prompt_lib_find_existing_fork',
            'datasources': 'datasources_find_existing_fork',
        }
        rpc_call = rpc_tools.RpcMixin().rpc.timeout(RPC_TIMEOUT)

        try:
            fork_input = ForkApplicationInput.parse_obj(fork_data)
        except ValidationError as e:
            errors['agents'].append(f'Validation error on item: {e}')
            return {'result': results, 'errors': errors}, 400

        to_delete_payload_uuid: dict = {'import_uuids': set(), 'import_version_uuids': set()}

        new_idxs = []

        for idx, fork_input_entity in enumerate(fork_input.applications):
            if fork_input_entity['entity'] == 'agents':
                processed_data, to_delete_uuids, status_code = ApplicationForkPayloadProcessor(
                    fork_input_entity, fork_input.applications, project_id
                ).process()
                if to_delete_uuids:
                    to_delete_payload_uuid['import_uuids'].update(
                        to_delete_uuids['import_uuids']
                    )
                    to_delete_payload_uuid['import_version_uuids'].update(
                        to_delete_uuids['import_version_uuids']
                    )
                parent_entity_id = fork_input_entity['id']
                parent_project_id = fork_input_entity['owner_id']
                for fork_input_entity_version in fork_input_entity['versions']:
                    if fork_input_entity_version.get('meta'):
                        parent_entity_id = fork_input_entity_version['meta'].get(
                            'parent_entity_id', fork_input_entity['id']
                        )
                        parent_project_id = fork_input_entity_version['meta'].get(
                            'parent_project_id', fork_input_entity['owner_id']
                        )

                forked_application_id, forked_application_version_id = rpc_tools.RpcMixin().rpc.call.applications_find_existing_fork(
                    target_project_id=project_id,
                    parent_entity_id=parent_entity_id,
                    parent_project_id=parent_project_id,
                )
                if forked_application_id and forked_application_version_id:
                    forked_application_details = rpc_tools.RpcMixin().rpc.call.applications_get_application_by_id(
                        project_id, forked_application_id
                    )
                    forked_application_details['import_uuid'] = fork_input_entity['import_uuid']
                    forked_application_details['index'] = idx
                    already_exists['agents'].append(forked_application_details)
                    continue
                if status_code != 200:
                    errors['agents'].append({
                        'index': idx,
                        'msg': processed_data.get('message')
                    })
                else:
                    processed_data['index'] = idx
                    results['agents'].append(processed_data)
            else:
                fork_input_entity['index'] = idx
                results['agents'].append(fork_input_entity)
            new_idxs.append(idx)

        if errors['agents']:
            return {'result': results, 'errors': errors}, 400

        log.debug(f'Before cleaning payload {results=}')
        log.debug(f'To delete uuids: {to_delete_payload_uuid=}')

        # filter items with versions
        for item in results['agents']:
            filtered_versions = []
            if item.get('versions'):
                for version in item['versions']:
                    meta = version.get('meta') or {}
                    if meta.get('icon_meta'):
                        meta['icon_meta'] = {}
                    if item['entity'] == 'datasources':
                        if not meta.get('parent_entity_id'):
                            item['meta'] = {
                                'parent_entity_id': item.get('shared_id') or item['id'],
                                'parent_project_id': item.get('shared_owner_id') or item['owner_id'],
                                'parent_author_id': version['author_id'],
                            }
                        filtered_versions.append(version)
                    else:
                        import_version_uuid = version.get('import_version_uuid')
                        if import_version_uuid and import_version_uuid not in to_delete_payload_uuid['import_version_uuids']:
                            if not meta.get('parent_entity_id'):
                                version_id = version.pop('id')
                                version['meta'] = {
                                    'parent_entity_id': version.get('shared_id') or item['id'],
                                    'parent_entity_version_id': version_id,
                                    'parent_project_id': version.get('shared_owner_id') or item['owner_id'],
                                    'parent_author_id': version['author_id'],
                                }
                            filtered_versions.append(version)
                        else:
                            existing_fork_function = ENTITY_EXISTING_FORK_RPC_MAPPER.get(item['entity'])
                            detail_function = ENTITY_DETAILS_RPC_MAPPER.get(item['entity'])
                            if detail_function:
                                parent_entity_id = item['id']
                                parent_project_id = item['owner_id']
                                for item_version in item['versions']:
                                    if item_version.get('meta'):
                                        parent_entity_id = item_version['meta'].get(
                                            'parent_entity_id', item['id']
                                        )
                                        parent_project_id = item_version['meta'].get(
                                            'parent_project_id', item['owner_id']
                                        )
                                forked_entity_id, _ = getattr(rpc_call, existing_fork_function)(
                                    target_project_id=project_id,
                                    parent_entity_id=parent_entity_id,
                                    parent_project_id=parent_project_id
                                )
                                forked_entity_details = getattr(rpc_call, detail_function)(
                                    project_id, forked_entity_id
                                )
                                forked_entity_details['index'] = item['index']
                                already_exists[item['entity']].append(forked_entity_details)
                            else:
                                log.error(f'Func {detail_function} was not found!')
            item['versions'] = filtered_versions

        filtered_items = []
        for item in results['agents']:
            if item.get('versions') and item['import_uuid'] not in to_delete_payload_uuid['import_uuids']:
                filtered_items.append(item)
            elif item['entity'] == 'toolkits' and item['import_uuid'] not in to_delete_payload_uuid['import_uuids']:
                meta = item.get('meta') or {}
                if meta.get('icon_meta'):
                    meta['icon_meta'] = {}
                if not meta.get('parent_entity_id'):
                    item['meta'] = {
                        'parent_entity_id': item.get('shared_id') or item['id'],
                        'parent_project_id': item.get('shared_owner_id') or item['owner_id'],
                        'parent_author_id': item['author_id'],
                    }
                filtered_items.append(item)
            else:
                if item['entity'] == 'datasources' or item['entity'] == 'toolkits':
                    parent_entity_id = item['id']
                    parent_project_id = item['owner_id']
                    if item.get('meta'):
                        parent_entity_id = item['meta'].get(
                            'parent_entity_id', item['id']
                        )
                        parent_project_id = item['meta'].get(
                            'parent_project_id', item['owner_id']
                        )
                    existing_fork_function = ENTITY_EXISTING_FORK_RPC_MAPPER.get(item['entity'])
                    detail_function = ENTITY_DETAILS_RPC_MAPPER.get(item['entity'])
                    if detail_function:
                        forked_entity_id = getattr(rpc_call, existing_fork_function)(
                            target_project_id=project_id,
                            parent_entity_id=parent_entity_id,
                            parent_project_id=parent_project_id
                        )
                        forked_entity_details = getattr(rpc_call, detail_function)(
                            project_id, forked_entity_id
                        )
                        forked_entity_details['index'] = item['index']
                        already_exists[item['entity']].append(forked_entity_details)
                    else:
                        log.error(f'Func {detail_function} was not found!')
        results['agents'] = filtered_items
        log.debug(f'After cleaning payload: {results=}')

        result, errors = self.module.context.rpc_manager.call.applications_import_wizard(
            results['agents'], project_id, author_id
        )

        for entity in result:
            for i in chain(result[entity], errors[entity]):
                i['index'] = new_idxs[i['index']]

        has_results = any(result[key] for key in result if result[key])
        has_errors = any(errors[key] for key in errors if errors[key])

        if not has_errors and has_results:
            status_code = 201
        elif has_errors and has_results:
            status_code = 207
        elif not has_errors and not has_errors:
            status_code = 200
        else:
            status_code = 400
        return {'result': result, 'already_exists': already_exists, 'errors': errors}, status_code


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
