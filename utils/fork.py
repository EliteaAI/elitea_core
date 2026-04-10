from typing import Tuple, Dict, List
from sqlalchemy.exc import ProgrammingError

from tools import db, rpc_tools

from ..models.all import Application
from ..utils.permissions import ProjectPermissionChecker, STATUS_CODE


class ApplicationForkPayloadProcessor:
    def __init__(
            self, application_data: dict,
            fork_input_entities: List[dict],
            target_project_id: int
    ):
        self._input_data: dict = application_data
        self._fork_input_entities: List[dict] = fork_input_entities
        self._target_project_id: int = target_project_id

    def process(self) -> Tuple[Dict, Dict, STATUS_CODE]:
        permission_checker = ProjectPermissionChecker(self._input_data['owner_id'])
        permission_result, status_code = permission_checker.check_permissions(
            ["models.applications.fork.post"]
        )
        if status_code != 200:
            return permission_result, {}, status_code

        try:
            return self._fork_application()
        except ValueError:
            return {'message': f'Payload entities do not have '
                               f'meta fields, import_uuid: {self._input_data["import_uuid"]}'}, {}, 400
        except ProgrammingError:
            return {'message': f'The project with id {self._input_data["owner_id"]} '
                               f'does not exist or DB issue'}, {}, 400

    def _fork_application(self) -> Tuple[Dict, Dict, STATUS_CODE]:
        with db.with_project_schema_session(self._input_data['owner_id']) as session:
            original_application = session.query(Application).filter(
                Application.id == self._input_data['id']
            ).first()
            if not original_application:
                return {'message': f'Application with id {self._input_data["id"]} does not exist'}, {}, 400

            new_application = original_application.to_json()
            new_application['versions'] = []
            input_application_versions = {
                version.get('id'): version for version in self._input_data['versions']
            }

            # contains all entity version uuids to delete them from payload
            # in case when target project already has such fork in tools
            to_delete_uuids: dict = {'import_uuids': set(), 'import_version_uuids': set()}

            for original_application_version in original_application.versions:
                if original_application_version.id in input_application_versions:
                    new_version_data, to_delete_version_tool_uuids = self._prepare_version(
                        original_application_version, input_application_versions, new_application['owner_id']
                    )
                    new_application['versions'].append(new_version_data)
                    to_delete_uuids['import_uuids'].update(
                        to_delete_version_tool_uuids['import_uuids']
                    )
                    to_delete_uuids['import_version_uuids'].update(
                        to_delete_version_tool_uuids['import_version_uuids']
                    )

            if not new_application['versions']:
                return {'message': f'No versions were found for the application: {self._input_data["id"]}'}, {}, 400

            new_application['entity'] = self._input_data['entity']
            new_application['original_exported'] = self._input_data['original_exported']
            new_application['import_uuid'] = self._input_data['import_uuid']

            if new_application['original_exported']:
                new_application.pop('id')

            return new_application, to_delete_uuids, 200

    def get_target_toolkit_by_import_uuid(self, toolkit_import_uuid: str) -> dict | None:
        for toolkit in self._fork_input_entities:
            if toolkit['entity'] == 'toolkits':
                if toolkit['import_uuid'] == toolkit_import_uuid:
                    return toolkit
        return None

    def _prepare_version(self, original_version, input_application_versions, owner_id) -> Tuple[Dict, Dict]:
        new_version = original_version.to_json()
        new_version['tags'] = [i.to_json() for i in original_version.tags]

        input_version = input_application_versions[original_version.id]
        new_version['variables'] = input_version.get('variables', [])

        tools = []
        to_delete_uuids: set = set()
        to_delete_version_uuids: set = set()
        for tool in input_version.get('tools', []):
            # remove from payload import_version_uuid if tool is fork
            # and target project has this fork
            target_toolkit = self.get_target_toolkit_by_import_uuid(tool.get('import_uuid'))
            meta = target_toolkit.get('meta') or {}
            if 'parent_entity_id' in meta:
                to_delete_uuid = rpc_tools.RpcMixin().rpc.call.applications_find_existing_toolkit_fork(
                    target_project_id=self._target_project_id,
                    parent_entity_id=meta.get('parent_entity_id'),
                    parent_project_id=meta.get('parent_project_id'),
                )
                if to_delete_uuid:
                    to_delete_version_uuids.add(to_delete_uuid)
                if nested_import_uuid := target_toolkit['settings'].get('import_uuid'):
                    to_delete_version_uuids.add(nested_import_uuid)
            else:
                tool_data, to_delete_uuid = self._update_tool_if_fork_exists(target_toolkit, owner_id)
            if to_delete_uuid:
                to_delete_uuids.add(target_toolkit['import_uuid'])
                if target_toolkit['type'] == 'datasource':
                    to_delete_uuids.add(to_delete_uuid)
                else:
                    to_delete_version_uuids.add(to_delete_uuid)
            else:
                tools.append(tool)

        new_version.update({
            'llm_settings': input_version.get('llm_settings'),
            'import_version_uuid': input_version.get('import_version_uuid'),
            'tools': tools,
        })

        meta = new_version.get('meta', {}) or {}
        meta['icon_meta'] = {}

        parent_entity_id = meta.get('parent_entity_id')
        parent_project_id = meta.get('parent_project_id')

        if not parent_entity_id and not parent_project_id:
            meta = self._update_meta_with_fork_details(meta, new_version, original_version)
            new_version['meta'] = meta

        return new_version, {
            'import_uuids': to_delete_uuids, 'import_version_uuids': to_delete_version_uuids
        }

    def _update_meta_with_fork_details(self, meta: dict, new_version, original_version) -> Dict:
        if 'parent_entity_id' not in meta:
            shared_id = new_version.get('shared_id')
            shared_owner_id = new_version.get('shared_owner_id')
            if shared_id and shared_owner_id:
                parent_entity_id = shared_id
                parent_project_id = shared_owner_id
            else:
                parent_entity_id = self._input_data['id']
                parent_project_id = self._input_data['owner_id']
            meta.update({
                'parent_entity_id': parent_entity_id,
                'parent_entity_version_id': original_version.id,
                'parent_project_id': parent_project_id,
                'parent_author_id': original_version.author_id,
            })
        return meta

    def _update_tool_if_fork_exists(
            self, input_tool: dict, owner_id: int
    ) -> Tuple[Dict, str]:
        tool_parent_entity_id = None
        for obj in self._fork_input_entities:
            if obj['entity'] == 'datasources':
                target_uuid = input_tool['settings'].get('import_uuid')
                if obj['import_uuid'] == target_uuid:
                    tool_parent_entity_id = obj.get('id')
                    break
            else:
                target_uuid = input_tool['settings'].get('import_version_uuid')
                for ver in obj.get('versions', []):
                    if ver.get('import_version_uuid') == target_uuid:
                        tool_parent_entity_id = obj.get('id')
                        break

        if tool_parent_entity_id is None:
            return input_tool, str()

        tool_parent_project_id = owner_id

        if not tool_parent_entity_id and not tool_parent_project_id:
            return input_tool, str()

        tool_update_map = {
            'application': 'applications_update_tool_with_existing_fork',
            'prompt': 'prompt_lib_update_tool_with_existing_fork',
            'datasource': 'datasources_update_tool_with_existing_fork',
        }
        tool_type = input_tool.get('type')

        rpc_call = rpc_tools.RpcMixin().rpc.call
        update_function = tool_update_map.get(tool_type)
        if update_function:
            return getattr(rpc_call, update_function)(
                self._target_project_id, input_tool, tool_parent_entity_id, tool_parent_project_id
            )
        else:
            return input_tool, str()
