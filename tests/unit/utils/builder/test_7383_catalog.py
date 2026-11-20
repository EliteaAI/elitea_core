"""#7383 AC-17, AC-18, AC-24: the operation catalog — discovery, exact schema, pre-dispatch validation,
scope filling and the destructive class."""
import pytest


API_TOOLS = [
    {'value': 'patch_tool_mode_project_id_tool_id', 'method': 'PATCH',
     'path': '/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}', 'tags': ['elitea_core/toolkits'],
     'description': 'Link Agent to Toolkit: attach or detach a toolkit to an agent version.',
     'args_schema': {
         'type': 'object', 'additionalProperties': False,
         'properties': {'project_id': {'type': 'integer'}, 'tool_id': {'type': 'integer'},
                        'entity_version_id': {'type': 'integer'}, 'has_relation': {'type': 'boolean'}},
         'required': ['project_id', 'tool_id', 'entity_version_id'],
     },
     'parameters': [{'name': 'project_id', 'in': 'path'}, {'name': 'tool_id', 'in': 'path'}]},
    {'value': 'get_toolkits_mode_project_id', 'method': 'GET',
     'path': '/api/v2/elitea_core/toolkits/{mode}/{project_id}', 'tags': ['elitea_core/toolkits'],
     'description': 'List Toolkits in the project.',
     'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'}}, 'required': ['project_id']},
     'parameters': [{'name': 'project_id', 'in': 'path'}]},
    {'value': 'delete_artifacts_mode_project_id_bucket', 'method': 'DELETE',
     'path': '/api/v2/artifacts/artifacts/{mode}/{project_id}/{bucket}', 'tags': ['artifacts'],
     'description': 'Delete Artifacts from a bucket.',
     'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'}, 'bucket': {'type': 'string'}},
                     'required': ['project_id', 'bucket']},
     'parameters': [{'name': 'project_id', 'in': 'path'}, {'name': 'bucket', 'in': 'path'}]},
    {'value': 'put_secret_mode_project_id_secret', 'method': 'PUT',
     'path': '/api/v2/secrets/secret/{mode}/{project_id}/{secret}', 'tags': ['secrets'],
     'description': 'Update Secret value.',
     'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'}, 'secret': {'type': 'string'},
                                                      'value': {'type': 'string'}},
                     'required': ['project_id', 'secret', 'value']},
     'parameters': [{'name': 'project_id', 'in': 'path'}, {'name': 'secret', 'in': 'path'}]},
    {'value': 'get_notifications', 'method': 'GET', 'path': '/api/v2/notifications/x', 'tags': ['notifications'],
     'description': 'Not a builder tag.', 'args_schema': {}, 'parameters': []},
]


@pytest.fixture
def catalog(builder):
    return builder.catalog.Catalog(API_TOOLS)


class TestDiscovery:

    def test_only_builder_tags_are_listed(self, catalog):
        assert 'get_notifications' not in {op['op_id'] for op in catalog.find('')}

    def test_ranking_puts_the_best_match_first(self, catalog):
        results = catalog.find('link toolkit to agent')
        assert results[0]['op_id'] == 'patch_tool_mode_project_id_tool_id'
        assert results[0]['kind'] == 'write'

    def test_kind_is_read_write_or_destructive(self, catalog):
        kinds = {op['op_id']: op['kind'] for op in catalog.find('')}
        assert kinds['get_toolkits_mode_project_id'] == 'read'
        assert kinds['delete_artifacts_mode_project_id_bucket'] == 'destructive'
        assert kinds['put_secret_mode_project_id_secret'] == 'destructive'

    def test_limit(self, catalog):
        assert len(catalog.find('', limit=2)) == 2


class TestSchema:

    def test_schema_hides_session_scoped_params(self, catalog):
        schema = catalog.schema('patch_tool_mode_project_id_tool_id')
        assert 'project_id' not in schema['properties']
        assert 'project_id' not in schema['required']
        assert schema['required'] == ['tool_id', 'entity_version_id']

    def test_unknown_operation(self, builder, catalog):
        with pytest.raises(builder.catalog.UnknownOperation):
            catalog.schema('drop_everything')


class TestValidate:

    def test_valid_args_fill_project_from_session(self, catalog):
        args, errors = catalog.validate(
            'patch_tool_mode_project_id_tool_id', {'tool_id': 3, 'entity_version_id': 9}, project_id=12)
        assert errors == []
        assert args == {'tool_id': 3, 'entity_version_id': 9, 'project_id': 12}

    def test_missing_and_extra_fields_are_reported(self, catalog):
        _, errors = catalog.validate('patch_tool_mode_project_id_tool_id', {'tool_id': 3, 'foo': 1}, project_id=12)
        assert {'field': 'entity_version_id', 'message': 'required'} in errors
        assert {'field': 'foo', 'message': 'not allowed'} in errors

    def test_wrong_type(self, catalog):
        _, errors = catalog.validate(
            'patch_tool_mode_project_id_tool_id', {'tool_id': 'three', 'entity_version_id': 9}, project_id=12)
        assert errors == [{'field': 'tool_id', 'message': 'must be integer'}]

    def test_other_project_is_refused(self, catalog):
        _, errors = catalog.validate(
            'patch_tool_mode_project_id_tool_id', {'tool_id': 3, 'entity_version_id': 9, 'project_id': 99},
            project_id=12)
        assert errors == [{'field': 'project_id', 'message': 'only the current project (12) can be changed'}]


def test_keeps_registry_tools_that_carry_no_tags(builder):
    """The registry filters by tag itself and its MCP tool dicts have no `tags` key."""
    tools = [{k: v for k, v in tool.items() if k != 'tags'} for tool in API_TOOLS[:3]]
    assert len(builder.catalog.Catalog(tools).find('')) == 3
