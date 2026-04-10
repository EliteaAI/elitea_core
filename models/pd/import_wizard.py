import uuid
from copy import deepcopy
from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, model_validator, Field, ConfigDict


class ImportData(BaseModel):
    import_uuid: str = Field(exclude=True)
    entity: str
    name: str
    description: str
    is_selected: bool = False

    @model_validator(mode='before')
    @classmethod
    def generate_import_uuid(cls, values):
        if isinstance(values, dict) and not values.get('import_uuid'):
            values['import_uuid'] = str(uuid.uuid4())
        return values

    def map_postponed_ids(self, imported_entity):
        return {}


class ImportVersionModel(BaseModel):
    name: str
    import_version_uuid: str = Field(exclude=True)

    model_config = ConfigDict(extra='allow')

    @model_validator(mode='before')
    @classmethod
    def generate_import_version_uuid(cls, values):
        if isinstance(values, dict) and not values.get('import_version_uuid'):
            values['import_version_uuid'] = str(uuid.uuid4())
        return values


class DatasourcesImport(ImportData):
    versions: List[dict]
    embedding_model: str
    embedding_model_settings: dict
    storage: str
    storage_settings: Optional[dict] = {}
    meta: Optional[dict] = {}

    def map_postponed_ids(self, imported_entity):
        return {
            self.import_uuid: imported_entity['id']
        }


class SelfImportToolSettings(BaseModel):
    import_uuid: str
    import_version_uuid: str
    variables: List[dict]


class DatasourceImportToolSettings(BaseModel):
    datasource_id: int
    selected_tools: list


class DatasourceSelfImportToolSettings(BaseModel):
    import_uuid: str
    selected_tools: list


class ApplicationImportToolSettings(BaseModel):
    application_id: int
    application_version_id: int
    variables: List[dict]


class ToolImportModelBase(ImportData):
    name: Optional[str] = None
    description: Optional[str] = None
    meta: Optional[dict] = {}

    @property
    def not_imported_yet_tool(self):
        return hasattr(self.settings, 'import_uuid')

    def dict_import_uuid_resolved(self, postponed_id_mapper):
        tool = self.dict()

        if self.not_imported_yet_tool:
            import_uuid = tool['settings'].pop('import_uuid')
            import_version_uuid = tool['settings'].pop('import_version_uuid', None)
            tool_type = tool['type']
            tool_id_key = f'{tool_type}_id'
            tool_version_id_key = f'{tool_type}_version_id'
            try:

                tool['settings'][tool_id_key] = postponed_id_mapper[import_uuid]
                if import_version_uuid:
                    tool['settings'][tool_version_id_key] = postponed_id_mapper[import_version_uuid]
            except KeyError:
                toolkit_import_uuid = self.import_uuid
                raise RuntimeError(
                   f"Unable to link {toolkit_import_uuid=} to {tool_type} {import_uuid=}({import_version_uuid=})") from None

        return tool

    def map_postponed_ids(self, imported_entity):
        return {
            self.import_uuid: imported_entity['id']
        }


class ApplicationToolImportModel(ToolImportModelBase):
    type: Literal['application']
    settings: SelfImportToolSettings | ApplicationImportToolSettings


class DatasourceToolImportModel(ToolImportModelBase):
    type: Literal['datasource']
    settings: DatasourceSelfImportToolSettings | DatasourceImportToolSettings


class OtherToolImportModel(ToolImportModelBase):
    type: str = Field(pattern=r'^(?!application|datasource).*')
    settings: dict

    model_config = ConfigDict(regex_engine='python-re')


class ToolImportModel(BaseModel):
    import_data: ApplicationToolImportModel | DatasourceToolImportModel | OtherToolImportModel = Field(union_mode='left_to_right')

    @model_validator(mode='before')
    def to_import_data(cls, values):
        return {'import_data': values}


class ApplicationSelfImportTool(BaseModel):
    import_uuid: str


class ApplicationExistingImportTool(BaseModel):
    id: int


class EmbeddedToolkitConfig(BaseModel):
    """Embedded toolkit config from MD import - needs to be created during import"""
    import_uuid: str = Field(exclude=True)  # Generated UUID for linking
    type: str
    name: Optional[str] = None
    toolkit_name: Optional[str] = None
    description: Optional[str] = None
    settings: dict = {}
    selected_tools: Optional[list] = []
    meta: Optional[dict] = {}
    # For application-type toolkits (nested agents): reference to the agent's import_uuid
    application_import_uuid: Optional[str] = None
    # For application-type toolkits (nested agents): name-based matching fallback (from MD import)
    application_name: Optional[str] = None
    # For application-type toolkits (nested agents): specific version name to link to
    application_version: Optional[str] = None

    @model_validator(mode='before')
    @classmethod
    def generate_import_uuid(cls, values):
        if isinstance(values, dict) and not values.get('import_uuid'):
            values['import_uuid'] = str(uuid.uuid4())
        return values


class AgentsImportVersion(ImportVersionModel):
    tools: List[ApplicationExistingImportTool] = []
    postponed_tools: List[ApplicationSelfImportTool] = Field(default_factory=list, exclude=True)
    embedded_toolkits: List[EmbeddedToolkitConfig] = Field(default_factory=list, exclude=True)
    # Explicitly declare meta to ensure it's properly serialized during import
    meta: Optional[dict] = Field(default_factory=dict)

    @model_validator(mode='before')
    @classmethod
    def split_tools_by_refs(cls, values):
        # Generate import_version_uuid if not provided (since child validators run before parent)
        if isinstance(values, dict) and not values.get('import_version_uuid'):
            values['import_version_uuid'] = str(uuid.uuid4())

        clean_tools = []
        postponed_tools = []
        embedded_toolkits = []
        for tool in values.get('tools', []):
            if 'import_uuid' in tool:
                # Reference to toolkit being imported separately
                postponed_tools.append(tool)
            elif 'id' in tool:
                # Reference to existing toolkit
                clean_tools.append(tool)
            elif 'type' in tool:
                # Embedded toolkit config (from MD import) - needs to be created
                # Generate import_uuid for linking later
                toolkit_config = dict(tool)
                if 'import_uuid' not in toolkit_config:
                    toolkit_config['import_uuid'] = str(uuid.uuid4())
                embedded_toolkits.append(toolkit_config)
                # Add to postponed_tools so it gets linked after creation
                postponed_tools.append({'import_uuid': toolkit_config['import_uuid']})
            else:
                # Empty or unknown tool format - skip silently
                continue

        values['tools'] = clean_tools
        values['postponed_tools'] = postponed_tools
        values['embedded_toolkits'] = embedded_toolkits

        return values


class AgentsImport(ImportData):
    versions: List[AgentsImportVersion]
    owner_id: Optional[int] = None
    shared_id: Optional[int] = None
    shared_owner_id: Optional[int] = None

    def has_postponed_toolkits(self):
        for version in self.versions:
            if version.postponed_tools:
                return True

    def has_embedded_toolkits(self):
        """Check if any version has embedded toolkit configs that need to be created"""
        for version in self.versions:
            if version.embedded_toolkits:
                return True
        return False

    def get_all_embedded_toolkits(self):
        """Get all embedded toolkits from all versions (deduplicated by import_uuid)"""
        seen = set()
        toolkits = []
        for version in self.versions:
            for toolkit in version.embedded_toolkits:
                if toolkit.import_uuid not in seen:
                    seen.add(toolkit.import_uuid)
                    toolkits.append(toolkit)
        return toolkits

    def map_postponed_ids(self, imported_entity: dict):
        ''' Map import_uuid with real id/version_id of app stored in db'''

        postponed_id_mapper = {
            self.import_uuid: imported_entity['id']
        }

        # First pass: exact name matching
        matched_model_indices = set()
        matched_entity_indices = set()

        for i, version in enumerate(self.versions):
            for j, imported_version in enumerate(imported_entity['versions']):
                if j in matched_entity_indices:
                    continue
                # find by unique version name within one entity id
                if version.name == imported_version['name']:
                    postponed_id_mapper[version.import_version_uuid] = imported_version['id']
                    matched_model_indices.add(i)
                    matched_entity_indices.add(j)
                    break

        # Second pass: handle versions renamed by base-version synthesis.
        # When a non-base version (e.g. "v3") is the only version, the import
        # renames it to "base".  The Pydantic model still holds the original
        # name, so exact matching fails.  Pair remaining unmatched versions
        # positionally when there is exactly one on each side.
        unmatched_model = [
            (i, v) for i, v in enumerate(self.versions)
            if i not in matched_model_indices
        ]
        unmatched_entity = [
            (j, v) for j, v in enumerate(imported_entity['versions'])
            if j not in matched_entity_indices
        ]

        if len(unmatched_model) == 1 and len(unmatched_entity) == 1:
            model_version = unmatched_model[0][1]
            entity_version = unmatched_entity[0][1]
            postponed_id_mapper[model_version.import_version_uuid] = entity_version['id']

        return postponed_id_mapper

    @model_validator(mode='before')
    @classmethod
    def ensure_base_version(cls, values):
        # Generate import_uuid if not provided (since child validators run before parent)
        if isinstance(values, dict) and not values.get('import_uuid'):
            values['import_uuid'] = str(uuid.uuid4())

        # Note: "ensure base version exists" logic is handled in applications_import_application
        # (rpc/application.py) - do NOT duplicate that logic here as it causes duplicate versions
        return values


IMPORT_MODEL_ENTITY_MAPPER = {
    'agents': AgentsImport,
    'toolkits': ToolImportModel
}

DEPRECATED_ENTITIES = ['prompts', 'prompt', 'datasources', 'datasource']
