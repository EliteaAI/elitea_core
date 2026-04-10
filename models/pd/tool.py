import re
from datetime import datetime
from typing import Optional, Any, Dict, List, ClassVar

from pydantic import BaseModel, Field, model_validator, model_serializer, ConfigDict, ValidationInfo
from pylon.core.tools import log

from tools import auth, this, serialize, db

from ..all import Application, ApplicationVersion
from ...models.enums.all import ToolEntityTypes
from ...models.pd.collection_base import AuthorBaseModel
from ...utils.authors import get_authors_data
from ...utils.toolkits_utils import get_mcp_schemas


sanitization_pattern = re.compile(r'[^a-zA-Z0-9_.-]')

SENSITIVE_KEYS = [
    'access_key', 'password', 'username',
    'api_key', 'access_token', 'token', 'api_key_type',
    'app_private_key', 'google_cse_id', 'google_api_key',
    'app_id', 'client_secret', 'gitlab_personal_access_token',
    'gitlab_personal_access_token', 'private_token',
    'sonar_token', 'qtest_api_token', 'client_id',
    'oauth2', 'slack_token',
    # Database and connection secrets
    'connection_string', 'database_password', 'db_password',
    'secret_key', 'secret', 'credentials',
    # AWS and cloud secrets
    'aws_secret_access_key', 'aws_access_key_id',
    'azure_key', 'gcp_key',
]

_PROVIDER_HUB_KEY_PREFIX = "toolkit_configuration_"


class ToolBase(BaseModel):
    type: str
    name: Optional[str] = None
    description: Optional[str] = None
    author_id: int
    settings: Any | dict = {}
    meta: Optional[dict] = {}

    model_config = ConfigDict(from_attributes=True)

    def fix_name(self, project_id: int) -> None:
        settings = self.settings
        if not isinstance(settings, dict):
            settings = serialize(self.settings)

        entity_name: Optional[str] = None

        try:
            if self.type == 'application':
                entity_id = settings.get('application_id')
                with db.get_session(project_id) as session:
                    entity_name = session.query(Application.name).where(
                        Application.id == entity_id
                    ).first()
                    if entity_name:
                        entity_name = entity_name[0]
        except Exception as e:
            log.exception('ToolBase.fix_name')
            log.warning(f"Can not read details for type={self.type} {settings=}")

        if entity_name:
            self.name = entity_name
        elif self.name is None and hasattr(self, 'toolkit_name'):
            self.name = self.toolkit_name


class ToolDetails(ToolBase):
    id: int
    toolkit_name: Optional[str] = None
    author: Optional[AuthorBaseModel] = None
    agent_type: Optional[str] = None
    created_at: datetime
    online: Optional[bool] = None
    icon_meta: Optional[dict] = None
    variables: Optional[List] = []
    is_pinned: bool = False

    def check_is_pinned(self, project_id: int) -> 'ToolDetails':
        try:
            from queue import Empty
            from tools import rpc_tools
            self.is_pinned = rpc_tools.RpcMixin().rpc.timeout(2).social_is_pinned(
                project_id=project_id, entity='toolkit', entity_id=self.id
            )
        except Empty:
            self.is_pinned = False
        return self

    @model_validator(mode='after')
    def add_author_data(self, info: ValidationInfo) -> 'ToolDetails':
        if self.author is None and self.author_id:
            # Check for pre-fetched authors in validation context (batch optimization)
            authors_map = info.context.get('authors_map') if info.context else None
            if authors_map is not None:
                # Batch mode: use pre-fetched data, don't fetch individually if not found
                if self.author_id in authors_map:
                    self.author = AuthorBaseModel(**authors_map[self.author_id])
                # If not in map, skip (author is optional for tools in app details)
            else:
                # No context provided - fallback to individual fetch for single-item cases
                authors_data: list = get_authors_data(author_ids=[self.author_id])
                if authors_data:
                    self.author = AuthorBaseModel(**authors_data[0])
        return self

    def set_agent_type(self, project_id: int) -> None:
        from tools import db
        if self.type == 'application':
            try:
                with db.get_session(project_id) as session:
                    agent_type = session.query(
                        ApplicationVersion.agent_type
                    ).filter(
                        ApplicationVersion.id == self.settings['application_version_id'],
                    ).first()
                    self.agent_type = agent_type[0] if agent_type else None
            except:
                log.exception("Failed to get agent type")

    @model_validator(mode='after')
    def set_toolkit_name(self) -> 'ToolDetails':
        if self.type == 'datasource':
            cleaned_string = re.sub(
                sanitization_pattern, '', str(self.name)
            ).replace('.', '_')
            self.toolkit_name = cleaned_string
            return self
        elif self.type in {'application', 'prompt'}:
            return self
        
        from pylon.core.tools import log
        from ...utils.application_tools import find_suggested_toolkit_name_field, find_suggested_toolkit_max_length
        log.debug(f"set_toolkit_name validator: type={self.type}, name={self.name}")

        suggested_toolkit_key = None
        suggested_toolkit_name = None
        for_configuration = False
        settings = {}
        
        if self.settings:
            if not isinstance(self.settings, dict):
                settings = self.settings.model_dump()
            else:
                settings = self.settings
        
        if settings:
            for_configuration = bool(settings.get("elitea_title"))
        
        if for_configuration:
            suggested_toolkit_key = find_suggested_toolkit_name_field(
                toolkit_type=self.type,
                for_configuration=for_configuration
            )
        
        if suggested_toolkit_key is None:
            suggested_toolkit_key = find_suggested_toolkit_name_field(toolkit_type=self.type)

        if suggested_toolkit_key is not None:
            suggested_toolkit_name = settings.get(suggested_toolkit_key)

        if suggested_toolkit_name is None:
            suggested_toolkit_name = self.name
        
        if suggested_toolkit_name is None:
            return self

        suggested_max_toolkit_length = find_suggested_toolkit_max_length(toolkit_type=self.type)

        if suggested_max_toolkit_length is None:
            suggested_max_toolkit_length = 0
        else:
            try:
                suggested_max_toolkit_length = int(suggested_max_toolkit_length)
            except Exception:
                log.error(f'Wrong max_toolkit_length from '
                          f'elitea_tools at {self.type}::{suggested_toolkit_key}')
                suggested_max_toolkit_length = 0

        cleaned_string = re.sub(
            sanitization_pattern, '', str(suggested_toolkit_name)
        ).replace('.', '_')
        
        self.toolkit_name = cleaned_string[:suggested_max_toolkit_length] if suggested_max_toolkit_length > 0 else cleaned_string
        return self

    def set_online(self, project_id: int, mcp_schemas: dict = None) -> None:
        # Check if this is an MCP toolkit (either by meta flag or type)
        is_mcp_toolkit = (self.meta and self.meta.get('mcp') is True) or (self.type == 'mcp')

        if is_mcp_toolkit:
            # Use pre-fetched schemas if provided (avoids N+1 queries)
            if mcp_schemas is not None:
                self.online = self.type in mcp_schemas
            else:
                try:
                    # TODO: pass user_id directly to get_all_toolkits
                    current_user = auth.current_user()
                    user_id = current_user['id']
                except Exception:
                    log.warning(f"Failed to set online status for toolkit id={self.id} {project_id=}: outside of context")
                else:
                    available_tools = get_mcp_schemas(project_id, user_id).keys()
                    self.online = self.type in available_tools

    def set_agent_meta_and_fields(self, project_id: int) -> None:
        settings = self.settings
        if self.type == 'application':
            entity_id = settings.get('application_id')
            entity_version_id = settings.get('application_version_id')
            with db.get_session(project_id) as session:
                entity_version = session.query(ApplicationVersion).where(
                    ApplicationVersion.application_id == entity_id,
                    ApplicationVersion.id == entity_version_id
                ).first()
                if entity_version:
                    if isinstance(entity_version.meta, dict):
                        self.icon_meta = entity_version.meta.get('icon_meta')
                    if entity_version.variables:
                        from .version import ApplicationVariableModel
                        self.variables = [
                            ApplicationVariableModel(name=var.name, value=var.value)
                            for var in entity_version.variables
                        ]


# Used in PATCH version, to construct correct configuration model
# which uses its expand configuration method later
class ToolValidatedDetails(ToolDetails):
    project_id: int = Field(..., exclude=True)
    user_id: int = Field(..., exclude=True)

    # Subclasses can override this to skip strict SDK validation (e.g., for exports)
    _skip_toolkit_validation: ClassVar[int] = False
    # Subclasses can override this to skip configuration expansion (e.g., for exports)
    _skip_configuration_expansion: ClassVar[int] = False

    # Sentinel key used to carry connection_errors through Pydantic's ValueError
    # so callers can distinguish them from settings validation errors.
    CONNECTION_ERROR_SENTINEL: ClassVar[str] = '__connection_errors__'

    @model_validator(mode='before')
    @classmethod
    def validate_settings(cls, values):
        from ...utils.application_tools import (
            expand_toolkit_settings,
            ValidatorNotSupportedError,
            raise_validation_error_if_any,
            ConfigurationExpandError
        )

        try:
            type_ = values['type']
            settings = values['settings']
            project_id = values['project_id']
            user_id = values['user_id']
        except KeyError as ex:
            raise ValueError(f"Missing {ex}")

        # Skip configuration expansion during export - we want to export raw references
        # Configuration expansion would fail if the referenced configurations don't exist
        if cls._skip_configuration_expansion:
            return values

        try:
            values['settings'] = expand_toolkit_settings(type_, settings, project_id, user_id)
        except ValidatorNotSupportedError as ex:
            log.warning(ex)
        except ConfigurationExpandError as ex:
            raise_validation_error_if_any(ex.errors, ToolValidatedDetails)
        except Exception as e:
            raise ValueError(f"Error validating settings: {e}")
        else:
            # Skip strict SDK validation if flag is set (e.g., during export)
            # Exports should serialize existing data without validating against SDK schemas
            # which may be out of sync with the stored data
            if not cls._skip_toolkit_validation:
                validation_result = this.module.toolkit_settings_validator(values['settings'], type_=type_, project_id=project_id, user_id=user_id)
                if not validation_result['ok']:
                    raise_validation_error_if_any(validation_result['error'], ToolValidatedDetails)

        return values

    @model_validator(mode='after')
    def check_connection(self, info: ValidationInfo) -> 'ToolValidatedDetails':
        """
        Run connection check using the already-expanded self.settings.
        Only executes when mcp_tokens (or an explicit check_connection=True flag)
        is present in the Pydantic validation context, which is set by
        validate_toolkit_details() in application_utils.py.

        Raises ValueError with a structured sentinel payload so the caller
        can re-raise it as ToolkitConnectionError and keep connection_errors
        separate from settings_errors in the API response.
        """
        context = info.context if info else {}
        if not context or not context.get('check_connection'):
            return self

        from ...utils.application_utils import _check_configurations_connection_from_expanded_settings

        mcp_tokens = context.get('mcp_tokens', {})

        connection_errors = _check_configurations_connection_from_expanded_settings(
            expanded_settings=self.settings,
            mcp_tokens=mcp_tokens,
        )
        if connection_errors:
            # Wrap in a sentinel ValueError so validate_toolkit_details can
            # intercept and raise ToolkitConnectionError instead.
            raise ValueError({self.CONNECTION_ERROR_SENTINEL: connection_errors})

        return self

    model_config = ConfigDict(from_attributes=False)


class ToolExportBase(ToolValidatedDetails):
    import_uuid: str = None

    # Skip strict SDK validation during export - we're serializing existing data,
    # not creating new data. The SDK schema may not match stored tool selections.
    _skip_toolkit_validation: ClassVar[int] = True
    # Skip configuration expansion during export - export raw references
    # so they can be imported into different projects with different configurations
    _skip_configuration_expansion: ClassVar[int] = True

    @model_validator(mode='after')
    def validate_repeatable_uuid(self):
        from ...utils.export_import import generate_repeatable_uuid

        self.import_uuid = generate_repeatable_uuid(
            prefix='ToolExportBase',
            values=self.settings,
            suffix=self.name or ''
        )
        return self


# Full validate_settings is required here to correclty generate unique import uuid
class ToolApplicationExportDetails(ToolExportBase):
    ''' In-application tool export model by ref only '''
    import_uuid: str = Field(None, exclude=False)  # Override to include

    @model_serializer
    def ser_model(self):
        return {'import_uuid': self.import_uuid}

    model_config = ConfigDict(from_attributes=False)


class ToolExportDetails(ToolExportBase):
    ''' Standalone tool export model '''
    description: Optional[str] = Field(None, exclude=True)
    toolkit_name: Optional[str] = Field(None, exclude=True)

    @staticmethod
    def remove_keys(obj: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        new_obj = {}
        if isinstance(obj, BaseModel):
            obj = obj.model_dump()
        for key, val in obj.items():
            if key in keys:
                continue
            # Also strip provider-hub prefixed sensitive fields.
            # Check exact match (e.g. "toolkit_configuration_api_key" -> "api_key")
            # and suffix match (e.g. "toolkit_configuration_git_password" -> ends with "_password")
            if key.startswith(_PROVIDER_HUB_KEY_PREFIX):
                unprefixed = key[len(_PROVIDER_HUB_KEY_PREFIX):]
                if unprefixed in keys or any(unprefixed.endswith('_' + k) for k in keys):
                    continue
            if isinstance(val, dict):
                val = ToolExportDetails.remove_keys(val, keys)
            elif isinstance(val, list):
                val = [ToolExportDetails.remove_keys(i, keys)
                       if isinstance(i, dict) else i for i in val]
            new_obj[key] = val
        return new_obj

    @model_validator(mode='after')
    def remove_sensitive_fields(self):
        self.settings = self.remove_keys(self.settings, SENSITIVE_KEYS)
        return self

    model_config = ConfigDict(from_attributes=False)


class ToolForkDetails(ToolExportDetails):
    meta: Optional[dict] = {}
    author_id: int
    owner_id: Optional[int] = None


class ToolCreateModel(ToolBase):
    name: str = Field(..., min_length=1)
    id: Optional[int] = None
    project_id: int = Field(..., exclude=True)
    user_id: int = Field(..., exclude=True)

    @model_validator(mode='before')
    @classmethod
    def validate_settings(cls, values):
        from ...utils.application_tools import (
            expand_toolkit_settings,
            ValidatorNotSupportedError,
            raise_validation_error_if_any,
            ConfigurationExpandError
        )

        try:
            type_ = values['type']
            settings = values['settings']
            project_id = values['project_id']
            user_id = values['user_id']
        except KeyError as ex:
            raise ValueError(f"Missing {ex}")

        try:
            settings = expand_toolkit_settings(type_, settings, project_id, user_id)
        except ValidatorNotSupportedError as ex:
            log.warning(ex)
        except ConfigurationExpandError as ex:
            raise_validation_error_if_any(ex.errors, ToolValidatedDetails)
        except Exception as e:
            raise ValueError(f"Error validating settings: {e}")
        else:
            validation_result = this.module.toolkit_settings_validator(settings, type_=type_, project_id=project_id, user_id=user_id)
            if not validation_result['ok']:
                raise_validation_error_if_any(validation_result['error'], ToolValidatedDetails)
        return values


class ToolImportModel(ToolCreateModel):
    author_id: int
    meta: Optional[dict] = {}


class ToolUpdateModel(ToolCreateModel):
    name: str = Field(..., min_length=1)
    id: Optional[int] = None


class ToolChatModel(ToolBase):
    id: Optional[int] = None


class ToolAPIUpdateModel(ToolCreateModel):
    pass


class ToolUpdateRelationModel(BaseModel):
    entity_id: int
    entity_version_id: int
    entity_type: ToolEntityTypes
    has_relation: bool = False
    selected_tools: Optional[List[str]] = None  # List of tool names to allow from this toolkit


class AttachmentToolkitRequestModel(BaseModel):
    toolkit_id: Optional[int] = Field(...)


class TestToolInputModel(BaseModel):
    input: Optional[list | str] = []
    output: Optional[list] = []
    input_mapping: Optional[dict] = {}
    input_variables: Optional[list] = []
    testing_name: Optional[str] = 'TestingToolNode'
    transition: Optional[str] = 'END'
    structured_output: Optional[bool] = False
    sid: Optional[str] = None
    tool: Optional[str] = None
    user_input: Optional[str] = None

