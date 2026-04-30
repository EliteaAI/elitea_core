"""
Internal tools injection utilities for chat conversations.

Handles dynamic injection of internal tools (like image generation, attachments)
into predict payloads based on conversation settings.
"""

from typing import Optional

from pylon.core.tools import log
from tools import VaultClient, rpc_tools, this


# ImageGen Constants
IMAGEGEN_PROVIDER_TYPE = 'ImageGenServiceProvider_ImageGen'
IMAGEGEN_TOOLKIT_NAME = 'ImageGen'
IMAGEGEN_DEFAULT_BUCKET = 'imagelibrary'
IMAGEGEN_INTERNAL_TOOL_KEY = 'image_generation'
IMAGEGEN_PROVIDER_NAME = 'ImageGenServiceProvider'

# Attachment Constants
ATTACHMENT_TOOLKIT_TYPE = 'artifact'
ATTACHMENT_TOOLKIT_NAME = 'Attachments'
ATTACHMENT_DEFAULT_BUCKET = 'attachments'
ATTACHMENT_INTERNAL_TOOL_KEY = 'attachments'
# TODO: clarify correct set of default tools
ATTACHMENT_DEFAULT_SELECTED_TOOLS = [
    'list_files',
    'read_file',
    'read_multiple_files',
]


class ImageGenConfigurationError(Exception):
    """Raised when image generation cannot be configured properly."""
    pass


def is_imagegen_provider_available(user_id: int, project_id: int) -> bool:
    """
    Check if ImageGen provider is available using the provider lookup mechanism.
    
    This uses the existing lookup_provider() method which:
    - Checks personal project, current project, and public project
    - Only returns providers that passed health check during init
    - Returns a descriptor if healthy provider exists, None otherwise
    
    Args:
        user_id: User ID for project expansion (personal/public projects)
        project_id: Current project ID
        
    Returns:
        True if a healthy ImageGen provider is available, False otherwise
    """
    try:
        provider_descriptor = this.module.lookup_provider(
            user_id=user_id,
            project_id=project_id,
            provider_name=IMAGEGEN_PROVIDER_NAME
        )
        
        if provider_descriptor:
            log.debug(f"ImageGen provider available: {provider_descriptor.name}")
            return True
        
        log.debug("No healthy ImageGen provider found in project hierarchy")
        return False
        
    except Exception as e:
        log.debug(f"ImageGen provider lookup failed: {e}")
        return False


def get_default_imagegen_model(project_id: int) -> str:
    """
    Get default image generation model from configurations.
    
    Args:
        project_id: Project ID
        
    Returns:
        Model name string
        
    Raises:
        ImageGenConfigurationError: If no default model is configured
    """
    try:
        image_gen_default = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_default_model(
            project_id=project_id,
            section='image_generation',
            include_shared=True
        )
        model_name = image_gen_default.get('model_name') if image_gen_default else None
        if not model_name:
            raise ImageGenConfigurationError(
                f"No default image generation model configured for project {project_id}. "
                "Please configure a default image generation model in project settings."
            )
        log.debug(f"Using image generation model: {model_name}")
        return model_name
        
    except ImageGenConfigurationError:
        raise
    except Exception as e:
        raise ImageGenConfigurationError(
            f"Failed to get default image generation model for project {project_id}: {e}"
        ) from e


def get_default_imagegen_bucket(project_id: int) -> str:
    """
    Get default bucket for image generation artifacts.
    
    Args:
        project_id: Project ID
        
    Returns:
        Bucket name string (defaults to 'imagelibrary' if not configured)
    """
    try:
        vault_client = VaultClient(project_id)
        secrets = vault_client.get_all_secrets()
        bucket = secrets.get('default_imagegen_bucket', IMAGEGEN_DEFAULT_BUCKET)
        log.debug(f"Using image generation bucket: {bucket}")
        return bucket
    except Exception as e:
        log.warning(f"Failed to get default imagegen bucket, using fallback '{IMAGEGEN_DEFAULT_BUCKET}': {e}")
        return IMAGEGEN_DEFAULT_BUCKET


def is_imagegen_toolkit(toolkit_payload: dict) -> bool:
    """
    Check if a toolkit payload is an ImageGen toolkit.
    
    Args:
        toolkit_payload: Toolkit configuration dict
        
    Returns:
        True if this is an ImageGen toolkit
    """
    toolkit_type = toolkit_payload.get('type', '')
    settings = toolkit_payload.get('settings', {})
    
    return (
        toolkit_type == IMAGEGEN_PROVIDER_TYPE or
        IMAGEGEN_TOOLKIT_NAME in toolkit_type or
        settings.get('toolkit') == IMAGEGEN_TOOLKIT_NAME or
        settings.get('provider') == IMAGEGEN_PROVIDER_NAME
    )


def inject_internal_imagegen_tool(
    conversation_meta: dict,
    user_id: int,
    project_id: int,
    existing_tools: list[dict],
    conversation_uuid: str = None,
) -> Optional[dict]:
    """
    Conditionally create auto-injected ImageGen toolkit payload.
    
    This function checks if the image_generation toggle is enabled in conversation
    settings and if so, injects an ImageGen toolkit with project defaults.
    
    Generated images are stored in the same bucket as chat attachments,
    under ``{conversation_uuid}/`` folder for isolation.
    
    The injection is skipped if:
    - Toggle is not enabled in conversation.meta.internal_tools
    - A manual ImageGen toolkit already exists in the conversation
    - The ImageGen provider is not available/healthy
    
    Args:
        conversation_meta: Conversation meta dict containing internal_tools
        user_id: User ID for provider lookup (project expansion)
        project_id: Project ID for configuration lookup
        existing_tools: Already collected manual toolkit payloads
        conversation_uuid: Conversation UUID for folder-based image isolation
        
    Returns:
        ImageGen toolkit payload dict, or None if should not inject
        
    Raises:
        ImageGenConfigurationError: If toggle is ON but no default model is configured
    """
    internal_tools = conversation_meta.get('internal_tools', [])
    
    # 1. Check if image_generation toggle is enabled
    if IMAGEGEN_INTERNAL_TOOL_KEY not in internal_tools:
        log.debug("Image generation internal tool not enabled in conversation")
        return None
    
    # 2. Check if manual ImageGen toolkit already exists
    for tool in existing_tools:
        if is_imagegen_toolkit(tool):
            log.debug(
                f"Manual ImageGen toolkit found (id={tool.get('id')}, name={tool.get('name')}), "
                "skipping auto-injection"
            )
            return None
    
    # 3. Check if provider is available
    if not is_imagegen_provider_available(user_id, project_id):
        log.warning(
            "Image generation toggle is ON but ImageGen provider is not available. "
            "Skipping auto-injection. Please ensure the ImageGen provider is running."
        )
        return None
    
    # 4. Get default model (raises ImageGenConfigurationError if not configured)
    model_name = get_default_imagegen_model(project_id)
    
    # 5. Use attachments bucket so images live alongside chat files
    bucket = get_default_attachment_bucket(project_id)
    
    # 6. Build name_prefix from conversation_uuid (trailing slash = folder)
    name_prefix = f"{conversation_uuid}/" if conversation_uuid else ""
    
    # 7. Build minimal toolkit payload matching manual toolkit structure
    settings = {
        'class': 'Toolkit',
        'module': 'plugins.provider_worker.utils.tools',
        'toolkit': IMAGEGEN_TOOLKIT_NAME,
        'provider': IMAGEGEN_PROVIDER_NAME,
        'selected_tools': ['generate_image'],
        'toolkit_configuration_bucket': bucket,
        'toolkit_configuration_image_generation_model': model_name,
    }
    if name_prefix:
        settings['toolkit_configuration_name_prefix'] = name_prefix
    
    imagegen_tool = {
        'type': IMAGEGEN_PROVIDER_TYPE,
        'name': IMAGEGEN_TOOLKIT_NAME,
        'toolkit_name': IMAGEGEN_TOOLKIT_NAME,
        'description': 'Auto-injected image generation toolkit',
        'settings': settings,
        'id': None,  # No DB entity - auto-injected
        'agent_type': None,
        'variables': [],
    }
    
    log.info(
        f"Auto-injecting ImageGen tool for project {project_id} "
        f"with model={model_name}, bucket={bucket}, name_prefix={name_prefix!r}"
    )
    return imagegen_tool


# =============================================================================
# Attachment Toolkit Injection
# =============================================================================

class AttachmentConfigurationError(Exception):
    """Raised when attachment toolkit cannot be configured properly."""
    pass


def get_default_attachment_bucket(project_id: int) -> str:
    """
    Get default bucket for chat attachments.
    
    Args:
        project_id: Project ID
        
    Returns:
        Bucket name string (defaults to 'attachments' if not configured)
    """
    try:
        vault_client = VaultClient(project_id)
        secrets = vault_client.get_all_secrets()
        bucket = secrets.get('default_attachment_bucket', ATTACHMENT_DEFAULT_BUCKET)
        log.debug(f"Using attachment bucket: {bucket}")
        return bucket
    except Exception as e:
        log.warning(f"Failed to get default attachment bucket, using fallback '{ATTACHMENT_DEFAULT_BUCKET}': {e}")
        return ATTACHMENT_DEFAULT_BUCKET


def get_default_pgvector_config(project_id: int) -> Optional[dict]:
    """
    Get default PgVector configuration for attachment indexing.
    
    First gets the default pgvector model name, then fetches the full configuration.
    
    Args:
        project_id: Project ID
        
    Returns:
        PgVector configuration dict or None if not configured
    """
    try:
        # Step 1: Get default pgvector model name
        pgvector_default = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_default_model(
            project_id=project_id,
            section='vectorstorage',
            include_shared=True
        )
        
        model_name = pgvector_default.get('model_name') if pgvector_default else None
        if not model_name:
            log.debug("No default pgvector configuration set for project")
            return None
        
        target_project_id = pgvector_default.get('target_project_id') or project_id
        
        # Step 2: Get the specific configuration by name
        configs_result = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_filtered_project(
            project_id=project_id,
            filter_fields={'type': 'pgvector'},
            include_shared=True
        )
        
        # Extract items list from response
        if isinstance(configs_result, dict):
            configs = configs_result.get('items', [])
        elif isinstance(configs_result, list):
            configs = configs_result
        else:
            configs = []
        
        # Find configuration matching the default model name and project
        # model_name is actually the elitea_title of the configuration
        for config in configs:
            config_elitea_title = config.get('elitea_title')
            config_project_id = config.get('project_id')
            
            if config_elitea_title == model_name and config_project_id == target_project_id:
                log.debug(f"Using default pgvector configuration: {config.get('label', config.get('id'))}")
                return {
                    'elitea_title': config.get('elitea_title'),
                    'private': not config.get('shared', False)
                }
        
        log.warning(f"Default pgvector '{model_name}' not found in configurations")
        return None
        
    except Exception as e:
        log.warning(f"Failed to get default pgvector config: {e}")
        return None


def get_default_embedding_model(project_id: int) -> Optional[str]:
    """
    Get default embedding model for attachment indexing.
    
    Args:
        project_id: Project ID
        
    Returns:
        Embedding model name or None if not configured
    """
    try:
        embedding_default = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_default_model(
            project_id=project_id,
            section='embedding',
            include_shared=True
        )
        model_name = embedding_default.get('model_name') if embedding_default else None
        if model_name:
            log.debug(f"Using embedding model: {model_name}")
        else:
            log.debug("No default embedding model configured")
        return model_name
    except Exception as e:
        log.warning(f"Failed to get default embedding model: {e}")
        return None


def inject_internal_attachment_tool(
    project_id: int,
    existing_tools: list[dict],
    internal_tools: list[str],
    always_inject: bool = False
) -> Optional[dict]:
    """
    Conditionally create auto-injected attachment (artifact) toolkit payload.
    
    This function injects an artifact toolkit for handling file attachments.
    
    The injection is skipped if:
    - Toggle is not enabled in internal_tools and always_inject is False
    
    Args:
        project_id: Project ID for configuration lookup
        existing_tools: Already collected toolkit payloads
        internal_tools: List of enabled internal tools from conversation/agent meta
        always_inject: If True, inject regardless of toggle (for LLM chats)
        
    Returns:
        Attachment toolkit payload dict, or None if should not inject
        
    Raises:
        AttachmentConfigurationError: If required pgvector or embedding configurations are missing
    """
    # 1. Check if attachment toggle is enabled (unless always_inject)
    if not always_inject and ATTACHMENT_INTERNAL_TOOL_KEY not in internal_tools:
        log.debug("Attachment internal tool not enabled, skipping auto-injection")
        return None
    
    # 2. Get default bucket
    bucket = get_default_attachment_bucket(project_id)
    
    # 3. Get required pgvector and embedding configurations
    pgvector_config = get_default_pgvector_config(project_id)
    if not pgvector_config:
        raise AttachmentConfigurationError(
            f"No default pgvector configuration found for project {project_id}. "
            "Please configure a default vector storage in project settings."
        )
    
    embedding_model = get_default_embedding_model(project_id)
    if not embedding_model:
        raise AttachmentConfigurationError(
            f"No default embedding model configured for project {project_id}. "
            "Please configure a default embedding model in project settings."
        )
    
    # 4. Build toolkit settings
    toolkit_settings = {
        'bucket': bucket,
        'selected_tools': ATTACHMENT_DEFAULT_SELECTED_TOOLS,
        'pgvector_configuration': pgvector_config,
        'embedding_model': embedding_model,
    }
    
    # 5. Build toolkit payload matching manual toolkit structure
    attachment_tool = {
        'type': ATTACHMENT_TOOLKIT_TYPE,
        'name': ATTACHMENT_TOOLKIT_NAME,
        'toolkit_name': ATTACHMENT_TOOLKIT_NAME,
        'description': 'Auto-injected attachment toolkit for attachment operations',
        'settings': toolkit_settings,
        'id': None,  # No DB entity - auto-injected
        'agent_type': None,
        'variables': [],
    }
    
    log.info(
        f"Auto-injecting attachment tool for project {project_id} "
        f"with bucket={bucket}, "
        f"pgvector={pgvector_config.get('elitea_title')}, embedding={embedding_model}"
    )
    return attachment_tool

