"""
Utility functions for Pipeline Trigger operations.

Contains helpers for:
- Webhook configuration (type config, secret management)
- URL building
- Secret display formatting
"""
import secrets
from typing import Optional

from pylon.core.tools import log
from tools import this, VaultClient

from ..utils.constants import PROMPT_LIB_MODE


# =============================================================================
# Webhook Type Configuration Registry (Issue #6)
# =============================================================================
# Single source of truth for webhook type-specific behavior.
# Adding a new webhook type only requires adding a dict entry here.

WEBHOOK_TYPE_CONFIG = {
    "github": {
        "signature_header": "x-hub-signature-256",
        "display_header": None,  # GitHub doesn't use a simple header check
        "display_instructions": "Enter this secret in your GitHub webhook configuration under 'Secret'",
        "masked_instructions": "Secret is masked for viewers. Editors can view the full secret.",
        "validation": "hmac_sha256",
    },
    "gitlab": {
        "signature_header": "x-gitlab-token",
        "display_header": None,
        "display_instructions": "Enter this token in your GitLab webhook configuration under 'Secret token'",
        "masked_instructions": "Secret is masked for viewers. Editors can view the full secret.",
        "validation": "token_match",
    },
    "custom": {
        "signature_header": "X-Webhook-Token",
        "display_header": "X-Webhook-Token",
        "display_instructions": "Add header 'X-Webhook-Token' with value '{value}' to your webhook request",
        "masked_instructions": "Secret is masked for viewers. Editors can view the full secret.",
        "validation": "token_match",
    },
}

# Webhook URL components
WEBHOOK_API_PATH = "webhook"


# =============================================================================
# Secret Management (Issue #4 - DRY)
# =============================================================================

def generate_webhook_secret() -> str:
    """
    Generate a secure webhook secret token.

    Returns:
        URL-safe base64-encoded 32-byte random token
    """
    return secrets.token_urlsafe(32)


def store_webhook_secret(project_id: int, version_id: int, secret_value: str) -> str:
    """
    Store webhook secret in vault and return vault reference string.

    Merges with existing project secrets to preserve other webhook secrets.

    Args:
        project_id: Project ID for vault access
        version_id: Version ID for secret key naming
        secret_value: The raw secret value to store

    Returns:
        Vault reference string (e.g., "{{secret.webhook_secret_v42}}")
    """
    secret_key = f"webhook_secret_v{version_id}"
    vault_client = VaultClient(project_id)
    project_secrets = vault_client.get_secrets() or {}
    project_secrets[secret_key] = secret_value
    vault_client.set_secrets(project_secrets)
    return f"{{{{secret.{secret_key}}}}}"


def get_webhook_secret_from_vault(project_id: int, vault_reference: str) -> Optional[str]:
    """
    Retrieve webhook secret from vault using reference string.

    Args:
        project_id: Project ID for vault access
        vault_reference: Vault reference string (e.g., "{{secret.webhook_secret_v42}}")

    Returns:
        The secret value, or None if not found
    """
    return VaultClient(project_id).unsecret(vault_reference)


def normalize_secret_value(secret: str) -> str:
    """
    Normalize secret value by stripping legacy header prefix if present.

    Legacy data may have format "header:value", we only need the value.

    Args:
        secret: Raw secret value (possibly with legacy prefix)

    Returns:
        Clean secret value
    """
    return secret.split(":", 1)[1] if ":" in secret else secret


# =============================================================================
# Secret Display Formatting
# =============================================================================

def mask_secret(secret: str) -> str:
    """
    Mask a secret value for display to viewers.

    Shows first 3 and last 3 characters with asterisks in between.

    Args:
        secret: The secret value to mask

    Returns:
        Masked secret (e.g., "abc********************xyz")
    """
    if not secret or len(secret) <= 8:
        return "***"
    return f"{secret[:3]}{'*' * min(len(secret) - 6, 20)}{secret[-3:]}"


def get_webhook_secret_for_display(
    project_id: int,
    trigger_data: dict,
    webhook_type: str,
    should_mask: bool = False,
) -> dict:
    """
    Get webhook secret from vault and format for display.

    Uses WEBHOOK_TYPE_CONFIG for type-specific formatting.

    Args:
        project_id: Project ID for vault access
        trigger_data: Trigger configuration dict containing webhook_secret reference
        webhook_type: Type of webhook (github, gitlab, custom)
        should_mask: If True, mask the secret value (for viewers)

    Returns:
        Dict with secret_configured, secret_header, secret_value, secret_instructions
    """
    empty_result = {
        "secret_configured": False,
        "secret_header": None,
        "secret_value": None,
        "secret_instructions": None,
    }

    webhook_secret_ref = trigger_data.get("webhook_secret") if trigger_data else None
    if not webhook_secret_ref:
        return empty_result

    secret = get_webhook_secret_from_vault(project_id, webhook_secret_ref)
    if not secret:
        return empty_result

    # Get config for this webhook type
    config = WEBHOOK_TYPE_CONFIG.get(webhook_type)
    if not config:
        # Unknown type - return basic info
        secret_value = normalize_secret_value(secret)
        return {
            "secret_configured": True,
            "secret_header": None,
            "secret_value": mask_secret(secret_value) if should_mask else secret_value,
            "secret_instructions": None,
        }

    # Normalize and optionally mask the secret
    secret_value = normalize_secret_value(secret)
    display_value = mask_secret(secret_value) if should_mask else secret_value

    # Build instructions
    if should_mask:
        instructions = config["masked_instructions"]
    else:
        instructions = config["display_instructions"].format(value=secret_value)

    return {
        "secret_configured": True,
        "secret_header": config["display_header"],
        "secret_value": display_value,
        "secret_instructions": instructions,
    }


# =============================================================================
# URL Building
# =============================================================================

def build_webhook_url(project_id: int, version_id: int, webhook_type: str) -> str:
    """
    Build the webhook URL for a pipeline trigger.

    URL pattern: /api/v2/{module_name}/webhook/{mode}/{project_id}/{version_id}/{webhook_type}
    Example: /api/v2/elitea_core/webhook/prompt_lib/1/42/custom

    Note: Hardcoding /api/v2 because context.url_prefix may be empty in some contexts.

    Args:
        project_id: Project ID
        version_id: Version ID
        webhook_type: Type of webhook (github, gitlab, custom)

    Returns:
        Webhook URL path (without host)
    """
    return f"/api/v2/{this.module_name}/{WEBHOOK_API_PATH}/{PROMPT_LIB_MODE}/{project_id}/{version_id}/{webhook_type}"


# =============================================================================
# Trigger Configuration Helpers (moved from pd/pipeline_trigger.py)
# =============================================================================

def get_trigger_from_pipeline_settings(pipeline_settings: dict) -> dict:
    """
    Extract trigger configuration from pipeline_settings with fallback.

    If no trigger is configured (legacy pipelines), defaults to chat_message.

    Args:
        pipeline_settings: The pipeline_settings dict from ApplicationVersion

    Returns:
        Trigger configuration dict
    """
    if not pipeline_settings:
        return {"type": "chat_message"}

    trigger = pipeline_settings.get("trigger")
    if not trigger:
        return {"type": "chat_message"}

    return trigger


def build_trigger_for_storage(update_data, user_id: int) -> dict:
    """
    Build trigger configuration dict for storage in pipeline_settings.

    Args:
        update_data: Validated UpdatePipelineTrigger request
        user_id: ID of the user making the update

    Returns:
        Trigger configuration dict ready for storage
    """
    from datetime import datetime, timezone

    trigger_type = update_data.type

    if trigger_type == "chat_message":
        return {"type": "chat_message"}

    elif trigger_type == "schedule":
        return {
            "type": "schedule",
            "cron": update_data.cron,
            "timezone": update_data.timezone,
            "last_run": datetime.now(timezone.utc).isoformat(),
            "created_by": user_id,
        }

    elif trigger_type == "webhook":
        return {
            "type": "webhook",
            "webhook_type": update_data.webhook_type,
            "created_by": user_id,
        }

    return {"type": trigger_type}


# =============================================================================
# Webhook Signature Validation (moved from api/v2/webhook.py)
# =============================================================================

def get_webhook_signature_header(webhook_type: str) -> Optional[str]:
    """
    Get the header name used for webhook signature based on type.

    Args:
        webhook_type: Type of webhook (github, gitlab, custom)

    Returns:
        Header name for signature, or None if type not recognized
    """
    config = WEBHOOK_TYPE_CONFIG.get(webhook_type)
    return config["signature_header"] if config else None


def validate_webhook_secret(
    webhook_type: str,
    secret_value: str,
    signature: str,
    raw_data: bytes = None,
) -> tuple[bool, Optional[str]]:
    """
    Validate webhook signature/token against stored secret.

    Uses WEBHOOK_TYPE_CONFIG to determine validation method.

    Args:
        webhook_type: Type of webhook (github, gitlab, custom)
        secret_value: The secret value from vault (already normalized)
        signature: The signature/token from request header
        raw_data: Raw request body (required for HMAC validation)

    Returns:
        Tuple of (is_valid, error_message or None)
    """
    from ..utils.utils import verify_signature
    from ..utils.exceptions import VerifySignatureError

    config = WEBHOOK_TYPE_CONFIG.get(webhook_type)
    if not config:
        return False, "Unknown webhook type"

    validation_method = config["validation"]

    try:
        if validation_method == "hmac_sha256":
            if raw_data is None:
                return False, "Raw data required for HMAC validation"
            verify_signature(raw_data, secret_value, signature)
            return True, None

        elif validation_method == "token_match":
            # Direct token comparison (gitlab, custom)
            if signature != secret_value:
                header_name = config["signature_header"]
                return False, f"{header_name} token mismatch!"
            return True, None

        else:
            return False, f"Unknown validation method: {validation_method}"

    except VerifySignatureError as e:
        return False, e.value.get("error", "Signature verification failed")
    except Exception as e:
        log.exception(f"Webhook validation error: {e}")
        return False, str(e)


def get_webhook_creator_id(trigger: dict, application) -> int:
    """
    Get the user ID to use for webhook execution.

    Uses the trigger creator (who set up the webhook) to ensure runs appear in their History.
    Falls back to application owner if trigger creator is not available.

    Args:
        trigger: Trigger configuration dict
        application: Application ORM object

    Returns:
        User ID for execution context
    """
    if trigger and trigger.get("created_by"):
        return trigger["created_by"]
    if application and application.owner_id:
        return application.owner_id
    return 1  # Fallback to system user ID 1
