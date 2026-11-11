"""The write rule for a pipeline version trigger.

A leaf module: it imports nothing, so create and save paths can use it without an import cycle.
"""


SECRET_KEYS = ("webhook_secret", "webhook_signing_secret")


def strip_trigger_secrets(settings: dict) -> dict:
    """pipeline_settings without the webhook secret references, for copies and exports (#6440)."""
    trigger = settings.get("trigger")
    if not isinstance(trigger, dict) or not any(key in trigger for key in SECRET_KEYS):
        return dict(settings)
    return {**settings, "trigger": {k: v for k, v in trigger.items() if k not in SECRET_KEYS}}


def sanitize_trigger_for_write(settings: dict, stored_trigger: dict | None) -> dict:
    """
    Return the pipeline_settings to store, so no write adds a legacy schedule or webhook trigger.

    A stored schedule trigger is kept until the migration moves it; otherwise a body
    schedule trigger is stored as chat_message. A body webhook trigger is stored only on a
    version that already stores one (#7402). Settings without a trigger are returned as is,
    so the save path keeps the stored trigger.

    Args:
        settings: The pipeline_settings dict carried by the write
        stored_trigger: The trigger already stored on the version, None on create

    Returns:
        A new pipeline_settings dict
    """
    if "trigger" not in settings:
        return dict(settings)
    if stored_trigger is None:  # a new version never inherits another version's secret
        settings = strip_trigger_secrets(settings)
    if (stored_trigger or {}).get("type") == "schedule":
        return {**settings, "trigger": stored_trigger}
    requested = (settings["trigger"] or {}).get("type")
    if requested == "schedule" or (requested == "webhook" and (stored_trigger or {}).get("type") != "webhook"):
        return {**settings, "trigger": {"type": "chat_message"}}
    return dict(settings)
