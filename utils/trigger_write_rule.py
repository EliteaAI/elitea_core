"""The write rule for a pipeline version trigger.

A leaf module: it imports nothing, so create and save paths can use it without an import cycle.
"""


def sanitize_trigger_for_write(settings: dict, stored_trigger: dict | None) -> dict:
    """
    Return the pipeline_settings to store, so no write adds a legacy schedule trigger.

    A stored schedule trigger is kept until the migration moves it; otherwise a body
    schedule trigger is stored as chat_message. Settings without a trigger are returned
    as is, so the save path keeps the stored trigger.

    Args:
        settings: The pipeline_settings dict carried by the write
        stored_trigger: The trigger already stored on the version, None on create

    Returns:
        A new pipeline_settings dict
    """
    if "trigger" not in settings:
        return dict(settings)
    if (stored_trigger or {}).get("type") == "schedule":
        return {**settings, "trigger": stored_trigger}
    if (settings["trigger"] or {}).get("type") == "schedule":
        return {**settings, "trigger": {"type": "chat_message"}}
    return dict(settings)
