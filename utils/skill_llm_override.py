from typing import Optional


def select_llm_override(entity_settings: dict, predict_payload) -> Optional[dict]:
    message_override = (
        predict_payload.llm_settings.model_dump(exclude_unset=True, exclude_none=True)
        if predict_payload.llm_settings else None
    )
    return message_override or entity_settings.get('llm_settings') or None
