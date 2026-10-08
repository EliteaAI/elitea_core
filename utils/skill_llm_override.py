from typing import Optional


def message_llm_override(llm_settings) -> Optional[dict]:
    if not llm_settings:
        return None
    return llm_settings.model_dump(exclude_unset=True, exclude_none=True) or None


def select_llm_override(entity_settings: dict, predict_payload) -> Optional[dict]:
    return message_llm_override(predict_payload.llm_settings) or entity_settings.get('llm_settings') or None
