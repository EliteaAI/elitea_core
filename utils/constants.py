from enum import Enum

PROMPT_LIB_MODE = 'prompt_lib'

ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API: int = 210

ICON_PATH_DELIMITER: str = '___'


class PredictionEvents(str, Enum):
    prediction_done = 'prediction_done'
