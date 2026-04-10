try:
    from enum import StrEnum
except ImportError:
    from enum import Enum


    class StrEnum(str, Enum):
        pass


class IndexingSchedule(StrEnum):
    created_by = 'created_by'