from pydantic import BaseModel


def deep_merge(base_dict: dict, updater_dict: dict, in_place: bool = False) -> dict:
    if in_place:
        result = base_dict
    else:
        result = base_dict.copy()
    for key, value in updater_dict.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value, in_place=in_place)
        else:
            result[key] = value
    return result


class MergeUpdateBase(BaseModel):
    def merge_update(self, other):
        this = self.model_dump(exclude_unset=True, exclude_none=True, exclude_defaults=True)
        updater = other.model_dump(exclude_unset=True, exclude_none=True, exclude_defaults=True)
        merged_dict = deep_merge(this, updater)
        return self.__class__.model_validate(merged_dict)

