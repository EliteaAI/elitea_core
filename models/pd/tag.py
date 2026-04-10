from .collection_base import TagBaseModel


class TagListModel(TagBaseModel):
    id: int

TagDetailModel = TagListModel
