from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class SkillRuntimeFileModel(BaseModel):
    path: str
    size: int
    content_type: str
    is_text: bool

    model_config = ConfigDict(from_attributes=True)


class SkillFileManifestModel(SkillRuntimeFileModel):
    sha256: str


class SkillFileModel(SkillFileManifestModel):
    created_at: datetime


class SkillFileTextWriteModel(BaseModel):
    content: str = Field(description="UTF-8 text to store at the path in the URL; replaces an existing file.")

    model_config = ConfigDict(extra="forbid")


class SkillFileRenameModel(BaseModel):
    new_path: str = Field(min_length=1, max_length=1024, description="New path under the skill root.")

    model_config = ConfigDict(extra="forbid")


def describe_runtime_files(files) -> list:
    """The files a runtime registry entry announces: path, size, type and text flag, never hash or bytes."""
    return [SkillRuntimeFileModel.model_validate(version_file).model_dump() for version_file in files]


def describe_manifest(files) -> list:
    """The files a copy of a version carries: what the runtime sees plus the content hash."""
    return [SkillFileManifestModel.model_validate(version_file).model_dump() for version_file in files]
