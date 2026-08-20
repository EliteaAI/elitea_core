"""RPCs for the platform eval-dimension registry — the ``admin`` plugin's entry point.

Kept as RPCs (rather than a cross-plugin import) to match the existing ``admin`` →
``elitea_core`` direction. Every verb returns plain dicts so the admin API can serialize
them directly. Writes touch the registry only — a project gets its own copy when it attaches
the dimension from the catalog, and a later definition edit reaches the projects already
holding a copy through the explicit resync verbs.
"""

from pylon.core.tools import web

from ..models.pd.eval_platform_dimension import (
    EvalPlatformDimensionCreateModel,
    EvalPlatformDimensionDetailModel,
    EvalPlatformDimensionUpdateModel,
)
from ..models.pd.evaluation import EvalDimensionDetailModel
from ..utils import eval_platform_dimension_utils as platform_dimensions


def _serialize(row) -> dict:
    return EvalPlatformDimensionDetailModel.model_validate(row).model_dump(mode='json')


class RPC:
    @web.rpc("elitea_core_platform_dimension_list", "platform_dimension_list")
    def platform_dimension_list(self, active_only: bool = False, **kwargs) -> list:
        return [_serialize(row) for row in platform_dimensions.list_registry(active_only=active_only)]

    @web.rpc("elitea_core_platform_dimension_get", "platform_dimension_get")
    def platform_dimension_get(self, dimension_uuid: str, **kwargs):
        row = platform_dimensions.get_registry(dimension_uuid)
        return _serialize(row) if row is not None else None

    @web.rpc("elitea_core_platform_dimension_create", "platform_dimension_create")
    def platform_dimension_create(self, payload: dict, owner_id: int = None, **kwargs) -> dict:
        data = EvalPlatformDimensionCreateModel.model_validate(payload)
        row = platform_dimensions.create_registry(data, owner_id=owner_id)
        return {'dimension': _serialize(row)}

    @web.rpc("elitea_core_platform_dimension_update", "platform_dimension_update")
    def platform_dimension_update(self, dimension_uuid: str, payload: dict, **kwargs) -> dict:
        data = EvalPlatformDimensionUpdateModel.model_validate(payload)
        row = platform_dimensions.update_registry(dimension_uuid, data)
        return {'dimension': _serialize(row)}

    @web.rpc("elitea_core_platform_dimension_set_active", "platform_dimension_set_active")
    def platform_dimension_set_active(self, dimension_uuid: str, is_active: bool, **kwargs) -> dict:
        row = platform_dimensions.set_active(dimension_uuid, is_active)
        return {'dimension': _serialize(row)}

    @web.rpc("elitea_core_platform_dimension_resync_one", "platform_dimension_resync_one")
    def platform_dimension_resync_one(self, dimension_uuid: str, **kwargs) -> dict:
        return platform_dimensions.resync_dimension(dimension_uuid)

    @web.rpc("elitea_core_platform_dimension_resync", "platform_dimension_resync")
    def platform_dimension_resync(self, **kwargs) -> dict:
        return platform_dimensions.resync_all()

    @web.rpc("elitea_core_platform_catalog_list", "platform_catalog_list")
    def platform_catalog_list(self, project_id: int, **kwargs) -> list:
        """The active registry, annotated with each entry's local id in this project."""
        catalog = []
        for row in platform_dimensions.list_registry(active_only=True):
            entry = _serialize(row)
            entry['local_dimension_id'] = platform_dimensions.local_dimension_id(
                project_id, str(row.uuid),
            )
            catalog.append(entry)
        return catalog

    @web.rpc("elitea_core_platform_catalog_materialize", "platform_catalog_materialize")
    def platform_catalog_materialize(self, project_id: int, dimension_uuid: str, **kwargs) -> dict:
        row = platform_dimensions.materialize(project_id, dimension_uuid)
        return EvalDimensionDetailModel.model_validate(row).model_dump(mode='json')
