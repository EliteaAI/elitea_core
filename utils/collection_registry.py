from dataclasses import make_dataclass
from functools import wraps
from operator import attrgetter

from pylon.core.tools import log
from tools import rpc_tools
from ..models.all import Application, ApplicationVersion
from ..utils.exceptions import (
    EntityNotAvailableCollectionError,
)


_RPC_CALL = rpc_tools.RpcMixin().rpc.call

#
# Extend the following global only with supported collection entities
_ENTITIES_INFO_IN = (
    (
        "application",
        "applications",
        Application,
        ApplicationVersion,
        _RPC_CALL.applications_export_application
    ),
)
#####################################################################


def _make_entity_registry():
    """ Construct entity registry with obj attrs, just for usage convenience """

    def _rpc_wrapper(rpc_fun, entity_name):
        """ Indicate that collection entity type is not available if rpc call was unsuccessful """

        @wraps(rpc_fun)
        def wrapper(*args, **kwargs):
            try:
                return rpc_fun(*args, **kwargs)
            except RuntimeError as ex:
                raise EntityNotAvailableCollectionError(str(ex)) from None
            except Exception as ex:
                log.error(ex)
                raise EntityNotAvailableCollectionError(
                        f"collection {entity_name=} is not available"
                ) from None
        return wrapper

    def _model_wrapper(model_class):
        """ Return the model class directly (no RPC call needed) """
        def wrapper():
            return model_class
        return wrapper

    ret = []
    for entity_name, entities_name, model_class, version_class, export_rpc in _ENTITIES_INFO_IN:
        reg_dict = {
            "entity_name":  entity_name,
            "entities_name": entities_name,
            "get_entity_type": _model_wrapper(model_class),
            "get_entity_version_type": _model_wrapper(version_class),
            "get_entity_field": attrgetter(entity_name),
            "get_entities_field": attrgetter(entities_name),
            "entity_export": _rpc_wrapper(export_rpc, entity_name),
        }
        ret.append(make_dataclass("EntityReg_"+entity_name, reg_dict)(**reg_dict))

    return ret


ENTITY_REG = _make_entity_registry()


def get_entity_info_by_name(name):
    for ent in ENTITY_REG:
        if name == ent.entity_name or name == ent.entities_name:
            return ent
    else:
        raise EntityNotAvailableCollectionError(
                f"collection entity name {name} is not available"
        )


def get_entity_type_by_name(name):
    entity_info = get_entity_info_by_name(name)
    return entity_info.get_entity_type()
