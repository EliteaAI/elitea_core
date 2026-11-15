"""The frame shape every pylon on the shared event bus can unpickle, whichever plugins it loads (#7499)."""
import datetime


def foreign_values(value, path='frame'):
    """Every value whose exact type a pylon without the sender's plugin could not rebuild."""
    if type(value) not in {str, int, float, bool, type(None), dict, list, tuple, datetime.datetime, datetime.date}:
        return [f'{path}: {type(value).__module__}.{type(value).__qualname__}']
    if isinstance(value, dict):
        return [found for key, item in value.items()
                for found in foreign_values(key, f'{path} key') + foreign_values(item, f'{path}[{key!r}]')]
    if isinstance(value, (list, tuple)):
        return [found for index, item in enumerate(value) for found in foreign_values(item, f'{path}[{index}]')]
    return []


def assert_bus_safe(name, payload):
    """Fail listing every value of the (name, payload) frame that would not unpickle in another pylon."""
    found = foreign_values({'name': name, 'payload': payload})
    assert not found, found
