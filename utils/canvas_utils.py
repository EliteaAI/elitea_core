import re

canvas_regex_pattern = r'(?P<is_shadow>shadow:)?canvas:(?P<project_id>\d+)_(?P<canvas_uuid>.+)'


def get_canvas_key(project_id: int, canvas_uuid: str) -> str:
    return f"canvas:{project_id}_{canvas_uuid}"


def get_canvas_details(canvas_key: str) -> dict | None:
    """
    Matches a canvas key (including 'shadow' prefixed ones) and extracts its details.

    Args:
        canvas_key: A string canvas key to process.

    Returns:
        A dictionary containing 'key', 'project_id', 'canvas_uuid', and 'is_shadow'
        if the key matches the pattern, otherwise None.
    """
    re_match = re.match(canvas_regex_pattern, canvas_key)
    if re_match:
        return {
            'key': canvas_key,
            'project_id': re_match.group('project_id'),
            'canvas_uuid': re_match.group('canvas_uuid'),
            'is_shadow': bool(re_match.group('is_shadow'))
        }
    return None


def get_list_canvas_details(input_keys: list[str], shadow: bool = False) -> list[dict]:
    """
    Processes a list of canvas keys, extracting details for each matching key.

    Args:
        input_keys: A list of string keys to process.
        shadow: Add shadow items to details list

    Returns:
        A list of dictionaries, where each dictionary contains 'key', 'project_id',
        'canvas_uuid', and 'is_shadow' for matching keys. Non-matching keys are
        filtered out.
    """
    list_canvas_details: list[dict] = []

    for key in input_keys:
        detail = get_canvas_details(key)
        if detail['is_shadow'] and not shadow:
            continue
        if detail is not None:
            list_canvas_details.append(detail)

    return list_canvas_details


def get_shadow_key(key_: str) -> str:
    return f"shadow:{key_}"


def get_origin_key_by_shadow(shadow_key: str) -> str:
    return shadow_key.removeprefix('shadow:')


def get_canvas_authors_key(project_id: int, canvas_uuid: str) -> str:
    return f"canvas_authors:{project_id}_{canvas_uuid}"
