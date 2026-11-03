"""Buckets the platform keeps in every project for itself (#7185): no attachment path, setting or user request may
name them, with or without the project's ``p--<id>.`` prefix."""
import re

SKILL_FILES_BUCKET = 'skill-files'
PLATFORM_MANAGED_BUCKETS = frozenset({SKILL_FILES_BUCKET})
PROJECT_BUCKET_PREFIX = re.compile(r'^p--\d+\.')


def is_platform_managed_bucket(bucket) -> bool:
    """Whether ``bucket`` is one the platform manages, however the caller spelled it."""
    return PROJECT_BUCKET_PREFIX.sub('', (bucket or '').strip().lower(), count=1) in PLATFORM_MANAGED_BUCKETS
