"""Shared parsing of admin task params ("key=value;key=value")."""


def dry_run_flag(param, default: bool = True) -> bool:
    """``default`` unless the param names ``dry_run``: ``dry_run=false`` (or 0 / no) runs for real; ``dry_run`` and
    ``dry_run=true`` mean a dry run."""
    for segment in (param or "").split(";"):
        key, _, value = segment.strip().partition("=")
        if key.strip().lower() == "dry_run":
            return value.strip().lower() not in ("false", "0", "no")
    return default
