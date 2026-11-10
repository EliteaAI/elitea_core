"""Shared parsing of admin task params ("key=value;key=value")."""


def dry_run_flag(param) -> bool:
    """A dry run unless the param says `dry_run=false`; `dry_run` and `dry_run=true` also mean a dry run."""
    for segment in (param or "").split(";"):
        key, _, value = segment.strip().partition("=")
        if key.strip().lower() == "dry_run":
            return value.strip().lower() not in ("false", "0", "no")
    return True
