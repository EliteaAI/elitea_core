"""Helpers for toolkit ``meta`` that crosses a project boundary."""


def drop_index_schedules(meta):
    """Return ``meta`` without ``indexes_meta``, for a toolkit entering another project.

    ``indexes_meta`` holds nothing but cron schedules, each carrying the credential title
    and the ``created_by`` of whoever armed it. The index data it schedules lives in the
    source toolkit's own vector schema and cannot travel — the copy gets a new toolkit id
    and an empty one, and the Indexes list is read from that data rather than from here.
    So a carried schedule fires forever against an index that does not exist, notifying an
    author who is not a member of the destination project, with no screen anywhere that
    could list or delete it. Where the destination happens to hold a credential of the same
    title it is worse: the schedule runs, indexing in a project its author cannot open.

    Always a dict, never the falsy argument back: callers assign the result straight into a
    payload, and writing ``None`` over an absent ``meta`` turns a field pydantic would have
    defaulted to ``{}`` into an explicit ``None`` - which reaches a ``nullable=False`` column.
    """
    if not meta:
        return {}
    return {key: value for key, value in meta.items() if key != 'indexes_meta'}
