# FROZEN reference of format 1's payload path at 0863bd1. Never edit.
# Test-only oracle for tests/test_mirror2.py; its sha256 is pinned there. Reproduces:
# - spool.py@0863bd1 lines 119-129 and 165-169: each row image's cells as
#   {name: {"kind", "value"}} in descriptor order, and the payload's canonical JSON;
# - apply.py@0863bd1 lines 725-739 and 797-802: json.loads of the payload, and full_old
#   from the descriptor's replica identity ("f").
"""Format 1's events as apply hands them to mirror.fold."""

import json


def cells(row, names):
    """row: a list of (kind, value) with kind in value/null/unchanged, or None."""
    if row is None:
        return None
    if len(row) != len(names):
        raise ValueError("tuple/descriptor width mismatch")
    return {name: {"kind": kind, "value": value} for name, (kind, value) in zip(names, row)}


def payload(old, new, names):
    return json.dumps(
        dict(old=cells(old, names), new=cells(new, names), truncate_options=None),
        sort_keys=True,
        separators=(",", ":"),
    )


def event(commit_lsn, event_ordinal, commit_ts_us, op, schema_version, text, identity):
    """apply's event dict for one change-log row, with full_old as fold_batch sets it."""
    return dict(
        commit_lsn=commit_lsn,
        event_ordinal=event_ordinal,
        commit_ts_us=commit_ts_us,
        op=op,
        schema_version=schema_version,
        payload=json.loads(text),
        full_old=op != "truncate" and identity == "f",
    )
