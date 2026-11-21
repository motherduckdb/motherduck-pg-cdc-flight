"""Per-run catalog checks: DDL that writes no WAL for the rows it
changes, judged at ingest's end read against tables.catalog_state (written only on
change). The signals, the cut and the append rules: dev-docs/apply.md, "Catalog checks"."""

import json

REASONS = {
    "pk_replaced": "the source table's primary key constraint was replaced; resync",
    "partitions_changed": "a partition of the source table was attached or detached; resync",
    "column_default_fill": (
        "a column was added with a default its existing rows read without WAL; resync"
    ),
    "column_rewrite": "a column was added while the source table was rewritten; resync",
}

APPEND_REASONS = {
    "partitions_changed": (
        "a partition of the source table was attached or detached; start-from-log "
        "(it drops the rows before the new start)"
    ),
}


def state_of(facts, key_version=0):
    """The facts a later read is compared with (catalog_facts' row), with the table's
    key version when they were read."""
    return dict(
        key_version=key_version,
        key=facts["key"],
        pk_oid=facts["pk_oid"],
        leaves=facts["leaves"],
        relfilenode=facts["relfilenode"],
        columns=list(facts["columns"]),
    )


def encode(state):
    return json.dumps(state, sort_keys=True, separators=(",", ":"))


def judge(stored, facts, *, append=False, key_version=0):
    """(reason or None, state to write or None, warning) for one table's clean read.

    `stored`: its catalog_state (None before this release): adopted, never judged. The
    state to write is None when nothing changed."""
    new = state_of(facts, key_version)
    if stored is None:
        return None, encode(new), False
    old = json.loads(stored)
    old.pop("reason", None)
    if old == new:
        return None, None, False
    # A key change since these facts were stored (the key, or a key version activated
    # meanwhile; a held table isn't read): its own constraint change is not a second
    # signal, so the PK check is skipped. Every other signal is still judged (no read
    # after an activation goes unjudged).
    key_moved = old.get("key") != new["key"] or old.get("key_version") != key_version
    reason, warning = None, False
    added = [c for c in new["columns"] if c not in old.get("columns", ())]
    rewritten = old.get("relfilenode") != new["relfilenode"]
    if (
        not append
        and not key_moved
        and facts["from_pk"]
        and old.get("pk_oid") is not None
        and new["pk_oid"] is not None
        and old["pk_oid"] != new["pk_oid"]
    ):
        reason = "pk_replaced"
    elif old.get("leaves") != new["leaves"]:
        reason = "partitions_changed"
    elif added and set(added) & set(facts["missing"]):
        if append:
            warning = True
        else:
            reason = "column_default_fill"
    elif added and rewritten and not append and facts["relkind"] != "p":
        reason = "column_rewrite"
    elif rewritten and not append:
        warning = True
    if reason is not None:
        new["reason"] = reason
    return reason, encode(new), warning


def pending_reason(stored):
    """The reason a queued key observation carries (None: a key change)."""
    return json.loads(stored).get("reason") if stored else None


def hold_detail(reason, *, append=False):
    """The held table's detail for a catalog reason (None: not a catalog hold)."""
    if reason is None:
        return None
    if append:
        return APPEND_REASONS.get(reason)
    return REASONS.get(reason)
