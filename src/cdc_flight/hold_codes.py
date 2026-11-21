"""The closed set of apply_status.hold_code values (docs/operations.md lists each one).

A hold's code is set where the hold is: `mirror.Blocked(..., code=)` for apply's holds, the
state writers for pause and expiry, the registry's held_reason for ingest's holds. Tests
check every code path against this set; nothing imports it at runtime but the tests and
readers."""

HOLD_CODES = frozenset(
    {
        # apply: needs_action
        "append_update",
        "append_delete",
        "append_keyed_frames",
        "type_changed",
        "schema_changed",
        "source_mismatch",
        "poison",
        "missing_baseline",
        "incomplete_insert",
        "unsupported_change",
        "key_cell",
        "key_change_ambiguous",
        "pin_invariant",  # the table pin's transfer hit an InvariantViolation
        # apply: needs_resync
        "key_changed",
        "epoch_changed",
        "history_lost",
        "retention_expired",
        "truncate",
        # catalog checks (catalog_checks.REASONS): needs_resync, or needs_action (append)
        "pk_replaced",
        "partitions_changed",
        "column_default_fill",
        "column_rewrite",
        # operator
        "paused",
        # registered, no generation yet
        "not_started",
        # registry (ingest_common.MEMBERSHIP_HOLDS, VALIDATION_HOLDS, append holds, removal)
        "dropped",
        "renamed",
        "replaced",
        "unpublished",
        "unmanaged",
        "removed",
        "invalid_kind",
        "no_select",
        "no_key",
        "identity_not_full",
        "leaf_invalid",
        "publication_filter",
        "append_identity",
        "append_partitioned",
    }
)
