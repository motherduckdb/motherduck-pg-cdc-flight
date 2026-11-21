"""The source, epoch and pin commands' prose and inventories. Tests and
docs read it; no Flight command imports it (tests/test_lifecycle_commands.py checks), so
lifecycle_spec, which the commands import, stays data only."""

from __future__ import annotations

from collections.abc import Mapping

from cdc_flight.generation_spec_text import Text

TEXT: Mapping[str, Text] = {
    "register": Text(
        requires=(
            "no source row (the transaction re-reads it): a new source starts at epoch 1",
            "the identity and binding preflight observed; the tables CDC_TABLES names",
        ),
        effects=(
            "the source row (lifecycle NULL: registered), its initial epoch row and one "
            "registry row per configured table, in one transaction",
        ),
        idempotent="a repeat finds the source registered: the steady state returns before "
        "any transaction; a missing table is TableRefused (add-table's job)",
        recovery="a crash before commit leaves nothing; preflight runs again",
    ),
    "record_created": Text(
        requires=(
            "the source registered (the transaction re-reads it); no lease: register runs "
            "from bootstrap/preflight, and no code ever DELETEs the source row",
        ),
        effects=("the created_* flags, monotonic: only ever set",),
        idempotent="a repeat sets nothing new",
        recovery="a crash between create and this write leaves a flag unset: the safe "
        "direction (delete-source prints the DDL instead of dropping)",
    ),
    "delete_start": Text(
        requires=(
            "the source lease (delete_source's leased()), its guard first in the transaction",
            "the lifecycle read under that lease: registered (NULL)",
            "from the action Flight: an unused, matching preview (claim())",
        ),
        effects=(
            "lifecycle deleting, the real slot name moved to deleted_slot_name, slot_name "
            "the tombstone deleted:<source_id> (the lease's cached slot moves with it, so the "
            "next guard compares the tombstone)",
        ),
        idempotent="a re-run finds deleting and continues from the drops",
        recovery="every Flight refuses a deleting source from this commit on; the drops "
        "and delete_finish complete on a re-run",
    ),
    "delete_finish": Text(
        requires=("the source lease, its guard first", "the lifecycle deleting"),
        effects=("lifecycle deleted and deleted_at",),
        idempotent="a re-run finds deleted: 'already', nothing written",
        recovery="none needed: the drops before it are idempotent",
    ),
    "begin_epoch": Text(
        requires=(
            "the source lease, its guard first (GUARD compares the epoch and slot)",
            "the next epoch (epoch + 1), the PostgreSQL work done",
            "every registry row moves: existing rows conflict per row with the unfiltered "
            "UPDATE; new rows come only from the four {s}tables INSERT sites, each in a "
            "transaction that writes the source row (register's INSERT, record_tables' "
            "guard, membership's guarded activate_member/_activate_append), so they "
            "conflict with this transaction's guard",
        ),
        effects=(
            "the epoch row, the source moved to the new epoch and slot, the protocol's "
            "capped marks cleared, the old epoch's partitions sealed, a key change and a "
            "needs_resync state for every registry row, and _activate_keys(abandoning) (its "
            "writes are the registry key machine: out of scope)",
            "in one guarded transaction",
        ),
        idempotent="a repeat of the same transition returns False (its existing check); a "
        "different one is SourceMismatch",
        recovery="a fault before the commit rolls everything back; after it the caller's "
        "fault point; the epoch Flight re-runs",
    ),
    "pin_transfer": Text(
        requires=(
            "the retention guard first (a DELETE is never a fence)",
            "the table pin this generation's, or its generation pin present and the lsn "
            "above the floor",
        ),
        effects=(
            "the table pin written (or kept), pin_state pinned, this generation's pin and "
            "the relation's expired sibling generation pins deleted",
        ),
        idempotent="a repeat finds the table pin owned: no put, any-count delete",
        recovery="publication calls it quietly (logged); apply's ensure_pin calls it "
        "directly before reading, so an InvariantViolation there fails that table's apply "
        "run loudly until an operator repairs the registry",
    ),
    "pin_expire": Text(
        requires=("the retention guard first", "the pin as compaction observed it"),
        effects=("the pin deleted and, for a table pin, pin_state released, together",),
        idempotent="a repeat finds no such pin: False, nothing written",
        recovery="expiry isolates an InvariantViolation per pin: outcome invariant, the "
        "pin kept, the run continues and reports status invariant",
    ),
    "pin_advance": Text(
        requires=("the reader's checkpoint committed (unguarded by design)",),
        effects=("the pin raised (never lowered)",),
        idempotent="greatest(): a repeat changes nothing",
        recovery="a lost advance leaves the pin lower: extra history, never less",
    ),
    "pin_release": Text(
        requires=("nothing: best effort",),
        effects=("the pins deleted",),
        idempotent="a repeat deletes nothing",
        recovery="a lost release is swept (sweep_pins) or kept as extra history",
    ),
}

# (command, statement) -> why its legal counts are legal, and what a 0 means.
WHY: Mapping[tuple[str, str], str] = {
    ("register", "source_insert"): "an INSERT adds one row or raises",
    ("register", "initial_epoch_insert"): "an INSERT adds one row or raises",
    ("register", "tables_insert"): "exactly the missing tables, re-read in this "
    "transaction and deduplicated by oid (no ON CONFLICT: it runs only with the id=1 "
    "source INSERT): a short count is corruption",
    ("record_created", "created_update"): "the source row was re-read in this "
    "transaction and no code deletes it: 0 is corruption",
    ("delete_start", "guard"): "the source lease expired or was taken: FenceLost",
    ("delete_start", "lifecycle_update"): "the lifecycle was read under this lease, and "
    "only delete_source writes it: 0 is corruption",
    ("delete_finish", "guard"): "as delete_start/guard",
    ("delete_finish", "lifecycle_update"): "as delete_start/lifecycle_update",
    ("begin_epoch", "guard"): "the source lease expired or was taken: FenceLost",
    ("begin_epoch", "epoch_insert"): "an INSERT adds one row or raises",
    ("begin_epoch", "source_update"): "AND epoch=? is redundant by design: unreachable "
    "under the guard (GUARD compares the epoch); kept so the exhaustive test can drive it",
    ("begin_epoch", "protocol_update"): "the protocol row exists from bootstrap on",
    ("begin_epoch", "partitions_seal"): "the old epoch's active days: any number",
    ("begin_epoch", "key_changes_insert"): "one per registry row: any number (the bulk "
    "UPDATE's count must equal it)",
    ("begin_epoch", "tables_update"): "every registry row: the same count as the key "
    "changes written over them in this transaction (a cross-check against one side "
    "drifting; concurrency is excluded by the write-write conflicts in requires)",
    ("pin_transfer", "put"): "an upsert writes one row",
    ("pin_transfer", "pin_state_update"): "the relation's registry row exists (no code "
    "deletes one): 0 is corruption, and the put and delete roll back with it",
    ("pin_transfer", "pins_delete"): "having just written the table pin, this generation's "
    "pin exists: at least one row (plus expired siblings)",
    ("pin_transfer", "pins_delete_owned"): "the table pin was already this generation's: "
    "a repeat may have deleted the generation pin, so any count",
    ("pin_expire", "pin_delete"): "0: not this pin any more (moved or advanced since "
    "compaction read it): False, nothing else written",
    ("pin_expire", "pin_state_update"): "the delete rolled back if the state update "
    "misses: no code deletes a registry row, so 0 is corruption",
    ("pin_advance", "pin_update"): "0: the pin isn't this generation's and epoch any more "
    "(or the cut isn't a unit boundary): False",
    ("pin_release", "pin_delete"): "best effort: any count",
}

# Inventories (tests only): module:qualname -> (status, write statements), checked both
# ways by tests/test_lifecycle_commands.py.
SOURCE: Mapping[str, tuple[str, int]] = {
    "ingest_common:LogBase.register": ("spec", 1),
    "ingest_common:LogBase.record_created": ("spec", 1),
    "delete_source:delete_source.action": ("spec", 2),
    "ingest_log2:IngestLog2.begin_epoch": ("spec", 1),
    # The source lease: spec'd by reference (its SQL shared with every ingest run).
    "ingest_common:GUARD": ("lease", 1),
    "ingest_common:LogBase.acquire": ("lease", 1),
    "ingest_common:RELEASE": ("lease", 1),
    # ingest's checkpoint and protocol bookkeeping (the hot path): a later PR.
    "ingest_log2:IngestLog2._record_progress": ("out_of_scope", 1),
    "ingest_log2:IngestLog2.append": ("out_of_scope", 1),
    "ingest_log2:IngestLog2.protocol_changed": ("out_of_scope", 1),
    "retention2:Retention2.drop_partition": ("out_of_scope", 1),
}
SOURCE_EPOCHS: Mapping[str, tuple[str, int]] = {
    "ingest_log2:IngestLog2._record_initial_epoch": ("spec", 1),  # register's
    "ingest_log2:IngestLog2.begin_epoch": ("spec", 1),
}
TABLES: Mapping[str, tuple[str, int]] = {
    "ingest_common:LogBase.register": ("spec", 1),
    "ingest_log2:IngestLog2.begin_epoch": ("spec", 1),
    "retention_common:RetentionBase._transfer": ("spec", 1),
    "retention_common:RetentionBase._expire": ("spec", 1),
    # The registry hold/key machine: a later change.
    "ingest_common:LogBase.record_tables": ("out_of_scope", 4),
    "ingest_log2:IngestLog2._activate_append": ("out_of_scope", 1),
    "ingest_log2:IngestLog2._activate_keys": ("out_of_scope", 2),
    "ingest_log2:IngestLog2._record_keys": ("out_of_scope", 4),
    "ingest_log2:IngestLog2.activate_member": ("out_of_scope", 2),
    "ingest_log2:IngestLog2.append": ("out_of_scope", 1),
    "membership:_add": ("out_of_scope", 1),
    "membership:_add_append": ("out_of_scope", 1),
    "membership:_relabel": ("out_of_scope", 1),
    "membership:remove_table.action": ("out_of_scope", 1),
    "retention_common:RetentionBase.statements": ("out_of_scope", 1),  # upgrade DDL
}
PINS: Mapping[str, tuple[str, int]] = {
    "retention_common:RetentionBase._register_generation": ("spec", 1),
    "retention_common:RetentionBase._put": ("spec", 1),
    "retention_common:RetentionBase._transfer": ("spec", 1),
    "retention_common:RetentionBase._expire": ("spec", 1),
    "retention_common:RetentionBase.advance": ("spec", 1),
    "retention2:Retention2.advance": ("spec", 1),
    "retention_common:RetentionBase._release": ("spec", 2),
    "membership:_release_pins": ("spec", 1),
}
# The only statements that may assign these {s}source columns (INSERT or SET):
SOURCE_COLUMN_WRITERS: Mapping[str, Mapping[str, int]] = {
    "lifecycle": {"delete_source:delete_source.action": 2},
    "epoch": {"ingest_common:LogBase.register": 1, "ingest_log2:IngestLog2.begin_epoch": 1},
    "slot_name": {
        "ingest_common:LogBase.register": 1,
        "ingest_log2:IngestLog2.begin_epoch": 1,
        "delete_source:delete_source.action": 1,
    },
    # Replica capture: the mode and the standby's timeline, written once.
    "heartbeat_writer": {"ingest_common:LogBase.register": 1},
    "timeline_id": {"ingest_common:LogBase.register": 1},
}
# The only {s}tables INSERT sites, each in a transaction that writes the source row
# (begin_epoch's every-row premise), with that write.
TABLES_INSERT_SITES: Mapping[str, str] = {
    "ingest_common:LogBase.register": "the source INSERT, in the same transaction",
    "ingest_common:LogBase.record_tables": "self.guard() first in its transaction",
    "ingest_log2:IngestLog2.activate_member": "membership's source-guarded transaction",
    "ingest_log2:IngestLog2._activate_append": "only via activate_member",
}
