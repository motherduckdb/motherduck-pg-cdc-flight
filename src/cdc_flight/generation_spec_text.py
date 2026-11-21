"""The generation commands' prose: what each requires, its effects, its repeat
result and recovery, and why each statement's counts are legal. Tests and docs read it; no
Flight command imports it (tests/test_publication_commands.py checks), so generation_spec,
which the commands import, stays data only."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Text:
    requires: tuple[str, ...] = ()
    effects: tuple[str, ...] = ()
    idempotent: str = ""
    recovery: str = ""


TEXT: Mapping[str, Text] = {
    "create": Text(
        requires=(
            "the table is registered, keyed and not source-held",
            "no active claim (the backfills primary key refuses a second one)",
            "the source key matches the key ingest recorded, and L follows its change",
            "the generation pin registered at L (before the transaction)",
            "from table-action start-snapshot or resync: an unused, matching preview (action_flight snapshot_check / resync_check refuse a stale one)",
        ),
        effects=("claim", "generation row (unleased)", "empty staging table"),
        idempotent="a second create conflicts on the claim: ApplyError, and run_table resumes the active generation",
        recovery="a pin without a generation row is swept once older than the lease TTL",
    ),
    "create_append": Text(
        requires=(
            "an append table at release level APPEND_SNAPSHOT or later, not source-held",
            "no active claim",
            "the generation pin registered at L",
            "from table-action start-snapshot or resync: an unused, matching preview",
        ),
        effects=("claim", "generation row leased by this run", "empty staging table"),
        idempotent="as create",
        recovery="as create",
    ),
    "acquire": Text(
        requires=("an active generation whose lease is free or expired",),
        effects=("token+1, holder, guard, expires_at",),
        idempotent="a repeat with the old token is LeaseBusy: never a second lease",
        recovery="an unreleased lease expires after lease_seconds",
    ),
    "guard": Text(
        requires=("this run's unexpired lease",),
        effects=("guard changed (the write fence), expires_at renewed",),
        idempotent="renews again",
        recovery="none needed: it commits with the caller's transaction",
    ),
    "release": Text(
        requires=(),
        effects=("holder and expires_at cleared; the token is kept",),
        idempotent="a repeat matches the same row or none",
        recovery="best effort in a finally: a lost release expires after lease_seconds",
    ),
    "progress_scan": Text(
        requires=("the guard first, in the same transaction", "the pin present"),
        effects=(
            "a snapshot chunk's rows and its progress, together",
            "external mode: the chunk's standby replay bound with them",
        ),
        idempotent="a repeat with the old progress is refused (InvariantViolation)",
        recovery="a crash before commit rolls back the chunk; after it, the next run resumes from chunk_key",
    ),
    "finish_scan": Text(
        requires=(
            "the guard first, in the same transaction",
            "the scan done (keyed: scan_done; append: the exported-snapshot copy)",
        ),
        effects=("state, the marker and the WAL position before it",),
        idempotent="a repeat is IllegalTransition (the state moved on)",
        recovery="a crash after commit: the next run writes the marker",
    ),
    "progress_catchup": Text(
        requires=(
            "the guard first, in the same transaction (with _write: guard= first, commit_state= last)",
            "the pin present, the same epoch",
        ),
        effects=("a catch-up batch's merge and its checkpoint, or H, together",),
        idempotent="a repeat with the old checkpoint is refused (InvariantViolation)",
        recovery="a crash before commit rolls back the batch; the next run redoes it",
    ),
    "fail": Text(
        requires=("this run's lease (run_table's GenerationFailed)",),
        effects=(
            "the state and detail",
            "the claim (backfills row) deleted",
            "the staging table dropped",
            "in one mirror transaction; the lease released after commit (best effort)",
        ),
        idempotent="a repeat finds the generation ended: the guard refuses, nothing written",
        recovery="the retention pin is released after the commit; if that fails, sweep_pins releases an ended generation's pin (stale_generation)",
    ),
    "fail_expiry": Text(
        requires=("compaction's lease on the source", "the generation past the lag limit"),
        effects=(
            "the state and detail",
            "the claim (backfills row) deleted",
            "the staging table dropped",
            "in one mirror transaction; the lease released after commit (best effort)",
        ),
        idempotent="as fail",
        recovery="the retention pin is released after the commit; if that fails, sweep_pins releases an ended generation's pin (stale_generation)",
    ),
    "abandon": Text(
        requires=(
            "an operator's CDC_BACKFILL_ABANDON on the backfill Flight (no preview)",
            "an active generation for the table",
        ),
        effects=(
            "the state and detail",
            "the claim (backfills row) deleted",
            "the staging table dropped",
            "in one mirror transaction; the lease released after commit (best effort)",
        ),
        idempotent="a repeat finds no active generation: ApplyError, nothing written",
        recovery="the retention pin is released after the commit; if that fails, sweep_pins releases an ended generation's pin (stale_generation)",
    ),
    "publish": Text(
        requires=(
            "the generation catching_up, under this run's generation lease (guard first)",
            "the table's apply lease when it has a mirror generation (previous), acquired "
            "with token= the value read with it",
            "a recreated table: the retired relation's apply lease, its retirement re-read in "
            "the transaction",
            "claim exclusivity: no other writer moves apply_state.generation_id while this "
            "generation's claim exists (start_append refuses under a claim, re-checked in its "
            "guarded transaction; the pinned set of generation_id writers keeps it so)",
        ),
        effects=(
            "the retired relation's mirror dropped (or kept aside, append) and its apply_state "
            "row deleted",
            "the previous mirror dropped (or kept aside, append), staging renamed onto the name",
            "apply_state inserted (no previous) or moved to this generation by compare-and-set",
            "the previous generation replaced, this one published, its claim deleted",
            "in one mirror transaction; the pin transferred and swept after the commit",
            "the apply and retired leases released in publish's finally; the generation lease "
            "in run_table's finally",
        ),
        idempotent="a repeat finds the generation published: IllegalTransition before any "
        "statement (run_table never publishes twice: the claim is gone)",
        recovery="an InvariantViolation rolls everything back; the generation stays "
        "catching_up with its claim, so the next run repeats catch-up and fails again at "
        "publish, loudly. Inspect the rows, then abandon (CDC_BACKFILL_ABANDON) or let expiry "
        "end it. A crash after the commit: apply's ensure_pin completes the pin transfer",
    ),
    "replace": Text(
        requires=(
            "inside publish's or start_append's transaction, after the apply_state "
            "compare-and-set that moved the mirror off this generation",
        ),
        effects=("state replaced and updated_at, with the caller's other effects",),
        idempotent="a repeat finds it replaced: 0 rows, InvariantViolation, rolled back",
        recovery="none of its own: it commits or rolls back with the caller",
    ),
    "create_log_only": Text(
        requires=(
            "no apply_state row for the relation (re-read in the transaction: ApplyError)",
            "the generation pin registered at the epoch's recorded start before the "
            "transaction (start_from_log; released on an ApplyError refusal)",
            "epoch 1, key version 0, the table observed empty or accept_incomplete",
        ),
        effects=("a published generation row and its apply_state row, in one transaction",),
        idempotent="a repeat finds the apply_state row: ApplyError, nothing written",
        recovery="the pin transfer runs after the commit; ensure_pin completes it if lost",
    ),
    "create_log_only_append": Text(
        requires=(
            "an append table, accept_incomplete, not source-held",
            "no claim (backfill_running), checked early and again inside the guarded transaction",
            "replacing: start_decision 'replace' again under the apply lease",
            "the generation pin registered at S before the transaction",
            "from the table action: an unused, matching preview (claim())",
        ),
        effects=(
            "a published log_incomplete generation; apply_state inserted, or (replacing) the "
            "old mirror kept aside, the old generation replaced and apply_state moved by "
            "compare-and-set, which also clears the lease",
            "the audit row, in the same transaction",
        ),
        idempotent="a repeat finds a live mirror generation: ApplyError, nothing written",
        recovery="the pin transfer after the commit releases the old generation's pins",
    ),
}

# (command, statement) -> why its legal counts are legal, and what a 0 means.
WHY: Mapping[tuple[str, str], str] = {
    ("create", "claim_insert"): "an INSERT adds one row or raises",
    ("create", "generation_insert"): "an INSERT adds one row or raises",
    ("create_append", "claim_insert"): "an INSERT adds one row or raises",
    ("create_append", "generation_insert"): "an INSERT adds one row or raises",
    ("acquire", "acquire"): "held by another run, or ended: LeaseBusy",
    ("guard", "guard"): "the lease expired, was replaced, or the generation ended: FenceLost",
    ("release", "release"): "0: another run holds it now (unchecked)",
    (
        "progress_scan",
        "advance",
    ): "the guard succeeded in this transaction, so only a stale read of our own or a writer without the lease can change the row",
    (
        "finish_scan",
        "advance",
    ): "the guard succeeded in this transaction, so only a stale read of our own or a writer without the lease can change the row",
    (
        "progress_catchup",
        "advance",
    ): "the guard succeeded in this transaction, so only a stale read of our own or a writer without the lease can change the row",
    ("fail", "guard"): "the lease expired, was replaced, or the generation ended: FenceLost",
    (
        "fail",
        "claim_delete",
    ): "an active generation always has its claim (create and publish move both in one transaction); a claim for another generation is corruption",
    (
        "fail",
        "advance",
    ): "the guard succeeded in this transaction, so only a stale read of our own or a writer without the lease can change the row",
    ("fail_expiry", "guard"): "the lease expired, was replaced, or the generation ended: FenceLost",
    (
        "fail_expiry",
        "claim_delete",
    ): "an active generation always has its claim (create and publish move both in one transaction); a claim for another generation is corruption",
    (
        "fail_expiry",
        "fail_cas",
    ): "expiry read the state before acquiring: the backfill may have moved on (snapshotting to catching_up), so a 0 is FenceLost ('moved')",
    ("abandon", "guard"): "the lease expired, was replaced, or the generation ended: FenceLost",
    (
        "abandon",
        "claim_delete",
    ): "an active generation always has its claim (create and publish move both in one transaction); a claim for another generation is corruption",
    (
        "abandon",
        "advance",
    ): "the guard succeeded in this transaction, so only a stale read of our own or a writer without the lease can change the row",
    ("publish", "guard"): "the generation lease expired, was replaced, or the generation "
    "ended: FenceLost",
    ("publish", "apply_guard"): "the table's apply lease expired or was taken: FenceLost",
    ("publish", "retired_guard"): "the retired relation's apply lease expired or was taken: "
    "FenceLost",
    ("publish", "retired_recheck"): "ingest moves the registry, which no apply lease fences: "
    "the retirement may have changed, FenceLost",
    ("publish", "retired_delete"): "a token compare-and-set after this transaction's guard of "
    "the same lease: 0 is a writer without the lease or corruption",
    ("publish", "apply_insert"): "an INSERT adds one row or raises",
    ("publish", "apply_cas"): "after this transaction's apply guard, under claim exclusivity "
    "(only publish and start_append move generation_id; start_append refuses under a claim, "
    "re-checked in its guarded transaction): 0 is a writer without the lease or corruption. "
    "Residual: the re-check reads the claim under snapshot isolation and shares no written "
    "row with backfill's create, so a start_append transaction open since before the claim "
    "committed and kept open through the whole snapshot and catch-up could still race",
    ("publish", "replace"): "the apply_state compare-and-set in this transaction proved this "
    "generation was the mirror's; every mirror generation is published (log-only rows have "
    "been inserted as published since 37f7df7, and format 2 needs a fresh store), so 0 is "
    "corruption",
    ("publish", "advance"): "the guard succeeded in this transaction: only a stale read of our "
    "own or a writer without the lease can change the row",
    ("publish", "claim_delete"): "an active generation always has its claim; publish is not a "
    "repair path (abandon and expiry are), so 0 is corruption",
    ("replace", "replace"): "as publish/replace",
    ("create_log_only", "generation_insert"): "an INSERT adds one row or raises",
    ("create_log_only", "apply_insert"): "an INSERT adds one row or raises",
    ("create_log_only_append", "generation_insert"): "an INSERT adds one row or raises",
    ("create_log_only_append", "apply_insert"): "an INSERT adds one row or raises",
    ("create_log_only_append", "apply_guard"): "the table's apply lease expired or was taken: "
    "FenceLost",
    ("create_log_only_append", "replace"): "as publish/replace",
    ("create_log_only_append", "apply_cas"): "a token compare-and-set after this "
    "transaction's apply guard: 0 is a writer without the lease or corruption",
}


# Inventories for the spec checks (tests only; generation_spec.INVENTORY covers generations/backfills).
# module:qualname -> (status, write statements). tests/test_publication_commands.py scans the
# source both ways, as test_generation_commands does for INVENTORY.
APPLY_STATE: Mapping[str, tuple[str, int]] = {
    # The publication edges (spec).
    "backfill:Backfill.publish": ("spec", 4),  # apply_insert x2, apply_cas x2
    "publication_commands:retired_delete": ("spec", 1),
    "apply:Apply._start_from_log": ("spec", 1),
    "append_apply:start_append": ("spec", 2),  # apply_insert, apply_cas
    # Not generation publication: a candidate for a later change.
    "apply:Apply.acquire": ("out_of_scope", 1),  # the table lease
    "apply:Apply.guard": ("out_of_scope", 1),
    "apply:Apply.release": ("out_of_scope", 1),
    "apply:Apply._set_state": ("out_of_scope", 1),  # holds, the checkpoint
    "apply:Apply.set_state": ("out_of_scope", 1),
    "append_apply:write_append": ("out_of_scope", 1),
    "membership:_fence_apply": ("out_of_scope", 1),
    "membership:_relabel": ("out_of_scope", 1),
    "table_actions:accept_types": ("out_of_scope", 1),
    "type_identity:accept": ("out_of_scope", 1),
}
# Statements that assign apply_state.generation_id (INSERT or SET), by site: publish's
# premise (claim exclusivity) holds only while this set is exactly these.
GENERATION_ID_WRITERS: Mapping[str, int] = {
    "backfill:Backfill.publish": 4,
    "apply:Apply._start_from_log": 1,
    "append_apply:start_append": 2,
}
# Every DROP TABLE / ... RENAME TO string in src (mirror or not): a new one must be listed.
MIRROR_DDL: Mapping[str, tuple[str, int]] = {
    "backfill:Backfill.publish": ("spec", 3),  # DROP the retired / the previous, RENAME staging
    "append_apply:_keep_mirror": ("spec", 1),  # keep_aside / start_append: renamed aside
    "backfill:Backfill.end": ("out_of_scope", 1),  # the staging table
    "generations:fail_generation": ("out_of_scope", 1),  # the staging table
    "membership:_relabel": ("out_of_scope", 1),  # a relabel's rename
    "append_apply:kept_refusal": ("out_of_scope", 1),  # an error message
    "delete_source:_drop": ("out_of_scope", 1),  # delete-source (S4 part 2)
    "delete_source:observe": ("out_of_scope", 1),
    "preflight:membership_plan": ("out_of_scope", 1),  # ALTER PUBLICATION ... DROP TABLE text
    "retention2:Retention2.drop_partition": ("out_of_scope", 1),
    "store:Store.compact": ("out_of_scope", 1),  # Phase 0
}
