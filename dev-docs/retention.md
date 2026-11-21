# Retention, compaction and source epochs (internals)

The customer-facing side is [docs/retention.md](../docs/retention.md).

## Retention (DESIGN Protocol 4)

Pins let Phase 3 compaction drop whole sealed log days without removing history a
consumer still needs. They live in `cdc_internal.<source>` (`src/cdc_flight/retention.py`).

| Object | Contents |
| :---- | :---- |
| `retention_pins` | One row per consumer: `owner` (`table:<relation_id>` or `generation:<id>`), kind, relation, generation, `source_epoch`, `pinned_lsn`, timestamps. `expires_at` is reserved for the Phase 3 storage budget. |
| `retention` | One row: the floor `(floor_epoch, floor_lsn)`, the shared guard column and the compaction lease. |

**The invariant.** Readers read with an inclusive lower bound (`commit_lsn >= low`).
A sealed day is dropped only when every commit in it is strictly below every pin,
and the floor is the highest `(epoch, max_commit_lsn)` ever dropped. So every commit
above the floor is present, and a pin at X is safe exactly when `(epoch, X) >
floor`. Ordering is by `(source_epoch, LSN)` throughout.

- **Register or transfer** (creates or lowers protection): writes the shared guard
  and checks the floor in one transaction, so it serializes with a drop. A pin
  request at or below the floor raises `HistoryDropped`.
- **Advance** (raises a pin): a plain update of the consumer's own row, without the
  guard, so tables do not contend with each other. A drop that races it sees the
  lower value and drops less. It is fenced on the pin's generation and epoch, so a
  stale worker of a replaced generation cannot raise the new generation's pin.
- Guarded pin transactions are idempotent and are retried on write conflicts.

**Generations.** The pin at L is registered before the mirror start commits. A
crash between leaves extra history. Before every chunk and catch-up batch, the
generation's pin must exist at or below `catchup_lsn`, or the generation fails
("retention pin missing"). The pin stays at L until publication. After the
publication commit, the table pin is **transferred** to the new generation at H
(lowering it if old apply ran past H) and the generation pin is released, in one
guarded transaction. The generation pin at L <= H is what lets that floor check
pass. A transfer whose generation pin is already gone (a delayed publisher of a
generation that was replaced meanwhile) changes nothing. After a failure or
abandonment commits, the pin is released. A start releases its pin only after a
refusal that rolled back. A lost commit response may follow a real commit, so the
pin then stays: the generation resumes under it, or the sweep removes it. A guard
conflict at registration is retried until the run's deadline (60 s without one).
Only after that does `run_table` return `busy` with an empty generation id, which
means nothing started: re-run it with `start=True`. Each backfill
start, and each publication, sweeps the relation's generation pins that protect
nothing: failed, abandoned or replaced generations, and pins with no generation
row that are older than the lease TTL by `cdc_internal`'s clock. A start whose
mirror transaction is slower than that loses its pin and fails closed at its first
chunk.

**Log-only starts** follow the same shape: a generation pin at `(epoch, 0)`, the
mirror commit, then the transfer. That needs a floor of nothing dropped. A table
awaiting its first generation has no pin, so once the log is compacted only a
backfill can start it.

**Apply.** Before every batch read, the table pin must belong to the mirror's
generation and epoch and be at or below `applied_through_lsn`. If only the
generation's pin exists, a transfer was interrupted, and apply completes it. If
neither exists, apply **fails closed**: it holds the table `needs_resync` and reads
nothing. After its reads, apply checks the floor again: a drop that raced the read
raised the floor in its own commit, and the table holds `needs_resync`. This is
defence in depth: with a correct pin no drop reaches the read, and a dropped day
table would make the read itself fail. The pin
advances after each mirror commit, including quiet ranges, and once more at the end
of the run, so a lost advance is repaired by the next run. A failed advance is only
logged: the pin stays lower, which keeps extra history. Held tables keep their pins.

**Drop primitive.** `Retention.drop_partition(day, token)` is the operation Phase 3
compaction schedules (`cdc-flight compact`, [retention.md](../docs/retention.md)). Under the
compaction lease, in one transaction, it:
- requires every `(day, epoch)` partition to be sealed, and every commit in it to be
  below every pin;
- refuses when there are no pins;
- drops the day table, marks the partitions `dropped`, and raises the floor.

Since Phase 3 it also prunes the ledger rows nothing can read again: the day's
`batches`, and `txn_manifests` at or below the new floor with no batch left (see
[retention.md](../docs/retention.md)). It keeps `schema_versions`, `tables` and `key_changes`.
Apply's key limit reads a later key version's cut for any mirror still pinned, so
key history may be pruned only for versions no pinned consumer was built under.
Ingest seals only active days, so a dropped day stays dropped.

## Compaction

### A run

1. Take the source's compaction lease (`compact:<source>`; a plain guard UPDATE with an
   affected-row check). If another run holds it, exit `busy`.
2. Try the days that aren't dropped yet, oldest first, each in its own transaction
   (`Retention.drop_partition`, which re-checks everything under the lease):
   - every `(day, epoch)` partition sealed, and every touching unit resolved
     (complete or explicitly abandoned), strictly below every retention pin;
   - at least one pin exists;
   - no published table has lost its pin (below);
   - the new floor stays below the ingest checkpoint and open-unit anchor;
   - the transaction changes both the retention guard and source guard, never
     the source token, lease expiry or checkpoint. Append conflicts roll back and
     become retryable only through the tagged pipeline catcher.
3. Stop at the first day that is kept. Ingestion days never move backwards and epochs
   only increase, so each day's greatest `(epoch, commit)` is non-decreasing: when one
   day is still needed, every later day is too. A run also stops at its runtime cap
   (`CDC_RUNTIME_SECONDS`) between days.
4. Prune the ledger of every dropped day (below), then release the lease and record
   the run in `compactions` (best effort).

### Ledger pruning

After drops commit, `Retention.prune` deletes `chunks_v2` rows for each dropped
(day,epoch), then resolved units at or below the floor with no chunks left. A
cross-day unit survives until its last physical day is gone. `chunks_pruned` and
`units_pruned` count rows actually deleted, not retained endpoints.

Complete endpoint rows referenced by current key_checked cuts, pending key queues
or key_changes history remain immutable metadata, read together in the prune
transaction. Their payload and chunks still reclaim; readers and new pins refuse
at or below the floor. These endpoint rows are not readable history or virtual
pins, and their growth follows table/key history rather than every commit.
Descriptors and immutable configurations are retained. A malformed pending queue
fails closed. Fresh writers reload only non-dropped physical days, while dropped
partition metadata remains available for audit and pruning.

**The prune doesn't hold the shared guard.** Every pin write (a backfill start, a
publication's transfer, apply's pin repair, add and remove) updates the retention
row's `guard` column, and so does the drop: they serialize, and the drop's
transaction is short (about a second). The prune renews the compaction lease through
`expires_at` and the token instead. DuckDB detects conflicts per column, so pin writes
never wait on it, while another compaction run's lease takeover still does. That is
safe: every row the prune deletes is at or below a committed floor, which readers and
pin requests refuse, and no writer touches such rows. A crash between the drop and the
prune leaves extra ledger rows, which the next run removes.

**Why a replay never meets a pruned row.** Ingest starts replication at the durable
checkpoint and refuses a slot whose confirmed position is ahead of it, so redelivered
commits are at or above the checkpoint; `append` dedups against the batch's own chunk
ids and commits. Pins sit at or below their consumers' checkpoints, which sit at or
below ingest's, and the floor is below every pin. The drop also checks the floor
against the checkpoint in its transaction and refuses (`FloorAboveCheckpoint`) rather
than prune past it. An open transaction's chunks count in its day's
`max_commit_lsn`, and its commit is above the checkpoint, so its day can't drop.

Only `IngestLog.append` and the drop write `batches` and `txn_manifests`
(`tests/test_compaction.py` pins the list).

### `pin_state`

`tables.pin_state` is `pinned` once a table has a table pin. It is set by the publication
transfer, in the pin's transaction, never at generation registration, so an abandoned
first generation leaves the table exempt. Stores from before Phase 3 are marked on
upgrade (every table with a table pin). A drop refuses (`held`) while a `pinned` table
other than a removed or unmanaged one has neither a table pin nor a generation pin.

## Consumer expiry

**How a table expires** (two databases, so two transactions, in this order):
1. The mirror, under the table's apply lease: a compare-and-set to `needs_resync` on the
   state, generation, epoch and checkpoint the run observed. A table that moved, or
   whose lease is held, is skipped (`moved`, `busy`) and keeps its pin.
2. `Retention.expire`, under the shared guard, deletes exactly the pin observed and marks
   the table `pin_state='released'`. A pin a publication transferred meanwhile is kept.

A crash between them leaves the table held with its pin (extra history only); the next
run releases it by the `needs_resync` rule. **A generation** past the limit fails under
its own lease (`generations.fail_generation`: the guard, a compare-and-set to `failed`,
its claim deleted and its staging table dropped), and then its pin is released. A
statement count no race explains (`generation_spec.InvariantViolation`) rolls that
generation back and keeps its pin (outcome `invariant`); the run goes on with the other
pins, the drops and the prune, then fails (status `invariant`). To recover, inspect the
generation and its claim in the mirror (`_cdc.generations`, `_cdc.backfills`), then
abandon it with the backfill Flight (`CDC_BACKFILL_ABANDON=1`). A generation whose claim
was already missing is failed anyway and reported (`invariant_repaired`). The pin of a published
generation the mirror is on is never expired as a generation: apply completes an
interrupted transfer from it; its table is judged instead. A table pin left on an older
generation (that transfer pending) is **superseded**: it never writes the mirror, is
reported, and the transfer replaces it.

## Source epoch transitions

LSNs order one replication history only. When that history has a gap, the source
moves to a new epoch, and every mirror built from the old history is rebuilt by a
backfill generation of the new one.

```sh
# The slot was dropped, is lost, or is ahead of the durable checkpoint:
uv run --env-file .env cdc-flight new-epoch --epoch 2 --reason slot_lost
# Abandon WAL that can't be ingested (the range is printed before anything changes):
uv run --env-file .env cdc-flight new-epoch --epoch 2 --reason skip --to-lsn 0/5A3B2C10
```

- **The epoch and slot come from the registration.** `source.epoch` and
  `source.slot_name` are read under the ingest lease, with the checkpoint. A set
  `CDC_EPOCH` is only an expectation: a mismatch fails closed. Ingest stamps events
  with the registered epoch (a value; the event encoding is unchanged). Apply and
  backfill read each table's log by its mirror's or generation's epoch.
- **The PostgreSQL step comes first, and is idempotent** (`epoch.py`):
  - `slot_lost` creates `<slot>_e<n>`, or reuses it if an earlier attempt created
    it (nothing acknowledges a slot before the metadata names it), and drops the
    old slot if it still exists. A healthy slot is refused ("nothing to recover").
  - `skip` advances the current slot to `--to-lsn` with
    `pg_replication_slot_advance` and keeps its name. The target must lie past the
    durable checkpoint and within the server's flushed WAL. If the metadata
    transaction then fails, ingest refuses with "ahead of the durable checkpoint"
    until the command is retried.
  - The slot name follows the cause, not the epoch number: a skip and then a slot
    loss give epoch 2 `<slot>` and epoch 3 `<slot>_e3`.
  - An active slot is never taken over: the command reports `busy`. It never
    terminates a backend. It does take the ingest lease, so running it while ingest
    streams stops that run (its next write is fenced); retry after the run ends.
  - A `slot_lost` attempt that stops before its metadata commits leaves
    `<slot>_e<n>` retaining WAL: retry the same command, which reuses it, or drop it.
  - `new-epoch` ignores `CDC_EPOCH`, which names one side of the transition.
- **Then one guarded `cdc_internal` transaction** (`IngestLog.begin_epoch`), under
  the ingest lease:
  - it appends a `source_epochs` row: the previous epoch, checkpoint and slot, the
    start, the reason, the timeline and, for a skip, the abandoned range;
  - it moves the checkpoint and slot to the new epoch's start;
  - it seals every active partition of the old epoch;
  - it appends a key version per table in the new epoch (`key_changes.epoch`,
    `new_from_lsn` = start), unconfirmed until ingest checks the keys;
  - it marks every table `needs_resync`.

  A retry of a committed transition reports `already`; a different transition to
  the same epoch is refused.
- **Consumers.** Apply holds a mirror of an older epoch as `needs_resync` ("source
  epoch changed"). A generation of an older epoch fails. A log-only start is
  refused after epoch 1, because the empty-after-slot evidence concerns epoch 1's
  slot. Retention orders pins, the floor and drops by `(epoch, LSN)`: a new epoch
  may restart at a lower LSN, and old-epoch days drop once every pin has moved to
  the new epoch. The transition day's new-epoch row seals at the next day boundary,
  so that day drops only after it.
- **Stale workers.** Two independent fences:
  - Every guarded write (renew, append, key and progress records) also requires the
    epoch and slot the run read under its lease. Every new replication status value
    follows a guarded write within `lease_seconds/3`: the first is preceded by a
    renew, later ones by the append they acknowledge. A lapsed fence renews first,
    and a failed renew stops the run without sending anything.
  - The slot: after a slot loss the old one is gone, and a skip needs it inactive and
    only moves it forward, past anything a stale worker would acknowledge.
