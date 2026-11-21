# Apply: unit consumers, the folds and the mirror

How apply and backfill read the format-2 log and fold it into typed mirrors. The fold rules are frozen: the
two folds (`mirror.py` and `sql_fold.py`) change together, and the frozen regions are pinned by
`tests/test_docs.py`.

<!-- frozen:apply-consumers begin -->
## Format 2: unit consumers

Apply, ApplyRun and Backfill read the format-2 log: complete **units** of grouped
commits, through `UnitReader` (`src/cdc_flight/unit_store.py`). The fold's semantics,
the MERGE, generation and publication algorithms below are unchanged. What is read,
where a checkpoint may land, and (for apply) where the fold runs differ: see
**Where apply folds**. `apply2.Apply2` and `backfill2.Backfill2`
remain as aliases of ApplyRun and Backfill. The format itself is described in
[phase1.md](log-format.md).

**Format-1 stores are refused.** Every consumer checks the store's `protocol_v2`
record first (`Apply.check_format`, read-only). A store without it, or with an
unknown format, raises `SourceMismatch("unsupported source protocol; recreate and
re-backfill")` before any mirror DDL or write. Nothing migrates a format-1 store.

**Cuts.** A checkpoint (`applied_through_lsn`, a generation's `catchup_lsn`, a
retention pin) is only ever the recorded epoch start (`source_epochs.start_lsn`) or
the end of a complete unit. The retention layer refuses any other position, and any
position at or below the floor. A table's first position is its epoch's recorded
start (`Retention2.initial_cut`), never inferred from a unit inventory that
compaction may have pruned.

**Reads.** Each batch is a page of contiguous complete units after the table's
checkpoint, below the durable high read in the same statement as the retention
floor. A page stops at a unit boundary, at `max_batch_events`, at `max_batch_bytes`
of ledger bytes or at 512 units. One unit larger than the caps is still read whole.
A gap, overlap, incomplete unit or a unit straddling the high fails the read rather
than skipping. A read whose history went below the retention floor becomes a
`needs_resync` hold for apply, or a failed generation for backfill. Expiry holds
a mirror before releasing its pin. On the Python fold, frames are decoded to the
same event dicts that format 1 produced, so the fold sees identical input.

**Where apply folds.** `ApplyRun` decides per table and page, inside its read:
- **The SQL fold** (`src/cdc_flight/sql_fold.py`), the default. One plan statement
  over the page's frames returns `frames()`' checks (position, retention floor,
  per-unit counts and gaps over the whole page, unknown record kinds, in that order)
  and the decision inputs: event and commit counts, the schema versions and key
  layouts, the first blocking event and the first key change. The MERGE's source then
  folds the frames in SQL where they are stored, so no frame reaches the Flight. The
  baseline precheck is the MERGE's own `WHEN NOT MATCHED ... THEN ERROR` clause, mapped
  to the same `needs_action` hold. Same round trips as the Python fold: the plan
  statement takes the frames read's place.

  The plan is per page, not per table. The first table of a run to plan a page reads
  its descriptors and runs one batched statement (`page_plan_sql`): the page's checks
  plus one row of facts per run table that has events there. Every later table planning
  the identical page (the Python fold's page key: units, digests, high, binding, config
  revision) takes its row from the run's plan cache. Each table still runs its own
  `units()` (its floor check), and decides from its own state: quarantine skips,
  descriptor, key layout and `reconcile` against its own mirror columns. Relations that
  aren't the run's tables are never judged. Between the cached checks and a later
  table's MERGE, that table's retention pin (at its checkpoint, below the page) keeps
  compaction from dropping the page. A planner or engine-capacity error from the batched
  statement makes each table plan alone with the per-table statement (`plan_sql`).
  Each table's MERGE still scans the page for its own frames: frames are stored in
  commit order, so every row group spans every table and `relation_id` can't skip any.
- **The Python fold** (`mirror2`/`mirror.fold`, a staged MERGE), per batch, whenever
  the plan rules the page out of the common case: a quarantined unit in the page, a
  blocking event (non-value key cell, incomplete insert, TRUNCATE of the table,
  unsupported op), a malformed image, a key change (the whole page, not a unit cut),
  more than one schema version or key layout, a key layout other than the mirror's, or
  a `reconcile` refusal. It holds, bisects poison and quarantines exactly as before. A
  data error from the SQL MERGE is bisected on this path, so poison holds are identical.

Backfill catch-up folds the same two ways, per batch (Backfill generations, step 4).

The two folds implement one contract (the fold rules below). `tests/test_sql_fold.py`
(seeded event sequences, staged rows compared exactly, plus mutants) and
`tests/test_sql_fold_e2e.py` (the same wire applied both ways, mirrors and state
compared) hold them equal; `CDC_TEST_SQL_FOLD_SEEDS` and
`CDC_TEST_SQL_FOLD_E2E_SEEDS` lengthen them. Change `mirror.py` and `sql_fold.py`
together.

**Holds, poison and quarantine act on whole units.**
- A blocking event holds the table before its entire unit. `blocked_commit_lsn`
  still names the commit, as a diagnosis; `blocked_unit_start_lsn` (written in the
  same statement) is the hold's position, and the checkpoint and pin stay at the
  unit's start.
- Poison isolation bisects by units, so the poison unit is held whole and every
  unit before it commits.
- Quarantine skips one whole unit for one table and generation. The operator
  confirms `QUARANTINE <table> UNIT <epoch> <start>..<end>`. The guarded
  transaction checks that the unit's end, digest and member count still match what
  was previewed, then records the skip and returns the table to `live`. The
  checkpoint stays at the unit's start; the next apply skips the unit for this table
  and advances through its end. Other tables still apply the unit.

**Backfill: L and H.** L, the generation's pin and snapshot position, is the
durable unit cut when the generation starts. H is the end of the complete unit that
contains the generation's end marker, so the marker's unit-mates are part of
catch-up. Catch-up replays whole units in `[L, H)`. A block, a key change, a
dropped history or a missing pin during catch-up fails the generation; it never
skips.

**Key changes activate at unit boundaries.** Ingest queues a key observation in
`tables.pending_keys_v2` while a unit is open, and leaves `key_checked_lsn` frozen
at the prior verified cut. Apply stops at that frozen cut. When the unit closes,
the queue becomes key versions (`key_changes`), each with its cut, and the table
holds `needs_resync`. The Dive's pending-key holds show the queue (reason
`key_change_pending`, its count, the frozen cut and the first observation).
Reclaiming payloads may keep the endpoint metadata that proves a referenced key
cut, but that metadata grants no read or pin at or below the floor.

**`held_day`.** A pin's `held_day` (compaction status, the Dive's Tables and
Retention) is the first sealed day still holding data that the pin has not read.
It can differ from the physical day compaction keeps only so that a unit's end
stays a valid position.
<!-- frozen:apply-consumers end -->

## Catalog checks

Some DDL changes rows without writing WAL for them. Ingest catches it from the source
catalog, at each run's end read:
- **One statement per read for every table** (`preflight.catalog_facts`; the start read
  needs only the keys). It returns the key, the primary key constraint's OID, the leaf
  set (a count and digest), the relfilenode, the live columns and those with
  `atthasmissing` (a partitioned table's from its leaves). Locally (PG 13/16/18), the end
  read takes about 2 ms for 9 tables and 3–4 ms for 50, against 2.5 and 12–13 ms for the
  per-table reads it replaces. It grows with a managed partitioned table's leaf count: a
  200-leaf root adds about 10 ms on PG 13/16 (3 ms on PG 18).
- **The facts are compared with `tables.catalog_state`**, which is written only when it
  changes (one batched UPDATE): nothing is written in the steady state. The first read
  after the release adopts what it finds and holds nothing.
- **Hard signals hold the table at the last verified cut,** through the key-change
  queue: the same key columns are queued as a key observation, so it activates as a key
  change (`needs_resync`) and apply never passes the cut. Its reason
  (`key_changes.reason`) is the hold's detail. The signals:
  - the primary key constraint was replaced, on a table keyed by its primary key;
  - a partitioned table's leaves changed;
  - a new column whose existing rows read a stored default (`atthasmissing`);
  - a new column while the table's file changed (a volatile default rewrote it).
- **A relfilenode change alone is a warning** (VACUUM FULL, CLUSTER, TRUNCATE, a
  same-type rewrite): logged and counted (`catalog_warnings`), never held.
- **While a key change or hold is in flight,** the stored facts are refreshed, unjudged:
  its resync covers that DDL, and one DDL never holds twice.
- **Append tables** have no key gate. A partition change writes a reasoned key change at
  the run's durable boundary, and apply holds the table `needs_action`; start-from-log
  starts past every hold cut at or before its start (`passed_catalog_version`). A new
  column's fill is a warning only.
- **Type identity** is checked by reconcile, not here: see the DDL matrix. Before
  deploying the release that adds it, `cdc-flight-tables types` lists every mirror column
  whose stored source type differs from the source's current one; resync each table it
  lists as `holds`, or `accept-types` it.

## Page-major schedule

One apply run steps its tables a page at a time, in rounds, rather than draining one
table before the next. Before, a first table with a backlog could use the whole run.

- Each table opens first: the head shortcuts that need no lease (missing, needs action,
  awaiting a generation, not live, busy, idle), then its apply lease, taken once for the
  run (lease 900 s, runtime 540 s). A table that finishes (caught up, held, busy, capped)
  writes its runs row and releases its lease at once.
- In a round, the open tables are grouped by checkpoint and stepped top-down. The top
  group pages up to the round's confirmed position, read once per round by its first
  table, so a commit mid-round never splits a page. Each lower group pages up to the
  next group's checkpoint as it stands after that group's step, with up to two steps.
  It gains a page a round on the group above and never passes it. Once it lands, the
  two groups are one, and every page they share is planned and read once per run. A
  table's own key limit (a pending replica identity change) still caps its pages.
- Every group below the confirmed position steps each round, so no table waits for a
  laggard. A page key includes its high, so groups share pages only once merged.
- A round starts only if the previous round's length still fits before the deadline
  (the first round always runs, unless the deadline has passed), and a round once begun
  is finished. A run overruns its runtime by at most one round, bounded by the Flight's
  `max_runtime_sec`. The run logs its round count and longest round.
- An unexpected error closes every open table (its committed pages kept, `capped`, the
  lease released) and is raised. The failing table writes no runs row, as before.

The page plans (SQL fold) are kept for the run, one per checkpoint group. They hold no
frames. The Python fold's frame cache below still holds one page.

## Mirror contract

Everything apply writes is in the source's mirror database, except retention
pins. Each MERGE, any additive ALTER, the checkpoint and the table's fencing guard
commit in one transaction there. The change log in `cdc_internal` is only read;
pins are written there in separate transactions (see Retention).

| Object | Contents |
| :---- | :---- |
| `<mirror>.<schema>.<table>` | Typed columns in source order with appended additions, `_cdc_commit_lsn UBIGINT`, `_cdc_updated_at TIMESTAMPTZ` (source commit time) and `PRIMARY KEY` on the registered replica identity. |
| `<mirror>._cdc.apply_state` | One row per relation: generation, epoch, `applied_through_lsn` (transaction-end LSN), mirror column list, state, held reason and blocking commit, and the apply lease (`token`, `holder`, `expires_at`, `guard`). |
| `<mirror>._cdc.generations` | One row per generation: mode (`log`, `log_incomplete` or `snapshot`), pin LSN L, marker end LSN H, state, and for snapshots the staging table, columns, resume key, chunk and row counts, catch-up checkpoint and lease. |
| `<mirror>._cdc.backfills` | The active snapshot generation per relation (at most one). |
| `<mirror>._cdc.runs` | Diagnostic outcome per run and table. Safe text only. |

The mirror table is created from the first applied schema version, in the same
transaction as its first MERGE. Source columns beginning with `_cdc_` are held as
`needs_action`.

Types follow the design's type mapping ([design.md](design.md#type-mapping)). Values remain PostgreSQL text in the log and are
cast inside the MERGE, so a value the mirror type rejects fails the transaction.
In detail:

- Arrays, `uuid`, `interval`, network types, enums, domains, ranges and unknown
  types are stored as their PostgreSQL text form. Arrays are not converted to JSON.
- `numeric` without a typmod, or with precision above 38 or a negative scale, is
  `VARCHAR` text. Inference from observed values (DECIMAL(38,s)) is not
  implemented.
- Preflight checks the regular **and** the replication session, because DSN
  options can differ between them. It requires `DateStyle` ISO, `bytea_output=hex`
  and `extra_float_digits >= 1`; below 1, float output is lossy. Casts never guess
  another style.

<!-- frozen:apply-float-keys begin -->
- **Float keys fold by canonical value.** PostgreSQL prints float `-0` as `-0`, but its
  unique index, like the mirror's typed MERGE, treats it as equal to `0`. Both folds
  (`mirror.fold` and `sql_fold.fold_sql`) group a key column of mirror type `DOUBLE`
  (`float4`, `float8`) by its canonical text, `-0` → `0`, and write that value. So a
  `-0` key is stored as `0`: equal under every comparison, without the sign bit. Every
  other text is its own key (one text per value under the bound `extra_float_digits`;
  `NaN` is one key), a non-key float keeps its `-0`, and a text key `'-0'` stays a
  distinct string. The plan statements still compare key texts, so an update from `0`
  to `-0` counts as a key change there and that batch takes the Python fold, which
  applies it in place. The stored frames keep the source text.
<!-- frozen:apply-float-keys end -->

- Values PostgreSQL accepts but the mirror type cannot hold are poison: they hold
  the table rather than coerce. Examples are `NaN` or `±Infinity` in
  `numeric(p,s)`, BC dates and timestamps, and dates outside DuckDB's range.

<!-- frozen:apply-protocol begin -->
## Apply protocol

For each configured table with state `live`, apply takes the table's lease. It
updates the same guard column as every later guarded write, so a stale worker
cannot commit. Then it repeats:

1. Read the source epoch and durable ingest checkpoint `H` from `cdc_internal`. The
   log below `H` is immutable. An epoch change holds the table as `needs_resync`.
2. Read the page of complete units starting at `applied_through_lsn`, which is
   the epoch start or a unit end (see Format 2: unit consumers). Units are
   contiguous, so nothing is read twice and nothing is skipped; within a unit,
   commits keep their order, including a commit whose LSN equals the previous
   commit's end.

   The page is capped at unit boundaries by `max_batch_events` (default 100,000),
   by ledger bytes (`max_batch_bytes`, default 256 MiB) and at 512 units. One
   larger unit is still applied whole. Every unit must be complete, with its end at
   or below H.
3. Reconcile each schema version in event order. Identical columns and appended
   columns are accepted. Varchar widening is accepted because the mirror type is
   unchanged. A dropped, renamed or retyped column, or a changed replica identity,
   holds the table.
4. Fold events in `(commit_lsn, event_ordinal)` order into one image per key.
   SQL NULL, unchanged TOAST and a column absent from the event's schema version
   are distinct. Unchanged cells inherit from an earlier image in the batch, then
   the FULL old image, then the existing mirror row. A changed key deletes the old
   key and upserts the new one; a TOASTed key unchanged on the new tuple is
   recovered from the FULL old image. A key-only old tuple is used only for its
   key cells, because pgoutput sends NULL placeholders for its other columns.
5. Stage images as Parquet (the Python fold), or fold in the MERGE's source (the SQL
   fold, format 2's default; see Where apply folds), then in one mirror transaction:
   guard, create or ALTER ADD COLUMN, verify every partial image has a mirror row,
   MERGE (delete, update known columns, insert complete images), and advance the
   checkpoint by compare-and-set on generation, epoch and the previous checkpoint.

A table with no events in the range still advances to `H`. Rerunning any range
yields the same mirror: a crash before commit changes nothing, and a lost commit
response leaves a checkpoint that makes the retry a no-op.
<!-- frozen:apply-protocol end -->

## Key changes

A REPLICA IDENTITY FULL Relation message flags every column as part of the
identity, so a primary-key change can't be seen in the log. A DEFAULT or INDEX
identity change is caught by `reconcile`. Ingest tracks keys outside the frozen
event encoding, as a monotonic **key version** per table in
`cdc_internal.<source>.tables` (`catalog_key`, `key_version`, `key_checked_lsn`),
plus an append-only `key_changes` history. The key is the one preflight derives:
the replica identity index, else the primary key.

Registration seeds version 0, confirmed at the registered start. Every ingest run
reads the catalog key under its lease, at the start and again after its last
flush:

- **Same key as the current version:** `key_checked_lsn` advances to the
  checkpoint whose captured WAL the read covered.
- **Different key:** a new version is appended. It records `cut_lsn`, the last
  boundary confirmed for the previous version, and `new_from_lsn`, a WAL position
  after the change. A change that is later reverted (A, B, A) is two new
  versions, so it can't masquerade as "no change".

Every mirror records the version it was built under (`apply_state.key_version`).

- **Apply, same version:** apply never passes that version's `key_checked_lsn`,
  and doesn't advance while it's unconfirmed.
- **Apply, newer version:** apply stops at the next version's `cut_lsn` and holds
  the table `needs_resync`.
- **Backfill:** builds under the current version and catalog key, refuses to
  start until ingest has recorded that key and passed its `new_from_lsn`, and
  before using H requires the version unchanged and confirmed through H.
  Otherwise it waits (`waiting_for_ingest`) or fails.
- **`start-from-log`:** refuses once any key change exists, because part of the
  log is under another key.

The registered key that ingest projects into `old_key`/`new_key` stays fixed, as
Phase 1 requires. On format 2, a key observed while a unit is open is queued and
becomes a version only at the unit's end (see Format 2: unit consumers), so every
`cut_lsn` is a unit end.
