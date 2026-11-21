# Limits

What the system doesn't do, or does only within a bound. Flight-platform limits (the Lite plan,
chaining) are in [flights.md](flights.md#limits), and what a source can sustain in
[performance.md](performance.md).

## Supported PostgreSQL versions

- PostgreSQL 13 is the minimum supported version. Preflight rejects older servers.
- Ingest uses pgoutput protocol version 1 on every supported version.
- The publication must list tables explicitly and publish through partition roots. On newer servers
  preflight also rejects publication features that change what pgoutput sends:

| Feature | Versions | Preflight result |
| --- | --- | --- |
| Row filters and column lists | 15+ | Rejected |
| `FOR TABLES IN SCHEMA` | 15+ | Rejected; it would publish tables created later |
| `publish_generated_columns` other than `none` | 18+ | Rejected; backfill reads only ordinary columns |

Generated columns are never published under these rules: STORED columns on every version, and
VIRTUAL columns on 18. A generated column is never sent, so it is never in the mirror.

A read replica (standby) can be the source on PostgreSQL 16 and later; see
[read-replica.md](read-replica.md) for the versions and providers qualified.

## Tested setups

Capturing from a primary. For a read replica as the source, see read-replica.md's
[provider qualification](read-replica.md#provider-qualification).

| Provider | Versions | Qualified | How |
|---|---|---|---|
| Self-managed PostgreSQL | 13, 16, 18 | Yes (every change) | the test suite runs against each version on every pull request (PostgreSQL against local DuckDB stores; MotherDuck end to end: see the RDS row), on the current minor release of the official image |
| Self-managed PostgreSQL | 14, 15, 17 | Not qualified | accepted by preflight, but not in the test matrix (17 is qualified as a read replica source) |
| Amazon RDS for PostgreSQL | 16.15 | Yes (2026-11-13) | a fresh end-to-end setup following [setup.md](setup.md), and the TPC-C runs in [performance.md](performance.md) (2026-11-09, on RDS for PostgreSQL 16) |
| Amazon RDS for PostgreSQL | 13–15, 17, 18 | Not qualified | untested |
| Aurora PostgreSQL | — | Not qualified | untested |
| Supabase, Neon | — | Not qualified | untested |
| Google Cloud SQL, Azure Database for PostgreSQL | — | Not qualified | untested |
| Others | — | Not qualified | untested |

"Not qualified" means not tested, not known not to work.

## Transaction size

The 1 GiB default transaction cap is a conservative resource limit, not a measured
promise of completion within 540 seconds. Replaying a prefix can exceed the runtime.
Two capped runs with the same partial commit and no new durable events set
`source.state='needs_action:large_transaction'` and a visible run status; progress
or a successful drain clears it. Size the runtime and transaction limits from the
source's own transactions.

A transaction beyond the 1 GiB cap stops that source at the same transaction on
every run until an operator changes the limits or resyncs ([runbooks.md](runbooks.md)).
During remote rollback after a crash, lease-acquisition write conflicts return `busy`,
just like a still-active lease.

## Tables and schema changes

- **A held table keeps its change log retained** (its retention pin) until
  `remove-table` or a relabel. The PostgreSQL slot still advances.
- **A table moved to another schema** can't be relabelled (a DuckDB rename keeps the
  schema): `remove-table`, then `add-table` and a backfill.
- **ALTER COLUMN TYPE is resolved by a resync,** or, when the source type changed but no
  value did, by `cdc-flight-tables accept-types` (a dry run, then its typed confirmation).
  Widening the mirror column in place isn't supported.
- **Some DDL writes no logical changes for existing rows.** The catalog checks hold most of
  it ([tables.md](tables.md#source-ddl)). What they don't see, the table keeps reading caught
  up, and its mirror differs until a resync:
  - a same-type `ALTER COLUMN … TYPE … USING` rewrite: existing rows keep their old values;
  - a leaf detached and re-attached between two ingest runs;
  - on an append table, a new column's fill (existing rows stay NULL; a warning);
  - DDL between a resync's snapshot and the next ingest read is covered by the snapshot.
  After such DDL, resync the table.
- **`ADD COLUMN ... DEFAULT`** holds a keyed table at the catalog check (`needs_resync`)
  instead of leaving existing mirror rows NULL; an append table's rows stay NULL (a
  warning). A resync fixes it.
- **Neither table command alters the publication.** The DBA adds or removes tables
  there; a table missing from it is refused (`add-table`) or held (`unpublished`).
- **Deletes are hard deletes** in the mirror; there is no soft-delete mode.
- **Keyless tables are insert-only** (append mode): an UPDATE holds the table, and a DELETE
  holds it unless `ignore_deletes` is set.

## Backfill

- **DDL during a backfill.** Staging columns come from the catalog when the
  generation starts. A logged schema version during the backfill that lacks one of those
  columns fails the generation, because a column added before the start cannot
  be told apart from a drop. Run it again once ingest has passed the DDL.
  Columns added after the start arrive additively through catch-up, with the same
  `ADD COLUMN ... DEFAULT` caveat as apply.
- **Chunk reads** use a read-only transaction with a server-side cursor. They
  stop at `chunk_rows` or `max_chunk_bytes` of UTF-8 text (default 64 MiB), under
  `chunk_timeout_seconds` (default 600 s). A timed-out chunk read propagates and
  retries; it does not fail the generation.
- **A backfill's retention pin stays at its start until it publishes,** so the change log is
  retained for the whole backfill.
- **Catch-up can run out of MotherDuck memory.** Catch-up folds on MotherDuck, and units are
  never split, so a very large unit costs MotherDuck memory. An out-of-memory error fails the
  generation: resync on a larger MotherDuck instance, or when the source's write load is lower.

## Apply

- **Key tracking costs at most one ingest run of freshness.** Apply stops at the
  last boundary at which ingest confirmed the key, normally the end of the latest
  ingest run. If an ingest run dies after a flush but before its end-of-run key
  check, apply stalls at the previous confirmation until the next ingest run;
  that is safe, but visible as no progress. A key changed and changed back
  between two ingest key reads is not observable by sampling.
- **Some batches are folded in the Flight.** Most batches fold on MotherDuck. A batch with a
  key change, a malformed image, several schema versions or a quarantined unit is folded in
  the Flight, with memory proportional to its events and payload size, bounded by
  `max_batch_bytes` of payload text plus parsing overhead. A single unit larger than that is
  still read whole.
- **Many tables.** Each table reads its own units; sources with more than about 50 tables
  changing at once haven't been profiled.
- **Poison isolation cost.** Isolating a poison unit costs about
  log2(units in the batch) extra MERGEs of up to `max_batch_bytes`, all
  rolled back, before the clean prefix commits.
- **Batch runtime.** The soft runtime is checked between rounds (one page per open
  table, two for a lagging group). A round's MERGEs are not interrupted, so size
  `max_batch_events` so that a round fits well within the Flight's hard limit.
- **`key_changes` is never pruned.** That is fine at the expected rate of key changes.
