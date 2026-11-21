# What the tests prove

Which tests prove which claims, the gate designs, and the mutation checks they kill. The policy and the
MotherDuck budget are in [testing.md](testing.md).

## Reference-model tests

The counts above record the original Phase 0 runs. Reference-model tests
`test_retention_pin_boundary_sealing_rollback_and_floor`,
`test_apply_pin_advances_only_after_commit_and_quiet_apply_releases_history` and
`test_new_epoch_with_lower_lsn_can_resync_and_retire_old_history` now run locally.
MotherDuck still runs the production counterparts
`test_pins_advance_after_commit_quiet_tables_and_floor`,
`test_drop_fails_closed_without_pins_and_crash_rolls_back`,
`test_old_epoch_history_drops_once_pins_move_and_new_pins_stay_above_the_floor` and
`test_new_epoch_appends_at_lower_lsns_and_same_identities`. Lease overlap remains
covered by `test_fence_takeover_rejects_stale_worker`,
`test_guard_renews_lease_using_database_clock` and
`test_lease_release_allows_next_run_and_stale_release_cannot_unlock`.

## Limits and later-phase work

- COMMIT-response loss is injected immediately after a real server commit, not
  through a network fault proxy.

- Default probe TTL is 900 seconds. After an abrupt crash without finally/release,
  takeover waits for expiry. The production TTL must exceed the longest interval
  between guarded writes, including upload/commit margin. Preserve the original
  exception if release also fails; this is a recorded Phase 1 nit.

## Validation and limits

Current format-2 proofs include the protocol/mapping golden, wire/builder fuzz,
mirror differential, grouped replay/reader/lifecycle suites, the pipeline ownership
proofs and the allowlisted remote frame/plan/atomic-DDL proof. Historical format-1
measurements below remain evidence of those revisions, not format-2 capacity.


`tests/test_phase1.py` covers deterministic replay across days, rollback and
ambiguous commit, incomplete manifests, bounded large-transaction spooling,
preflight provisioning, real standby feedback, heartbeat drain, schema history,
TRUNCATE and externally advanced slots. Subprocess crash cases exit while
building a chunk, before destination commit, after commit and before feedback;
recovery checks event identity uniqueness, complete manifests and lease release.
The crash harness explicitly expires the dead actor's lease using database time
instead of making each test wait 15 minutes.

Run with `CDC_TEST_MD=1` and `CDC_TEST_PG_DSN` as in the README. The integration
source is PostgreSQL 16 in Docker; the destination is real MotherDuck. These are
not managed-provider egress or PG 13/18 compatibility tests (Phase 4).

The on-demand `flights/batch_benchmark.py` measures direct Parquet append and
server-side MERGE on an actual F16. It varies 1/10/50 MiB batches with 25,000
high-entropy payloads at 1 KiB and 10 KiB. This isolates destination overhead;
it excludes pgoutput decoding, event envelopes, manifests and FULL update old
images, so it is not an end-to-end throughput acceptance result. The 50 MiB
1 KiB case contains only about 25 MiB of input. Production performance sizing
and the full workload envelope remain open; do not claim 20,000 events/sec
from these destination-only measurements.

### Crash and rollback

Abrupt client exit can leave a remote transaction pending until MotherDuck cleans
up that session. The crash test allows up to 180 seconds for that rollback before
simulating lease expiry; recovery then checks every expected source insert as well
as uniqueness. The normal 900-second lease is not shortened in production.

## Format-2 evidence

| Claim | Tests |
| -- | -- |
| Format-1 and unknown stores refused before any mirror write | `test_apply2.py::test_a_legacy_or_unknown_store_is_refused_before_any_mirror_write`, `…::test_consumer_for_dispatches_on_the_stored_format`, `test_backfill2.py::test_backfill_for_dispatches_on_the_stored_format` |
| Whole units across pages; cuts at the epoch start or a unit end | `test_apply2.py::test_whole_units_apply_across_pages_and_count`, `…::test_a_log_only_start_records_the_same_epoch_start_in_state_and_pin`, `…::test_a_key_bound_that_is_not_a_unit_end_fails_closed` |
| Whole-unit holds, poison and quarantine | `test_apply2.py::test_a_blocking_commit_holds_before_its_whole_unit`, `…::test_a_poison_unit_is_isolated_by_whole_units`, `…::test_quarantine_skips_the_whole_unit_and_advances_through_it`, `test_table_actions*.py` |
| Binding and key-layout refusals | `test_apply2.py::test_a_reader_binding_refusal_holds_needs_action`, `…::test_a_unit_under_another_key_layout_holds_needs_resync_at_its_start` |
| The unchanged fold over unit events | `test_apply2.py::test_cells_truncate_and_an_empty_projection_through_the_unchanged_fold`, `test_mirror2_content.py` (G2: the same wire stream materializes the same mirror as the frozen format-1 reference) |
| Deferred key activation | `test_apply2.py::test_a_pending_key_change_stops_apply_at_the_frozen_cut_then_holds_at_it`, `…::test_after_activation_the_registered_projection_matches_the_next_spools_units` |
| Backfill L and H | `test_backfill2.py` (H is the end of the marker's unit; catch-up by whole units; failures), `test_backfill2_pg.py::test_a_v2_generation_end_to_end_then_apply_continues` |
| Backfill catch-up on the SQL fold | `test_backfill_sql_fold.py` (each scenario on both folds: staging rows, generation row and failure reason equal; fallbacks, the Python redo, the oversized-page and OOM failures, fencing propagation, no frames read), `test_roundtrips.py::test_backfill_catch_up_batch_counts` |
| Backfill under the seeded workload; catch-up's inclusive bound | `test_gate_backfill.py::test_phase2_gate_backfill_every_table_under_seeded_workload` on a format-2 store; `…::test_catch_up_folds_the_unit_starting_at_each_batch_boundary` (each batch starts at the previous checkpoint, a unit end; replaces format 1's commit-adjacency test) |
| Page cache | `test_apply2_cache.py`: the cached split equals per-table reads; sharing only at identical pages and highs; errors never cached; a floor advance between tables holds the second table; per-table quarantine; the uncached fallback; clearing |
| The Phase 2 suites on format 2 | `test_phase2.py` (the synthetic harness and the live gate) and `test_backfill.py` run on format-2 stores |

Flight chaining is described in [flights.md](../docs/flights.md).

## Apply and backfill gates

The dedicated `test_adjacent_commit_lsn_is_applied` and key-change/key-revert
MotherDuck variants are also retired; these regressions remain local/PG checks.
The catch-up adjacent-bound test was already local-only. Real MVCC/shared-column
fencing and the stale-catalog publish guard remain in the remote allowlist.
Single-table backfill, resync, slot loss and native type fidelity retain remote
proofs, but do not replace the historical multi-table interleaving evidence.

The apply and backfill exit criterion: *source `EXCEPT ALL` mirror is empty at a quiescent marker
under the seeded workload, including backfill under load, resync after slot loss, and
the schema cases.*
- At sign-off every test below ran on real PostgreSQL 16 with the change log and mirrors on both
  local DuckDB and real MotherDuck. CI also runs them on PostgreSQL 13 and 18 with
  local stores.
- Each compares with `source EXCEPT ALL mirror` and the reverse, plus equal row counts.
- The seeded workload: three tables (FULL and default identity, a composite key, 16
  types with TOASTed values), small key spaces with reinserts, key moves and swap
  cycles, and three interleaved sessions, one holding a transaction across an ingest
  run.

| Criterion | Proven by |
| -- | -- |
| Live apply | `test_phase2.py::test_phase2_gate_source_equals_mirror_under_seeded_workload`: log-only start, six rounds, two ADD COLUMNs, an apply crash, a commit exactly at a stored checkpoint. |
| Backfill under load | `test_gate_backfill.py::test_phase2_gate_backfill_every_table_under_seeded_workload`: every table backfilled one chunk at a time while the workload writes. It covers changes to already-scanned keys, crashes in and after chunks and after publication, then live rounds and a resync under load. Also `test_gate_backfill.py::test_catch_up_includes_a_commit_at_its_batch_boundary` and `test_backfill.py::test_backfill_under_load_crashes_and_publication`. |
| Resync after slot loss | `test_gate_epoch.py::test_phase2_gate_resync_after_slot_loss_under_seeded_workload`, described below. Also `test_epoch_pg.py` (stale workers, skip, lower LSNs) and `test_epoch.py`. |
| Schema cases | The DDL table below. |

**The slot-loss gate:**
- **Setup:** the mirrors are built by backfill and checked after a live round.
- **The loss:** the slot is lost while ingest lags. Committed changes it never
  delivered are lost with it, the workload keeps writing on every table while there is
  no slot, and a transaction stays open across the new slot's creation.
- **The transition:** `new-epoch` waits for that transaction, then begins epoch 2 on
  `<slot>_e2`.
- **The resync:** every mirror holds `needs_resync` until a new-epoch generation
  rebuilds it under load. Each table is compared right after its publication, and the
  rows changed while capture was broken are correct.
- **The end:** live rounds, then a final check at a quiescent marker (workload stopped,
  ingest drained to its marker, every table caught up at that boundary).

Every resync in these tests ends with the table's retention pin owned by the new
generation and no generation pin left.

## Retention

**Evidence.** `tests/test_retention.py` runs on local DuckDB and MotherDuck. The
pin tests in `tests/test_backfill.py` also run on PG16.
- Pins advance only after their commits, including for quiet tables. A lost
  response after the mirror commit keeps the old pin, and a drop is refused.
- The floor rejects a pin at or below it, including one exactly at the floor.
- A dropped day stays dropped after a later day seals.
- A missing table pin holds apply `needs_resync`. An interrupted transfer is
  completed by apply, and completion is refused once the floor has passed the
  checkpoint.
- A stale generation cannot raise the pin. Transfer releases leaked pins of
  replaced generations but keeps the active and young ones.
- The post-read floor check catches a raced drop.
- One table's unguarded advance commits while another consumer holds the guard. A
  drop or transfer conflicts with an in-flight registration.
- Backfill:
  - the pin commits before the generation;
  - abandonment and failure release the pin, and a crash between keeps it;
  - a missing pin fails the generation;
  - publication transfers the pin, and a failure after the RENAME changes no pin;
  - a lost publication response is completed by apply;
  - a lost response after a start's commit (backfill or log-only) keeps the pin,
    and the generation resumes or apply completes the transfer;
  - a delayed transfer of a replaced generation leaves the table pin alone;
  - a replaced generation's pin, left by an interrupted transfer, is gone after the
    next publication.

Mutation checks, each killed by those tests:
- advancing before the mirror commit, or not advancing at the end of the run;
- registering the generation pin after the mirror start;
- a non-strict floor;
- transferring before the publication commit, or without the guard;
- advancing without the generation fence, or `ensure_pin` ignoring the generation;
- sweeping a published, current generation's pin, or releasing young pins in a
  transfer;
- removing the post-read floor check;
- releasing a start's pin on any error, in either start path;
- transferring without the generation pin.

## Validation history

The following evidence records the broad historical suite. For current backend
selection, use the fixed allowlist in [the testing policy](testing.md).

`tests/test_phase2.py` runs on local DuckDB and real MotherDuck. With
`CDC_TEST_PG_DSN`, it also runs against PostgreSQL 16.

- Unit: type mapping and casts; schema reconciliation; blocking cases; and a
  randomized fold of 30 seeded full-identity workloads. Each workload includes
  key changes, deletes and unchanged TOAST, and the fold must equal both the
  Phase 0 reference `normalize` and an independent model.
- Synthetic log:
  - a schema change mid-stream;
  - a crash inside the MERGE transaction and a lost commit response;
  - the batch cap;
  - partial images for default-identity updates, and a missing baseline;
  - the exact poison cut;
  - TRUNCATE after earlier rows;
  - lease busy and stale-worker rejection;
  - refusing to replace a generation;
  - refusing a log-only start without emptiness evidence, and the recorded
    override;
  - transactions whose commit LSN equals the previous end LSN, at a batch
    boundary and at a schema cut.
- Gate: a seeded workload over three tables.
  - Tables: FULL and default identity, a composite key, and 16 types (including
    NaN/Infinity doubles, unbounded numeric, jsonb, bytea, arrays, timestamptz,
    intervals) with TOASTed values.
  - Keys come from small spaces, so deleted keys are reinserted, rows move into
    freed keys, and swap cycles run inside one transaction.
  - Three sessions interleave. Transaction B stays open while A transactions
    commit, and C spans a whole ingest run and commits right after the drained
    marker. The test asserts that some transaction's commit LSN equals a stored
    apply checkpoint.
  - Six rounds include two ADD COLUMNs and an apply crash. After every round the
    source is quiescent at the shared boundary, and `source EXCEPT ALL mirror` and
    the reverse are empty for every table, with equal row counts.
  - An unsupported type change then holds one table after applying its earlier
    insert while the others stay current, and TRUNCATE holds `items` as
    `needs_resync`.
- Backfill gate (`tests/test_gate_backfill.py`): the same workload, tables and sessions,
  but every table already holds rows when capture begins (a log-only start is
  refused), so every mirror is built by a backfill generation while the source changes.
  - The three tables are backfilled round-robin, one chunk at a time. Between chunks
    the workload commits A transactions, B stays open across a chunk read, C stays
    open across ingest runs and an end marker, and ingest runs, so changes reach the
    log between one chunk and the next.
  - After each table's first chunk, the driver updates, deletes and inserts keys that
    generation has already scanned. Only catch-up can deliver those, and the test
    asserts at least one of each per table (the `items` update leaves its TOASTed
    value unchanged, a partial image).
  - Crashes inside a chunk transaction, after a chunk commit, and after a
    publication commit. After the last, the test checks that the pin transfer has not
    run, then that apply's read precondition completes it.
  - Right after each publication, before the workload moves on, the mirror is caught
    up and compared with the source: later changes could otherwise repair a stale
    row in the small key spaces.
  - Live apply rounds follow (one adds a column), then a resync of a live table
    under load: the old generation is `replaced`, the table pin moves to the new
    one, no generation pin remains, and every table is compared again.
  - Structure asserted so a resize can't drop a case: at least 3 snapshot chunks per
    generation, the after-scan changes above, B across a chunk read and C across
    a marker.
  - Mutations killed: publishing without catch-up, a chunk resuming one row late,
    catch-up ignoring deletes, catch-up never overwriting a staged row, and L
    recorded after the first chunk.
  - An exclusive lower bound for catch-up alone needs a commit exactly at a catch-up
    batch boundary, which random interleaving doesn't form. A separate PG test
    (`test_catch_up_includes_a_commit_at_its_batch_boundary`, not `md_gate`) builds
    one: one event per batch, and two adjacent commits on already-scanned keys, made
    with the log-only gate's advisory-lock construction (`commit_right_after`). It
    kills that mutant.
- Type fidelity (independent of apply's casts): mirror values are compared with
  psycopg2's native values. Doubles are compared bit for bit, so `-0.0`, NaN and
  denormals count. Other cases: bigint and smallint limits, numeric scale limits,
  microsecond timestamps, timestamptz rendered by a `+05:45` walsender session,
  `date 'infinity'`, `time '24:00:00'`, bytea with every byte value, and JSON with
  Unicode and escapes. A `numeric(12,3)` NaN then holds the table at exactly its
  transaction, after the clean insert before it. Preflight rejects
  `extra_float_digits=0`, a non-ISO DateStyle and escape bytea output.
- Backfill (`tests/test_backfill.py`, PG16, local DuckDB and MotherDuck):
  - **Under load.** Writes land inside, ahead of and behind the chunk being read;
    there are deletes of unread rows, key moves across the scan frontier, TOAST
    updates, and a transaction held open across a chunk read. Crashes inside a
    chunk transaction and after its commit resume without skipping or
    duplicating. ADD COLUMN during catch-up, then publish, apply from H, and
    source EXCEPT ALL mirror both ways. A log-only start of that populated table
    is refused.
  - **Resync of a stale log-only mirror.** Abandon mid-scan; a stale worker of
    the abandoned generation is fenced; apply keeps running during the resync;
    the held apply lease defers publication (`busy_publish`); the old generation
    becomes `replaced`; the replaced checkpoint can't be written; then source
    equals mirror.
  - **TRUNCATE during a backfill** fails the generation, drops its staging table
    and leaves no mirror.
  - **Publication faults.** A failure after the RENAME rolls back to the old
    mirror and generation. A lost response after commit leaves the generation
    published exactly once, and a resume-only retry starts nothing.
  - **Schema drift mid-snapshot.** DROP COLUMN between chunks fails the
    generation with its reason. Once ingest has passed the DDL, a new generation
    publishes and the mirror equals the source.
  - **Key change on a FULL-identity table.** After a primary-key change, rows that
    duplicate the old key hold apply as `needs_resync`, leaving the mirror
    untouched instead of collapsing the rows. A backfill before ingest has recorded
    the key is refused. The resync re-keys the mirror, and apply then matches the
    source.
  - **Key change and revert (A→B→A).** The mirror key equals the catalog key
    again, but apply still holds as `needs_resync`, and a resync under the new
    version re-establishes an exact mirror. A synthetic test also covers a stale
    observation of the old key after a change: apply stops at the cut, and a
    log-only start is refused.
  - **Chunk byte cap.** The under-load test uses an 8 KB cap, so chunks with
    TOASTed rows stop on bytes.
- Backfill mutation checks: disabling apply's key limit, the snapshot column
  check, or the byte-cap rule for `scan_done` each makes its test fail.
  Ignoring the key version (using only the latest confirmation) fails both
  revert tests.
- Mutation checks: suppressing the old-key delete on a key change makes the gate
  fail. Restoring an exclusive lower bound makes both the gate and the synthetic
  adjacent-commit test fail.

## Routine validation and historical compaction evidence

The full routine suite follows the [29-case remote budget](testing.md). On
MotherDuck, `test_compaction_drops_days_and_keeps_the_ledger_bounded` exercises the
real compaction driver; retained retention tests also prove strict pin equality,
physical DROP rollback and pin/drop serialization. The other compaction cases
(midnight manifests, epoch pruning, replay above the floor, missing pins and
concurrent append/prune permutations) retain local executions but no routine MD
variants. Earlier broader MD sign-off and performance measurements are historical,
not a claim that all those remote cases still run. No `--full` tier restores them.

## PostgreSQL version matrix

### Version differences found

- **Empty transactions.** PostgreSQL 13 and 14 send a BEGIN and COMMIT for a
  transaction with no published changes, such as DDL. PostgreSQL 15 and later
  skip them. Ingest already handles both; the whole suite passes on 13.
- **Partitioned roots.** With `publish_via_partition_root`, PostgreSQL 13 lists
  the root, not the leaves, in `pg_publication_tables`. Repeated preflight over
  a partitioned table passes on every version and has a regression test.
- No change to decoding or the event encoding was needed.

### Running the matrix

`compose.yaml` keeps PostgreSQL 16 as the default service. PostgreSQL 13 and 18
are behind the `matrix` profile on their own ports:

| Version | Service | Port |
| --- | --- | --- |
| 13 | `postgres13` | 55413 |
| 16 | `postgres` | 55432 |
| 18 | `postgres18` | 55418 |

Each server runs with `wal_level=logical` and 40 replication slots and WAL
senders. Set `CDC_TEST_PG_DSN` to one server and run the suite. Optionally set
`CDC_TEST_PG_MAJOR` to the expected major version. The test session then fails
at start unless the DSN reaches that version, so a wrong port cannot turn every
Postgres test into a skip.

#### Primary + standby pairs (replica capture spike)

The `replica` profile runs a primary and a physical streaming standby per major, for
replica capture (docs/read-replica.md; tests marked `replica`, skipped unless configured):

| Major | Primary | Standby |
| --- | --- | --- |
| 16 | `replica16-primary`, 55616 | `replica16-standby`, 55626 |
| 17 | `replica17-primary`, 55617 | `replica17-standby`, 55627 |
| 18 | `replica18-primary`, 55618 | `replica18-standby`, 55628 |

The standby is a `pg_basebackup` of the primary (`tests/replica/standby.sh`), streaming
through the physical slot `standby_slot` (so `primary_slot_name` is set), with
`hot_standby_feedback` on (in its postgresql.conf, so a test can turn it off with ALTER
SYSTEM) and `wal_level=logical`. Run, for example:

```sh
docker compose --profile replica up -d --wait replica16-primary replica16-standby
CDC_TEST_REPLICA_DSNS='{"16": {"primary": "host=127.0.0.1 port=55616 dbname=cdc_phase0 user=postgres password=phase0-local-only", "standby": "host=127.0.0.1 port=55626 dbname=cdc_phase0 user=postgres password=phase0-local-only"}}' \
  uv run pytest -m replica
docker compose --profile replica down -v
```

`-m replica` runs the spike (`tests/test_replica_spike.py`) and capture from the standby
(`tests/test_replica_capture.py`: external mode's preflight, bounded slot wait, drain past
the replay position, delete-source's split). CI's `replica` job runs them on the PG16 pair.

### CI

`.github/workflows/ci.yml` runs on pull requests and on pushes to `main`.

- A lint job runs `ruff check` and `ruff format --check`.
- A test job per version (13, 16, 18) starts that server from `compose.yaml` and
  runs `pytest -q -rs -k 'not benchmark'` with `CDC_TEST_PG_MAJOR` set.

The workflow uses no secrets. MotherDuck variants and benchmarks skip, as they do
locally without `CDC_TEST_MD` and `CDC_BENCHMARK`. The servers start from
`compose.yaml` rather than GitHub Actions service containers, because a service
container cannot pass `-c wal_level=logical` to the server. PostgreSQL 13's initdb
has no `-c` option either, so `POSTGRES_INITDB_ARGS` cannot set it.

The compose healthcheck connects over TCP. The image's entrypoint first runs a
temporary server on the Unix socket only, so a socket check can pass before the
real server is listening. That window matters on CI, where every container is new.

### Limits

- Compose and CI use major-version tags, so minor versions drift. The table above
  records the minors that were tested.
- The `postgres:18` image stores data under `/var/lib/postgresql/18/docker`.
  Compose mounts no volume, so this does not matter for tests.
- RDS and other managed providers are not yet validated.

## Source DDL matrix

The customer table is in [docs/tables.md](../docs/tables.md#source-ddl); these are the tests behind each
row.

| Source DDL | Observed | Test |
| -- | -- | -- |
| ADD COLUMN | mirror column added, values carried | both gates |
| ADD COLUMN … DEFAULT on existing rows | `needs_resync` at the last verified cut, before the ALTER (the catalog check sees `atthasmissing`; a volatile default rewrites the table, also held); resync. An append table: a warning only, its existing rows stay NULL | `test_schema_cases.py::test_schema_cases_hold_where_designed_and_resync_restores_equality`, `test_catalog_checks.py` |
| ALTER COLUMN TYPE, mirror type changes | `needs_action` exactly at the first transaction after the ALTER; resync | same, and the log-only gate |
| ALTER COLUMN TYPE, mirror type unchanged, source type changed (`text` → `varchar(n)`, `char(n)` → `text`, a `USING` into another type) | `needs_action` at the first transaction after the ALTER (reconcile compares the source type OID and typmod); resync, or `accept-types` when no value changed. A widening that keeps every value (a longer or unlimited `varchar`, `varchar` → `text`, a wider integer) is kept, with no hold | `test_type_identity.py` |
| ALTER COLUMN TYPE to the same type with `USING` (a same-type rewrite) | **not detected**: existing mirror rows keep their old values; resync then | none (a known limit, below) |
| RENAME COLUMN, DROP COLUMN | `needs_action` at a transaction that writes rows on both sides of the DDL, with none of it applied; resync | same |
| ADD / DROP PRIMARY KEY, seen by an ingest run | `needs_resync` at the key's last confirmed boundary; resync re-keys | `test_backfill.py::test_key_change_on_full_identity_holds_apply_then_resync_rekeys`, `…::test_key_change_and_revert_is_still_a_key_change` |
| PRIMARY KEY changed and restored between two ingest runs, REPLICA IDENTITY FULL | `needs_resync` at the last verified cut: the catalog check sees a new primary key constraint (REINDEX, VACUUM FULL and CLUSTER keep it); resync | `test_catalog_facts.py`, `test_catalog_checks.py` |
| ATTACH / DETACH PARTITION (publish_via_partition_root) | `needs_resync` at the last verified cut (the catalog check compares the leaf set); resync. An append table: `needs_action`; start-from-log. A leaf detached and re-attached between two runs isn't seen | `test_catalog_facts.py`, `test_catalog_checks.py`, `test_keyless_append.py` |
| TRUNCATE | `needs_resync`; resync | `test_schema_cases.py`, the log-only gate, `test_backfill.py::test_truncate_during_backfill_fails_the_generation` |
| DROP TABLE | that table holds (`dropped`), the rest of the source runs and the slot advances; `remove-table` releases it | `test_table_hold.py::test_a_dropped_table_holds_alone_and_remove_table_releases_it` |
| RENAME TABLE | that table holds (`renamed`) while its changes keep being logged; `add-table` under the new name relabels it in place, with no resync | `test_table_hold.py::test_a_rename_holds_then_relabels_in_place_without_a_resync` |
| DROP + CREATE under the same name | the old table holds (`replaced`); once published, the new one is logged as `unmanaged`; `add-table` adopts it, and its backfill replaces the old mirror under the same name | `test_table_hold.py::test_a_recreated_table_is_adopted_and_replaces_the_old_mirror` |
| New table | logged as `unmanaged` once published; `add-table`, then backfill | `test_table_hold.py::test_a_new_table_is_added_and_an_unregistered_one_is_refused` |
