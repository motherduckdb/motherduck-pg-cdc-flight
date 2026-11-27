# Project status

Updated 2026-11-20. What is built, the open work, and the constraints to preserve. This is an
experimental release without official support. How to work in the repository is in
[DEVELOPING.md](DEVELOPING.md).

## What is built

- **Ingest** ([dev-docs/log-format.md](dev-docs/log-format.md)): preflight, then bounded `pgoutput` ingest
  into the durable format-2 change log in whole units, with pipelined flush (collect, fence, acknowledge,
  submit) and replication feedback that follows only durable unit ends. Format 2 is the only format; its
  golden freezes the encoding.
- **Apply** ([dev-docs/apply.md](dev-docs/apply.md)): typed mirrors per table, folded server-side in SQL,
  with the Python fold for the batches SQL can't take; the two folds are held equal by differential tests.
  Per-table holds, quarantine of whole units, catalog checks for DDL that writes no WAL.
- **Backfill** ([dev-docs/backfill.md](dev-docs/backfill.md)): generations from a snapshot, caught up from
  the log and published atomically; resync and abandonment; log-only starts; the Flight re-triggers itself
  until published.
- **Table membership** ([docs/tables.md](docs/tables.md)): the registry is the table set; one bad table is
  held alone and ingest continues; `cdc-flight-tables add/remove`.
- **Keyless insert-only capture**: append mode for tables without a key (inserts only; see open work).
- **Retention** ([docs/retention.md](docs/retention.md)): consumer pins, daily compaction, ledger pruning,
  consumer expiry (report mode by default), source epoch transitions.
- **Operations**: every Flight writes its status to `cdc_ops` (best effort, never fails a run); the
  operations Dive reads it ([docs/dive.md](docs/dive.md)). Operator-action Flights are owner-only, with a
  15-minute dry-run preview and typed confirmations: table-action, quarantine, epoch, table-membership and
  delete-source ([docs/operations.md](docs/operations.md)).
- **Flights**: every Flight is a package stub pinned to a merged commit; `deploy_flights.py` plans and
  applies a source's Flights; ingest chains apply, and backfill chains itself
  ([docs/flights.md](docs/flights.md)). Verify-full TLS with pinned RDS CAs (`--pg-ca`).
- **PostgreSQL**: 13 to 18 ([docs/limits.md](docs/limits.md)); a read replica (standby) can be the source,
  on PostgreSQL 16+, qualified on self-managed 16, 17 and 18 and on RDS for PostgreSQL 16.15
  ([docs/read-replica.md](docs/read-replica.md)).
- **Performance**: measured on F16 in us-east-1 ([docs/performance.md](docs/performance.md)). TPC-C on
  format 2 sustains 4,500 tpmC on a 2-minute schedule, and 5,000 with little margin. The default ingest
  schedule is `*/3`.
- **Guards**: authority and budget registries checked against the code
  ([dev-docs/authority.md](dev-docs/authority.md)); typed state transitions with checked row counts; bounded
  invariant audits; `tests/test_docs.py` for links, doc paths cited from code and the frozen pins.

## Open work

1. **Package on PyPI.** Flights install the package from this repository's GitHub URL at a pinned commit.
   Publishing it to PyPI would let them install it from the package index instead.
2. **Provider qualification**, in this order: Amazon Aurora PostgreSQL, Supabase, Neon, Google Cloud SQL for
   PostgreSQL, Azure Database for PostgreSQL. Each run checks enabling logical replication and the grants, TLS with
   `verify-full` (only RDS CA bundles ship today; Cloud SQL and Azure likely need another CA option), whether a
   Flight can reach it (IPv4-only egress versus IPv6-only endpoints, private networking), poolers, what failover
   does to the slot, idle suspension (Neon), and a full ingest, backfill, apply and delete-source cycle. Results go
   into the qualification tables in [docs/read-replica.md](docs/read-replica.md) and [docs/limits.md](docs/limits.md).
3. **CDC logs in MotherDuck.** Optionally write each Flight run's structured log lines to a table in the operations
   database, with a retention period (7 days by default), tagged with the run, the Flight, the source and, where
   they apply, the table, the replication slot, the epoch and the mirror database. The operations Dive gets a log
   view filtered by those tags. Install failures happen before the package runs, so they still show only in the
   Flight's own run log.
4. **Raising the ingest ceiling.** The TPC-C log analysis is done: format-2 ingest is bound by MotherDuck
   appends (about 1.13 s per ~5.9k-frame batch, about 230 txn/s); a longer schedule, a native decoder and
   splitting slots each buy little or need a large design. Ingest logs per-append timing; the next step
   is deciding, from those logs under load, between configuration levers (DuckDB threads, larger batches, the
   checkpoint threshold) and a separate spool process. Not yet attributed: one ~2 s append COMMIT per batched run; a post-commit
   stall seen with many tables on one page. Sources over 50 tables are unprofiled.
5. **5,000 tpmC with the package install** is untested and close (about 1.4 s of a 3.2 s margin); the
   `*/3` ceiling and apply lag are unmeasured. Dependency installs over 10 s happen in about 1 in 55 runs;
   caching or vendoring them is an option, not started.
6. **Read replica follow-ups.** Ending an ingest run early when the heartbeat job has stopped (needs a design
   round; never when upstream contact is unknown). Failover and promotion are unsupported.
7. **UPDATE and DELETE on keyless tables** (needs a design). Append mode captures inserts only: an UPDATE
   holds the table, and a DELETE holds it unless `ignore_deletes` is set. Candidates under `REPLICA
   IDENTITY FULL`: a multiset delete/update matching one row equal to the old image, or recording each
   change as history. It changes append apply, not the frozen protocol files.
8. **Deferred:** the notifier, and the operator drill (the destructive Flights get dry runs only until
   then). Not planned: a separate source/mirror verifier.
9. **Small follow-ups:** recheck conflict-message matching when upgrading the pinned DuckDB 1.5.6; consider
   mapping a registration conflict before lease acquisition to busy (today it fails loudly and can be
   retried); measure the named metadata projections in-region before claiming a gain; still unconverted to
   typed specs: the other `apply_state` writers, ingest's hot-path source writes and the registry's
   hold/key writes.

## Constraints

- **Frozen protocols.** Format 2's encoding, digests, chunk boundaries and the fold rules are frozen
  (DEVELOPING.md, "Frozen protocols"). Before changing the canonical event encoding again, implement the
  documented fenced non-resync format migration. Never bypass a source binding or edit checkpoints to
  proceed.
- **Ingest can't be rolled back to a version from before the compaction tables**
  ([docs/upgrading.md](docs/upgrading.md)): older ingest
  writes positionally into `log_partitions`, which has since gained a column. Every INSERT names its
  columns (`tests/test_insert_columns.py`).
- One database per correctness-critical write transaction. Lease takeover and guarded mutations update the
  **same guard column**, with an ordinary UPDATE and an affected-row check; stale workers must not commit.
- DuckDB 1.5.6 skips write-write conflict detection for `UPDATE ... RETURNING`, and for a `DELETE` against
  a concurrent `UPDATE`, and detects conflicts per column. A fence is always a plain UPDATE of the guard
  column, never `RETURNING` and never a `DELETE`; a `DELETE` is safe only after its transaction's guard
  UPDATE.
- DuckDB treats an UPDATE that writes the stored value as no write: every fence writes a value that always
  changes (`uuid()`, `token+1`) ([dev-docs/invariants.md](dev-docs/invariants.md)).
- `COMMIT` of a transaction that hit a conflict returns silently after rolling back: a COMMIT's return is
  no proof that writes landed after a caught conflict error.
- PostgreSQL feedback follows durable complete units, never server WAL alone. Preserve retry identity
  across restarts, midnight and ambiguous commit responses.
- A deleting or deleted source is refused by every entry point; a deleted source id is final.
- New MotherDuck test variants run locally unless added to `tests/md_allowlist.json` with a reason; the
  suite is capped at ten minutes ([dev-docs/testing.md](dev-docs/testing.md)).
- Don't claim the 20,000 events/s design target.
- Keep the repository free of secrets and of internal working notes.
