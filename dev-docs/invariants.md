# Protocol decisions and invariants

The decisions the protocols rest on, with the DuckDB behaviour behind them.

## Decisions and executable evidence

1. **Durability and identity.** Persist/acknowledge transaction-end LSNs; identify
   events by source, epoch, commit LSN and ordinal. UBIGINT is tested above 2^63.
   Batch IDs include the ordinal range, and retries verify a content digest.
   Schema descriptors are stored once per epoch/OID/version in the append
   transaction. Empty complete transactions advance the checkpoint. A simulated
   lost COMMIT response leaves no duplicate effects on retry.
2. **Database boundaries.** Log/manifest/checkpoint writes use the internal database.
   Mirror/generation/checkpoint writes use the mirror database. Publication reads
   the internal log inside a mirror-only write transaction. A negative test proves
   that writing both databases in one transaction fails and rolls back.
3. **Fencing and leases.** Acquisition and guarded writes both mutate the same
   column, using ordinary UPDATE and an affected-row check. A token read plus a
   write to another column is insufficient under DuckDB's column-level conflicts;
   UPDATE RETURNING also behaved differently in the local 1.5.6 probe. Active
   leases refuse overlap; expiry permits takeover. A fence must also **change the
   value**: DuckDB 1.5.6, both local and MotherDuck (verified 2026-11-05), can report
   one affected row for an unchanged-value UPDATE without registering a conflicting
   write. Production guards assign a fresh UUID or increment a token; the same
   column alone is insufficient. Both stale-snapshot and
   independent-MotherDuck-process conflicts are tested. Database time controls
   expiry, and guards renew the persisted TTL. `Store.lease()` releases in finally
   on success or exception; stale release cannot unlock a newer holder.
4. **Backfill publication.** Validate L against the active ingest epoch/checkpoint,
   register the pin before scanning, and commit each chunk with its checkpoint and
   digest. A fresh connection reads those durable chunks and the persisted log,
   validates the recorded heartbeat marker at H, and folds (L,H]. No Python scan
   state is reused. `CREATE OR REPLACE TABLE AS SELECT`, generation publication and
   apply checkpoint commit atomically in the mirror database. Failed publication
   restores the old mirror; delayed old-generation retries cannot change the
   current generation or recreate its pin.
5. **Retention.** Pin creation and compaction mutate a shared guard. Missing
   expected pins or no pins fail closed. Publication transfers the generation pin
   to the table only after the mirror commit. Apply advances the table pin after
   its commit, including quiet ranges. A lost response leaves an older, conservative
   pin until retry. Abandoned pins are released only after abandonment commits.
   The retention floor rejects requests for already-dropped history.
6. **Replication histories.** Checkpoints, pins, partition bounds and the floor use
   `(epoch, LSN)` ordering. An explicit fenced epoch transition precedes new
   ingestion. Resync can replace an old mirror with a new epoch at lower LSNs,
   then retire old-epoch history. Stale-epoch appends/apply are rejected.

Sources: [Postgres wire format](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html),
[DuckDB transactions](https://duckdb.org/docs/stable/sql/statements/transactions),
and the connected MotherDuck Flight guide (DuckDB 1.5.6 pin).
