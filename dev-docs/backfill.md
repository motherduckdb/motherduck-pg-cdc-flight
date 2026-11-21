# Backfill generations (DESIGN Protocol 2)

A generation never merges into an existing mirror. It builds
`<schema>.<table>__gen_<id>` from scratch, so a resync also removes rows the source
deleted while they went unobserved. The `generations` row holds its state and
progress, under its own lease (the same guard-column pattern as apply). A
`backfills` row per relation, inserted in the start transaction, allows at most
one active generation per table.

1. **Start:** in one mirror transaction, record the generation at the durable
   ingest checkpoint L (`pin_lsn`), with the staging columns and the current key
   read from the PostgreSQL catalog. Create the empty staging table with that
   PRIMARY KEY and no secondary indexes. Start is refused until ingest has
   recorded the current key (see Key changes).
2. **Snapshot:** read `chunk_rows` rows at a time in replica-identity key order
   (`WHERE ROW(keys) > ROW(last key) ORDER BY keys`). Each chunk's rows and its
   resume key commit in one transaction, with a compare-and-set on the chunk
   count. A kill redoes at most one chunk. Snapshot rows carry
   `_cdc_commit_lsn = L`. Before every chunk, the source columns and key are
   checked against the generation. A dropped, renamed or retyped column, a key
   change, or a renamed table fails the generation instead of retrying forever.
3. **Marker:** after the last chunk, record `pg_current_wal_lsn()` and move to
   `catching_up`. Then write the generation id into the heartbeat table (row
   `backfill:<relation_id>`). Once ingest commits that transaction, H is the end
   of the complete unit containing it. Every chunk was read before the marker, so
   H follows every read. The source is checked again after H is durable.
4. **Catch-up:** fold the logged changes in the whole units of `[L, H)` into
   staging with apply's fold and MERGE, in bounded batches with a `catchup_lsn`
   checkpoint at unit ends. Snapshot reads happened between L and H, so replaying every change
   in that range gives the source state at H. Rows untouched in the range keep
   their snapshot values, and the last change wins for every other key.

   Each batch takes apply's SQL fold (Where apply folds): the per-table plan
   statement (`plan_sql`), then one MERGE into staging whose source folds the
   frames where they are stored, so no frame reaches the Flight. The decision is
   apply's (`sql_path.decide`), against the staging columns and the generation's
   key. In place of the Python path's key check, a row-free check on the page's
   descriptors sends any page with a descriptor of another key layout to the
   Python fold. The Python fold takes every batch the plan rules out (TRUNCATE in
   `[L, H)`, several schema versions or key layouts, a key change, a blocking
   event, a `reconcile` refusal) and fails the generation exactly as before.
   - A data error from the SQL MERGE, or its baseline clause, rolls the batch back,
     and the same page is redone once on the Python fold, whose outcome stands. So
     a failed generation's `detail` is the Python fold's. The redo runs only for a
     page of at most 128 MiB of ledger bytes (apply's page cache bound). A larger
     page fails the generation with `<error> in catch-up (SQL fold) units L..H`
     instead, since reading it into the Flight could run out of memory there,
     which no generation state would record.
   - A MotherDuck out-of-memory error on the plan or the MERGE fails the generation
     (operations.md, Backfill catch-up).
   - Fencing (`FenceLost`), transaction conflicts, I/O errors and interrupts
     propagate, as on the Python fold; nothing is redone under a lease that may be
     lost.

   Each batch is one mirror transaction either way: the generation guard, the
   MERGE into staging and the `catchup_lsn` compare-and-set.
   `tests/test_backfill_sql_fold.py` runs each catch-up scenario on both folds and
   compares the staging rows, the generation row and the failure reason.
5. **Publish:** take the table's apply lease (if a mirror exists), then in one
   mirror transaction:
   - `DROP TABLE` the current mirror and `RENAME` staging to its name;
   - set `apply_state` to the new generation with `applied_through_lsn = H`;
   - mark the old generation `replaced` and this one `published`.

   Apply then continues from H. If apply holds the lease, the run reports
   `busy_publish` and retries later.

`tests/test_publish.py` proves the publication primitive on local DuckDB and
MotherDuck:
- the PRIMARY KEY survives DROP plus RENAME;
- a concurrent reader sees the old generation until commit;
- an injected failure after the rename rolls back to the old table and state;
- a stale compare-and-set changes nothing, and a concurrent guard write
  conflicts;
- a later MERGE works.

DuckDB refuses to rename a table that has an index, so staging tables never get
secondary indexes. User views over a mirror bind by name and follow the
published table.

| Generation outcome | Effect |
| :---- | :---- |
| `published` | Mirror replaced; the old generation is `replaced`. Fenced writers of the old generation fail their compare-and-set. |
| `failed` | Staging dropped, mirror untouched, reason in `detail`. Causes: TRUNCATE in `[L, H)`, a schema transition apply would hold, source columns or key changed during the backfill, a value the mirror type rejects, an unmanaged table already holding the mirror's name, or a source epoch change. Run backfill again to start a new generation. |
| `abandoned` | Operator abandonment (`--abandon`). Staging dropped, and the lease is required, so a running worker isn't cut off mid-chunk. |
