# Upgrading

Steps that a store or a source created by an earlier version needs before or after an upgrade.

## Upgrading a store, and rolling back

The current code upgrades a store created by earlier code as it runs, add-only. It creates
the compaction tables, adds `log_partitions.bytes` and `tables.pin_state`, and marks every
table that already has a table pin `pinned`. The first compaction then drops only the sealed
days at or below the lowest pin.

**Ingest can't be rolled back to a version from before the compaction tables once the new
code has run.** Earlier ingest inserts into `log_partitions` positionally and fails on the added
`bytes` column. Recover by rolling forward. Apply, backfill and preflight from before it still run
on an upgraded store.

## Upgrading to the delete-source version

The delete-source version adds columns to the change log's `source` table (`lifecycle`,
`created_*`, `deleted_*`).
- **Update ingest first,** so its own initialize adds them.
- **If apply or backfill is updated first,** their initialize adds `lifecycle` instead. That
  ALTER can conflict once with a concurrent ingest transaction. It's retried once; the apply
  run may then fail, and the next run is fine.

## Upgrading to the canonical float-key fold

Releases before it folded a float key's `-0` and `0` as two keys: a transaction that deleted `0`
and inserted `-0` (or the reverse) left the mirror row deleted. The fix doesn't repair rows already
lost that way: a float-key table that hit it needs a resync.

## Upgrading to the catalog checks

A column added, or a partition attached, before the release that checks it isn't caught: the first
read after the upgrade adopts what it finds. If such DDL filled existing rows, resync the table.
