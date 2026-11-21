# Capturing from a read replica

CDC can read from a **read replica (standby)** instead of the primary: a physical streaming
replica running PostgreSQL 16 or newer. The Flights connect only to that read replica. The
primary is touched once, by the DBA, to create the heartbeat table, the publication and a
scheduled heartbeat job. This page is the read replica's part of [setup.md](setup.md) (steps 2,
3, 6 and 7 differ), its diagnostics, its runbooks and the providers it has been qualified on.

The mode is `CDC_HEARTBEAT_WRITER=external` (`[postgres] heartbeat_writer = "external"` in the
deploy file). It is recorded when the source registers; changing it later is refused.

## Requirements

On the read replica (preflight checks each one, on the read replica only):
- PostgreSQL 16 or newer, `wal_level = logical`, and `hot_standby_feedback = on`. Without
  feedback the primary can remove catalog rows the slot needs, and the slot is invalidated.
- `primary_slot_name` set (a physical slot upstream). Recommended, not required: preflight warns
  without it.
- A free replication slot and WAL sender (`max_replication_slots`, `max_wal_senders`).
- The read replica instance's **own endpoint**. Preflight refuses RDS/Aurora cluster, reader,
  custom-cluster and proxy endpoints: they may move to another instance.

The CDC role needs `REPLICATION` (on RDS: `GRANT rds_replication TO <role>;`) and `SELECT` on the
captured tables and the heartbeat table. It needs no `INSERT` or `UPDATE` anywhere. Optionally,
`GRANT pg_read_all_stats TO <role>;` (or `pg_monitor`) lets the diagnostics show the read
replica's upstream connection. Without it they show "unknown". Grants are made **on the
primary**; the read replica receives them.

## The DBA script, on the primary

The read replica can't be written, so preflight never creates the heartbeat table or the
publication there. If either is missing, it refuses with this SQL:

```sql
CREATE TABLE public.md_cdc_heartbeat (id text PRIMARY KEY, marker text NOT NULL);
CREATE PUBLICATION md_cdc_<source>_pub FOR TABLE <tables>, public.md_cdc_heartbeat
  WITH (publish_via_partition_root = true);
-- any scheduler; pg_cron shown. One minute is the portable default.
SELECT cron.schedule('md_cdc_heartbeat_<source>', '* * * * *', $$
  INSERT INTO public.md_cdc_heartbeat (id, marker)
  VALUES ('external:<source>', clock_timestamp()::text)
  ON CONFLICT (id) DO UPDATE SET marker = excluded.marker $$);
```

The job does that upsert and nothing else. It is liveness only: it guarantees that a quiet
source still produces a commit, so ingest runs and backfills can finish. Set
`[postgres] heartbeat_interval_seconds` to the job's interval (default 60). It is used only to
display the heartbeat's freshness.

## Slot creation and `waiting_for_primary`

Preflight with `CDC_CREATE_OBJECTS=1` creates only the replication slot, on the read replica.
Creating a slot on a read replica waits until the primary logs a standby snapshot. The wait is
bounded (`CDC_SLOT_WAIT_SECONDS`, default 90 s). When it runs out:
- preflight reports `waiting_for_primary` and exits 3 (the run shows FAILED); nothing is
  registered, so retrying is safe;
- an append table's snapshot and the epoch Flight report `waiting_for_primary` too, and retry.

The fix is on the primary: make sure the heartbeat job runs, or ask the DBA to run
`SELECT pg_log_standby_snapshot();`. **It helps only once no transaction is open on the
primary**: an open transaction keeps the wait going until it ends.

## Backfill from a read replica

- A keyed table is read in chunks. Each chunk records the read replica's replay position, and the
  backfill finishes at the first change-log unit past the highest one. A chunk cancelled by a
  recovery conflict is retried up to 3 times in a run, with half the rows each time.
- An append table's snapshot is copied in one exported snapshot. A recovery conflict during the
  copy fails the backfill (the snapshot can't be resumed): resync it.
- Recovery conflicts come from the primary's activity: locks (DDL, `VACUUM` truncation) and, with
  `hot_standby_feedback` off, vacuumed rows. Raise `max_standby_streaming_delay` on the read
  replica if long backfills are cancelled often.
- A backfill **fails** (it doesn't wait) if the read replica's replay position goes backwards or
  its identity or timeline changes: another or a rewound instance. Resync it.

## Diagnostics

Each ingest run of a read replica source records, in `cdc_ops.ingest_status.standby` (and per
run in `ingest_runs.standby`), a JSON object (`"v": 1`, then):

| Field | Meaning | Clock |
|---|---|---|
| `replay_lsn` | the read replica's replay position (the run's B) | |
| `receive_lsn` | the WAL it has received and flushed | |
| `replay_lag_bytes` | `receive_lsn − replay_lsn` | |
| `last_replay_commit_ts` | the commit time of the last replayed transaction | primary |
| `receiver_status` | the WAL receiver's status, or `unknown` without `pg_read_all_stats` | |
| `upstream_contact_ts` | the last message from the primary, or null (unknown) | read replica |
| `heartbeat_marker`, `heartbeat_ts` | the DBA job's last value (text, and parsed) | primary |
| `heartbeat_interval_s` | the declared job interval | |
| `observed_ts` | when the run read these | read replica |

`standby_as_of` is when the record was read: a run that fails before reading it keeps the previous
record, so show its age with it. **All of it is advisory.** No run reads these values to decide anything. A heartbeat older than 3 ×
its interval is shown as "stale or upstream unreachable", never as "down": without primary
access the two can't be told apart. The heartbeat and last-commit times come from the primary's
clock and the observation time from the read replica's, so clock skew shows up as staleness. Without
`pg_read_all_stats`, the upstream status and contact are "unknown", not "unreachable".

## Runbooks

- **`waiting_for_primary`** (preflight, an append snapshot, epoch): see above.
- **The slot was invalidated by a recovery conflict** (`invalidated_by_conflict` in the slot
  detail; `wal_status` `lost`): turn `hot_standby_feedback` back on if it was off, then run the
  epoch Flight's **Recover slot**. It recreates the slot on the read replica (under the same
  bounded wait) and starts a new epoch; tables then resync.
- **Promotion, failover, a timeline change or another instance:** not supported. Ingest refuses
  (the source's recorded timeline no longer matches) and backfills fail. Recreate the source
  against the new read replica and backfill it.
- **The heartbeat job stopped:** a busy source keeps working. A quiet one ends each ingest run at
  its runtime cap (`capped`) and backfills wait (`waiting_past_replay`) until a commit arrives.
  Restart the job.
- **delete-source:** drops the slot on the read replica and prints the cleanup to run on the
  primary: `SELECT cron.unschedule('md_cdc_heartbeat_<source>');`, then the publication and the
  heartbeat table (only if nothing else uses them).

## Amazon RDS for PostgreSQL

Found while qualifying RDS for PostgreSQL 16.15 (2026-11-15):
- **A read replica can't be created from a source that uses the RDS-managed master password**
  (`ManageMasterUserPassword`). Use an unmanaged master password on a source you replicate.
- A read replica takes its source's parameter group (one can't be given at creation). Set
  `rds.logical_replication = 1` and `hot_standby_feedback = 1` there.
- The master user (`rds_superuser`) has no `REPLICATION`: `GRANT rds_replication TO <role>;` on the
  primary. `pg_log_standby_snapshot()` is allowed for `rds_superuser`.
- The read replica has `primary_slot_name` set by RDS.
- The RDS primary is never idle, yet a slot's creation still waited past 120 s without the
  heartbeat job or `pg_log_standby_snapshot()`.
- `pg_read_all_stats` / `pg_monitor`: granted by `rds_superuser` per AWS's documentation; not
  tested.

## Provider qualification

| Provider | Versions | Qualified | How |
|---|---|---|---|
| Self-managed PostgreSQL (streaming replica) | 16.15, 17.12, 18.6 | Yes (2026-11-16) | the compose `replica` fixture: every replica test; CI runs 16 |
| Amazon RDS for PostgreSQL (read replica) | 16.15 | Yes (2026-11-15) | the slot, snapshot and exactly-once tests, on RDS |
| Amazon RDS for PostgreSQL | 17, 18 | Not qualified | untested |
| Aurora PostgreSQL | — | Not qualified | untested; its readers aren't physical streaming replicas, and its cluster and reader endpoints are refused |
| Google Cloud SQL, Azure Database for PostgreSQL, Supabase, Neon, Crunchy Bridge, others | — | Not qualified | untested |

"Not qualified" means not tested, not known not to work.

## Limits

- Failover and promotion are not supported (above).
- `external` is for a read replica only; a primary keeps the default `self` mode.
- A stale heartbeat never stops ingest; on a quiet source it costs each run its full runtime (an
  early exit isn't in this release).
