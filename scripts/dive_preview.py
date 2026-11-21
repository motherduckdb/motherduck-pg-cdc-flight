"""Build `.dive-preview/cdc_ops.duckdb` for the Dive's local preview (docs/dive.md):

    uv run python scripts/dive_preview.py
    cd dive && CDC_OPS_DB=../.dive-preview/cdc_ops.duckdb npm run dev

The data is the Dive query test's scenario on local stores (a held table, a quarantine,
an expiration, a lost slot, failed and refused runs), plus a hand-written healthy source,
`demo` (three live tables, recent runs; its runs read stale ten minutes after the build):
no PostgreSQL, no MotherDuck.
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEST = "tests/test_dive_queries.py::test_each_runbook_step_reads_its_screen"

# A healthy source for the preview only: what a working pipeline looks like on Overview.
DEMO = """
INSERT INTO ingest_runs BY NAME
  SELECT 'demo' AS source_id, 'demo-ingest-' || k AS run_id,
         epoch_us(current_timestamp - INTERVAL (k * 2) MINUTE) AS as_of,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'drained' AS status,
         12 AS transactions, 1 AS chunks, 3.2 AS duration_s, 65536 AS retained_bytes, 2048 AS lag_bytes,
         'verify-full' AS tls_mode, 'rds' AS tls_ca, true AS tls_in_use
  FROM range(6) t(k);
INSERT INTO ingest_status BY NAME
  SELECT 'demo' AS source_id, current_timestamp AS written_at, epoch_us(current_timestamp) AS status_as_of,
         'demo-ingest-0' AS run_id, 'drained' AS status, 1 AS epoch, 4096 AS checkpoint_lsn,
         current_timestamp - INTERVAL 40 SECOND AS durable_through_commit_ts, 1 AS durable_through_epoch,
         4096 AS durable_through_lsn,
         epoch_us(current_timestamp) AS last_success_as_of, 'demo-ingest-0' AS last_success_run_id,
         epoch_us(current_timestamp) AS slots_as_of,
         '[{"v":1,"name":"cdc_demo","epoch":1,"is_current":true,"active":false,"wal_status":"reserved",'
         || '"retained_bytes":65536,"lag_bytes":2048,"restart_gap_bytes":0}]' AS slots, '[]' AS holds;
INSERT INTO apply_runs BY NAME
  SELECT 'demo' AS source_id, 'demo-apply-' || k AS run_id, NULL AS table_name,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'done' AS status
  FROM range(6) t(k);
-- A week of per-table apply results, every 4 hours: orders was held for a while 3-4 days
-- ago (a day and a half: mixed, failed, mixed), and one whole apply run failed 5 days ago.
INSERT INTO apply_runs BY NAME
  SELECT 'demo' AS source_id, 'demo-apply-h' || h AS run_id, t AS table_name,
         current_timestamp - INTERVAL (h * 4) HOUR AS written_at,
         CASE WHEN t = 'public.orders' AND h BETWEEN 11 AND 24 THEN 'needs_action' ELSE 'caught_up' END AS status,
         CASE WHEN t = 'public.regions' OR (t = 'public.orders' AND h BETWEEN 11 AND 24) THEN 0
              ELSE 10 + (h * 7) % 50 END AS events
  FROM (VALUES ('public.customers'), ('public.orders'), ('public.regions')) v(t), range(42) r(h);
INSERT INTO apply_runs BY NAME
  SELECT 'demo' AS source_id, 'demo-apply-crash' AS run_id, NULL AS table_name,
         current_timestamp - INTERVAL 5 DAY AS written_at, 'failed' AS status, 'BinderException' AS error,
         'attach' AS step;
INSERT INTO apply_status BY NAME
  SELECT 'demo' AS source_id, rel AS relation_id, epoch_us(current_timestamp) AS as_of,
         current_timestamp AS written_at, 'demo-apply-0' AS run_id, 'public' AS schema_name, t AS table_name,
         'live' AS state, 1 AS epoch, 4096 AS applied_lsn, 'g' || rel AS generation_id, 1 AS key_version,
         0 AS quarantine_count, current_timestamp - INTERVAL 40 SECOND AS applied_through_commit_ts,
         1 AS applied_through_epoch, 4096 AS applied_through_lsn
  FROM (VALUES (11, 'customers'), (12, 'orders'), (13, 'regions')) v(rel, t);
"""

# The redesign's states, one source each (preview only):
# - tpcc: WAL limit 62 %, a paused table 9 h from retention expiry, a rebuilding table,
#   a stale one, one in the publication but never enabled;
# - app: recovering after a lost slot (epoch 2): rebuilt, rebuilding, failed, queued.
STATES = """
INSERT INTO sources BY NAME SELECT 'tpcc' AS source_id, current_timestamp AS written_at,
  'tpcc' AS label, 'tpcc-loadtest (RDS, us-east-1)' AS description, 'data-platform@example.com' AS owner_user;
INSERT INTO ingest_runs BY NAME
  SELECT s AS source_id, s || '-ingest-' || k AS run_id,
         epoch_us(current_timestamp - INTERVAL (k * 2) MINUTE) AS as_of,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'drained' AS status,
         CASE s WHEN 'tpcc' THEN (30 + k % 5) * 1073741824 ELSE 900000000 END AS retained_bytes, 2048 AS lag_bytes
  FROM (VALUES ('tpcc'), ('app')) v(s), range(0, 7 * 24 * 30, 7) t(k);
INSERT INTO ingest_status BY NAME
  SELECT s AS source_id, current_timestamp AS written_at, epoch_us(current_timestamp) AS status_as_of,
         s || '-ingest-0' AS run_id, 'drained' AS status, e AS epoch, 8192 AS checkpoint_lsn,
         epoch_us(current_timestamp) AS last_success_as_of, epoch_us(current_timestamp) AS slots_as_of,
         current_timestamp - INTERVAL 30 SECOND AS durable_through_commit_ts, e AS durable_through_epoch,
         8192 AS durable_through_lsn,
         '[{"v":1,"name":"md_cdc_' || s || '_e' || e || '","epoch":' || e || ',"is_current":true,"active":false,'
         || '"wal_status":"' || CASE s WHEN 'tpcc' THEN 'extended' ELSE 'reserved' END || '","retained_bytes":'
         || CASE s WHEN 'tpcc' THEN 33285996544 ELSE 900000000 END || ',"safe_wal_size":'
         || CASE s WHEN 'tpcc' THEN 20401094656 ELSE 52787286016 END || ',"lag_bytes":2048,"restart_gap_bytes":0}]' AS slots,
         CASE s WHEN 'tpcc' THEN '[{"v":1,"relation_id":99,"table":"public.stock_audit","reason":"unmanaged",'
           || '"detail":"run add-table, or remove it from the publication","held_since":"'
           || strftime(current_timestamp - INTERVAL 5 DAY, '%Y-%m-%d %H:%M:%S+00') || '"}]' ELSE '[]' END AS holds
  FROM (VALUES ('tpcc', 1), ('app', 2)) v(s, e);
INSERT INTO apply_runs BY NAME
  SELECT s AS source_id, s || '-apply-' || k AS run_id, NULL AS table_name,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'done' AS status
  FROM (VALUES ('tpcc'), ('app')) v(s), range(8) t(k);
INSERT INTO apply_runs BY NAME
  SELECT s AS source_id, s || '-apply-h' || h AS run_id, 'public.' || t AS table_name,
         current_timestamp - INTERVAL (h * 6) HOUR AS written_at, 'caught_up' AS status,
         (h * 37 + length(t) * 101) % 5000 AS events
  FROM (VALUES ('tpcc', 'order_line'), ('tpcc', 'stock'), ('tpcc', 'orders'), ('tpcc', 'history'), ('tpcc', 'district'),
               ('tpcc', 'item'), ('app', 'users'), ('app', 'invoices'), ('app', 'events'), ('app', 'sessions'),
               ('app', 'webhooks')) v(s, t), range(28) r(h);
INSERT INTO apply_status BY NAME
  SELECT s AS source_id, rel AS relation_id, epoch_us(current_timestamp) AS as_of, current_timestamp AS written_at,
         'r' AS run_id, 'public' AS schema_name, t AS table_name, st AS state, te AS epoch,
         CASE WHEN lag_min = 0 THEN 8192 ELSE 6000 END AS applied_lsn, 'g' || rel AS generation_id, 1 AS key_version,
         0 AS quarantine_count,
         CASE WHEN st = 'paused' THEN current_timestamp - INTERVAL 54 HOUR
              WHEN st = 'needs_resync' THEN current_timestamp - INTERVAL 70 MINUTE END AS held_since,
         CASE WHEN st = 'needs_resync' THEN 'source epoch changed' END AS detail,
         current_timestamp - INTERVAL 30 SECOND - INTERVAL (lag_min) MINUTE AS applied_through_commit_ts,
         te AS applied_through_epoch, CASE WHEN lag_min = 0 THEN 8192 ELSE 6000 END AS applied_through_lsn,
         CASE WHEN st = 'paused' THEN 5000 END AS pin_lsn,
         CASE WHEN st = 'paused' THEN current_timestamp - INTERVAL 54 HOUR END AS pin_updated_at
  FROM (VALUES ('tpcc', 21, 'order_line', 'live', 1, 0), ('tpcc', 22, 'stock', 'live', 1, 0),
               ('tpcc', 23, 'orders', 'paused', 1, 3240), ('tpcc', 24, 'history', 'live', 1, 52),
               ('tpcc', 25, 'district', 'live', 1, 0), ('tpcc', 26, 'item', 'live', 1, 0),
               ('app', 31, 'users', 'live', 2, 0), ('app', 32, 'invoices', 'live', 2, 0),
               ('app', 33, 'events', 'needs_resync', 1, 75), ('app', 34, 'sessions', 'needs_resync', 1, 75),
               ('app', 35, 'webhooks', 'needs_resync', 1, 75)) v(s, rel, t, st, te, lag_min);
INSERT INTO backfill_status BY NAME
  SELECT s AS source_id, gen AS generation_id, epoch_us(current_timestamp) AS as_of, current_timestamp AS written_at,
         'b' AS run_id, rel AS relation_id, 'public.' || t AS table_name, 'snapshot' AS mode, st AS state,
         c AS chunks, ct AS chunks_total, c * 50000 AS rows, false AS scan_done,
         current_timestamp - INTERVAL (m) MINUTE AS created_at, current_timestamp - INTERVAL 1 MINUTE AS updated_at
  FROM (VALUES ('tpcc', 'gen-stock-3', 22, 'stock', 'snapshotting', 14, 22, 31),
               ('app', 'gen-events-4', 33, 'events', 'snapshotting', 31, 58, 22),
               ('app', 'gen-sessions-4', 34, 'sessions', 'failed', 17, 40, 50)) v(s, gen, rel, t, st, c, ct, m);
INSERT INTO backfill_runs BY NAME
  SELECT 'app' AS source_id, 'bf-1' AS run_id, current_timestamp - INTERVAL 15 MINUTE AS written_at,
         'public.sessions' AS table_name, 'gen-sessions-4' AS generation_id, 'failed' AS status,
         'OperationalError' AS error, 'scan' AS step;
INSERT INTO compaction_status BY NAME
  SELECT 'tpcc' AS source_id, current_timestamp AS written_at, epoch_us(current_timestamp) AS run_as_of, 'c' AS run_id,
         'done' AS status, '{"expire":"report","max_retained_lag_hours":168}' AS limits,
         '[{"v":1,"owner":"table:23","kind":"table","relation_id":23,"lsn":5000,"updated_at_us":'
         || epoch_us(current_timestamp - INTERVAL 54 HOUR) || ',"rule":"lag","expires_at_us":'
         || epoch_us(current_timestamp + INTERVAL 9 HOUR) || '},{"v":1,"owner":"generation:gen-stock-3",'
         || '"kind":"generation","generation_id":"gen-stock-3","lsn":7000,"updated_at_us":'
         || epoch_us(current_timestamp - INTERVAL 31 MINUTE) || ',"rule":"lag","expires_at_us":'
         || epoch_us(current_timestamp + INTERVAL 7 DAY) || '}]' AS pins,
         '[{"v":1,"day":"' || strftime(current_date - 6, '%Y%m%d') || '","epoch":1,"status":"sealed","bytes":9800000000},'
         || '{"v":1,"day":"' || strftime(current_date, '%Y%m%d') || '","epoch":1,"status":"active","bytes":8600000000}]' AS days;
INSERT INTO audit BY NAME
  SELECT 'tpcc' AS source_id, 'a' || k AS run_id, current_timestamp - INTERVAL (m) MINUTE AS written_at,
         'alice@example.com' AS token_user, a AS action, '{"table":"public.' || t || '"}' AS inputs, false AS dry_run,
         '{"status":"done","detail":""}' AS result, 'alice@example.com' AS started_by
  FROM (VALUES (1, 'pause', 'orders', 3241), (2, 'resync', 'stock', 32)) v(k, a, t, m);
"""

# Phase 3: a source of insert-only (append mode) tables and the catalog and type holds, one
# table per state, with the writers' stored texts (tests/test_dive_queries.py checks them).
APPEND = """
INSERT INTO sources BY NAME SELECT 'logs' AS source_id, current_timestamp AS written_at,
  'logs' AS label, 'event logs (insert-only tables)' AS description, 'alice@example.com' AS owner_user;
INSERT INTO ingest_status BY NAME
  SELECT 'logs' AS source_id, current_timestamp AS written_at, epoch_us(current_timestamp) AS status_as_of,
         'logs-ingest-0' AS run_id, 'drained' AS status, 1 AS epoch, 8192 AS checkpoint_lsn,
         epoch_us(current_timestamp) AS last_success_as_of, epoch_us(current_timestamp) AS slots_as_of,
         current_timestamp - INTERVAL 30 SECOND AS durable_through_commit_ts, 1 AS durable_through_epoch,
         8192 AS durable_through_lsn,
         '[{"v":1,"name":"md_cdc_logs_e1","epoch":1,"is_current":true,"active":false,"wal_status":"reserved",'
         || '"retained_bytes":400000000,"safe_wal_size":52787286016,"lag_bytes":1024,"restart_gap_bytes":0}]' AS slots,
         '[{"v":1,"relation_id":58,"table":"public.audit_trail","reason":"append_identity",'
         || '"detail":"public.audit_trail: append mode needs REPLICA IDENTITY FULL","held_since":"'
         || strftime(current_timestamp - INTERVAL 3 HOUR, '%Y-%m-%d %H:%M:%S+00') || '"}]' AS holds;
INSERT INTO ingest_runs BY NAME
  SELECT 'logs' AS source_id, 'logs-ingest-' || k AS run_id, epoch_us(current_timestamp - INTERVAL (k * 2) MINUTE) AS as_of,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'drained' AS status,
         400000000 AS retained_bytes, 1024 AS lag_bytes
  FROM range(0, 7 * 24 * 30, 7) t(k);
INSERT INTO apply_runs BY NAME
  SELECT 'logs' AS source_id, 'logs-apply-' || k AS run_id, NULL AS table_name,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'done' AS status
  FROM range(8) t(k);
INSERT INTO apply_runs BY NAME
  SELECT 'logs' AS source_id, 'logs-apply-0' AS run_id, 'public.big_events' AS table_name,
         current_timestamp - INTERVAL 1 MINUTE AS written_at, 'awaiting_generation' AS status;
INSERT INTO apply_status BY NAME
  SELECT 'logs' AS source_id, rel AS relation_id, epoch_us(current_timestamp) AS as_of, current_timestamp AS written_at,
         'logs-apply-0' AS run_id, 'public' AS schema_name, t AS table_name, st AS state, 1 AS epoch,
         CASE WHEN st = 'live' THEN 8192 ELSE 6000 END AS applied_lsn, g AS generation_id, 1 AS key_version,
         0 AS quarantine_count, mode AS capture_mode, del AS ignore_deletes, CASE WHEN del THEN 1834 END AS deletes_ignored,
         CASE WHEN t = 'clicks' THEN 7000 END AS last_truncate_lsn,
         CASE WHEN st <> 'live' THEN current_timestamp - INTERVAL 40 MINUTE END AS held_since, detail,
         CASE WHEN st = 'needs_action' AND detail LIKE 'append_%' THEN 6100 END AS blocked_commit_lsn,
         CASE WHEN st = 'needs_action' AND detail LIKE 'append_%' THEN 6050 END AS blocked_unit_start_lsn,
         CASE WHEN st = 'needs_action' AND detail LIKE 'append_%' THEN 6200 END AS blocked_unit_end_lsn,
         current_timestamp - INTERVAL 30 SECOND - INTERVAL (CASE WHEN st = 'live' THEN 0 ELSE 40 END) MINUTE AS applied_through_commit_ts,
         1 AS applied_through_epoch, CASE WHEN st = 'live' THEN 8192 ELSE 6000 END AS applied_through_lsn
  FROM (VALUES
    (51, 'page_views', 'live', 'append', true, 'gen-pv-2', NULL),
    (52, 'clicks', 'live', 'append', false, 'gen-clicks-1', NULL),
    (53, 'orders_log', 'needs_action', 'append', false, 'gen-ol-1',
     'append_update: an UPDATE on an insert-only table; resync it (an exact re-snapshot, which holds again if the table keeps changing rows), quarantine its unit, start it again from the log, or remove the table'),
    (54, 'metrics', 'needs_action', 'append', false, 'gen-m-1',
     'a partition of the source table was attached or detached; start-from-log (it drops the rows before the new start)'),
    (55, 'accounts', 'needs_resync', NULL, NULL, 'gen-a-1', 'the source table''s primary key constraint was replaced; resync'),
    (56, 'prices', 'needs_action', NULL, NULL, 'gen-p-1',
     'column ''amount'' changed type (its source type, under the same mirror type: values may differ); resync, or start-from-log for an append table'),
    (57, 'sessions_log', 'live', 'append', false, 'gen-sl-1', NULL))
    v(rel, t, st, mode, del, g, detail);
INSERT INTO backfill_status BY NAME
  SELECT 'logs' AS source_id, g AS generation_id, 'public.' || t AS table_name, m AS mode, st AS state, pin AS pin_lsn,
         current_timestamp - INTERVAL (h) HOUR AS created_at, current_timestamp - INTERVAL (h) HOUR AS updated_at
  FROM (VALUES ('3f9c2a7b11d04e2a', 'page_views', 'append_snapshot', 'replaced', 5000, 30),
               ('gen-pv-2', 'page_views', 'log_incomplete', 'published', 5200, 26),
               ('gen-sl-2', 'sessions_log', 'append_snapshot', 'failed', NULL, 1)) v(g, t, m, st, pin, h);
INSERT INTO backfill_runs BY NAME
  SELECT 'logs' AS source_id, 'logs-bf-' || k AS run_id, current_timestamp - INTERVAL (m) MINUTE AS written_at,
         'public.' || t AS table_name, g AS generation_id, st AS status, d AS detail
  FROM (VALUES (1, 'sessions_log', 'gen-sl-2', 'failed', 'the copy can''t finish in this run: use start-from-log', 50),
               (2, 'big_events', NULL, 'refused', 'public.big_events: is 5368709120 bytes, over the append snapshot limit 1073741824 (CDC_APPEND_SNAPSHOT_MAX_BYTES): use start-from-log, or raise the limit', 3),
               (3, 'accounts', NULL, 'refused', 'public.accounts: ingest has not yet passed the source key change; run ingest, then retry', 2))
    v(k, t, g, st, d, m);
"""

# Example audit rows the preview's simulated runs read (dive/runtime/query-provider.tsx):
# a dry run's `would` row per action (the Flights' own wording, the table generalized), and
# a real run's result per outcome. Action 'example' keeps them off every table's history.
WOULD = {
    "pause": "pause: apply skips it until resumed",
    "resume": "resume: apply continues from its checkpoint",
    "retry": "retry: apply re-reads from its checkpoint",
    "resync": "resync the table: a full reload of the table from the source",
    "start-from-log": "start the mirror from the log at the current position: only rows "
    "inserted from then on are captured (rows already in the table are not copied)",
    "resync-all": "resync the table (longest held): a full reload of the table from the source",
    "compact-now": "compact now with the owner's settings",
    "add": "register the table, then rebuild it from a snapshot",
    "remove": "remove the table: stop capturing it and release its pins; the mirror table is kept",
    "quarantine": "quarantine: the whole unit 0/1780..0/1838 (3 commits), held on commit "
    "0/17D4 (KeyError: 'id'); rows touched by unit 0/1780..0/1838 (3 commits) in "
    "public.example may diverge until a resync",
    "recover-slot": "create md_cdc_logs_e2 and begin epoch 2; WAL after 0/2000 that the old "
    "slot lost is abandoned; every mirror then needs a resync",
    "skip-to-lsn": "advance the slot to the target and begin the next epoch: the WAL before "
    "it is never ingested; every mirror then needs a resync",
    "delete-source": "delete source: stop capturing it; drop 2 object(s) this code created; "
    "1 left for the DBA; mirrors and the log are kept",
}
EXAMPLES = (
    """
INSERT INTO audit BY NAME
  SELECT 'logs' AS source_id, gen_random_uuid()::VARCHAR AS run_id,
         current_timestamp - INTERVAL 1 DAY AS written_at, 'alice@example.com' AS token_user,
         'example' AS action, '{"table":"public.example"}' AS inputs, true AS dry_run,
         json_object('status', 'would', 'detail', d, 'next_steps', []::VARCHAR[],
                     'drop', CASE WHEN a = 'delete-source' THEN ['publication md_cdc_logs_pub', 'role md_cdc_logs_ingest'] END,
                     'dba', CASE WHEN a = 'delete-source' THEN ['DROP ROLE md_cdc_logs_owner;'] END) AS result,
         'alice@example.com' AS started_by, 'dive-example-would-' || a AS request_id
  FROM (VALUES """
    + ", ".join(f"('{a}', '{d.replace(chr(39), chr(39) * 2)}')" for a, d in WOULD.items())
    + """) v(a, d);
"""
)
EXAMPLES += """
INSERT INTO audit BY NAME
  SELECT 'logs' AS source_id, gen_random_uuid()::VARCHAR AS run_id, current_timestamp - INTERVAL 1 DAY AS written_at,
         'alice@example.com' AS token_user, 'example' AS action, '{"table":"public.example"}' AS inputs,
         false AS dry_run, r AS result, 'alice@example.com' AS started_by, q AS request_id
  FROM (VALUES
    ('dive-example-done', '{"status":"done","detail":"(preview) a simulated run: in the Dive this is the Flight''s own result","next_steps":[]}'),
    ('dive-example-refused', '{"status":"refused","detail":"(preview) a simulated refusal: the state changed since the preview","next_steps":["preview again"]}'),
    ('dive-example-busy', '{"status":"busy","detail":"(preview) a simulated busy run: another run holds the lease","next_steps":[]}'))
    v(q, r);
"""


# Replica capture (docs/read-replica.md): read replica sources, one per diagnostic state, to
# review the wording. `standby` is advisory JSON (clocks labelled in the doc).
def _standby(lag, replayed_ago, status, contact_ago, beat_ago):
    contact = (
        "NULL"
        if contact_ago is None
        else f"'\"' || strftime(current_timestamp - INTERVAL {contact_ago} SECOND, '%Y-%m-%dT%H:%M:%S+00:00') || '\"'"
    )
    return (
        '\'{"v":1,"replay_lsn":90000,"receive_lsn":\' || (90000 + ' + str(lag) + ") || "
        f'\',"replay_lag_bytes":{lag},"last_replay_commit_ts":"\' || '
        f"strftime(current_timestamp - INTERVAL {replayed_ago} SECOND, '%Y-%m-%dT%H:%M:%S+00:00') || "
        f'\'","receiver_status":"{status}","upstream_contact_ts":\' || '
        + ("'null'" if contact_ago is None else contact)
        + ' || \',"heartbeat_marker":"x","heartbeat_ts":"\' || '
        f"strftime(current_timestamp - INTERVAL {beat_ago} SECOND, '%Y-%m-%dT%H:%M:%S+00:00') || "
        '\'","heartbeat_interval_s":60,"observed_ts":"\' || '
        "strftime(current_timestamp, '%Y-%m-%dT%H:%M:%S+00:00') || '\"}'"
    )


def _replica_source(source, label, standby, slot_extra=""):
    return f"""
INSERT INTO sources BY NAME SELECT '{source}' AS source_id, current_timestamp AS written_at,
  '{source}' AS label, '{label}' AS description, 'alice@example.com' AS owner_user;
INSERT INTO ingest_status BY NAME
  SELECT '{source}' AS source_id, current_timestamp AS written_at, epoch_us(current_timestamp) AS status_as_of,
         '{source}-ingest-0' AS run_id, 'drained' AS status, 1 AS epoch, 90000 AS checkpoint_lsn,
         epoch_us(current_timestamp) AS last_success_as_of, epoch_us(current_timestamp) AS slots_as_of,
         '[{{"v":1,"name":"md_cdc_{source}","epoch":1,"is_current":true,"active":false,"wal_status":"reserved",'
         || '"retained_bytes":1000000,"safe_wal_size":52787286016,"lag_bytes":2048,"restart_gap_bytes":0{slot_extra}}}]' AS slots,
         epoch_us(current_timestamp) AS standby_as_of, {standby} AS standby;
INSERT INTO ingest_runs BY NAME
  SELECT '{source}' AS source_id, '{source}-ingest-' || k AS run_id, epoch_us(current_timestamp - INTERVAL (k * 2) MINUTE) AS as_of,
         current_timestamp - INTERVAL (k * 2) MINUTE AS written_at, 'drained' AS status,
         1000000 AS retained_bytes, 2048 AS lag_bytes, {standby} AS standby
  FROM range(0, 60, 1) t(k);
"""


REPLICA = (
    _replica_source("rr_ok", "read replica: healthy", _standby(4096, 5, "streaming", 2, 30))
    + _replica_source(
        "rr_stale", "read replica: heartbeat stale", _standby(0, 900, "streaming", 2, 900)
    )
    + _replica_source(
        "rr_unknown", "read replica: no pg_read_all_stats", _standby(512, 20, "unknown", None, 40)
    )
    + _replica_source(
        "rr_conflict",
        "read replica: slot invalidated by a recovery conflict",
        _standby(0, 10, "streaming", 1, 20),
        ',"invalidated_by_conflict":"rows_removed"',
    ).replace('"wal_status":"reserved"', '"wal_status":"lost"')
)


# Heartbeat job not running (design-early-exit U0): the last runs all capped with one marker,
# 9 min apart; upstream contact moves with each run (rr_job_dead) or is unknown (rr_job_unknown).
def _capped_runs(source, contact):
    patch = (
        "json_merge_patch(standby, json_object('upstream_contact_ts', "
        "strftime(written_at - INTERVAL 2 SECOND, '%Y-%m-%dT%H:%M:%S+00:00')))::VARCHAR"
        if contact
        else "standby"
    )
    return f"""
UPDATE ingest_runs SET status = 'capped', standby = {patch},
  written_at = current_timestamp - INTERVAL (CAST(split_part(run_id, '-', -1) AS INTEGER) * 9) MINUTE,
  as_of = epoch_us(current_timestamp - INTERVAL (CAST(split_part(run_id, '-', -1) AS INTEGER) * 9) MINUTE)
WHERE source_id = '{source}';
UPDATE ingest_status SET status = 'capped' WHERE source_id = '{source}';
"""


REPLICA += (
    _replica_source(
        "rr_job_dead",
        "read replica: heartbeat job not running",
        _standby(0, 1500, "streaming", 2, 1500),
    )
    + _capped_runs("rr_job_dead", contact=True)
    + _replica_source(
        "rr_job_unknown",
        "read replica: heartbeat unchanged, contact unknown",
        _standby(0, 1500, "unknown", None, 1500),
    )
    + _capped_runs("rr_job_unknown", contact=False)
)


if __name__ == "__main__":
    out = ROOT / ".dive-preview" / "cdc_ops.duckdb"
    out.parent.mkdir(exist_ok=True)
    out.unlink(missing_ok=True)
    env = dict(os.environ, CDC_DIVE_PREVIEW=str(out), CDC_TEST_MD="0")
    env.pop("MOTHERDUCK_TOKEN", None)
    env.pop("motherduck_token", None)
    code = subprocess.call(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", TEST], cwd=ROOT, env=env
    )
    if code or not out.is_file():
        sys.exit(code or 1)
    import duckdb

    with duckdb.connect(str(out)) as con:
        con.execute(DEMO)
        con.execute(STATES)
        con.execute(APPEND)
        con.execute(EXAMPLES)
        con.execute(REPLICA)
    print(f"wrote {out}")
