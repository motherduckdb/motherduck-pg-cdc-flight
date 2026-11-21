// Every SQL string the Dive runs, in one place, with the columns each returns.
//
// - Every reference is "ops"."main".<table>: the share of `cdc_ops` declared in
//   REQUIRED_DATABASES is the only thing the Dive reads (docs/dive.md).
// - JSON list parts (slots, holds, pins, days, warnings, quarantines, expirations) are
//   unnested here with from_json(col, '["json"]') and read by key, so keys the writers add
//   later never break a query.
// - `*_as_of` columns are epoch microseconds of server time. Times leave SQL as epoch
//   seconds and the browser formats them in the viewer's zone; ages of status rows use the
//   server's current_timestamp, while the Source page's "read … ago" and "No sync for"
//   use the browser clock.
// - scripts/dump-queries.ts renders every query into queries.snapshot.json, which
//   tests/test_dive_queries.py runs against a real cdc_ops.
import { q } from './format.ts';
import { ALIAS } from './share.ts';

export type Filters = { source: string | null };
export const ALL: Filters = { source: null };

const T = (t: string) => `"${ALIAS}"."main"."${t}"`;
const bySource = (f: Filters, column = 'source_id') => (f.source ? ` AND ${column} = ${q(f.source)}` : '');
const fromUs = (us: string) => `make_timestamp(${us})`;
// TIMESTAMPTZ values are shown in UTC whatever the session's time zone (a Dive viewer's
// may be their browser's); make_timestamp(us) is already a UTC TIMESTAMP.
const fmt = (ts: string) => `strftime((${ts}) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S UTC')`;
const age = (ts: string) => `round(epoch(current_timestamp) - epoch(${ts}))`;
/** An LSN (UBIGINT) as PostgreSQL writes it: X/Y in hex. */
export const lsn = (v: string) => `CASE WHEN ${v} IS NULL THEN NULL ELSE format('{:X}/{:X}', (${v})::UBIGINT >> 32, (${v})::UBIGINT & 4294967295) END`;
const items = (column: string) => `unnest(from_json(coalesce(${column}, '[]'), '["json"]'))`;
const j = (key: string) => `(i ->> '${key}')`;

/**
 * Staleness (docs/dive.md): a writer is stale when its newest run is older than
 * max(floor, 2 x the upper-quartile interval of its last 20 runs); below 5 runs, the
 * floor alone. Manual runs only ever add short intervals, so the upper quartile (not the
 * median) keeps a burst of them from shrinking the cadence while over a quarter of the
 * intervals are the schedule's. Ingest chains apply, so apply is never stale sooner than
 * its source's ingest. The floors are the demo cadence; the share's own update lag
 * (about a minute) is well below each.
 */
export const FLOOR_S = { ingest: 600, apply: 1800, compaction: 26 * 3600 } as const;
export const MIN_RUNS = 5;
export const STALE_X = 2;
/**
 * Needs action: ingest or apply
 * is stale and its last successful run is older than max(1 h, 6 x the same upper-quartile
 * interval); below MIN_RUNS, the 1 h floor. Apply is never urgent sooner than its source's
 * ingest. A run that fails before the pipeline starts (a package install, a secret) writes no
 * row, so only time can show it. Compaction is left out.
 */
export const URGENT_X = 6;
export const URGENT_FLOOR_S = 3600;

/**
 * What feeds each writer's health, never a snapshot's written_at (a failed apply run
 * still refreshes apply_status):
 * - ingest: ingest_status.status_as_of (the last leased run) and last_failure_at;
 *   cadence from ingest_runs.as_of;
 * - apply: the newest apply_runs row with table_name NULL (the run's own row);
 * - backfill: the newest backfill_runs row (no cadence: it runs only while a backfill does);
 * - compaction: compaction_status.run_as_of and status.
 * Each writer's run statuses map to health explicitly (STATUS_HEALTH); a status a writer
 * adds later reads `unknown`, never ok. Busy runs never set the status or reset staleness:
 * the newest non-busy run does, and `busy_since` notes a newer busy run.
 */
export const STATUS_HEALTH: Record<string, Record<string, 'ok' | 'attention' | 'failed'>> = {
  // conflict (format 2): the run kept its durable progress, then stopped on a compaction
  // conflict; the next run resumes. Repeated conflicts are flagged separately (CONFLICTS_ATTENTION).
  ingest: { drained: 'ok', capped: 'ok', busy: 'ok', conflict: 'ok', failed: 'failed' },
  // needs_operator: the run stopped on held tables (exit 2).
  apply: { done: 'ok', needs_operator: 'attention', failed: 'failed' },
  // A stalled or un-retriggered backfill exits 3 and is recorded as failed (SystemExit).
  backfill: {
    published: 'ok', idle: 'ok', snapshotting: 'ok', catching_up: 'ok', waiting_for_ingest: 'ok',
    waiting_for_marker: 'ok', busy: 'ok', busy_publish: 'ok', done: 'ok', abandoned: 'ok', failed: 'failed',
    // a read replica source (docs/read-replica.md): replay catches up on its own, a commit past
    // replay comes from the heartbeat job; the primary's standby snapshot may need the DBA
    waiting_for_replay: 'ok', waiting_past_replay: 'ok', waiting_for_primary: 'attention',
  },
  // held: a published table lost its pin, so nothing more drops until an operator acts.
  compaction: { done: 'ok', capped: 'ok', busy: 'ok', held: 'attention', failed: 'failed', invariant: 'failed' },
};
/** Two or more conflict runs in 24 h turn ingest's health to attention: compaction and
 * ingest schedules overlap (docs/operations.md). */
export const CONFLICTS_ATTENTION = { count: 2, hours: 24 } as const;
const okStatuses = (flight: string) => Object.entries(STATUS_HEALTH[flight]).filter(([, h]) => h === 'ok').map(([st]) => q(st)).join(', ');
const HEALTH_OF = `CASE ${Object.entries(STATUS_HEALTH)
  .flatMap(([flight, map]) => Object.entries(map).map(([status, h]) => `WHEN flight = '${flight}' AND status = '${status}' THEN '${h}'`))
  .join(' ')} ELSE 'unknown' END`;

function cadence(runs: string, time: string, extra = '') {
  return `SELECT source_id, count(*) AS runs, quantile_cont(gap, 0.75) AS gap_s FROM (
    SELECT source_id, ${time} - lag(${time}) OVER (PARTITION BY source_id ORDER BY ${time}) AS gap,
           row_number() OVER (PARTITION BY source_id ORDER BY ${time} DESC) AS k
    FROM ${runs} WHERE ${time} IS NOT NULL${extra}) WHERE k <= 20 GROUP BY source_id`;
}

export const health = (f: Filters) => `
  WITH ingest AS (
    SELECT s.source_id, 'ingest' AS flight, s.status_as_of / 1e6 AS last_s,
           CASE WHEN s.last_failure_at IS NOT NULL
                 AND (s.status_as_of IS NULL OR epoch(s.last_failure_at) > s.status_as_of / 1e6)
                THEN 'failed' ELSE s.status END AS status,
           CASE WHEN s.last_failure_at IS NOT NULL
                 AND (s.status_as_of IS NULL OR epoch(s.last_failure_at) > s.status_as_of / 1e6)
                THEN s.last_failure_error ELSE s.error END AS error,
           CASE WHEN s.last_failure_at IS NOT NULL
                 AND (s.status_as_of IS NULL OR epoch(s.last_failure_at) > s.status_as_of / 1e6)
                THEN s.last_failure_step ELSE s.step END AS step,
           c.runs, c.gap_s, ${FLOOR_S.ingest} AS floor_s,
           (SELECT count(*) FROM ${T('ingest_runs')} r WHERE r.source_id = s.source_id AND r.status = 'conflict'
              AND r.written_at > current_timestamp - INTERVAL ${CONFLICTS_ATTENTION.hours} HOUR) AS conflicts_24h,
           s.last_conflict_at, s.last_success_as_of / 1e6 AS ok_s
    FROM ${T('ingest_status')} s
    LEFT JOIN (${cadence(T('ingest_runs'), 'as_of / 1e6')}) c USING (source_id)
    WHERE true${bySource(f, 's.source_id')}
  ), apply_rows AS (
    SELECT source_id, epoch(written_at) AS t, status, error, step,
           row_number() OVER (PARTITION BY source_id, status = 'busy' ORDER BY written_at DESC) AS k
    FROM ${T('apply_runs')} WHERE table_name IS NULL${bySource(f)}
  ), apply AS (
    SELECT n.source_id, 'apply' AS flight, n.t AS last_s, n.status, n.error, n.step,
           CASE WHEN b.t > n.t THEN b.t END AS busy_s, c.runs, c.gap_s, ${FLOOR_S.apply} AS floor_s,
           (SELECT max(o.t) FROM apply_rows o WHERE o.source_id = a.source_id AND o.status IN (${okStatuses('apply')})) AS ok_s
    FROM (SELECT DISTINCT source_id FROM apply_rows) a
    LEFT JOIN apply_rows n ON n.source_id = a.source_id AND n.status <> 'busy' AND n.k = 1
    LEFT JOIN apply_rows b ON b.source_id = a.source_id AND b.status = 'busy' AND b.k = 1
    LEFT JOIN (${cadence(T('apply_runs'), 'epoch(written_at)', " AND table_name IS NULL AND status <> 'busy'")}) c ON c.source_id = a.source_id
  ), backfill AS (
    SELECT source_id, 'backfill' AS flight, epoch(written_at) AS last_s, status, error, step,
           NULL::BIGINT AS runs, NULL::DOUBLE AS gap_s, NULL::INTEGER AS floor_s
    FROM ${T('backfill_runs')} WHERE true${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id ORDER BY written_at DESC) = 1
  ), compaction AS (
    SELECT source_id, 'compaction' AS flight, run_as_of / 1e6 AS last_s, status, error, NULL AS step,
           NULL::BIGINT AS runs, NULL::DOUBLE AS gap_s, ${FLOOR_S.compaction} AS floor_s
    FROM ${T('compaction_status')} WHERE true${bySource(f)}
  ), every AS (
    SELECT * FROM ingest UNION ALL BY NAME SELECT * FROM apply
    UNION ALL BY NAME SELECT * FROM backfill UNION ALL BY NAME SELECT * FROM compaction
  ), own AS (
    SELECT *, CASE WHEN floor_s IS NULL THEN NULL
                   WHEN coalesce(runs, 0) < ${MIN_RUNS} THEN floor_s
                   ELSE greatest(floor_s, round(${STALE_X} * gap_s)) END AS own_after_s,
           CASE WHEN flight NOT IN ('ingest', 'apply') THEN NULL
                WHEN coalesce(runs, 0) < ${MIN_RUNS} THEN ${URGENT_FLOOR_S}
                ELSE greatest(${URGENT_FLOOR_S}, round(${URGENT_X} * gap_s)) END AS own_urgent_s
    FROM every
  ), limits AS (
    SELECT *, CASE WHEN flight = 'apply'
                   THEN greatest(own_after_s, max(CASE WHEN flight = 'ingest' THEN own_after_s END)
                                              OVER (PARTITION BY source_id))
                   ELSE own_after_s END AS after_s,
           CASE WHEN flight = 'apply'
                THEN greatest(own_urgent_s, max(CASE WHEN flight = 'ingest' THEN own_urgent_s END)
                                            OVER (PARTITION BY source_id))
                ELSE own_urgent_s END AS urgent_after_s
    FROM own
  )
  SELECT source_id, flight, status, error, step,
         ${fmt('to_timestamp(last_s)')} AS last_run_at, ${fmt('to_timestamp(busy_s)')} AS busy_since,
         conflicts_24h, ${fmt('last_conflict_at')} AS last_conflict_at,
         round(epoch(current_timestamp) - last_s) AS age_s,
         after_s AS stale_after_s,
         CASE WHEN error = 'SourceDeleted' THEN 'deleted'
              WHEN status IS NULL THEN CASE WHEN last_s IS NULL THEN 'no_runs' ELSE 'unknown' END
              WHEN ${HEALTH_OF} <> 'ok' THEN ${HEALTH_OF}
              WHEN after_s IS NOT NULL AND (last_s IS NULL OR epoch(current_timestamp) - last_s > after_s)
                   THEN 'stale'
              WHEN coalesce(conflicts_24h, 0) >= ${CONFLICTS_ATTENTION.count} THEN 'attention'
              ELSE 'ok' END AS health,
         coalesce(ok_s, last_s) AS ok_at_s, urgent_after_s,
         coalesce(health = 'stale' AND epoch(current_timestamp) - coalesce(ok_s, last_s) > urgent_after_s, false) AS urgent
  FROM limits ORDER BY source_id, flight`;

export const sources = () => `
  SELECT DISTINCT source_id FROM (
    SELECT source_id FROM ${T('ingest_status')} UNION ALL SELECT source_id FROM ${T('apply_status')}
    UNION ALL SELECT source_id FROM ${T('compaction_status')} UNION ALL SELECT source_id FROM ${T('preflight_status')}
    UNION ALL SELECT source_id FROM ${T('apply_runs')} UNION ALL SELECT source_id FROM ${T('backfill_runs')})
  ORDER BY source_id`;

// ── Status ──────────────────────────────────────────────────────────────────────────

/**
 * PostgreSQL TLS (docs/dive.md; the run rows' TLS columns). TLS is baked per Flight bundle, so
 * every PostgreSQL-connecting writer counts: ingest (ingest_runs), backfill (backfill_runs)
 * and the connecting actions (audit rows of TLS_ACTIONS with a recorded value; the other
 * actions never connect and are filtered out, never read as unknown). A writer's state is
 * its last observed run in its window (ingest_runs is pruned at 7 days; backfill_runs and
 * audit never are, so an old one-off run can't mark a source forever): requested from its
 * newest row, negotiated from its newest row with tls_in_use set (a run that opened no
 * connection doesn't blank it). verify-full needs the negotiated run itself to have been
 * verify-full; a verify-full request not yet seen connecting fails closed ("not yet
 * connected", ranked with verify-full). A source shows its weakest known writer.
 */
export const TLS_WINDOW_DAYS = { ingest: 7, backfill: 30, action: 30 } as const;
export const TLS_ACTIONS = ['add', 'remove', 'recover-slot', 'skip-to-lsn', 'delete-source'] as const;
const TLS_RANK = `CASE state WHEN 'PLAINTEXT' THEN 1 WHEN 'encrypted, not verified' THEN 2
  WHEN 'requested only' THEN 3 WHEN 'not yet connected' THEN 4 WHEN 'verify-full' THEN 4 END`;
// Within a rank, the weaker evidence first (not yet connected before verify-full), then the
// writer's name: the source's state never depends on row order.
const TLS_ORDER = `rank, CASE state WHEN 'verify-full' THEN 1 ELSE 0 END, writer`;

export const tls = (f: Filters) => `
  WITH r AS (
    SELECT source_id, 'ingest' AS writer, written_at, tls_mode, tls_ca, tls_in_use FROM ${T('ingest_runs')}
    WHERE written_at > current_timestamp - INTERVAL ${TLS_WINDOW_DAYS.ingest} DAY${bySource(f)}
    UNION ALL
    SELECT source_id, 'backfill', written_at, tls_mode, tls_ca, tls_in_use FROM ${T('backfill_runs')}
    WHERE written_at > current_timestamp - INTERVAL ${TLS_WINDOW_DAYS.backfill} DAY${bySource(f)}
    UNION ALL
    SELECT source_id, 'action ' || action, written_at, tls_mode, tls_ca, tls_in_use FROM ${T('audit')}
    WHERE written_at > current_timestamp - INTERVAL ${TLS_WINDOW_DAYS.action} DAY
      AND action IN (${TLS_ACTIONS.map(q).join(', ')})
      AND (tls_mode IS NOT NULL OR tls_in_use IS NOT NULL)${bySource(f)}
  ), req AS (
    SELECT source_id, writer, tls_mode, tls_ca, written_at AS req_at FROM r
    QUALIFY row_number() OVER (PARTITION BY source_id, writer ORDER BY written_at DESC) = 1
  ), neg AS (
    SELECT source_id, writer, tls_mode AS neg_mode, tls_in_use, written_at AS neg_at FROM r
    WHERE tls_in_use IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY source_id, writer ORDER BY written_at DESC) = 1
  ), w AS (
    SELECT req.source_id, req.writer, req.tls_ca, req.req_at, neg.neg_at,
           CASE WHEN neg.tls_in_use = false THEN 'PLAINTEXT'
                WHEN req.tls_mode = 'verify-full' AND neg.tls_in_use AND neg.neg_mode = 'verify-full'
                  THEN 'verify-full'
                WHEN req.tls_mode = 'verify-full' THEN 'not yet connected'
                WHEN neg.tls_in_use THEN 'encrypted, not verified'
                WHEN req.tls_mode IS NOT NULL THEN 'requested only'
                ELSE 'unknown' END AS state
    FROM req LEFT JOIN neg USING (source_id, writer)
  ), ranked AS (
    SELECT *, ${TLS_RANK} AS rank,
           state || CASE WHEN state = 'verify-full' AND tls_ca IS NOT NULL THEN ' (' || tls_ca || ')' ELSE '' END
             AS label,
           -- the time of the row the state rests on: the negotiated one, except for a request
           -- not yet seen connecting, whose own row is the evidence
           epoch(CASE WHEN state IN ('not yet connected', 'requested only', 'unknown') THEN req_at ELSE neg_at END)::VARCHAR AS as_of
    FROM w
  )
  SELECT source_id,
         coalesce(first(label ORDER BY ${TLS_ORDER}) FILTER (WHERE rank IS NOT NULL), 'unknown') AS tls,
         coalesce(min(rank) FILTER (WHERE rank IS NOT NULL), 4) < 4 AS warn,
         -- each writer's time as @<epoch seconds>@: the browser formats it (model.ts tlsText)
         string_agg(writer || ' ' || label || ' (as of @' || coalesce(as_of, '') || '@)', '; '
                    ORDER BY coalesce(rank, 5), CASE state WHEN 'verify-full' THEN 1 ELSE 0 END, writer)
           AS breakdown
  FROM ranked GROUP BY source_id ORDER BY source_id`;

/** Per-table apply_runs statuses: ok (applied what it read) and failed (held: nothing
 * applied); anything else (busy, paused, awaiting_generation, not_configured, new) is
 * neither. */
export const APPLY_OK = ['caught_up', 'capped'];
export const APPLY_FAILED = ['needs_action', 'needs_resync', 'source_missing'];
export const APPLY_DAYS = 7;
/**
 * Expirations (gaps compaction opened). A gap is open while the table's current
 * generation is still the one that expired, or the table isn't live; a table since
 * removed from apply closes it (labelled "table removed").
 */
export const expirations = (f: Filters) => `
  SELECT e.source_id, ${fmt('e.expired_at')} AS expired_at, epoch(e.expired_at) AS expired_at_s, e.owner, e.kind,
         coalesce(a.schema_name || '.' || a.table_name, e.owner || ' (table removed)') AS table_name, e.reason, e.mode, e.outcome,
         ${lsn('e.pinned_lsn')} AS pinned_lsn, e.epoch, a.state,
         e.outcome = 'expired' AND a.relation_id IS NOT NULL AND (a.generation_id = e.generation_id OR a.state <> 'live') AS open
  FROM (
    SELECT source_id, ${j('expired_at')}::TIMESTAMPTZ AS expired_at, ${j('owner')} AS owner, ${j('kind')} AS kind,
           ${j('relation_id')}::UINTEGER AS relation_id, ${j('generation_id')} AS generation_id,
           ${j('epoch')}::INTEGER AS epoch, ${j('pinned_lsn')}::UBIGINT AS pinned_lsn, ${j('reason')} AS reason,
           ${j('mode')} AS mode, ${j('outcome')} AS outcome
    FROM (SELECT source_id, ${items('expirations')} AS i FROM ${T('compaction_status')} WHERE true${bySource(f)})) e
  LEFT JOIN ${T('apply_status')} a ON a.source_id = e.source_id AND a.relation_id = e.relation_id
  ORDER BY e.expired_at DESC`;

/**
 * A source's lifecycle, derived (no writer of its own): the newest delete-source audit
 * row's `lifecycle`; else ingest's observed `lifecycle` (a tombstoned slot after a
 * failure); else a pipeline refusal `SourceDeleted`. NULL is active.
 */
export const lifecycle = (f: Filters) => `
  WITH audit AS (
    SELECT source_id, result ->> 'lifecycle' AS lifecycle FROM ${T('audit')}
    WHERE action = 'delete-source' AND (result ->> 'lifecycle') IS NOT NULL${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id ORDER BY written_at DESC) = 1
  ), refused AS (
    SELECT DISTINCT source_id FROM (
      SELECT source_id FROM ${T('ingest_status')} WHERE last_failure_error = 'SourceDeleted'${bySource(f)}
      UNION ALL SELECT source_id FROM ${T('apply_runs')} WHERE error = 'SourceDeleted'${bySource(f)})
  ), src AS (${sources()})
  SELECT src.source_id,
         coalesce(audit.lifecycle, i.lifecycle, CASE WHEN refused.source_id IS NOT NULL THEN 'deleting_or_deleted' END) AS lifecycle
  FROM src LEFT JOIN audit USING (source_id) LEFT JOIN ${T('ingest_status')} i USING (source_id)
  LEFT JOIN refused USING (source_id)
  WHERE true${bySource(f, 'src.source_id')}
  ORDER BY src.source_id`;

// ── The redesign (2026-11-10): Overview, Source page, table drawer ──────────────────
//
// Times leave SQL as epoch seconds (`*_s` ending in `_at_s`): the browser formats them in
// the viewer's zone. Each table's status is decided here, so tests/test_dive_queries.py
// checks it against the real writers; dive/src/model.ts turns the fields into the fixed
// details text.

const epochS = (ts: string) => `epoch(${ts})`;
/** Backfill generations still being built (backfill_status.state). */
export const ACTIVE_GENERATION = ['snapshotting', 'catching_up'] as const;
/** Stale over a day, or paused within a day of retention expiry, needs action. */
export const URGENT_S = 24 * 3600;

/**
 * Hold reasons in apply_status.detail, a contract with the writers (tests check each text
 * against src/cdc_flight): an insert-only table's changed row (append_apply), the catalog
 * checks' sentences (catalog_checks.REASONS for a keyed table, APPEND_REASONS for an
 * insert-only one) and a source column's type change (mirror.py). The codes never reach
 * cdc_ops (key_changes stays in cdc_internal), so the Dive matches the stored text.
 */
export const CATALOG_REASONS = {
  pk_replaced: "the source table's primary key constraint was replaced; resync",
  partitions_changed: 'a partition of the source table was attached or detached; resync',
  column_default_fill: 'a column was added with a default its existing rows read without WAL; resync',
  column_rewrite: 'a column was added while the source table was rewritten; resync',
} as const;
export const APPEND_CATALOG_REASONS = {
  partitions_changed: 'a partition of the source table was attached or detached; start-from-log (it drops the rows before the new start)',
} as const;
/** Registry holds of an insert-only table that is no longer valid for it (preflight). */
export const APPEND_INVALID = ['append_identity', 'append_partitioned'] as const;

/**
 * apply_status.hold_code (cdc_flight.hold_codes.HOLD_CODES) → the Dive's state. A
 * non-NULL code is trusted; NULL on a held table (an older writer, or a code the writer
 * found stale) falls back to the stored texts above. The test checks that every code is
 * mapped (an unmapped one would read by its apply state alone).
 */
export const HOLD_CODE_KIND: Record<string, string> = {
  append_update: 'append_changed', append_delete: 'append_changed',
  append_keyed_frames: 'append_invalid', append_identity: 'append_invalid', append_partitioned: 'append_invalid',
  type_changed: 'type_changed',
  ...Object.fromEntries(Object.keys(CATALOG_REASONS).map((c) => [c, 'catalog_changed'])),
  ...Object.fromEntries(['schema_changed', 'source_mismatch', 'poison', 'missing_baseline', 'incomplete_insert',
    'unsupported_change', 'key_cell', 'key_change_ambiguous', 'pin_invariant'].map((c) => [c, 'blocked'])),
  ...Object.fromEntries(['key_changed', 'epoch_changed', 'history_lost', 'retention_expired', 'truncate'].map((c) => [c, 'needs_resync'])),
  paused: 'paused',
  not_started: 'not_started',
  removed: 'removed',
  unmanaged: 'not_enabled',
  ...Object.fromEntries(['dropped', 'renamed', 'replaced', 'unpublished', 'invalid_kind', 'no_select', 'no_key',
    'identity_not_full', 'leaf_invalid', 'publication_filter'].map((c) => [c, 'registry'])),
};
/**
 * Columns tableStatus reads that an older writer's ops db may lack (append capture,
 * backfill, hold_code). The screens probe them first (opsColumns) and read
 * an absent one as a typed NULL, so the status falls back to the stored texts instead of
 * failing to bind. A deploy adds them (the writers' DDL); a preview or another db may not.
 */
export const OPTIONAL_COLUMNS: Record<string, string> = {
  'apply_status.hold_code': 'VARCHAR',
  'apply_status.capture_mode': 'VARCHAR',
  'apply_status.ignore_deletes': 'BOOLEAN',
  'apply_status.deletes_ignored': 'UBIGINT',
  'apply_status.last_truncate_lsn': 'UBIGINT',
  'backfill_status.pin_lsn': 'UBIGINT',
  'backfill_runs.detail': 'VARCHAR',
  'ingest_status.standby': 'VARCHAR',
  'ingest_runs.standby': 'VARCHAR',
};
/** Every optional column present (the deployed schema). */
export const ALL_COLUMNS: ReadonlySet<string> = new Set(Object.keys(OPTIONAL_COLUMNS));
/** Which optional columns the attached ops db has. */
export const opsColumns = () => `
  SELECT table_name || '.' || column_name AS col FROM duckdb_columns()
  WHERE database_name = '${ALIAS}' AND schema_name = 'main'
    AND table_name || '.' || column_name IN (${Object.keys(OPTIONAL_COLUMNS).map(q).join(', ')})`;
/** An optional column, or a typed NULL under its name when the db lacks it. */
const opt = (has: ReadonlySet<string>, table: string, column: string, as = column) =>
  has.has(`${table}.${column}`) ? (as === column ? column : `${column} AS ${as}`) : `NULL::${OPTIONAL_COLUMNS[`${table}.${column}`]} AS ${as}`;
const codesOf = (...kinds: string[]) => Object.keys(HOLD_CODE_KIND).filter((c) => kinds.includes(HOLD_CODE_KIND[c])).map(q).join(', ');
/** Registry codes: a hold even before ingest's holds list carries it. */
const REGISTRY_CODES = codesOf('registry', 'append_invalid', 'removed', 'not_enabled');
/** Backfill's refusal while ingest hasn't yet passed a catalog hold's cut (backfill.py):
 * the next ingest run clears it, so the Dive reads it as waiting, not failed. */
export const WAITS_FOR_INGEST = 'ingest has not yet passed the source key change';
const reasonOf = (detail: string) => `CASE
      WHEN regexp_matches(${detail}, '^append_update: ') THEN 'append_update'
      WHEN regexp_matches(${detail}, '^append_delete: ') THEN 'append_delete'
      WHEN regexp_matches(${detail}, '^column .* changed type') THEN 'type_changed'
${Object.entries(CATALOG_REASONS).map(([k, v]) => `      WHEN ${detail} = ${q(v)} THEN ${q(k)}`).join('\n')}
${Object.entries(APPEND_CATALOG_REASONS).map(([k, v]) => `      WHEN ${detail} = ${q(v)} THEN ${q(k)}`).join('\n')}
    END`;

/**
 * One row per table and source with the table's status (STATUSES, first match wins):
 * - removed: the registry removed it (the screens leave it out);
 * - not_enabled: in the publication, never added (registry `unmanaged`);
 * - needs_action / registry: any other registry hold (dropped, renamed, no_key, …);
 * - needs_action / blocked: apply stopped on a failed batch (state needs_action); a held
 *   table (any needs_action kind) reads rebuilding while its resync runs;
 * - needs_resync: rebuilding (an active generation), needs_action / rebuild_failed (its
 *   newest generation failed), queued (the source is recovering: the table's epoch is
 *   behind ingest's), else needs_action / needs_resync;
 * - rebuilding: an active generation for a live table (add or resync);
 * - paused (needs_action when retention expires within URGENT_S);
 * - unknown: a state the Dive doesn't know;
 * - waiting: the source's sync stopped (ingest or apply failed or stale, slot lost or
 *   missing, source deleted): the cause shows on the source;
 * - stale: not caught up for longer than apply's own staleness limit (needs_action past
 *   URGENT_S). Caught up: (apply_status.epoch, applied_lsn) reached ingest's (epoch,
 *   checkpoint_lsn). Behind by: ingest's durable_through_commit_ts minus the table's
 *   applied_through_commit_ts, each only when its (epoch, lsn) pair is the row's own
 *   position, else unknown, never stale;
 * - ok.
 */
export const STATUSES = ['removed', 'not_enabled', 'needs_action', 'rebuilding', 'queued', 'paused', 'unknown', 'waiting', 'stale', 'ok'] as const;
export const tableStatus = (f: Filters, has: ReadonlySet<string> = ALL_COLUMNS) => `
  WITH ingest AS (
    SELECT source_id, epoch AS source_epoch, checkpoint_lsn, lifecycle,
           CASE WHEN durable_through_epoch = epoch AND durable_through_lsn = checkpoint_lsn
                THEN ${epochS('durable_through_commit_ts')} END AS durable_at_s,
           (SELECT ${j('wal_status')} FROM (SELECT ${items('slots')} AS i) WHERE ${j('is_current')}::BOOLEAN) AS wal_status
    FROM ${T('ingest_status')} WHERE true${bySource(f)}
  ), flights AS (
    SELECT source_id,
           bool_or(flight IN ('ingest', 'apply') AND health IN ('failed', 'stale', 'deleted')) AS stopped,
           max(stale_after_s) FILTER (WHERE flight = 'apply') AS stale_after_s
    FROM (${health(f)}) GROUP BY source_id
  ), apply AS (
    SELECT source_id, relation_id, schema_name || '.' || table_name AS table_name, state, detail,
           epoch, applied_lsn, generation_id, held_since, blocked_commit_lsn, blocked_unit_start_lsn, blocked_unit_end_lsn,
           pin_lsn, pin_updated_at, coalesce(quarantine_count, 0) AS quarantines,
           ${opt(has, 'apply_status', 'capture_mode')}, ${opt(has, 'apply_status', 'ignore_deletes')},
           ${opt(has, 'apply_status', 'deletes_ignored')}, ${opt(has, 'apply_status', 'last_truncate_lsn')},
           coalesce(${has.has('apply_status.hold_code') ? 'hold_code' : 'NULL'}, ${reasonOf('detail')}) AS reason,
           CASE WHEN applied_through_epoch = epoch AND applied_through_lsn = applied_lsn
                THEN ${epochS('applied_through_commit_ts')} END AS updated_at_s
    FROM ${T('apply_status')} a WHERE true${bySource(f)}
      -- apply_status is an upsert that never deletes: a not_started row older than the
      -- source's newest full snapshot (the as_of of its newest run row's run) is stale
      AND NOT coalesce(state = 'not_started' AND as_of < (
        SELECT max(s.as_of) FROM ${T('apply_status')} s
        WHERE s.source_id = a.source_id AND s.run_id = (
          SELECT arg_max(r.run_id, r.written_at) FROM ${T('apply_runs')} r
          WHERE r.source_id = a.source_id AND r.table_name IS NULL)), false)
  ), holds AS (
    SELECT source_id, table_name, arg_min(reason, k) AS reason, arg_min(detail, k) AS detail,
           min(held_since) AS held_since
    FROM (SELECT source_id, ${j('table')} AS table_name, ${j('reason')} AS reason, ${j('detail')} AS detail,
                 ${j('held_since')}::TIMESTAMPTZ AS held_since,
                 CASE ${j('reason')} WHEN 'removed' THEN 2 WHEN 'unmanaged' THEN 1 ELSE 0 END AS k
          FROM (SELECT source_id, ${items('holds')} AS i FROM ${T('ingest_status')} WHERE true${bySource(f)})
          WHERE ${j('reason')} <> 'key_change_pending')
    GROUP BY ALL
  ), gen AS (
    SELECT source_id, table_name, generation_id, state AS gen_state, mode AS gen_mode, ${opt(has, 'backfill_status', 'pin_lsn', 'gen_pin_lsn')},
           chunks, chunks_total, created_at
    FROM ${T('backfill_status')} WHERE true${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id, table_name ORDER BY created_at DESC) = 1
  ), gen_run AS (
    -- a generation's failure reason is in detail (error names a crashed run's exception)
    SELECT source_id, generation_id, coalesce(${has.has('backfill_runs.detail') ? 'detail' : 'NULL'}, error) AS gen_error, written_at AS gen_run_at,
           status AS gen_run_status
    FROM ${T('backfill_runs')} WHERE true${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id, generation_id ORDER BY written_at DESC) = 1
  ), kept AS (
    -- the generation an insert-only table's newest re-snapshot or start-from-log replaced:
    -- its mirror is kept aside as <table>__gen_<its first 8 hex> (append_apply._keep_mirror)
    SELECT source_id, table_name, generation_id AS kept_generation
    FROM ${T('backfill_status')} WHERE state = 'replaced'${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id, table_name ORDER BY coalesce(updated_at, created_at) DESC) = 1
  ), refusal AS (
    -- the table's newest refused backfill start (no generation): over the snapshot limit,
    -- or waiting for ingest to pass a catalog hold
    SELECT source_id, table_name, ${opt(has, 'backfill_runs', 'detail', 'refused_detail')}, written_at AS refused_at
    FROM ${T('backfill_runs')} WHERE status = 'refused' AND table_name IS NOT NULL${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id, table_name ORDER BY written_at DESC) = 1
  ), awaiting AS (
    -- an insert-only table registered with no mirror generation yet (apply_run)
    SELECT source_id, table_name, max(written_at) AS awaiting_at
    FROM ${T('apply_runs')} WHERE status = 'awaiting_generation' AND table_name IS NOT NULL${bySource(f)}
    GROUP BY ALL
  ), pin AS (
    SELECT source_id, ${j('relation_id')}::UINTEGER AS relation_id, ${j('expires_at_us')}::BIGINT AS expires_at_us
    FROM (SELECT source_id, ${items('pins')} AS i FROM ${T('compaction_status')} WHERE true${bySource(f)})
    WHERE ${j('kind')} = 'table'
  ), acts AS (
    SELECT source_id, action, inputs ->> 'table' AS table_name, written_at,
           coalesce(started_by, token_user) AS actor
    FROM ${T('audit')}
    WHERE NOT dry_run AND (result ->> 'status') = 'done'
      AND action IN ('pause', 'resync', 'add', 'resync-all', 'recover-slot')${bySource(f)}
  ), applied AS (
    SELECT source_id, table_name,
           sum(events) FILTER (WHERE written_at > current_timestamp - INTERVAL ${APPLY_DAYS} DAY) AS changes
    FROM ${T('apply_runs')} WHERE table_name IS NOT NULL AND events > 0${bySource(f)} GROUP BY ALL
  ), every AS (
    SELECT source_id, table_name FROM apply UNION SELECT source_id, table_name FROM holds
    UNION SELECT source_id, table_name FROM gen WHERE gen_state IN (${ACTIVE_GENERATION.map(q).join(', ')}, 'failed')
    UNION SELECT source_id, table_name FROM awaiting
  ), joined AS (
    SELECT e.source_id, e.table_name, a.relation_id, a.state, a.detail AS apply_detail, a.epoch, i.source_epoch,
           a.applied_lsn, a.generation_id, a.held_since, a.blocked_commit_lsn, a.blocked_unit_start_lsn, a.blocked_unit_end_lsn,
           a.pin_lsn, a.pin_updated_at, coalesce(a.quarantines, 0) AS quarantines, a.updated_at_s,
           -- a registry hold: ingest's holds list, else apply's registry code
           coalesce(h.reason, CASE WHEN a.reason IN (${REGISTRY_CODES}) AND a.reason <> 'append_keyed_frames' THEN a.reason END) AS hold_reason,
           coalesce(h.detail, CASE WHEN a.reason IN (${REGISTRY_CODES}) THEN a.detail END) AS hold_detail,
           coalesce(h.held_since, CASE WHEN a.reason IN (${REGISTRY_CODES}) THEN a.held_since END) AS hold_since,
           g.generation_id AS gen_id, g.gen_state, g.chunks, g.chunks_total, g.created_at AS gen_created_at,
           gr.gen_error, gr.gen_run_at, gr.gen_run_status, p.expires_at_us,
           a.capture_mode, a.ignore_deletes, a.deletes_ignored, a.last_truncate_lsn, a.reason,
           CASE WHEN g.gen_mode = 'log_incomplete' AND g.gen_state = 'published' AND g.generation_id = a.generation_id
                THEN g.gen_pin_lsn END AS log_start_lsn,
           CASE WHEN a.capture_mode = 'append' AND k.kept_generation IS NOT NULL
                THEN e.table_name || '__gen_' || left(k.kept_generation, 8) END AS kept_aside,
           -- a refusal newer than the table's newest generation: the resync (or first
           -- snapshot) didn't start
           CASE WHEN rf.refused_at > coalesce(g.created_at, '-infinity'::TIMESTAMPTZ) THEN rf.refused_detail END AS refused_detail,
           aw.awaiting_at IS NOT NULL AS awaiting,
           (a.epoch = i.source_epoch AND a.applied_lsn >= i.checkpoint_lsn) AS caught_up,
           i.durable_at_s, i.checkpoint_lsn,
           coalesce(fl.stopped, false) OR i.wal_status IN ('lost', 'missing') OR i.lifecycle IS NOT NULL AS stopped,
           fl.stale_after_s, coalesce(ap.changes, 0) AS changes_7d
    FROM every e
    LEFT JOIN apply a USING (source_id, table_name) LEFT JOIN holds h USING (source_id, table_name)
    LEFT JOIN gen g USING (source_id, table_name)
    LEFT JOIN gen_run gr ON gr.source_id = g.source_id AND gr.generation_id = g.generation_id
    LEFT JOIN pin p ON p.source_id = e.source_id AND p.relation_id = a.relation_id
    LEFT JOIN ingest i ON i.source_id = e.source_id LEFT JOIN flights fl ON fl.source_id = e.source_id
    LEFT JOIN applied ap ON ap.source_id = e.source_id AND ap.table_name = e.table_name
    LEFT JOIN kept k ON k.source_id = e.source_id AND k.table_name = e.table_name
    LEFT JOIN refusal rf ON rf.source_id = e.source_id AND rf.table_name = e.table_name
    LEFT JOIN awaiting aw ON aw.source_id = e.source_id AND aw.table_name = e.table_name
  ), judged AS (
    SELECT *,
           CASE WHEN caught_up THEN 0 WHEN durable_at_s IS NOT NULL AND updated_at_s IS NOT NULL
                THEN greatest(0, durable_at_s - updated_at_s) END AS behind_s,
           gen_state IN (${ACTIVE_GENERATION.map(q).join(', ')}) AS rebuilding,
           (expires_at_us / 1e6) - epoch(current_timestamp) AS expires_in_s
    FROM joined
  ), classed AS (
    SELECT *,
      CASE WHEN hold_reason = 'removed' THEN 'removed'
           WHEN hold_reason = 'unmanaged' THEN 'not_enabled'
           WHEN hold_reason IN (${APPEND_INVALID.map(q).join(', ')}) OR reason = 'append_keyed_frames' THEN 'append_invalid'
           WHEN hold_reason IS NOT NULL THEN 'registry'
           -- a held table's resync runs beside its held mirror (keyed, or an insert-only
           -- re-snapshot): it reads Rebuilding until it publishes, or Needs action again if it fails
           WHEN state = 'needs_action' AND rebuilding THEN 'rebuilding'
           WHEN state = 'needs_action' AND reason IN ('append_update', 'append_delete') THEN 'append_changed'
           WHEN state = 'needs_action' AND reason = 'type_changed' THEN 'type_changed'
           WHEN state = 'needs_action' AND reason IN (${codesOf('catalog_changed')}) THEN 'catalog_changed'
           WHEN state = 'needs_action' THEN 'blocked'
           WHEN state = 'needs_resync' AND rebuilding THEN 'rebuilding'
           WHEN state = 'needs_resync' AND gen_state = 'failed' THEN 'rebuild_failed'
           -- resync-all skips insert-only tables (re-snapshots are per table): never queued
           WHEN state = 'needs_resync' AND epoch < source_epoch AND capture_mode IS DISTINCT FROM 'append' THEN 'queued'
           WHEN state = 'needs_resync' AND reason IN (${codesOf('catalog_changed')}) THEN 'catalog_changed'
           WHEN state = 'needs_resync' THEN 'needs_resync'
           WHEN rebuilding THEN 'rebuilding'
           -- no mirror yet: a not_started row (hold_code), or an older writer's
           -- awaiting_generation run row; a running first backfill reads rebuilding above
           WHEN coalesce(state, 'not_started') = 'not_started' AND gen_state = 'failed' THEN 'rebuild_failed'
           WHEN state = 'not_started' OR (state IS NULL AND (awaiting OR refused_detail IS NOT NULL)) THEN 'not_started'
           WHEN state = 'paused' THEN 'paused'
           WHEN state IS NULL OR state <> 'live' THEN 'unknown'
           WHEN stopped THEN 'waiting'
           WHEN behind_s > ${URGENT_S} THEN 'stale'
           WHEN behind_s > stale_after_s THEN 'stale'
           ELSE 'ok' END AS kind
    FROM judged
  )
  SELECT c.source_id, c.table_name, c.relation_id,
         CASE WHEN kind IN ('registry', 'blocked', 'rebuild_failed', 'needs_resync', 'append_changed', 'catalog_changed',
                           'type_changed', 'append_invalid', 'not_started') THEN 'needs_action'
              WHEN kind = 'paused' AND expires_in_s < ${URGENT_S} THEN 'needs_action'
              WHEN kind = 'stale' AND behind_s > ${URGENT_S} THEN 'needs_action'
              ELSE kind END AS status,
         kind, c.state,
         CASE kind WHEN 'registry' THEN hold_reason || coalesce(': ' || hold_detail, '')
                   WHEN 'append_invalid' THEN hold_detail
                   WHEN 'not_started' THEN refused_detail
                   WHEN 'rebuild_failed' THEN gen_error ELSE apply_detail END AS detail,
         round(behind_s) AS behind_s, round(expires_in_s) AS expires_in_s, expires_at_us / 1e6 AS expires_at_s,
         CASE WHEN kind IN ('rebuilding') THEN ${epochS('c.gen_created_at')}
              WHEN kind = 'rebuild_failed' THEN ${epochS('gen_run_at')}
              WHEN kind IN ('registry', 'not_enabled', 'removed', 'append_invalid') THEN ${epochS('hold_since')}
              ELSE ${epochS('c.held_since')} END AS since_at_s,
         ${age('c.held_since')} AS held_for_s,
         x.actor, x.action AS actor_action,
         c.chunks, c.chunks_total, c.gen_id AS generation_id, c.gen_state, c.gen_run_status,
         c.updated_at_s, c.durable_at_s, c.epoch, c.source_epoch,
         ${lsn('c.applied_lsn')} AS applied_lsn, ${lsn('c.checkpoint_lsn')} AS checkpoint_lsn,
         ${lsn('c.blocked_commit_lsn')} AS blocked_commit_lsn,
         CASE WHEN c.blocked_unit_start_lsn IS NOT NULL
              THEN ${lsn('c.blocked_unit_start_lsn')} || '..' || ${lsn('c.blocked_unit_end_lsn')} END AS blocked_unit,
         ${lsn('c.pin_lsn')} AS pin_lsn, ${epochS('c.pin_updated_at')} AS pin_moved_at_s,
         c.quarantines, c.changes_7d,
         c.reason, c.capture_mode, c.ignore_deletes, c.deletes_ignored, ${lsn('c.last_truncate_lsn')} AS last_truncate_lsn,
         ${lsn('c.log_start_lsn')} AS log_start_lsn, c.kept_aside,
         -- a failed rebuild beside a live mirror (any state but rebuild_failed's own)
         CASE WHEN c.gen_state = 'failed' AND kind <> 'rebuild_failed' AND c.gen_id IS DISTINCT FROM c.generation_id
              THEN c.gen_error END AS rebuild_error,
         CASE WHEN kind <> 'not_started' THEN c.refused_detail END AS refused_detail,
         contains(coalesce(c.refused_detail, ''), ${q(WAITS_FOR_INGEST)}) AS waits_for_ingest
  FROM classed c
  -- who started it: the newest real run of the matching action before it began
  LEFT JOIN LATERAL (
    SELECT actor, action FROM acts
    WHERE acts.source_id = c.source_id
      AND ((c.kind = 'paused' AND action = 'pause' AND acts.table_name = c.table_name
            AND written_at <= c.held_since + INTERVAL 10 MINUTE)
        OR (c.kind = 'rebuilding' AND written_at <= c.gen_created_at + INTERVAL 10 MINUTE
            AND written_at >= c.gen_created_at - INTERVAL 1 DAY
            AND (action IN ('resync-all', 'recover-slot') OR (action IN ('resync', 'add') AND acts.table_name = c.table_name))))
    ORDER BY written_at DESC LIMIT 1) x ON true
  ORDER BY c.source_id, c.table_name`;

/** Sync history: per table, 28 marks of 6 h (oldest first, the newest still filling).
 * A mark reads ok (only ok runs), some (both), failed (only failed) or none; a failed
 * apply run (its own row) reached no table, so it counts for every table of its source. */
export const MARKS = 28;
export const MARK_S = 6 * 3600;
export const syncMarks = (f: Filters) => `
  WITH now_k AS (SELECT floor(epoch(current_timestamp) / ${MARK_S})::BIGINT AS k),
  runs AS (
    SELECT source_id, table_name, floor(epoch(written_at) / ${MARK_S})::BIGINT AS k, status
    FROM ${T('apply_runs')}, now_k
    WHERE floor(epoch(written_at) / ${MARK_S}) > now_k.k - ${MARKS}${bySource(f)}
  ), per_table AS (
    SELECT source_id, table_name, k,
           count(*) FILTER (WHERE status IN (${APPLY_OK.map(q).join(', ')})) AS ok,
           count(*) FILTER (WHERE status IN (${APPLY_FAILED.map(q).join(', ')})) AS failed
    FROM runs WHERE table_name IS NOT NULL GROUP BY ALL
  ), per_run AS (
    SELECT source_id, k, count(*) AS failed FROM runs WHERE table_name IS NULL AND status = 'failed' GROUP BY ALL
  ), tables AS (
    SELECT DISTINCT source_id, schema_name || '.' || table_name AS table_name FROM ${T('apply_status')} WHERE true${bySource(f)}
    UNION SELECT DISTINCT source_id, table_name FROM per_table
  ), marks AS (
    SELECT t.source_id, t.table_name, m.i, now_k.k - ${MARKS - 1} + m.i AS k
    FROM tables t, now_k, range(${MARKS}) m(i)
  )
  SELECT m.source_id, m.table_name,
         string_agg(CASE WHEN coalesce(p.failed, 0) + coalesce(r.failed, 0) = 0
                         THEN CASE WHEN coalesce(p.ok, 0) > 0 THEN 'ok' ELSE 'none' END
                         WHEN coalesce(p.ok, 0) > 0 THEN 'some' ELSE 'failed' END, ',' ORDER BY m.i) AS marks
  FROM marks m
  LEFT JOIN per_table p ON p.source_id = m.source_id AND p.table_name = m.table_name AND p.k = m.k
  LEFT JOIN per_run r ON r.source_id = m.source_id AND r.k = m.k
  GROUP BY ALL ORDER BY m.source_id, m.table_name`;

/** "Usually every ~N": the median gap of the last 20 runs, never a hardcoded schedule. */
function medianGap(runs: string, time: string, extra = '') {
  return `SELECT source_id, median(gap) AS every_s, count(*) AS gaps FROM (
    SELECT source_id, ${time} - lag(${time}) OVER (PARTITION BY source_id ORDER BY ${time}) AS gap,
           row_number() OVER (PARTITION BY source_id ORDER BY ${time} DESC) AS k
    FROM ${runs} WHERE true${extra}) WHERE k <= 20 AND gap IS NOT NULL GROUP BY source_id`;
}

/**
 * One row per source: its label (cdc_ops.sources: coalesce(label, source_id) on
 * screen), its last sync (the newest ingest run), its cadence, the current slot (WAL
 * limit used = retained / (retained + safe_wal_size); NULL safe_wal_size is no cap or a
 * lost slot), and why its sync stopped, if it did.
 */
export const sourceStatus = (f: Filters) => `
  WITH slot AS (
    SELECT source_id, slots_as_of, ${j('name')} AS slot_name, ${j('wal_status')} AS wal_status,
           ${j('active')}::BOOLEAN AS slot_active,
           ${j('retained_bytes')}::BIGINT AS retained_bytes, ${j('safe_wal_size')}::BIGINT AS safe_wal_size,
           ${j('lag_bytes')}::BIGINT AS lag_bytes, ${j('oldest_xact_age_s')}::DOUBLE AS oldest_xact_age_s,
           ${j('invalidated_by_conflict')} AS slot_conflict
    FROM (SELECT source_id, slots_as_of, ${items('slots')} AS i FROM ${T('ingest_status')} WHERE true${bySource(f)})
    WHERE ${j('is_current')}::BOOLEAN
  ), last_run AS (
    SELECT source_id, written_at, status FROM ${T('ingest_runs')} WHERE true${bySource(f)}
    QUALIFY row_number() OVER (PARTITION BY source_id ORDER BY written_at DESC) = 1
  ), fl AS (
    SELECT source_id,
           max(health) FILTER (WHERE flight = 'ingest') AS ingest_health,
           max(health) FILTER (WHERE flight = 'apply') AS apply_health,
           max(error) FILTER (WHERE flight = 'ingest') AS ingest_error,
           max(error) FILTER (WHERE flight = 'apply') AS apply_error,
           bool_or(urgent) FILTER (WHERE flight = 'ingest') AS ingest_urgent,
           bool_or(urgent) FILTER (WHERE flight = 'apply') AS apply_urgent,
           max(ok_at_s) FILTER (WHERE flight = 'ingest') AS ingest_ok_at_s,
           max(ok_at_s) FILTER (WHERE flight = 'apply') AS apply_ok_at_s
    FROM (${health(f)}) GROUP BY source_id
  ), src AS (${sources()})
  SELECT src.source_id, l.label, l.description, l.owner_user,
         ${epochS('r.written_at')} AS last_sync_at_s, r.status AS last_sync_status,
         round(c.every_s) AS every_s, coalesce(c.gaps, 0) AS gaps,
         s.slot_name, s.wal_status, s.slot_active, s.retained_bytes, s.safe_wal_size, s.lag_bytes, s.oldest_xact_age_s,
         s.slots_as_of / 1e6 AS slot_at_s, s.slot_conflict,
         CASE WHEN s.wal_status NOT IN ('lost', 'missing') AND s.retained_bytes IS NOT NULL AND s.safe_wal_size IS NOT NULL AND s.retained_bytes + s.safe_wal_size > 0
              THEN round(100.0 * s.retained_bytes / (s.retained_bytes + s.safe_wal_size), 1) END AS wal_pct,
         fl.ingest_health, fl.apply_health, fl.ingest_error, fl.apply_error,
         coalesce(fl.ingest_urgent, false) AS ingest_urgent, coalesce(fl.apply_urgent, false) AS apply_urgent,
         fl.ingest_ok_at_s, fl.apply_ok_at_s,
         ${epochS('i.last_failure_at')} AS failing_since_at_s, i.last_success_as_of / 1e6 AS last_ok_at_s,
         i.epoch, ${lsn('i.checkpoint_lsn')} AS checkpoint_lsn, lc.lifecycle,
         CASE WHEN i.durable_through_epoch = i.epoch AND i.durable_through_lsn = i.checkpoint_lsn
              THEN ${epochS('i.durable_through_commit_ts')} END AS durable_at_s
  FROM src
  LEFT JOIN ${T('sources')} l USING (source_id)
  LEFT JOIN last_run r USING (source_id)
  LEFT JOIN (${medianGap(T('ingest_runs'), 'epoch(written_at)', bySource(f))}) c USING (source_id)
  LEFT JOIN slot s USING (source_id) LEFT JOIN fl USING (source_id)
  LEFT JOIN ${T('ingest_status')} i USING (source_id) LEFT JOIN (${lifecycle(f)}) lc USING (source_id)
  WHERE true${bySource(f, 'src.source_id')}
  ORDER BY src.source_id`;

/** Every slot of a source (Source page → WAL limit and slot): the current one, a pending
 * `<slot>_e<n>` (a recovery that was interrupted) and a leftover old one, which keeps
 * retaining WAL until it is dropped. */
export const sourceSlots = (f: Filters) => `
  SELECT source_id, ${j('name')} AS slot_name, ${j('epoch')}::INTEGER AS epoch, ${j('is_current')}::BOOLEAN AS is_current,
         coalesce(${j('role')}, CASE WHEN ${j('is_current')}::BOOLEAN THEN 'current' ELSE 'pending' END) AS role,
         ${j('active')}::BOOLEAN AS active, ${j('wal_status')} AS wal_status,
         ${j('retained_bytes')}::BIGINT AS retained_bytes, ${j('lag_bytes')}::BIGINT AS lag_bytes
  FROM (SELECT source_id, ${items('slots')} AS i FROM ${T('ingest_status')} WHERE true${bySource(f)})
  ORDER BY source_id, is_current DESC, epoch`;

/**
 * A read replica source's diagnostics (ingest_status.standby, docs/read-replica.md): one row
 * per read replica source (the presence of `standby` is what makes it one). All advisory: the
 * heartbeat and last replayed commit are the primary's clock, the observation the read
 * replica's, so every age is taken at the observation (skew shows as staleness). Stale: the
 * heartbeat is older than STANDBY_STALE_X x its declared interval. An ops db without the
 * column (an older writer's) has no read replica source: no rows.
 *
 * Heartbeat job (design-early-exit U0, display only): over the source's last
 * HEARTBEAT_JOB_RUNS runs, every one ended capped with the same non-empty replicated marker
 * (plain equality, no parsing, no age). Then `not_running` when the WAL receiver streamed in
 * every run and its last contact moved forward from the oldest run to the newest (the read
 * replica's clock against itself), or `unknown` when any run's contact was unknown. Nothing
 * when replay stood still with WAL waiting (paused, or held by a recovery conflict): the
 * marker can't move then, whatever the job does. Runs are ordered, and the span taken, by
 * written_at (the ops db's clock; a run that failed before its lease has no as_of). No clock
 * is compared with another.
 */
export const STANDBY_STALE_X = 3;
export const HEARTBEAT_JOB_RUNS = 3;
export const sourceStandby = (f: Filters, has: ReadonlySet<string> = ALL_COLUMNS) => has.has('ingest_status.standby') ? `
  WITH s AS (
    SELECT source_id, standby AS i,
           TRY_CAST(standby ->> 'observed_ts' AS TIMESTAMPTZ) AS observed,
           TRY_CAST(standby ->> 'heartbeat_ts' AS TIMESTAMPTZ) AS beat,
           TRY_CAST(standby ->> 'last_replay_commit_ts' AS TIMESTAMPTZ) AS replayed,
           TRY_CAST(standby ->> 'upstream_contact_ts' AS TIMESTAMPTZ) AS contact,
           TRY_CAST(standby ->> 'heartbeat_interval_s' AS DOUBLE) AS interval_s
    FROM ${T('ingest_status')} WHERE standby IS NOT NULL${bySource(f)}
  ), runs AS (
    SELECT source_id, status, epoch(written_at) AS t, ${has.has('ingest_runs.standby') ? 'standby' : 'NULL::VARCHAR'} AS r,
           TRY_CAST(${has.has('ingest_runs.standby') ? 'standby' : 'NULL::VARCHAR'} ->> 'upstream_contact_ts' AS TIMESTAMPTZ) AS contact
    FROM ${T('ingest_runs')} WHERE source_id IN (SELECT source_id FROM s)
    QUALIFY row_number() OVER (PARTITION BY source_id ORDER BY written_at DESC) <= ${HEARTBEAT_JOB_RUNS}
  ), job AS (
    SELECT source_id, count(*) AS n,
           bool_and(status = 'capped' AND r IS NOT NULL) AS capped,
           bool_and(coalesce(r ->> 'heartbeat_marker', '') <> '') AND count(DISTINCT r ->> 'heartbeat_marker') = 1 AS same_marker,
           bool_and(r ->> 'receiver_status' = 'streaming' AND contact IS NOT NULL) AS streaming,
           bool_or(coalesce(r ->> 'receiver_status', 'unknown') = 'unknown' OR contact IS NULL) AS any_unknown,
           arg_max(contact, t) > arg_min(contact, t) AS advanced,
           count(DISTINCT r ->> 'replay_lsn') = 1 AND arg_max(TRY_CAST(r ->> 'replay_lag_bytes' AS BIGINT), t) > 0 AS replay_stalled,
           max(t) - min(t) AS span_s
    FROM runs GROUP BY source_id)
  SELECT source_id, ${lsn("TRY_CAST(i ->> 'replay_lsn' AS UBIGINT)")} AS replay_lsn,
         TRY_CAST(i ->> 'replay_lag_bytes' AS BIGINT) AS replay_lag_bytes,
         round(epoch(observed) - epoch(replayed)) AS replay_age_s,
         coalesce(i ->> 'receiver_status', 'unknown') AS receiver_status,
         round(epoch(observed) - epoch(contact)) AS contact_age_s,
         round(epoch(observed) - epoch(beat)) AS heartbeat_age_s, interval_s AS heartbeat_interval_s,
         coalesce(epoch(observed) - epoch(beat) > ${STANDBY_STALE_X} * interval_s, beat IS NULL) AS heartbeat_stale,
         epoch(observed) AS observed_at_s,
         CASE WHEN job.n = ${HEARTBEAT_JOB_RUNS} AND job.capped AND job.same_marker AND NOT coalesce(job.replay_stalled, false)
              THEN CASE WHEN job.streaming AND job.advanced THEN 'not_running'
                        WHEN job.any_unknown THEN 'unknown' END END AS heartbeat_job,
         CASE WHEN heartbeat_job IS NOT NULL THEN round(job.span_s) END AS heartbeat_job_span_s
  FROM s LEFT JOIN job USING (source_id) ORDER BY source_id` : `
  SELECT NULL::VARCHAR AS source_id, NULL::VARCHAR AS replay_lsn, NULL::BIGINT AS replay_lag_bytes, NULL::DOUBLE AS replay_age_s,
         NULL::VARCHAR AS receiver_status, NULL::DOUBLE AS contact_age_s, NULL::DOUBLE AS heartbeat_age_s,
         NULL::DOUBLE AS heartbeat_interval_s, NULL::BOOLEAN AS heartbeat_stale, NULL::DOUBLE AS observed_at_s,
         NULL::VARCHAR AS heartbeat_job, NULL::DOUBLE AS heartbeat_job_span_s
  FROM ${T('ingest_status')} WHERE false`;

/** "Status as of": the newest status any Flight wrote (the age of the Dive's data). */
export const statusAsOf = (f: Filters) => `
  SELECT max(t) AS as_of_at_s FROM (
    SELECT max(epoch(written_at)) AS t FROM ${T('ingest_status')} WHERE true${bySource(f)}
    UNION ALL SELECT max(epoch(written_at)) FROM ${T('apply_runs')} WHERE true${bySource(f)}
    UNION ALL SELECT max(epoch(written_at)) FROM ${T('compaction_status')} WHERE true${bySource(f)})`;

/** A table's history (the drawer): its newest action audit rows, 30 days. */
export const HISTORY_ROWS = 20;
export const tableHistory = (f: Filters) => `
  SELECT source_id, coalesce(inputs ->> 'table', result ->> 'table') AS table_name, ${epochS('written_at')} AS at_s,
         action, dry_run, result ->> 'status' AS status, result ->> 'detail' AS detail,
         coalesce(started_by, token_user) AS actor
  FROM ${T('audit')}
  WHERE written_at > current_timestamp - INTERVAL 30 DAY
    AND coalesce(inputs ->> 'table', result ->> 'table') IS NOT NULL${bySource(f)}
  QUALIFY row_number() OVER (PARTITION BY source_id, table_name ORDER BY written_at DESC) <= ${HISTORY_ROWS}
  ORDER BY written_at DESC`;

/** A table's gaps (the drawer): quarantined batches and expirations, 30 days, with whether
 * each is still open (an expiration closes once the table is rebuilt). */
export const tableGaps = (f: Filters) => `
  SELECT source_id, table_name, kind, at_s, what, open FROM (
    SELECT g.source_id, g.table_name, 'quarantine' AS kind, ${epochS('g.created_at')} AS at_s,
           'Batch ' || CASE WHEN g.unit_start IS NOT NULL THEN ${lsn('g.unit_start')} || '..' || ${lsn('g.unit_end')}
                            ELSE ${lsn('g.commit_lsn')} END || ' quarantined' AS what,
           NULL::BOOLEAN AS open
    FROM (SELECT source_id, schema_name || '.' || table_name AS table_name, epoch AS table_epoch,
                 ${j('epoch')}::INTEGER AS epoch, ${j('commit_lsn')}::UBIGINT AS commit_lsn,
                 ${j('created_at')}::TIMESTAMPTZ AS created_at,
                 ${j('unit_start_lsn')}::UBIGINT AS unit_start, ${j('unit_end_lsn')}::UBIGINT AS unit_end
          FROM (SELECT *, ${items('quarantines')} AS i FROM ${T('apply_status')} WHERE true${bySource(f)})) g
    UNION ALL
    SELECT source_id, table_name, 'expiration', expired_at_s,
           'Retention expired (' || coalesce(reason, 'unknown') || ')', open
    FROM (${expirations(f)}) WHERE outcome = 'expired')
  WHERE at_s > epoch(current_timestamp) - 30 * 86400
  ORDER BY at_s DESC`;

/** The Source page's pipeline strip: failures in 24 h per Flight, apply's last run and
 * cadence, and the change log's retained days (compaction_status.days: wire bytes). */
export const sourcePipeline = (f: Filters) => `
  WITH ing AS (
    SELECT source_id, count(*) FILTER (WHERE status = 'failed' AND written_at > current_timestamp - INTERVAL 1 DAY) AS ingest_failed_24h
    FROM ${T('ingest_runs')} WHERE true${bySource(f)} GROUP BY source_id
  ), app AS (
    SELECT source_id, max(epoch(written_at)) AS apply_last_at_s,
           arg_max(status, written_at) AS apply_last_status,
           count(*) FILTER (WHERE status = 'failed' AND written_at > current_timestamp - INTERVAL 1 DAY) AS apply_failed_24h
    FROM ${T('apply_runs')} WHERE table_name IS NULL AND status <> 'busy'${bySource(f)} GROUP BY source_id
  ), log AS (
    SELECT source_id, count(DISTINCT ${j('day')}) AS log_days, sum(${j('bytes')}::BIGINT) AS log_bytes, min(${j('day')}) AS oldest_day
    FROM (SELECT source_id, ${items('days')} AS i FROM ${T('compaction_status')} WHERE true${bySource(f)}) GROUP BY source_id
  ), bf AS (
    SELECT source_id, count(*) AS active_backfills FROM ${T('backfill_status')}
    WHERE state IN (${ACTIVE_GENERATION.map(q).join(', ')})${bySource(f)} GROUP BY source_id
  ), src AS (${sources()})
  SELECT src.source_id, coalesce(ing.ingest_failed_24h, 0) AS ingest_failed_24h,
         app.apply_last_at_s, app.apply_last_status, round(ac.every_s) AS apply_every_s, coalesce(ac.gaps, 0) AS apply_gaps,
         coalesce(app.apply_failed_24h, 0) AS apply_failed_24h,
         log.log_days, log.log_bytes, log.oldest_day, (c.limits ->> 'max_retained_lag_hours')::DOUBLE AS keep_hours,
         coalesce(bf.active_backfills, 0) AS active_backfills
  FROM src LEFT JOIN ing USING (source_id) LEFT JOIN app USING (source_id) LEFT JOIN log USING (source_id)
  LEFT JOIN bf USING (source_id) LEFT JOIN ${T('compaction_status')} c USING (source_id)
  LEFT JOIN (${medianGap(T('apply_runs'), 'epoch(written_at)', ` AND table_name IS NULL AND status <> 'busy'${bySource(f)}`)}) ac USING (source_id)
  WHERE true${bySource(f, 'src.source_id')}
  ORDER BY src.source_id`;

/** Retained WAL over 7 days (ingest_runs; the WAL limit itself isn't kept per run). */
export const walHistory = (f: Filters) => `
  SELECT source_id, as_of / 1e6 AS at_s, retained_bytes
  FROM ${T('ingest_runs')} WHERE as_of IS NOT NULL${bySource(f)}
    AND written_at > current_timestamp - INTERVAL 7 DAY
  ORDER BY source_id, as_of`;

/** Pins on the change log, oldest first, with the table each keeps (a table pin, or a
 * rebuild's generation pin) and the estimated expiry. */
export const sourcePins = (f: Filters) => `
  WITH p AS (
    SELECT source_id, ${j('kind')} AS kind, ${j('relation_id')}::UINTEGER AS relation_id, ${j('generation_id')} AS generation_id,
           ${j('updated_at_us')}::BIGINT / 1e6 AS updated_at_s, ${j('expires_at_us')}::BIGINT / 1e6 AS expires_at_s, ${j('rule')} AS rule
    FROM (SELECT source_id, ${items('pins')} AS i FROM ${T('compaction_status')} WHERE true${bySource(f)})
  )
  SELECT p.source_id, coalesce(a.schema_name || '.' || a.table_name, b.table_name, p.kind || ' ' || coalesce(p.generation_id, p.relation_id::VARCHAR)) AS table_name,
         p.kind, a.state, b.state AS gen_state, p.updated_at_s, p.expires_at_s, p.rule
  FROM p
  LEFT JOIN ${T('apply_status')} a ON a.source_id = p.source_id AND a.relation_id = p.relation_id AND p.kind = 'table'
  LEFT JOIN ${T('backfill_status')} b ON b.source_id = p.source_id AND b.generation_id = p.generation_id AND p.kind = 'generation'
  ORDER BY p.source_id, p.updated_at_s NULLS FIRST`;

/** Setup check: the newest preflight with its warnings. */
export const sourceSetup = (f: Filters) => `
  SELECT source_id, as_of / 1e6 AS at_s, status, error,
         (SELECT list(w ->> 'warning') FROM (SELECT unnest(from_json(coalesce(warnings, '[]'), '["json"]')) AS w)) AS warnings
  FROM ${T('preflight_status')} WHERE true${bySource(f)} ORDER BY source_id`;

// ── The action panel (phase 2) ──────────────────────────────────────────────────────

/** The audit row of a run the panel started, found by the CDC_REQUEST_ID it sent (a run
 * refused for invalid input writes none). `audit.request_id` exists wherever the Dive is
 * deployed: the deploy's add-only DDL creates it. */
export const auditRun = (requestId: string) => `
  SELECT run_id, request_id, action, dry_run, ${epochS('written_at')} AS at_s,
         result ->> 'status' AS status, result ->> 'table' AS table_name, result ->> 'detail' AS detail,
         (SELECT list(x) FROM (SELECT unnest(from_json(coalesce(result -> 'next_steps', '[]'), '["varchar"]')) AS x)) AS next_steps,
         CASE WHEN (result -> 'triggered') IS NOT NULL
              THEN (result -> 'triggered' ->> 'flight') || ' run ' || (result -> 'triggered' ->> 'run_number') END AS triggered,
         (result ->> 'remaining')::INTEGER AS remaining, result ->> 'lifecycle' AS lifecycle,
         coalesce(started_by, token_user) AS actor, (result -> 'observed')::VARCHAR AS observed,
         -- delete-source: what it drops itself (a dry run's drop, a real run's dropped)
         -- and the statements it leaves for a DBA
         coalesce(result -> 'dropped', result -> 'drop')::VARCHAR AS drops, (result -> 'dba')::VARCHAR AS dba
  FROM ${T('audit')} WHERE request_id = ${q(requestId)}
  ORDER BY written_at DESC LIMIT 1`;
export const EXAMPLE_RUN = '00000000-0000-4000-8000-000000000000';
export const EXAMPLE_REQUEST = 'dive-example-request';

/** The source's newest completed epoch transition (recover-slot or skip-to-lsn), with its
 * inputs: Finish recovery (a leftover slot) repeats exactly these, never a new epoch. */
export const lastTransition = (f: Filters) => `
  SELECT source_id, action, (inputs ->> 'expected_epoch')::INTEGER AS expected_epoch,
         ${lsn("(inputs ->> 'to_lsn')::UBIGINT")} AS to_lsn, inputs ->> 'confirm' AS confirm, ${epochS('written_at')} AS at_s
  FROM ${T('audit')}
  WHERE action IN ('recover-slot', 'skip-to-lsn') AND NOT dry_run AND (result ->> 'status') = 'done'${bySource(f)}
  QUALIFY row_number() OVER (PARTITION BY source_id ORDER BY written_at DESC) = 1
  ORDER BY source_id`;

const STANDBY_COLUMNS = ['source_id', 'replay_lsn', 'replay_lag_bytes', 'replay_age_s', 'receiver_status', 'contact_age_s',
  'heartbeat_age_s', 'heartbeat_interval_s', 'heartbeat_stale', 'observed_at_s',
  'heartbeat_job', 'heartbeat_job_span_s'];
/** Every query, with the columns it returns (checked by tests/test_dive_queries.py). */
export const QUERIES: Record<string, { columns: string[]; sql: (f: Filters) => string }> = {
  tableStatus: {
    sql: tableStatus,
    columns: ['source_id', 'table_name', 'relation_id', 'status', 'kind', 'state', 'detail', 'behind_s', 'expires_in_s', 'expires_at_s', 'since_at_s', 'held_for_s', 'actor', 'actor_action', 'chunks', 'chunks_total', 'generation_id', 'gen_state', 'gen_run_status', 'updated_at_s', 'durable_at_s', 'epoch', 'source_epoch', 'applied_lsn', 'checkpoint_lsn', 'blocked_commit_lsn', 'blocked_unit', 'pin_lsn', 'pin_moved_at_s', 'quarantines', 'changes_7d',
      'reason', 'capture_mode', 'ignore_deletes', 'deletes_ignored', 'last_truncate_lsn', 'log_start_lsn', 'kept_aside', 'rebuild_error', 'refused_detail', 'waits_for_ingest'],
  },
  // the same status on an ops db without any optional column (an older writer's)
  tableStatusOld: {
    sql: (f: Filters) => tableStatus(f, new Set()),
    columns: ['source_id', 'table_name', 'relation_id', 'status', 'kind', 'state', 'detail', 'behind_s', 'expires_in_s', 'expires_at_s', 'since_at_s', 'held_for_s', 'actor', 'actor_action', 'chunks', 'chunks_total', 'generation_id', 'gen_state', 'gen_run_status', 'updated_at_s', 'durable_at_s', 'epoch', 'source_epoch', 'applied_lsn', 'checkpoint_lsn', 'blocked_commit_lsn', 'blocked_unit', 'pin_lsn', 'pin_moved_at_s', 'quarantines', 'changes_7d',
      'reason', 'capture_mode', 'ignore_deletes', 'deletes_ignored', 'last_truncate_lsn', 'log_start_lsn', 'kept_aside', 'rebuild_error', 'refused_detail', 'waits_for_ingest'],
  },
  opsColumns: { sql: opsColumns, columns: ['col'] },
  syncMarks: { sql: syncMarks, columns: ['source_id', 'table_name', 'marks'] },
  sourceStatus: {
    sql: sourceStatus,
    columns: ['source_id', 'label', 'description', 'owner_user', 'last_sync_at_s', 'last_sync_status', 'every_s', 'gaps', 'slot_name', 'wal_status', 'slot_active', 'retained_bytes', 'safe_wal_size', 'lag_bytes', 'oldest_xact_age_s', 'slot_at_s', 'slot_conflict', 'wal_pct', 'ingest_health', 'apply_health', 'ingest_error', 'apply_error', 'ingest_urgent', 'apply_urgent', 'ingest_ok_at_s', 'apply_ok_at_s', 'failing_since_at_s', 'last_ok_at_s', 'epoch', 'checkpoint_lsn', 'lifecycle', 'durable_at_s'],
  },
  sourceStandby: { sql: sourceStandby, columns: STANDBY_COLUMNS },
  // the same on an ops db without the column (no read replica source)
  sourceStandbyOld: { sql: (f: Filters) => sourceStandby(f, new Set()), columns: STANDBY_COLUMNS },
  sourceSlots: { sql: sourceSlots, columns: ['source_id', 'slot_name', 'epoch', 'is_current', 'role', 'active', 'wal_status', 'retained_bytes', 'lag_bytes'] },
  auditRun: {
    sql: () => auditRun(EXAMPLE_REQUEST),
    columns: ['run_id', 'request_id', 'action', 'dry_run', 'at_s', 'status', 'table_name', 'detail', 'next_steps', 'triggered', 'remaining', 'lifecycle', 'actor', 'observed', 'drops', 'dba'],
  },
  lastTransition: { sql: lastTransition, columns: ['source_id', 'action', 'expected_epoch', 'to_lsn', 'confirm', 'at_s'] },
  statusAsOf: { sql: statusAsOf, columns: ['as_of_at_s'] },
  tableHistory: { sql: tableHistory, columns: ['source_id', 'table_name', 'at_s', 'action', 'dry_run', 'status', 'detail', 'actor'] },
  tableGaps: { sql: tableGaps, columns: ['source_id', 'table_name', 'kind', 'at_s', 'what', 'open'] },
  sourcePipeline: {
    sql: sourcePipeline,
    columns: ['source_id', 'ingest_failed_24h', 'apply_last_at_s', 'apply_last_status', 'apply_every_s', 'apply_gaps', 'apply_failed_24h', 'log_days', 'log_bytes', 'oldest_day', 'keep_hours', 'active_backfills'],
  },
  walHistory: { sql: walHistory, columns: ['source_id', 'at_s', 'retained_bytes'] },
  sourcePins: { sql: sourcePins, columns: ['source_id', 'table_name', 'kind', 'state', 'gen_state', 'updated_at_s', 'expires_at_s', 'rule'] },
  sourceSetup: { sql: sourceSetup, columns: ['source_id', 'at_s', 'status', 'error', 'warnings'] },
  health: { sql: health, columns: ['source_id', 'flight', 'status', 'error', 'step', 'last_run_at', 'busy_since', 'conflicts_24h', 'last_conflict_at', 'age_s', 'stale_after_s', 'health', 'ok_at_s', 'urgent_after_s', 'urgent'] },
  // The filter's domain: every source, whatever the filter.
  sources: { sql: () => sources(), columns: ['source_id'] },
  tls: { sql: tls, columns: ['source_id', 'tls', 'warn', 'breakdown'] },
};
