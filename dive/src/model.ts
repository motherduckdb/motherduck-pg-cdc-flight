// From query rows to what the screens show: each table's pill and its fixed details text
// (the design's templates, filled from cdc_ops fields: no generated prose), each source's
// pill and headline, the recovery buckets, and the sort order (problems first).
import { formatAge, formatBytes, formatInt, formatTs, N, S } from './format';
import { STATUS_ORDER, type SourceStatus, type Status, type Tick } from './theme';
import { HEARTBEAT_JOB_RUNS } from './queries';

export type TableRow = {
  source_id: string; table_name: string; status: string; kind: string; state: string | null;
  detail: string | null; behind_s: number | null; expires_in_s: number | null; expires_at_s: number | null;
  since_at_s: number | null; held_for_s: number | null; actor: string | null; actor_action: string | null;
  chunks: number | null; chunks_total: number | null; generation_id: string | null; gen_state: string | null;
  /** the generation's newest backfill run status (a read replica's waits: REPLICA_WAITS) */
  gen_run_status?: string | null;
  updated_at_s: number | null; durable_at_s: number | null; epoch: number | null; source_epoch: number | null;
  applied_lsn: string | null; checkpoint_lsn: string | null; blocked_commit_lsn: string | null; blocked_unit: string | null;
  pin_lsn: string | null; pin_moved_at_s: number | null; quarantines: number; changes_7d: number;
  /** why it's held, from the stored detail (queries.ts CATALOG_REASONS and the append prefixes) */
  reason: string | null;
  /** 'append' for an insert-only table; NULL is keyed */
  capture_mode: string | null; ignore_deletes: boolean | null; deletes_ignored: number | null;
  last_truncate_lsn: string | null; log_start_lsn: string | null; kept_aside: string | null;
  rebuild_error: string | null; refused_detail: string | null; waits_for_ingest: boolean | null;
};

export type SourceRow = {
  source_id: string; label: string | null; description: string | null; owner_user: string | null;
  last_sync_at_s: number | null; last_sync_status: string | null; every_s: number | null; gaps: number;
  slot_name: string | null; wal_status: string | null; slot_active: boolean | null;
  retained_bytes: number | null; safe_wal_size: number | null; lag_bytes: number | null;
  oldest_xact_age_s: number | null; slot_at_s: number | null; wal_pct: number | null;
  /** a read replica's slot invalidated by a recovery conflict: its reason (`lost` on PG16: none given) */
  slot_conflict?: string | null;
  ingest_health: string | null; apply_health: string | null; ingest_error: string | null; apply_error: string | null;
  failing_since_at_s: number | null; last_ok_at_s: number | null; epoch: number | null;
  /** stale and no successful run for max(1 h, 6 x the usual interval): Needs action */
  ingest_urgent?: boolean; apply_urgent?: boolean; ingest_ok_at_s?: number | null; apply_ok_at_s?: number | null;
  checkpoint_lsn: string | null; lifecycle: string | null; durable_at_s: number | null;
};

/** DuckDB values (BigInt, decimals) as plain numbers; NULL stays null. */
const num = (v: unknown): number | null => (v == null ? null : N(v));
export function tableRow(r: Record<string, unknown>): TableRow {
  const o: Record<string, unknown> = { ...r };
  for (const k of ['behind_s', 'expires_in_s', 'expires_at_s', 'since_at_s', 'held_for_s', 'chunks', 'chunks_total', 'updated_at_s', 'durable_at_s', 'epoch', 'source_epoch', 'pin_moved_at_s', 'deletes_ignored']) o[k] = num(r[k]);
  o.quarantines = N(r.quarantines);
  o.changes_7d = N(r.changes_7d);
  return o as TableRow;
}
export function sourceRow(r: Record<string, unknown>): SourceRow {
  const o: Record<string, unknown> = { ...r };
  for (const k of ['last_sync_at_s', 'every_s', 'retained_bytes', 'safe_wal_size', 'lag_bytes', 'oldest_xact_age_s', 'slot_at_s', 'wal_pct', 'failing_since_at_s', 'last_ok_at_s', 'epoch', 'durable_at_s', 'ingest_ok_at_s', 'apply_ok_at_s']) o[k] = num(r[k]);
  o.gaps = N(r.gaps);
  o.ingest_urgent = r.ingest_urgent === true;
  o.apply_urgent = r.apply_urgent === true;
  return o as SourceRow;
}

/** A read replica source's diagnostics (queries.ts sourceStandby); advisory, display only. */
export type StandbyRow = {
  source_id: string; replay_lsn: string | null; replay_lag_bytes: number | null; replay_age_s: number | null;
  receiver_status: string; contact_age_s: number | null; heartbeat_age_s: number | null;
  heartbeat_interval_s: number | null; heartbeat_stale: boolean; observed_at_s: number | null;
  heartbeat_job?: 'not_running' | 'unknown' | null; heartbeat_job_span_s?: number | null;
};
export function standbyRow(r: Record<string, unknown>): StandbyRow {
  const o: Record<string, unknown> = { ...r };
  for (const k of ['replay_lag_bytes', 'replay_age_s', 'contact_age_s', 'heartbeat_age_s', 'heartbeat_interval_s', 'observed_at_s', 'heartbeat_job_span_s']) o[k] = num(r[k]);
  o.heartbeat_job = r.heartbeat_job === 'not_running' || r.heartbeat_job === 'unknown' ? r.heartbeat_job : null;
  o.receiver_status = r.receiver_status == null ? 'unknown' : S(r.receiver_status);
  o.heartbeat_stale = r.heartbeat_stale === true;
  return o as StandbyRow;
}

/** The read replica's name: "read replica (standby)" on a page's first mention, then "read replica". */
export const READ_REPLICA_FIRST = 'read replica (standby)';
const ago = (s: number | null) => (s == null ? '—' : `${formatAge(Math.max(0, s))} ago`);
const every = (s: number | null) => (s == null ? 'no interval declared' : `expected every ${formatAge(s)}`);
/** The Read replica card's rows, in the approved wording. */
export function standbyView(r: StandbyRow, lastRun: string | null) {
  const unknown = r.receiver_status === 'unknown';
  const job = heartbeatJob(r);
  return {
    status: (r.heartbeat_stale || job ? 'warning' : 'healthy') as 'warning' | 'healthy',
    lag: r.replay_lag_bytes == null ? '—' : r.replay_lag_bytes === 0 ? '0 B: replay is caught up' : `${formatBytes(r.replay_lag_bytes)} received, not yet replayed`,
    replayed: r.replay_age_s == null ? '—' : `${ago(r.replay_age_s)} (primary's clock)`,
    upstream: unknown
      ? 'Unknown: no WAL receiver is visible. Either it isn’t running, or the replication role lacks pg_read_all_stats (grant it, or pg_monitor, on the primary).'
      : `${r.receiver_status === 'streaming' ? 'Streaming from the primary' : `WAL receiver ${r.receiver_status}`} · last contact ${ago(r.contact_age_s)}`,
    heartbeat: job ? job.line : !r.heartbeat_stale
      ? `${ago(r.heartbeat_age_s)} · ${every(r.heartbeat_interval_s)}`
      : r.heartbeat_age_s == null
        ? 'Stale or upstream unreachable: no heartbeat recorded, or its marker isn’t readable as a time.'
        : `Stale or upstream unreachable: last heartbeat ${ago(r.heartbeat_age_s)}, ${every(r.heartbeat_interval_s)}.`,
    staleHint: job ? job.hint : r.heartbeat_stale
      ? 'A busy source keeps syncing. On a quiet source, ingest runs stop at their cap and backfills wait until a commit arrives. Check that the heartbeat job runs on the primary.'
      : null,
    // only a drain's two ends: a failed or conflict run shows on the pipeline, not here
    lastRun: lastRun === 'drained' ? 'Reached the replay position' : lastRun === 'capped' ? 'Stopped at its runtime cap' : null,
  };
}
/**
 * The heartbeat job's state from the last runs (queries.ts heartbeat_job, the agreed
 * wording): it replaces the heartbeat line
 * and its hint, and warns on the card only.
 */
function heartbeatJob(r: StandbyRow): { line: string; hint: string } | null {
  if (!r.heartbeat_job) return null;
  const runs = `the last ${HEARTBEAT_JOB_RUNS} runs${r.heartbeat_job_span_s ? ` (over ${formatAge(r.heartbeat_job_span_s)})` : ''}`;
  return r.heartbeat_job === 'not_running'
    ? {
        line: `Heartbeat job not running: the marker hasn’t changed in ${runs}, while the read replica kept hearing from the primary.`,
        hint: 'Each run waits to its runtime cap because no commit arrives. Restart the heartbeat job on the primary. With pg_cron, SELECT * FROM cron.job_run_details ORDER BY start_time DESC LIMIT 5; shows its recent runs and errors.',
      }
    : {
        line: `Heartbeat unchanged in ${runs}. Upstream contact is unknown, so this can’t tell a stopped heartbeat job from a read replica that isn’t receiving from the primary.`,
        hint: 'Check that the heartbeat job runs on the primary.',
      };
}
export const STANDBY_FOOTER = 'Advisory only: sync never depends on these figures. Times marked “primary’s clock” come from the primary; ages are measured on the read replica’s clock, so clock skew can look like staleness.';

/** A read replica backfill's waits (backfill_runs.status): its table line and fix line. */
export const REPLICA_WAITS: Record<string, { why: string; fix: string }> = {
  waiting_for_replay: { why: 'Waiting for the read replica to replay up to the backfill’s start', fix: 'Nothing to do; it continues on the next run. If it lasts, check replay lag.' },
  waiting_past_replay: { why: 'Waiting for a commit past the read replica’s replay position', fix: 'On a quiet source, check that the heartbeat job runs on the primary.' },
  waiting_for_primary: { why: 'Waiting for the primary to log a standby snapshot', fix: 'Make sure the heartbeat job runs, or ask the DBA to run SELECT pg_log_standby_snapshot(); on the primary. It only helps once no transaction is open there.' },
};
/** The slot's recovery conflict, as the banner and pipeline say it ("" for PG16's `lost`). */
export const conflictReason = (s: SourceRow): string | null =>
  s.slot_conflict == null ? null : s.slot_conflict === 'lost' ? '' : s.slot_conflict;

/** The screens' statuses (removed tables are left out; `unknown` is an open design item). */
export type Shown = Status | 'unknown';
export const shownStatus = (t: TableRow): Shown => (STATUS_ORDER as string[]).includes(t.status) ? (t.status as Status) : 'unknown';
const RANK: Shown[] = ['needs_action', 'unknown', 'stale', 'paused', 'rebuilding', 'not_enabled', 'queued', 'waiting', 'ok'];
export const rank = (s: Shown): number => RANK.indexOf(s);
/** Problems: what "Only issues" keeps (queued and waiting point at their source instead). */
export const isIssue = (s: Shown): boolean => rank(s) <= rank('not_enabled');
export const visible = (t: TableRow): boolean => t.status !== 'removed';
export const byProblem = (a: TableRow, b: TableRow): number =>
  rank(shownStatus(a)) - rank(shownStatus(b)) || a.table_name.localeCompare(b.table_name);

/** A user name as the design writes it: the part before the @ ("alice"). */
export const who = (user: string | null | undefined): string => (user ? S(user).split('@')[0] : 'someone');
const ACTION_LABEL: Record<string, string> = { resync: 'resync', add: 'enable', 'resync-all': 'resync all', 'recover-slot': 'recover slot', pause: 'pause' };

/** The time left before a date, coarse ("9 h", "6 d"). */
const left = (s: number | null) => (s == null ? null : formatAge(Math.max(0, s)).replace(/ \d+ (min|h)$/, ''));

/**
 * The details column: line 1 says what's wrong, line 2 since when and by whom
 * (the design's "Details templates"). `tz` is for tests only.
 */
export function details(t: TableRow, tz?: string): { why: string; since: string } {
  const ts = (s: number | null) => formatTs(s, tz);
  switch (t.kind) {
    case 'blocked':
      // No per-table failure step exists in cdc_ops: the Flight's own fixed message instead.
      return { why: `Sync failed: ${t.detail ?? 'unknown error'}`, since: `Blocked since ${ts(t.since_at_s)} (${formatAge(t.held_for_s)})` };
    case 'registry':
      return { why: `Blocked by the source: ${t.detail ?? 'registry hold'}`, since: t.since_at_s ? `Since ${ts(t.since_at_s)}` : '' };
    case 'needs_resync':
      return { why: `Needs resync: ${t.detail ?? ''}`.replace(/: $/, ''), since: t.since_at_s ? `Since ${ts(t.since_at_s)} (${formatAge(t.held_for_s)})` : '' };
    case 'rebuild_failed':
      return { why: `Rebuild failed: ${t.detail ?? 'unknown error'}`, since: `Newest backfill run ${ts(t.since_at_s)}` };
    case 'paused': {
      const l = left(t.expires_in_s);
      const by = t.actor ? `Paused by ${who(t.actor)} ${ts(t.since_at_s)}` : `Paused ${ts(t.since_at_s)}`;
      return {
        why: l ? `Paused · retention expires in ${l}` : 'Paused',
        since: t.expires_at_s ? `${by} · est. expiry ${ts(t.expires_at_s)}` : by,
      };
    }
    case 'rebuilding': {
      const wait = REPLICA_WAITS[S(t.gen_run_status)];
      if (wait) return { why: wait.why, since: wait.fix };
      const chunks = t.chunks_total ? `${formatInt(t.chunks)} of ~${formatInt(t.chunks_total)} chunks` : `${formatInt(t.chunks ?? 0)} chunks`;
      const by = t.actor ? ` by ${who(t.actor)} (${ACTION_LABEL[S(t.actor_action)] ?? S(t.actor_action)})` : '';
      return { why: `Backfill · ${chunks}`, since: `Started ${ts(t.since_at_s)}${by}` };
    }
    case 'queued':
      return { why: 'Queued for rebuild', since: '' };
    case 'stale':
      return { why: `Stale over ${formatAge(t.behind_s)}`, since: '' };
    case 'not_enabled':
      return { why: 'In publication, not enabled', since: `First seen ${ts(t.since_at_s)}` };
    case 'waiting':
      return { why: 'Source sync stopped', since: '' };
    case 'append_changed':
      return { why: `A row was ${t.reason === 'append_delete' ? 'deleted' : 'changed'} in an insert-only table`, since: `Blocked since ${ts(t.since_at_s)} (${formatAge(t.held_for_s)})` };
    case 'catalog_changed':
      return {
        why: `Source table changed: ${CATALOG_TEXT[S(t.reason)] ?? 'a change the change log doesn’t carry'}`,
        since: t.waits_for_ingest ? 'Resync waits for the next ingest run' : t.since_at_s ? `Since ${ts(t.since_at_s)} (${formatAge(t.held_for_s)})` : '',
      };
    case 'type_changed':
      return { why: `A column changed type: ${typedColumn(t.detail) ?? 'a source column'}`, since: `Blocked since ${ts(t.since_at_s)} (${formatAge(t.held_for_s)})` };
    case 'append_invalid':
      return { why: `Can’t capture as insert-only: ${t.reason === 'append_keyed_frames' ? 'its change log carries a key' : t.reason === 'append_partitioned' || /partitioned/.test(S(t.detail)) ? 'the table is partitioned' : 'its replica identity isn’t FULL'}`, since: t.since_at_s ? `Since ${ts(t.since_at_s)}` : '' };
    case 'not_started':
      return { why: t.detail ? `Not started: ${refusalText(t.detail)}` : 'Not started yet', since: '' };
    case 'unknown':
      return { why: `Unknown state: ${t.state ?? 'none'}`, since: '' };
    default:
      return { why: '—', since: '' };
  }
}

/** A catalog check's reason (queries.ts CATALOG_REASONS) in plain words. */
const CATALOG_TEXT: Record<string, string> = {
  pk_replaced: 'its primary key constraint was replaced',
  partitions_changed: 'a partition was attached or detached',
  column_default_fill: 'a column was added with a default',
  column_rewrite: 'a column was added while the table was rewritten',
};
/** The column a type hold names (mirror.py: `column 'c' changed type …`). */
export const typedColumn = (detail: string | null): string | null => /^column '(.+?)' changed type/.exec(S(detail))?.[1] ?? null;
/** A backfill refusal without its leading `<table>: `. */
export const afterTable = (detail: string): string => detail.replace(/^[^\s:]+\.[^\s:]+: /, '');

/** A backfill refusal in plain words: the snapshot size limit's, else the Flight's own text. */
export function refusalText(detail: string): string {
  const m = /is (\d+) bytes, over the append snapshot limit (\d+)/.exec(detail);
  return m ? `the table is ${formatBytes(Number(m[1]))}, over the ${formatBytes(Number(m[2]))} snapshot limit` : afterTable(detail);
}

/** An insert-only table's facts for the drawer. */
export function appendFacts(t: TableRow): string[] {
  if (t.capture_mode !== 'append') return [];
  const out = ['Insert-only (append mode)'];
  if (t.ignore_deletes) out.push(`DELETEs skipped: ${formatInt(t.deletes_ignored ?? 0)} so far`);
  if (t.last_truncate_lsn) out.push(`Emptied by a source TRUNCATE at ${t.last_truncate_lsn}: only rows inserted after it are in the replica`);
  if (t.log_start_lsn) out.push(`Started from the log at ${t.log_start_lsn}: rows inserted before it aren’t in the replica`);
  return out;
}

/** The fixed help text per state (the drawer's Status section). */
export function help(t: TableRow): string {
  switch (t.kind) {
    case 'blocked':
      return 'One batch of changes for this table failed, so newer changes for it wait in the change log behind that batch. Other tables on this source keep updating. The replica table keeps its data as of Last updated.';
    case 'paused':
      return 'Changes for this table are kept in the change log but not applied. If the oldest kept change reaches the retention limit before you resume, the table needs a resync.';
    case 'rebuild_failed':
      return 'This table’s rebuild stopped. Until it is rebuilt, the replica table keeps the data it had before.';
    case 'rebuilding':
      return 'This table is being rebuilt from a fresh snapshot. The replica table keeps its old data until the rebuild finishes, then catches up from the change log.';
    case 'queued':
      return 'This table waits for its rebuild as part of a recovery. The replica table keeps its old data until then.';
    case 'needs_resync':
      return 'The change log can no longer bring this table up to date, so it must be rebuilt from a fresh snapshot. The replica table keeps its data as of Last updated.';
    case 'registry':
      return 'The source table changed in a way the pipeline won’t guess about, so this table stopped updating. The replica table keeps its data as of Last updated.';
    case 'stale':
      return 'This table’s Last updated time is behind how far the source has synced.';
    case 'waiting':
      return 'The source’s sync has stopped, so no new changes arrive for this table. The cause shows on the source.';
    case 'not_enabled':
      return 'This table is in the publication, but no one has enabled it, so it isn’t replicated.';
    case 'append_changed':
      return 'This table is insert-only (append mode), but the source changed or deleted a row, which append mode can’t apply. Newer changes for it wait in the change log. Resync takes an exact re-snapshot, which holds again if rows keep changing. The replica table keeps its data as of Last updated.';
    case 'catalog_changed':
      return t.capture_mode === 'append'
        ? 'The source table changed in a way the change log doesn’t carry, so this table stopped updating. Resync takes an exact re-snapshot; Start from log continues from now, without the rows before it. The replica table keeps its data as of Last updated.'
        : 'The source table changed in a way the change log doesn’t carry, so the replica table may differ from it. It must be rebuilt from a fresh snapshot. The replica table keeps its data as of Last updated.';
    case 'type_changed':
      return 'A source column changed type but the replica column kept its type, so values may differ. Resync rebuilds the table; if the values are fine, accept the new types and retry.';
    case 'append_invalid':
      if (t.reason === 'append_keyed_frames') return 'This insert-only table’s change log carries a key, so append mode can’t apply it. Remove the table and add it again.';
      return t.reason === 'append_partitioned' || /partitioned/.test(S(t.detail))
        ? 'A partitioned table without a key can’t be captured as insert-only. Remove it, or give it a primary key, then remove it and add it again (keyed).'
        : 'Insert-only (append mode) needs REPLICA IDENTITY FULL on the source table. The hold clears by itself once it is FULL again, with no resync.';
    case 'not_started':
      return 'This table is enabled, but its first copy hasn’t started, so it isn’t in the replica yet.';
    case 'unknown':
      return 'The Dive doesn’t know this table’s state.';
    default:
      return 'This table’s Last updated time matches how far the source has synced.';
  }
}

/** "Last updated" (the table's commit time on the source) or "unknown". */
export const lastUpdated = (t: TableRow, tz?: string): string =>
  t.kind === 'not_enabled' ? '—' : t.updated_at_s != null ? formatTs(t.updated_at_s, tz) : 'unknown';

/** The TLS breakdown with each writer's `@<epoch seconds>@` as a viewer-local time. */
export const tlsText = (breakdown: unknown, tz?: string): string =>
  S(breakdown).replace(/@([0-9.]*)@/g, (_, t: string) => (t ? formatTs(Number(t), tz) : 'unknown'));

export type SlotRow = { slot_name: string; role: string; is_current: boolean; retained_bytes: number | null; wal_status: string | null; epoch: number | null };
/** A slot other than the current one that still retains WAL: a pending or leftover slot. */
export const extraSlots = (slots: SlotRow[]): SlotRow[] => slots.filter((x) => !x.is_current);
/** A slot's role: the writer's, else by epoch (at or below the current one is an old slot
 * whose drop was missed; above it, a recovery that was interrupted). */
export function slotRole(x: SlotRow, slots: SlotRow[]): string {
  if (x.is_current) return 'current';
  const current = slots.find((y) => y.is_current)?.epoch;
  const leftover = x.role === 'leftover' || (x.role !== 'pending' && x.epoch != null && current != null && x.epoch <= current);
  return leftover ? 'leftover from an earlier epoch: drop it by finishing the recovery' : 'pending: a recovery was interrupted';
}

export const ticksOf = (marks: unknown): Tick[] => S(marks).split(',').filter(Boolean) as Tick[];

// ── Sources ───────────────────────────────────────────────────────────────────────

export const sourceName = (s: SourceRow): string => s.label || s.source_id;

/** "usually every ~2 min" from the median of recent run gaps; nothing below 3 gaps. */
export const usually = (s: SourceRow): string => (s.every_s != null && s.gaps >= 3 ? `usually every ~${formatAge(s.every_s).replace(/ \d+ min$/, '')}` : '');

export type Wal = { pct: number | null; label: string; lost: boolean; alarm: boolean; text: string };
/** WAL limit used: retained / (retained + safe_wal_size) of the current slot. */
export function wal(s: SourceRow): Wal {
  const lost = s.wal_status === 'lost' || s.wal_status === 'missing';
  if (lost) return { pct: 100, label: 'Lost', lost, alarm: true, text: s.wal_status === 'missing' ? 'The replication slot is missing.' : 'Postgres invalidated the replication slot.' };
  if (s.wal_status === 'unreserved')
    return { pct: 100, label: 'Over', lost, alarm: true, text: 'Postgres no longer reserves the WAL this source needs: the slot will be invalidated at the next checkpoint.' };
  if (s.wal_pct == null)
    return { pct: null, label: s.retained_bytes != null ? 'No cap' : '—', lost, alarm: false, text: s.retained_bytes != null ? `${formatBytes(s.retained_bytes)} retained · no cap: disk is the limit` : 'Not reported yet' };
  return {
    pct: s.wal_pct, label: `${Math.round(s.wal_pct)}%`, lost, alarm: false,
    text: `${formatBytes(s.retained_bytes)} retained of ${formatBytes(N(s.retained_bytes) + N(s.safe_wal_size))} limit`,
  };
}
export type Recovery = { rebuilt: TableRow[]; rebuilding: TableRow[]; failed: TableRow[]; queued: TableRow[]; insertOnly: TableRow[] };
/** Recovery buckets (CDC decision 7): only while some counted table is behind the source's epoch. */
export function recovery(s: SourceRow, tables: TableRow[]): Recovery | null {
  const counted = tables.filter((t) => t.kind !== 'removed' && t.kind !== 'not_enabled' && t.kind !== 'registry');
  if (s.epoch == null || !counted.some((t) => t.epoch != null && t.epoch < N(s.epoch))) return null;
  const behind = (t: TableRow) => t.epoch == null || t.epoch < N(s.epoch);
  return {
    rebuilt: counted.filter((t) => t.state === 'live' && !behind(t)),
    rebuilding: counted.filter((t) => t.kind === 'rebuilding'),
    failed: counted.filter((t) => t.kind === 'rebuild_failed' && t.capture_mode !== 'append'),
    // resync-all skips these: each is resynced (or started from the log) from its drawer
    insertOnly: counted.filter((t) => t.capture_mode === 'append' && behind(t) && t.kind !== 'rebuilding'),
    // resync-all's queue: never an insert-only table (its re-snapshot is per table)
    queued: counted.filter((t) => t.capture_mode !== 'append' && (t.kind === 'queued' || (t.kind === 'needs_resync' && behind(t)))),
  };
}

/**
 * A stale ingest or apply (an urgent one first, then ingest before apply: apply is chained to
 * it): how long since its last
 * successful run, whether that's past the Needs action limit (queries.ts URGENT_X), and the
 * approved hint.
 */
export function noSuccess(s: SourceRow, now = Date.now() / 1000): { flight: 'ingest' | 'apply'; urgent: boolean; never: boolean; age_s: number; hint: string } | null {
  // urgent first (ingest, then apply), then stale: a newly stale ingest never hides an urgent apply
  const flight = s.ingest_health === 'stale' && s.ingest_urgent ? 'ingest'
    : s.apply_health === 'stale' && s.apply_urgent ? 'apply'
    : s.ingest_health === 'stale' ? 'ingest' : s.apply_health === 'stale' ? 'apply' : null;
  if (!flight) return null;
  const at = flight === 'ingest' ? (s.ingest_ok_at_s ?? s.last_ok_at_s) : s.apply_ok_at_s;
  // never: ingest_status.last_success_as_of is NULL (no ingest ever succeeded; the age then runs
  // from the newest run). Apply's successes come from retained runs only, so it's never "never".
  const never = flight === 'ingest' && s.last_ok_at_s == null;
  return {
    flight,
    urgent: (flight === 'ingest' ? s.ingest_urgent : s.apply_urgent) === true,
    never,
    age_s: Math.max(0, now - N(at)),
    hint: `No ${flight} run has finished${never ? '' : ' since then'}. If the Flight is running on schedule, its runs are failing before the pipeline starts (for example installing the package or reading a secret), so they leave no record here. Open the ${flight} Flight’s run log.`,
  };
}
/**
 * A stale stage's line: "Last successful run 3 h ago (usually every ~1 h)", or "No successful
 * run recorded" when none ever succeeded (at_s NULL: pass the raw last success, not one that
 * falls back to the newest run).
 */
export const lastSuccessLine = (at_s: number | null | undefined, every: string, now = Date.now() / 1000) =>
  `${at_s == null ? 'No successful run recorded' : `Last successful run ${formatAge(Math.max(0, now - at_s))} ago`}${every ? ` (${every})` : ''}`;

const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;

/** A source's pill, headline and detail line (problems first: its own, then its tables'). */
export function sourceHeadline(s: SourceRow, tables: TableRow[]): { status: SourceStatus; label: string; headline: string; detail: string | null; rank: number } {
  const shown = tables.filter(visible);
  const n = shown.filter((t) => t.kind !== 'not_enabled').length;
  const count = plural(n, 'table');
  const w = wal(s);
  if (s.lifecycle || s.ingest_health === 'deleted' || s.apply_health === 'deleted')
    return { status: 'needs_action', label: 'Needs action', headline: `Source ${s.lifecycle === 'deleted' ? 'deleted' : s.lifecycle === 'deleting' ? 'being deleted' : 'deleting or deleted'} · ${count}`, detail: null, rank: 0 };
  if (w.lost) return { status: 'needs_action', label: 'Needs action', headline: `Replication slot invalidated · ${count}`, detail: s.ingest_error ? `Sync failing: ${s.ingest_error}` : null, rank: 0 };
  if (s.ingest_health === 'failed' || s.apply_health === 'failed') {
    const err = s.ingest_health === 'failed' ? s.ingest_error : s.apply_error;
    return { status: 'needs_action', label: 'Needs action', headline: `Sync failing · ${count}`, detail: err ? `Sync failing: ${err}` : null, rank: 0 };
  }
  const rec = recovery(s, shown);
  if (rec) {
    const parts = [`${rec.rebuilt.length} rebuilt`, `${rec.rebuilding.length} rebuilding`, `${rec.failed.length} failed`, `${rec.queued.length} queued`];
    return { status: 'recovering', label: 'Recovering', headline: parts.join(' · '), detail: null, rank: rec.failed.length ? 0 : 2 };
  }
  const quiet = noSuccess(s);
  if (quiet?.urgent)
    return { status: 'needs_action', label: 'Needs action', headline: `No successful ${quiet.flight} ${quiet.never ? 'recorded' : `for ${formatAge(quiet.age_s)}`} · ${count}`, detail: null, rank: 0 };
  if (s.ingest_health === 'stale' || s.apply_health === 'stale')
    return { status: 'warning', label: 'Warning', headline: `No sync for ${formatAge(Date.now() / 1000 - N(s.last_sync_at_s))} · ${count}`, detail: null, rank: 1 };
  const walHead = w.alarm || (w.pct != null && w.pct >= 50)
    ? { status: (w.alarm ? 'needs_action' : 'warning') as SourceStatus, label: w.alarm ? 'Needs action' : 'Warning', headline: `WAL limit ${w.label} · ${count}`, detail: null, rank: w.alarm ? 0 : 1 }
    : null;
  if (walHead?.rank === 0) return walHead;
  const worst = shown.reduce((m, t) => Math.min(m, rank(shownStatus(t))), rank('ok'));
  if (worst <= rank('not_enabled')) {
    const counts = new Map<string, number>();
    for (const t of shown) counts.set(shownStatus(t), (counts.get(shownStatus(t)) ?? 0) + 1);
    const summary = [...counts.entries()]
      .sort(([a], [b]) => rank(a as Shown) - rank(b as Shown))
      .map(([k, v]) => `${v} ${k === 'ok' ? 'ok' : k.replace('_', ' ')}`)
      .join(' · ');
    const status: SourceStatus = worst <= rank('unknown') ? 'needs_action' : worst <= rank('paused') ? 'warning' : worst === rank('rebuilding') ? 'in_progress' : 'not_enabled';
    if (walHead && status !== 'needs_action') return walHead;
    return { status, label: status === 'needs_action' ? 'Needs action' : status === 'warning' ? 'Warning' : status === 'in_progress' ? 'In progress' : 'Not enabled', headline: `${count} · ${summary}`, detail: null, rank: [0, 1, 2, 3][['needs_action', 'warning', 'in_progress', 'not_enabled'].indexOf(status)] };
  }
  if (walHead) return walHead;
  return { status: 'healthy', label: 'Healthy', headline: n ? `All ${count} replicating` : 'No tables yet', detail: null, rank: 5 };
}
