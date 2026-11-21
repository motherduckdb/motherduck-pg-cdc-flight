import { test } from 'node:test';
import assert from 'node:assert/strict';
import { appendFacts, byProblem, conflictReason, details, lastSuccessLine, noSuccess, REPLICA_WAITS, standbyView, type StandbyRow, slotRole, tlsText, isIssue, recovery, sourceHeadline, type SourceRow, type TableRow, usually, wal } from './model.ts';

const T0 = Date.UTC(2026, 10, 9, 21, 6, 41) / 1000;
const table = (o: Partial<TableRow>): TableRow => ({
  source_id: 'demo', table_name: 'public.t', status: 'ok', kind: 'ok', state: 'live', detail: null, behind_s: 0,
  expires_in_s: null, expires_at_s: null, since_at_s: null, held_for_s: null, actor: null, actor_action: null,
  chunks: null, chunks_total: null, generation_id: null, gen_state: null, updated_at_s: T0, durable_at_s: T0,
  epoch: 1, source_epoch: 1, applied_lsn: null, checkpoint_lsn: null, blocked_commit_lsn: null, blocked_unit: null, pin_lsn: null,
  pin_moved_at_s: null, quarantines: 0, changes_7d: 0,
  reason: null, capture_mode: null, ignore_deletes: null, deletes_ignored: null, last_truncate_lsn: null,
  log_start_lsn: null, kept_aside: null, rebuild_error: null, refused_detail: null, waits_for_ingest: null, ...o,
});
const source = (o: Partial<SourceRow>): SourceRow => ({
  source_id: 'tpcc', label: null, description: null, owner_user: null, last_sync_at_s: T0, last_sync_status: 'drained',
  every_s: 120, gaps: 19, slot_name: 's', wal_status: 'reserved', slot_active: false, retained_bytes: 1, safe_wal_size: 99,
  lag_bytes: 0, oldest_xact_age_s: null, slot_at_s: T0, wal_pct: 1, ingest_health: 'ok', apply_health: 'ok',
  ingest_error: null, apply_error: null, failing_since_at_s: null, last_ok_at_s: T0, epoch: 1, checkpoint_lsn: null,
  lifecycle: null, durable_at_s: T0, ...o,
});
const tz = 'America/Los_Angeles';

test('details follow the design templates', () => {
  assert.deepEqual(details(table({ kind: 'blocked', status: 'needs_action', detail: 'ConversionException applying transaction 1800', since_at_s: T0, held_for_s: 3360 }), tz), {
    why: 'Sync failed: ConversionException applying transaction 1800',
    since: 'Blocked since 2026-11-09 13:06:41 PST (56 min)',
  });
  assert.deepEqual(details(table({ kind: 'paused', status: 'needs_action', expires_in_s: 9 * 3600 + 120, expires_at_s: T0 + 9 * 3600, since_at_s: T0, actor: 'alice@motherduck.com' }), tz), {
    why: 'Paused · retention expires in 9 h',
    since: 'Paused by alice 2026-11-09 13:06:41 PST · est. expiry 2026-11-09 22:06:41 PST',
  });
  assert.equal(details(table({ kind: 'stale', behind_s: 52 * 60 })).why, 'Stale over 52 min');
  assert.deepEqual(details(table({ kind: 'rebuilding', chunks: 14, chunks_total: 22, since_at_s: T0, actor: 'alice', actor_action: 'resync' }), tz), {
    why: 'Backfill · 14 of ~22 chunks', since: 'Started 2026-11-09 13:06:41 PST by alice (resync)',
  });
  assert.equal(details(table({ kind: 'not_enabled', since_at_s: T0 }), tz).since, 'First seen 2026-11-09 13:06:41 PST');
  assert.equal(details(table({ kind: 'rebuild_failed', detail: 'OperationalError' })).why, 'Rebuild failed: OperationalError');
  assert.equal(details(table({})).why, '—');
});

test('problems sort first, then by name; only issues keeps problems', () => {
  const rows = [table({ table_name: 'a' }), table({ table_name: 'z', status: 'needs_action' }), table({ table_name: 'b', status: 'paused' })];
  assert.deepEqual(rows.sort(byProblem).map((t) => t.table_name), ['z', 'b', 'a']);
  assert.equal(isIssue('not_enabled'), true);
  assert.equal(isIssue('waiting'), false);
  assert.equal(isIssue('ok'), false);
});

test('WAL limit: percent, no cap, lost, unreserved', () => {
  assert.equal(wal(source({ wal_pct: 62.4, retained_bytes: 31 * 1024 ** 3, safe_wal_size: 19 * 1024 ** 3 })).label, '62%');
  assert.equal(wal(source({ wal_pct: null, safe_wal_size: null, retained_bytes: 5 })).label, 'No cap');
  assert.equal(wal(source({ wal_status: 'lost' })).label, 'Lost');
  assert.equal(wal(source({ wal_status: 'unreserved' })).alarm, true);
  assert.equal(usually(source({})), 'usually every ~2 min');
  assert.equal(usually(source({ gaps: 1 })), '');
});

test('a source shows its own problem first, then recovery, then its worst table', () => {
  assert.equal(sourceHeadline(source({ wal_status: 'lost', ingest_error: 'ObjectNotInPrerequisiteState' }), [table({})]).detail, 'Sync failing: ObjectNotInPrerequisiteState');
  const rec = [
    table({ table_name: 'a', epoch: 2 }), table({ table_name: 'b', epoch: 1, state: 'needs_resync', kind: 'rebuilding', status: 'rebuilding' }),
    table({ table_name: 'c', epoch: 1, state: 'needs_resync', kind: 'rebuild_failed', status: 'needs_action' }),
    table({ table_name: 'd', epoch: 1, state: 'needs_resync', kind: 'queued', status: 'queued' }),
  ];
  const h = sourceHeadline(source({ epoch: 2 }), rec);
  assert.equal(h.status, 'recovering');
  assert.equal(h.headline, '1 rebuilt · 1 rebuilding · 1 failed · 1 queued');
  assert.equal(recovery(source({ epoch: 2 }), [table({ epoch: 2 })]), null);
  assert.equal(sourceHeadline(source({}), [table({}), table({ status: 'needs_action', kind: 'blocked' })]).status, 'needs_action');
  assert.equal(sourceHeadline(source({}), [table({})]).headline, 'All 1 table replicating');
  const hot = source({ wal_pct: 62 });
  assert.equal(sourceHeadline(hot, [table({})]).headline, 'WAL limit 62% · 1 table');
  assert.equal(sourceHeadline(hot, [table({ status: 'needs_action', kind: 'paused' })]).status, 'needs_action');
  assert.equal(sourceHeadline(hot, [table({ status: 'rebuilding', kind: 'rebuilding' })]).status, 'warning');
  assert.equal(sourceHeadline(source({ lifecycle: 'deleted' }), [table({})]).headline, 'Source deleted · 1 table');
});

test('no successful run: Warning while stale, Needs action past the limit', () => {
  const now = Date.now() / 1000;
  const h = 3600;
  const stale = source({ ingest_health: 'stale', ingest_ok_at_s: now - 3 * h, last_sync_at_s: now - 3 * h });
  const warn = sourceHeadline(stale, [table({})]);
  assert.equal(warn.status, 'warning');
  assert.match(warn.headline, /^No sync for 3 h · 1 table$/);
  const urgent = sourceHeadline(source({ ingest_health: 'stale', ingest_urgent: true, ingest_ok_at_s: now - 68 * h }), [table({})]);
  assert.equal(urgent.status, 'needs_action');
  assert.equal(urgent.label, 'Needs action');
  assert.match(urgent.headline, /^No successful ingest for 2 d 20 h · 1 table$/);
  const applyOnly = sourceHeadline(source({ apply_health: 'stale', apply_urgent: true, apply_ok_at_s: now - 7 * h }), [table({})]);
  assert.match(applyOnly.headline, /^No successful apply for 7 h · 1 table$/);
  // ingest first: apply is chained to it
  assert.equal(noSuccess(source({ ingest_health: 'stale', apply_health: 'stale', ingest_ok_at_s: now - h }))?.flight, 'ingest');
  assert.equal(noSuccess(source({})), null);
  // An urgent apply stays Needs action when ingest turns stale but isn't urgent yet
  const both = source({ ingest_health: 'stale', ingest_ok_at_s: now - 2.5 * h, apply_health: 'stale', apply_urgent: true, apply_ok_at_s: now - 7 * h });
  assert.equal(noSuccess(both)?.flight, 'apply');
  assert.equal(sourceHeadline(both, [table({})]).status, 'needs_action');
  assert.match(sourceHeadline(both, [table({})]).headline, /^No successful apply for 7 h · 1 table$/);
  assert.equal(noSuccess(stale)?.hint, 'No ingest run has finished since then. If the Flight is running on schedule, its runs are failing before the pipeline starts (for example installing the package or reading a secret), so they leave no record here. Open the ingest Flight’s run log.');
  assert.equal(lastSuccessLine(now - 3 * h, 'usually every ~1 h', now), 'Last successful run 3 h ago (usually every ~1 h)');
  assert.equal(lastSuccessLine(now - 3 * h, '', now), 'Last successful run 3 h ago');
  // No ingest ever succeeded (last_success_as_of NULL): say so, don't label the newest run a success
  const never = source({ ingest_health: 'stale', ingest_urgent: true, ingest_ok_at_s: now - 2 * h, last_ok_at_s: null });
  assert.equal(noSuccess(never)?.never, true);
  assert.match(sourceHeadline(never, [table({})]).headline, /^No successful ingest recorded · 1 table$/);
  assert.equal(noSuccess(never)?.hint.startsWith('No ingest run has finished. If the Flight'), true);
  assert.equal(lastSuccessLine(null, 'usually every ~1 h', now), 'No successful run recorded (usually every ~1 h)');
  assert.equal(noSuccess(stale)?.never, false);
  assert.equal(noSuccess(source({ apply_health: 'stale', apply_ok_at_s: now - h, last_ok_at_s: null }))?.never, false);
});

test('TLS breakdown times are viewer-local', () => {
  assert.equal(tlsText(`ingest verify-full (as of @${T0}@); backfill unknown (as of @@)`, 'America/Los_Angeles'),
    'ingest verify-full (as of 2026-11-09 13:06:41 PST); backfill unknown (as of unknown)');
});

test('a slot without a role is leftover at or below the current epoch, else pending', () => {
  const slot = (name: string, epoch: number, is_current = false, role = '') => ({ slot_name: name, epoch, is_current, role, retained_bytes: 0, wal_status: 'reserved' });
  const slots = [slot('s_e3', 3, true, 'current'), slot('s_e2', 2), slot('s_e4', 4), slot('s', 1, false, 'leftover')];
  assert.match(slotRole(slots[1], slots), /^leftover/);
  assert.match(slotRole(slots[2], slots), /^pending/);
  assert.match(slotRole(slots[3], slots), /^leftover/);
  assert.equal(slotRole(slots[0], slots), 'current');
});

test('insert-only and catalog states read in plain words, with the facts', () => {
  const why = (o: Partial<TableRow>) => details(table({ status: 'needs_action', ...o }), 'UTC').why;
  assert.equal(why({ kind: 'append_changed', reason: 'append_delete' }), 'A row was deleted in an insert-only table');
  assert.equal(why({ kind: 'catalog_changed', reason: 'pk_replaced' }), 'Source table changed: its primary key constraint was replaced');
  assert.equal(why({ kind: 'type_changed', detail: "column 'price' changed type (its source type, under the same mirror type: values may differ); resync" }), 'A column changed type: price');
  assert.equal(why({ kind: 'append_invalid', detail: 'public.t: append mode needs REPLICA IDENTITY FULL' }), 'Can’t capture as insert-only: its replica identity isn’t FULL');
  assert.equal(why({ kind: 'not_started', detail: 'public.t: no free replication slot or walsender; retry later' }), 'Not started: no free replication slot or walsender; retry later');
  assert.equal(why({ kind: 'not_started', detail: 'public.t: is 5368709120 bytes, over the append snapshot limit 1073741824 (CDC_APPEND_SNAPSHOT_MAX_BYTES): use start-from-log, or raise the limit' }), 'Not started: the table is 5 GB, over the 1 GB snapshot limit');
  assert.equal(details(table({ kind: 'catalog_changed', reason: 'column_rewrite', waits_for_ingest: true }), 'UTC').since, 'Resync waits for the next ingest run');
  assert.deepEqual(appendFacts(table({ capture_mode: 'append', ignore_deletes: true, deletes_ignored: 1234, last_truncate_lsn: '1/28', log_start_lsn: '1/50' })), [
    'Insert-only (append mode)',
    'DELETEs skipped: 1,234 so far',
    'Emptied by a source TRUNCATE at 1/28: only rows inserted after it are in the replica',
    'Started from the log at 1/50: rows inserted before it aren’t in the replica',
  ]);
  assert.deepEqual(appendFacts(table({})), []);
});

test('resync-all’s queue never holds an insert-only table', () => {
  const s = { epoch: 2 } as SourceRow;
  const rec = recovery(s, [
    table({ table_name: 'public.k', kind: 'queued', state: 'needs_resync', epoch: 1 }),
    table({ table_name: 'public.a', kind: 'needs_resync', state: 'needs_resync', epoch: 1, capture_mode: 'append' }),
  ]);
  assert.deepEqual(rec?.queued.map((t) => t.table_name), ['public.k']);
});

test('after a slot recovery an insert-only table has its own bucket, never a stuck panel', () => {
  const rec = recovery({ epoch: 2 } as SourceRow, [
    table({ table_name: 'public.k', state: 'live', epoch: 2 }),
    table({ table_name: 'public.a', kind: 'needs_resync', status: 'needs_action', state: 'needs_resync', epoch: 1, capture_mode: 'append' }),
  ]);
  assert.deepEqual([rec?.rebuilt.length, rec?.queued.length, rec?.failed.length, rec?.insertOnly.map((t) => t.table_name)], [1, 0, 0, ['public.a']]);
});

test('a read replica card says the approved wording', () => {
  const r = (o: Partial<StandbyRow>): StandbyRow => ({
    source_id: 'rr', replay_lsn: '0/15F90', replay_lag_bytes: 4096, replay_age_s: 5, receiver_status: 'streaming',
    contact_age_s: 2, heartbeat_age_s: 30, heartbeat_interval_s: 60, heartbeat_stale: false, observed_at_s: T0, ...o,
  });
  const ok = standbyView(r({}), 'drained');
  assert.equal(ok.status, 'healthy');
  assert.equal(ok.lag, '4 KB received, not yet replayed');
  assert.match(ok.replayed, /ago \(primary's clock\)$/);
  assert.match(ok.upstream, /^Streaming from the primary · last contact /);
  assert.match(ok.heartbeat, / · expected every /);
  assert.equal(ok.staleHint, null);
  assert.equal(ok.lastRun, 'Reached the replay position');
  assert.equal(standbyView(r({ replay_lag_bytes: 0 }), 'capped').lag, '0 B: replay is caught up');
  assert.equal(standbyView(r({}), 'capped').lastRun, 'Stopped at its runtime cap');

  const stale = standbyView(r({ heartbeat_age_s: 900, heartbeat_stale: true }), 'capped');
  assert.equal(stale.status, 'warning');
  assert.match(stale.heartbeat, /^Stale or upstream unreachable: last heartbeat .* ago, expected every /);
  assert.ok(stale.staleHint);
  assert.equal(standbyView(r({ heartbeat_age_s: null, heartbeat_stale: true }), null).heartbeat, 'Stale or upstream unreachable: no heartbeat recorded, or its marker isn’t readable as a time.');
  assert.equal(standbyView(r({}), 'failed').lastRun, null);
  assert.equal(standbyView(r({}), 'conflict').lastRun, null);

  const unknown = standbyView(r({ receiver_status: 'unknown', contact_age_s: null }), 'drained');
  assert.equal(unknown.status, 'healthy'); // unknown upstream never warns
  assert.match(unknown.upstream, /^Unknown: .*pg_read_all_stats/);
  assert.match(unknown.upstream, /no WAL receiver is visible\. Either it isn’t running/);
  for (const v of [ok, stale, unknown]) for (const text of Object.values(v)) assert.doesNotMatch(String(text), /\bdown\b/i);
  assert.doesNotMatch(unknown.upstream, /unreachable/);
});

test('a dead heartbeat job reads the approved wording', () => {
  const r = (o: Partial<StandbyRow>): StandbyRow => ({
    source_id: 'rr', replay_lsn: '0/15F90', replay_lag_bytes: 0, replay_age_s: 1500, receiver_status: 'streaming',
    contact_age_s: 2, heartbeat_age_s: 1500, heartbeat_interval_s: 60, heartbeat_stale: true, observed_at_s: T0,
    heartbeat_job: null, heartbeat_job_span_s: null, ...o,
  });
  const dead = standbyView(r({ heartbeat_job: 'not_running', heartbeat_job_span_s: 18 * 60 }), 'capped');
  assert.equal(dead.status, 'warning');
  assert.equal(dead.heartbeat, 'Heartbeat job not running: the marker hasn’t changed in the last 3 runs (over 18 min), while the read replica kept hearing from the primary.');
  assert.equal(dead.staleHint, 'Each run waits to its runtime cap because no commit arrives. Restart the heartbeat job on the primary. With pg_cron, SELECT * FROM cron.job_run_details ORDER BY start_time DESC LIMIT 5; shows its recent runs and errors.');

  // the unknown variant warns too (the marker standing still is the finding), and never says "unreachable"
  const unknown = standbyView(r({ receiver_status: 'unknown', contact_age_s: null, heartbeat_stale: false, heartbeat_age_s: 40, heartbeat_job: 'unknown', heartbeat_job_span_s: 18 * 60 }), 'capped');
  assert.equal(unknown.status, 'warning');
  assert.equal(unknown.heartbeat, 'Heartbeat unchanged in the last 3 runs (over 18 min). Upstream contact is unknown, so this can’t tell a stopped heartbeat job from a read replica that isn’t receiving from the primary.');
  assert.equal(unknown.staleHint, 'Check that the heartbeat job runs on the primary.');
  for (const v of [dead, unknown]) for (const text of Object.values(v)) assert.doesNotMatch(String(text), /\bdown\b|unreachable/i);

  // without the signal, the stale line stays
  assert.match(standbyView(r({}), 'capped').heartbeat, /^Stale or upstream unreachable: /);
});

test('a read replica backfill wait reads its line and fix; a conflict slot its reason', () => {
  const t = details(table({ kind: 'rebuilding', status: 'rebuilding', gen_run_status: 'waiting_for_primary', since_at_s: T0 }), tz);
  assert.deepEqual(t, { why: REPLICA_WAITS.waiting_for_primary.why, since: REPLICA_WAITS.waiting_for_primary.fix });
  assert.match(details(table({ kind: 'rebuilding', status: 'rebuilding', chunks: 1, since_at_s: T0 }), tz).why, /^Backfill · /);
  assert.equal(conflictReason(source({ wal_status: 'lost', slot_conflict: 'rows_removed' })), 'rows_removed');
  assert.equal(conflictReason(source({ wal_status: 'lost', slot_conflict: 'lost' })), '');
  assert.equal(conflictReason(source({ wal_status: 'lost' })), null);
});
