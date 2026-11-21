import { test } from 'node:test';
import assert from 'node:assert/strict';
import { finishRecovery, fixOptions } from './fixes.ts';
import type { TableRow } from './model.ts';

const t = (o: Partial<TableRow>): TableRow => ({
  source_id: 'demo', table_name: 'public.t', status: 'ok', kind: 'ok', state: 'live', detail: null, behind_s: 0,
  expires_in_s: null, expires_at_s: null, since_at_s: null, held_for_s: null, actor: null, actor_action: null,
  chunks: null, chunks_total: null, generation_id: null, gen_state: null, updated_at_s: null, durable_at_s: null,
  epoch: 3, source_epoch: 3, applied_lsn: null, checkpoint_lsn: null, blocked_commit_lsn: null, blocked_unit: null,
  pin_lsn: null, pin_moved_at_s: null, quarantines: 0, changes_7d: 0,
  reason: null, capture_mode: null, ignore_deletes: null, deletes_ignored: null, last_truncate_lsn: null,
  log_start_lsn: null, kept_aside: null, rebuild_error: null, refused_detail: null, waits_for_ingest: null, ...o,
});
const names = (rows: ReturnType<typeof fixOptions>) => rows.map((o) => `${o.name}${o.na ? ' (na)' : ''}${o.suggested ? ' *' : ''}`);

test('a blocked table: retry suggested, quarantine of its whole unit, resync, remove', () => {
  const opts = fixOptions(t({ kind: 'blocked', status: 'needs_action', state: 'needs_action', blocked_commit_lsn: '1A/7E000010', blocked_unit: '1A/7E000000..1A/7E1F2A88' }));
  assert.deepEqual(names(opts), ['Retry *', 'Quarantine', 'Resync', 'Remove']);
  assert.deepEqual(opts[0].spec?.inputs, { table: 'public.t', expectedBlockedLsn: '1A/7E000010' });
  assert.deepEqual(opts[1].spec?.inputs, { table: 'public.t', expectedEpoch: 3, unitStart: '1A/7E000000', unitEnd: '1A/7E1F2A88' });
  assert.deepEqual(opts[2].spec?.inputs, { table: 'public.t', expectedState: 'needs_action' });
  // a table that still exists is a forced remove, with the Flight's literal
  assert.deepEqual(opts[3].spec?.inputs, { table: 'public.t', force: true, confirm: 'REMOVE public.t' });
});

test('a blocked table without its unit can\'t be quarantined; retry sends none', () => {
  const opts = fixOptions(t({ kind: 'blocked', state: 'needs_action' }));
  assert.equal(opts[1].na, 'The blocked batch isn’t reported yet.');
  assert.equal(opts[0].spec?.inputs.expectedBlockedLsn, 'none');
});

test('each state offers what applies and says why the rest don\'t', () => {
  assert.deepEqual(names(fixOptions(t({ kind: 'paused', state: 'paused' }))), ['Resume *', 'Quarantine (na)', 'Resync', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ kind: 'rebuild_failed', state: 'needs_resync' }))), ['Retry (na)', 'Quarantine (na)', 'Resync *', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ kind: 'rebuilding', state: 'needs_resync' }))), ['Retry (na)', 'Quarantine (na)', 'Resync (na)', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ kind: 'queued', state: 'needs_resync' }))), ['Retry (na)', 'Quarantine (na)', 'Resync *', 'Remove']);
  assert.deepEqual(names(fixOptions(t({}))), ['Retry (na)', 'Quarantine (na)', 'Resync', 'Pause', 'Remove']);
  const registry = fixOptions(t({ kind: 'registry', state: 'live' }));
  assert.deepEqual(names(registry), ['Retry (na)', 'Quarantine (na)', 'Resync (na)', 'Remove *']);
  assert.deepEqual(registry[3].spec?.inputs, { table: 'public.t' }); // source gone: a plain remove
  const enable = fixOptions(t({ kind: 'not_enabled', state: null }));
  assert.equal(enable[0].spec?.action, 'add');
  assert.equal(fixOptions(t({}))[3].spec?.inputs.expectedState, 'live');
});

test('a removed table has no options; a held table\'s remove names its hold', () => {
  assert.deepEqual(fixOptions(t({ kind: 'removed', status: 'removed' })), []);
  const held = fixOptions(t({ kind: 'registry', state: 'live', detail: 'source_missing: dropped at the source' }));
  assert.match(held[3].spec!.would, /which the source holds \(source_missing: dropped at the source\)/);
});

test('finish recovery repeats the recorded transition, else the recover-slot that began the current epoch', () => {
  const recorded = finishRecovery('demo', 3, { action: 'skip-to-lsn', expected_epoch: 2, to_lsn: '5/AB', confirm: 'SKIP demo TO 5/AB' });
  assert.equal(recorded?.action, 'skip-to-lsn');
  assert.deepEqual(recorded?.inputs, { expectedEpoch: 2, toLsn: '5/AB', confirm: 'SKIP demo TO 5/AB' });
  assert.equal(recorded?.title, 'Finish recovery');
  // no recorded transition (the audit copy is best effort): keyed on the current epoch
  const derived = finishRecovery('demo', 3, null);
  assert.deepEqual([derived?.action, derived?.inputs], ['recover-slot', { expectedEpoch: 2, confirm: 'RECOVER demo EPOCH 3' }]);
  // a recorded transition from an older epoch isn't the one that began this one
  assert.deepEqual(finishRecovery('demo', 4, { action: 'recover-slot', expected_epoch: 1, to_lsn: null, confirm: 'RECOVER demo EPOCH 2' })?.inputs, { expectedEpoch: 3, confirm: 'RECOVER demo EPOCH 4' });
  assert.equal(finishRecovery('demo', 1, null), null); // epoch 1: nothing to finish
});

test('insert-only and catalog holds: Resync first, never Retry on an append hold', () => {
  const held = { status: 'needs_action', state: 'needs_action', capture_mode: 'append', blocked_commit_lsn: '1/10', blocked_unit: '1/0..1/20' };
  assert.deepEqual(names(fixOptions(t({ ...held, kind: 'append_changed', reason: 'append_update' }))), ['Resync *', 'Quarantine', 'Start from log', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ ...held, kind: 'catalog_changed', reason: 'partitions_changed' }))), ['Resync *', 'Start from log', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ status: 'needs_action', kind: 'catalog_changed', state: 'needs_resync', reason: 'pk_replaced' }))), ['Resync *', 'Remove']);
  const typed = fixOptions(t({ status: 'needs_action', kind: 'type_changed', state: 'needs_action', reason: 'type_changed', blocked_commit_lsn: '1/10' }));
  assert.deepEqual(names(typed), ['Resync *', 'Accept types', 'Retry', 'Remove']);
  assert.equal(typed[1].spec, undefined);
  assert.match(typed[1].cli?.[1] ?? '', /^cdc-flight-tables accept-types --table public\.t --dry-run$/);
  assert.deepEqual(names(fixOptions(t({ ...held, kind: 'type_changed', reason: 'type_changed' }))), ['Resync *', 'Accept types', 'Retry', 'Start from log', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ status: 'needs_action', kind: 'append_invalid', state: null }))), ['Remove']);
  const start = fixOptions(t({ ...held, kind: 'append_changed' }))[2].spec;
  assert.deepEqual([start?.action, start?.inputs], ['start-from-log', { table: 'public.t' }]);
});

test('a not-started table: start from log over the snapshot limit; no panel resync without a state', () => {
  const over = 'public.t: is 2000000000 bytes, over the append snapshot limit 1073741824 (CDC_APPEND_SNAPSHOT_MAX_BYTES): use start-from-log, or raise the limit';
  assert.deepEqual(names(fixOptions(t({ status: 'needs_action', kind: 'not_started', state: null, capture_mode: 'append', detail: over }))), ['Start from log *', 'Resync (na)', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ status: 'needs_action', kind: 'not_started', state: 'not_started', capture_mode: 'append', detail: null }))), ['Resync *', 'Start from log', 'Remove']);
});

test('an insert-only resync under ignore_deletes says purged rows stay only in the kept-aside table', () => {
  const r = fixOptions(t({ capture_mode: 'append', ignore_deletes: true }))[2];
  assert.equal(r.name, 'Resync');
  assert.match(r.cost, /Refused while the table is paused/);
  assert.match(r.cost, /they stay only in the kept-aside table\.$/);
  assert.doesNotMatch(fixOptions(t({ capture_mode: 'append' }))[2].cost, /purged/);
});

test('an insert-only table needing resync, or whose rebuild failed, can also start from the log', () => {
  const a = { capture_mode: 'append', status: 'needs_action' };
  assert.deepEqual(names(fixOptions(t({ ...a, kind: 'needs_resync', state: 'needs_resync' }))), ['Retry (na)', 'Quarantine (na)', 'Resync *', 'Start from log', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ ...a, kind: 'rebuild_failed', state: 'needs_resync' }))), ['Retry (na)', 'Quarantine (na)', 'Resync *', 'Start from log', 'Remove']);
  assert.deepEqual(names(fixOptions(t({ kind: 'needs_resync', status: 'needs_action', state: 'needs_resync' }))), ['Retry (na)', 'Quarantine (na)', 'Resync *', 'Remove']);
});

test('a keyed not-started table is never pointed at Start from log', () => {
  const keyed = fixOptions(t({ status: 'needs_action', kind: 'not_started', state: null, capture_mode: null, detail: 'public.t: no free replication slot or walsender; retry later' }));
  assert.deepEqual(names(keyed), ['Resync (na)', 'Remove']);
  assert.doesNotMatch(keyed[0].na ?? '', /from the log/);
  const append = fixOptions(t({ status: 'needs_action', kind: 'not_started', state: null, capture_mode: 'append', detail: null }));
  assert.match(append[0].na ?? '', /start it from the log/);
});
