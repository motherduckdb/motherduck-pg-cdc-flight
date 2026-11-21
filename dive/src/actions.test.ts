import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  ACTIONS,
  EXAMPLES,
  FLIGHT_KEYS,
  MESSAGES,
  PREVIEW_OFFER_MS,
  buildConfig,
  needsNewPreview,
  outcomeOf,
  reduce,
  typedConfirmation,
  type ActionName,
  type Event,
  type Phase,
} from './actions.ts';

const ID = '00000000-0000-4000-8000-000000000001';
const run = (events: Event[], start: Phase = { kind: 'idle' }) => events.reduce(reduce, start);

test('every config sends every declared key, blank when unused', () => {
  for (const action of Object.keys(ACTIONS) as ActionName[]) {
    const keys = FLIGHT_KEYS[ACTIONS[action].flight];
    const dry = buildConfig(action, EXAMPLES[action][0], { dryRun: true });
    assert.deepEqual(Object.keys(dry).sort(), [...keys].sort(), action);
    assert.equal(dry.CDC_ACTION, action);
    assert.equal(dry.CDC_DRY_RUN, '1');
    assert.equal(dry.CDC_PREVIEW_ID, '');
    const real = buildConfig(action, EXAMPLES[action][0], { previewId: ID });
    assert.equal(real.CDC_DRY_RUN, '');
    assert.equal(real.CDC_PREVIEW_ID, ID);
    assert.ok(Object.values(real).every((v) => typeof v === 'string'), action);
  }
});

test('only an action\'s own inputs are sent', () => {
  const pause = buildConfig('pause', { table: 't.a', expectedState: 'live', expectedBlockedLsn: '0/1', force: true }, { dryRun: true });
  assert.equal(pause.CDC_EXPECTED_BLOCKED_LSN, '');
  assert.equal(pause.CDC_EXPECTED_STATE, 'live');
  const retry = buildConfig('retry', { table: 't.a', expectedBlockedLsn: 'none', expectedState: 'needs_action' }, { dryRun: true });
  assert.equal(retry.CDC_EXPECTED_BLOCKED_LSN, 'none');
  assert.equal(retry.CDC_EXPECTED_STATE, '');
  const all = buildConfig('resync-all', { table: 't.a' }, { dryRun: true });
  assert.equal(all.CDC_TABLE, '');
  const remove = buildConfig('remove', { table: 't.a', force: false }, { dryRun: true });
  assert.equal(remove.CDC_FORCE, '');
  assert.equal(buildConfig('remove', { table: 't.a', force: true, confirm: 'REMOVE t.a' }, { dryRun: true }).CDC_FORCE, '1');
});

test('typed confirmation: forced remove exactly; resync except from needs_resync', () => {
  assert.equal(typedConfirmation('remove', { table: 't.a', force: true }), 'REMOVE t.a');
  assert.equal(typedConfirmation('remove', { table: 't.a' }), null);
  for (const state of ['live', 'paused', 'needs_action']) {
    assert.equal(typedConfirmation('resync', { table: 't.a', expectedState: state }), 't.a');
  }
  assert.equal(typedConfirmation('resync', { table: 't.a', expectedState: 'needs_resync' }), null);
  assert.equal(typedConfirmation('pause', { table: 't.a', expectedState: 'live' }), null);
  assert.equal(typedConfirmation('resync-all', {}), null);
});

test('exit codes drive the outcome', () => {
  assert.equal(outcomeOf('SUCCEEDED', 0), 'ok');
  assert.equal(outcomeOf('FAILED', 2), 'refused');
  assert.equal(outcomeOf('FAILED', 3), 'busy');
  assert.equal(outcomeOf('FAILED', 1), 'failed');
  assert.equal(outcomeOf('CANCELLED', null), 'cancelled');
  assert.equal(outcomeOf('RUNNING', null), null);
  assert.equal(outcomeOf(undefined, undefined), null);
});

test('only a would row enables Confirm; already and a missing row do not', () => {
  const done = [{ type: 'preview' }, { type: 'preview_done', runId: ID, outcome: 'ok', at: 0 }] as Event[];
  const ready = run([...done, { type: 'row', result: { status: 'would', detail: 'pause' } }]);
  assert.equal(ready.kind, 'ready');
  const already = run([...done, { type: 'row', result: { status: 'already', detail: 'already live' } }]);
  assert.ok(already.kind === 'finished' && already.message === MESSAGES.nothing);
  const missing = run([...done, { type: 'row', result: null }]);
  assert.ok(missing.kind === 'finished' && missing.message === MESSAGES.notRecorded);
  // Confirm is ignored until a would row arrives.
  assert.equal(run([...done, { type: 'confirm', at: 1 }]).kind, 'awaiting_row');
});

test('a refused dry run with no row says details were not recorded', () => {
  const refused = run([{ type: 'preview' }, { type: 'preview_done', runId: ID, outcome: 'refused', at: 0 }]);
  assert.ok(refused.kind === 'finished' && refused.outcome === 'refused' && refused.message === MESSAGES.noDetails);
  const busy = run([{ type: 'preview' }, { type: 'preview_done', runId: ID, outcome: 'busy', at: 0 }]);
  assert.ok(busy.kind === 'finished' && busy.message === MESSAGES.busy);
});

test('the real run carries the preview id; refusal leads to a new preview', () => {
  const ready = run([
    { type: 'preview' },
    { type: 'preview_done', runId: ID, outcome: 'ok', at: 0 },
    { type: 'row', result: { status: 'would' } },
  ]);
  const running = reduce(ready, { type: 'confirm', at: 60_000 });
  assert.deepEqual(running, { kind: 'running', previewId: ID });
  // A second confirm (double click) changes nothing.
  assert.deepEqual(reduce(running, { type: 'confirm', at: 60_001 }), running);
  // Another viewer spent the table's state first: refused, so preview again.
  const refused = reduce(running, { type: 'run_done', outcome: 'refused', result: { status: 'refused', detail: 'no matching unused preview' } });
  assert.ok(needsNewPreview(refused));
  const done = reduce(running, { type: 'run_done', outcome: 'ok', result: { status: 'done', detail: 'paused' } });
  assert.ok(done.kind === 'finished' && done.outcome === 'ok' && !needsNewPreview(done));
});

test('an old preview is not offered: expired, preview again', () => {
  const ready = run([
    { type: 'preview' },
    { type: 'preview_done', runId: ID, outcome: 'ok', at: 0 },
    { type: 'row', result: { status: 'would' } },
  ]);
  const late = reduce(ready, { type: 'confirm', at: PREVIEW_OFFER_MS });
  assert.ok(late.kind === 'finished' && late.message === MESSAGES.expired);
  assert.equal(reduce(ready, { type: 'confirm', at: PREVIEW_OFFER_MS - 1 }).kind, 'running');
});

test('a new preview starts only from idle or a finished step', () => {
  assert.equal(reduce({ kind: 'running', previewId: ID }, { type: 'preview' }).kind, 'running');
  assert.equal(reduce({ kind: 'finished', step: 'run', outcome: 'ok', result: null, message: '' }, { type: 'preview' }).kind, 'previewing');
});

test('a refused dry run shows its audit row\'s detail when recorded (demo finding)', () => {
  const refused = run([
    { type: 'preview' },
    { type: 'preview_done', runId: ID, outcome: 'refused', at: 0, result: { status: 'refused', detail: 'state changed: expected live, found paused' } },
  ]);
  assert.ok(refused.kind === 'finished' && refused.message === 'state changed: expected live, found paused');
});

// ── Typed-first actions ────────────────────────────────────────────────────────────────
import { TYPED_FIRST, continuable, flightLiteral, lifecycleBlocks, normalizeLsn } from './actions.ts';

test('each typed-first action\'s literal is the one its examples send (checked against the Flights in Python)', () => {
  for (const action of TYPED_FIRST) {
    for (const inputs of EXAMPLES[action]) {
      assert.equal(flightLiteral(action, inputs, 'phase2'), inputs.confirm, action);
    }
  }
  assert.equal(flightLiteral('skip-to-lsn', { expectedEpoch: 1, toLsn: 'nope' }, 's'), null);
  assert.equal(flightLiteral('recover-slot', {}, 's'), null);
});

test('LSNs are normalized as the Flights write them', () => {
  assert.equal(normalizeLsn('0/5a'), '0/5A');
  assert.equal(normalizeLsn(' 00A/000001 '), 'A/1');
  assert.equal(normalizeLsn('5A'), null);
  assert.equal(normalizeLsn('0/5G'), null);
});

test('only a deleting delete and an epoch repeat continue to Finish', () => {
  assert.ok(continuable('delete-source', { status: 'already', lifecycle: 'deleting' }));
  assert.ok(!continuable('delete-source', { status: 'already', lifecycle: 'deleted' }));
  // Finish recovery's own dry run returns the repeat path's `already`: it continues.
  assert.ok(continuable('recover-slot', { status: 'already' }));
  assert.ok(continuable('skip-to-lsn', { status: 'already' }));
  assert.ok(!continuable('pause', { status: 'already' }));
  assert.ok(!continuable('recover-slot', { status: 'would' }));
  const done = [{ type: 'preview' }, { type: 'preview_done', runId: ID, outcome: 'ok', at: 0 }] as Event[];
  const cont = run([...done, { type: 'row', result: { status: 'already', lifecycle: 'deleting' }, continuable: true }]);
  assert.equal(cont.kind, 'continue');
  assert.deepEqual(reduce(cont, { type: 'finish' }), { kind: 'running', previewId: ID });
  const nothing = run([...done, { type: 'row', result: { status: 'already' }, continuable: false }]);
  assert.ok(nothing.kind === 'finished' && nothing.message === MESSAGES.nothing);
});

test('waiting: a dry run asks to preview again later; a real run offers Run again', () => {
  const done = [{ type: 'preview' }, { type: 'preview_done', runId: ID, outcome: 'ok', at: 0 }] as Event[];
  const dry = run([...done, { type: 'row', result: { status: 'waiting', detail: 'ingest holds the lease; retry' } }]);
  assert.ok(dry.kind === 'finished' && dry.outcome === 'waiting' && dry.message.includes(MESSAGES.waitingPreview));
  const running: Phase = { kind: 'running', previewId: ID };
  const waiting = reduce(running, { type: 'run_done', outcome: 'ok', result: { status: 'waiting', detail: 'a reader holds the slot' } });
  assert.ok(waiting.kind === 'finished' && waiting.outcome === 'waiting' && waiting.previewId === ID);
  assert.ok(!needsNewPreview(waiting));
  assert.deepEqual(reduce(waiting, { type: 'again' }), { kind: 'running', previewId: ID });
  // A spent preview then comes back refused: preview again.
  const refused = reduce({ kind: 'running', previewId: ID }, { type: 'run_done', outcome: 'refused', result: { status: 'refused' } });
  assert.ok(needsNewPreview(refused));
});

test('a deleted or deleting source disables its actions, except finishing the delete', () => {
  assert.equal(lifecycleBlocks('pause', undefined), null);
  assert.ok(lifecycleBlocks('pause', 'deleting'));
  assert.ok(lifecycleBlocks('recover-slot', 'deleting_or_deleted'));
  assert.equal(lifecycleBlocks('delete-source', 'deleting'), null);
  assert.equal(lifecycleBlocks('delete-source', 'deleting_or_deleted'), null);
  assert.ok(lifecycleBlocks('delete-source', 'deleted'));
});

test('a typed-first real run that exits 0 without its row is not called Done', () => {
  const running: Phase = { kind: 'running', previewId: ID };
  const unknown = reduce(running, { type: 'run_done', outcome: 'ok', result: null, exitZeroAmbiguous: true });
  assert.ok(unknown.kind === 'finished' && unknown.message === MESSAGES.unrecorded);
  const p7b = reduce(running, { type: 'run_done', outcome: 'ok', result: null });
  assert.ok(p7b.kind === 'finished' && p7b.message === 'Done.');
});

test('every run sends its own request id, in the Flights\' format', async () => {
  const { REQUEST_ID, newRequestId } = await import('./actions.ts');
  const a = newRequestId();
  const b = newRequestId();
  assert.match(a, REQUEST_ID);
  assert.notEqual(a, b);
  const dry = buildConfig('pause', { table: 't.a', expectedState: 'live' }, { dryRun: true, requestId: a });
  assert.equal(dry.CDC_REQUEST_ID, a);
  assert.equal(buildConfig('pause', { table: 't.a', expectedState: 'live' }, { dryRun: true }).CDC_REQUEST_ID, '');
  assert.throws(() => buildConfig('pause', { table: 't.a' }, { dryRun: true, requestId: 'bad id' }), /not a request id/);
  assert.throws(() => buildConfig('pause', { table: 't.a' }, { dryRun: true, requestId: 'x'.repeat(65) }), /not a request id/);
});
