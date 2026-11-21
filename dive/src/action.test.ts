import { test } from 'node:test';
import assert from 'node:assert/strict';
import { auditNext, consequence, deleteLists, disabledReason, EXPIRED_NOTE, plainReason, panelInputs, previewTimedOut, readyButtons, observedFacts, panelStep, resultView } from './panel.ts';

const spec = { action: 'retry' as const, verb: 'retry' };

test('results read in the copy round’s words', () => {
  const pause = { action: 'pause' as const, verb: 'pause', inputs: { table: 'public.accounts' } };
  const done = resultView({ kind: 'finished', step: 'run', outcome: 'ok', result: { status: 'done', detail: '' }, message: '' }, pause);
  assert.deepEqual([done.title, done.text, done.next, done.again], ['Pause Completed', 'Updates to this table have been paused. Changelogs are still being ingested into MotherDuck, and will be applied to the replica table upon Resume.', '', false]);
  const started = resultView({ kind: 'finished', step: 'run', outcome: 'ok', result: { status: 'done', triggered: 'backfill run 4' }, message: '' }, { action: 'resync', verb: 'resync', inputs: { table: 'public.accounts' } });
  assert.deepEqual([started.title, started.text], ['Resync Started', 'The replica table is being rebuilt from a fresh snapshot (backfill run 4).']);
  const busy = resultView({ kind: 'finished', step: 'run', outcome: 'busy', result: null, message: '' }, pause);
  assert.deepEqual([busy.title, busy.text, busy.again], ['Blocked By Another Operation', 'Conflicting action running on this table. Please try again in a minute.', true]);
  const sourceBusy = resultView({ kind: 'finished', step: 'run', outcome: 'waiting', result: { status: 'waiting' }, message: '', previewId: 'p' }, { action: 'recover-slot', verb: 'recover slot' });
  assert.equal(sourceBusy.title, 'Blocked By Another Operation');
  const changed = resultView({ kind: 'finished', step: 'run', outcome: 'refused', result: { status: 'refused', detail: 'state changed' }, message: '' }, pause);
  assert.deepEqual([changed.title, changed.text], ['Action Rejected', 'Table state changed since preview. Preview the action again to continue.']);
  const already = resultView({ kind: 'finished', step: 'run', outcome: 'ok', result: { status: 'already', detail: 'x' }, message: '' }, pause);
  assert.deepEqual([already.title, already.text, already.next], ['Pause Completed', '', '']);
  const nothing = resultView({ kind: 'finished', step: 'preview', outcome: 'ok', result: { status: 'already' }, message: '' }, pause);
  assert.deepEqual([nothing.title, nothing.text], ['Preview Actions', 'This table is already paused.']);
  const prefused = resultView({ kind: 'finished', step: 'preview', outcome: 'refused', result: { status: 'refused', detail: 'public.accounts: it is paused: resume it instead' }, message: '' }, pause);
  assert.deepEqual([prefused.title, prefused.text], ['Action Rejected', 'Reason: This table is paused. Resume it first.']);
  const unknown = resultView({ kind: 'finished', step: 'run', outcome: 'ok', result: null, message: '' }, pause);
  assert.deepEqual([unknown.title, unknown.text], ['Result Unknown', 'Check status again in a few minutes.']);
  const none = resultView({ kind: 'finished', step: 'preview', outcome: 'ok', result: null, message: '' }, pause);
  assert.equal(none.title, 'No preview available');
  for (const v of [done, started, busy, changed, already, nothing, prefused, unknown, none]) assert.doesNotMatch(`${v.title} ${v.text} ${v.next}`, /Nothing was changed|usually under a minute|Status updates|Flight says|mirror/);
});

test('observed facts: words for keys, X/Y for LSNs', () => {
  assert.deepEqual(observedFacts({ state: 'needs_action', blocked_lsn: 4294967306, consistent: true, unit: { commits: 3 }, empty: null }), [
    ['State', 'needs_action'],
    ['Blocked lsn', '1/A'],
    ['Consistent', 'yes'],
    ['Unit', '{"commits":3}'],
  ]);
});

test('a delete that drops nothing itself still lists the DBA statements', () => {
  assert.deepEqual(deleteLists('[]', '["DROP PUBLICATION p;", "SELECT pg_drop_replication_slot(\'s\');"]'), {
    drops: [],
    dba: ['DROP PUBLICATION p;', "SELECT pg_drop_replication_slot('s');"],
  });
  assert.deepEqual(deleteLists('[{"kind":"slot","name":"s"}]', '[]'), { drops: ['slot s'], dba: [] });
  assert.equal(deleteLists(null, null), null); // any other action
});

test('a refusal with no audit row is the Flight rejecting its inputs, not a state refusal', () => {
  for (const step of ['preview', 'run'] as const) {
    const v = resultView({ kind: 'finished', step, outcome: 'refused', result: null, message: '' }, spec);
    assert.equal(v.title, 'Action Rejected');
    assert.match(v.text, /^Flight inputs were rejected/);
    assert.equal(v.again, true);
  }
});

test('Finish recovery completes under its own name', () => {
  const spec = { action: 'recover-slot' as const, verb: 'finish recovery', title: 'Finish recovery' };
  const v = resultView({ kind: 'finished', step: 'run', outcome: 'ok', result: { status: 'already', detail: 'old slot still has a reader' }, message: '' }, spec);
  assert.equal(v.title, 'Finish Recovery Completed');
  const done = resultView({ kind: 'finished', step: 'run', outcome: 'ok', result: { status: 'done' }, message: '' }, spec);
  assert.deepEqual([done.title, done.text], ['Finish Recovery Completed', 'The leftover slot has been dropped.']);
});

test('a finished preview stays on Preview until the viewer continues; Back returns to it (user report)', () => {
  assert.equal(panelStep('previewing', false), 0);
  assert.equal(panelStep('ready', false), 0); // the preview's result, on screen
  assert.equal(panelStep('ready', true), 1); // Continue to confirm
  assert.equal(panelStep('ready', false), 0); // Back
  assert.equal(panelStep('running', true), 2);
  assert.equal(panelStep('finished', true, 'run'), 3);
  assert.equal(panelStep('finished', false, 'preview'), 0);
});

test('a finished run waits for its audit row on a lagging share before "not recorded" (3 min)', () => {
  const wait = 3 * 60_000;
  assert.equal(auditNext(false, 5_000, false, wait), 'read_again'); // the share lags ~1 min
  assert.equal(auditNext(false, 90_000, false, wait), 'read_again');
  assert.equal(auditNext(true, 70_000, false, wait), 'settle'); // the row arrived
  assert.equal(auditNext(false, wait, false, wait), 'settle'); // gave up after 3 min
  assert.equal(auditNext(false, 10_000, true, wait), 'settle'); // Stop waiting
});

test('a finished dry run still waiting for its row never reads as timed out', () => {
  const limit = 3 * 60_000;
  // launched at 0, finished at 90 s, row not yet visible at 181 s: still waiting, not timed out
  assert.equal(previewTimedOut('previewing', 0, true, 181_000, limit), false);
  assert.equal(previewTimedOut('previewing', 0, false, 181_000, limit), true); // never finished
  assert.equal(previewTimedOut('previewing', 0, false, 60_000, limit), false);
  assert.equal(previewTimedOut('ready', 0, false, 181_000, limit), false);
});

test('the preview’s second line states only a new fact, in plain words (user)', () => {
  const none = (a: string, d: string, i: { force?: boolean } = {}) => assert.equal(consequence(a, i, d), null, a);
  none('resync', 'resync public.t: a full reload of the table from the source');
  none('resync', 'resync public.t: an exact snapshot of the source table replaces the mirror; the current mirror is kept aside as t__gen_1a2b3c4d');
  none('pause', 'pause: apply skips it until resumed');
  none('compact-now', "compact now with the owner's settings");
  none('remove', 'remove public.t: stop capturing it and release its pins; the mirror table is kept');
  none('start-from-log', 'start the mirror from the log at the current position: only rows inserted from then on are captured');
  none('skip-to-lsn', 'advance md_cdc_s_e1 to 1A/80 and begin epoch 2: everything from 1A/70 up to 1A/80 is never ingested');
  none('delete-source', 'delete source logs: stop capturing it; drop 2 object(s) this code created; 1 left for the DBA');
  assert.equal(
    consequence('remove', { force: true }, 'remove public.t: stop capturing it and release its pins; the mirror table is kept'),
    'Only re-adding the table and a full backfill undoes this. The replica table is kept as it is.',
  );
  assert.equal(
    consequence('quarantine', {}, 'quarantine: the whole unit 1A/7E..1A/7F, held on commit 1A/7E80 (KeyError); rows touched by unit 1A/7E..1A/7F (3 commits) in public.t may diverge until a resync'),
    'That batch holds 3 commits.',
  );
  assert.equal(
    consequence('quarantine', {}, 'quarantine: …; the inserts of unit 1A/7E..1A/7F (1 commits) in public.t are not in the mirror: a recorded gap until a resync (an exact re-snapshot)'),
    'That batch holds 1 commit; its inserts won’t be in the replica until a resync.',
  );
  assert.equal(
    consequence('recover-slot', {}, 'create md_cdc_s_e2 and begin epoch 2; WAL after 1A/7E000000 that the old slot lost is abandoned; every mirror then needs a resync'),
    'Changelogs after 1A/7E000000 that the old slot lost are never ingested into MotherDuck.',
  );
  assert.equal(
    consequence('resync', {}, 'resync public.t: an exact snapshot of the source table replaces the mirror; the current mirror is kept aside as t__gen_1a2b3c4d. DELETEs are skipped for this table, so rows the source has purged remain only in t__gen_1a2b3c4d'),
    'Rows the source deleted drop out of the replica table; they survive only in the kept-aside table t__gen_1a2b3c4d.',
  );
  // no Dive line ever says "mirror" or "pins"
  for (const a of ['quarantine', 'remove', 'recover-slot', 'resync'])
    assert.doesNotMatch(consequence(a, { force: true }, 'x (2 commits) in t are not in the mirror; WAL after 1/0 that the old slot lost; kept aside as k. DELETEs are skipped') ?? '', /mirror|pins/);
});

test('an expired preview can only be re-run, at Preview and at Confirm; no countdown (user)', () => {
  assert.deepEqual(readyButtons(false, false), ['continue']);
  assert.deepEqual(readyButtons(false, true), ['rerun']); // Continue to confirm gives way
  assert.deepEqual(readyButtons(true, false), ['back', 'run']);
  assert.deepEqual(readyButtons(true, true), ['back', 'rerun']); // Run gives way: never a stale preview
  assert.equal(EXPIRED_NOTE, 'Preview expired, re-run preview to continue.');
});

test('one helper decides what a panel sends and whether it may preview', () => {
  const q = { action: 'quarantine' as const, source: 'logs', inputs: { table: 'public.accounts', expectedEpoch: 1, unitStart: '1A/7E', unitEnd: '1A/7F' } };
  const lit = 'QUARANTINE public.accounts UNIT 1 1A/7E..1A/7F';
  assert.deepEqual([panelInputs(q, '', '').literal, panelInputs(q, '', '').literalOk], [lit, false]);
  const ok = panelInputs(q, lit, '');
  assert.deepEqual([ok.literalOk, ok.inputs.confirm], [true, lit]);
  const pause = panelInputs({ action: 'pause', source: 'logs', inputs: { table: 'public.accounts', expectedState: 'live' } }, '', '');
  assert.deepEqual([pause.typedFirst, pause.literal, pause.literalOk], [false, null, true]);
});

test('a disabled action button says why, in a sentence (user)', () => {
  assert.equal(disabledReason('source deleting'), 'Not available while the source is being deleted.');
  assert.equal(disabledReason('source deleting or deleted'), 'Not available: the source is being deleted or is deleted.');
  assert.equal(disabledReason('Another action is running.'), 'Another action is running.');
  assert.equal(disabledReason('No table-action Flight is set up for app.'), 'No table-action Flight is set up for app.');
});

test('Completed only when done: a leftover or kept slot says what is left', () => {
  const finish = { action: 'recover-slot' as const, verb: 'finish recovery', title: 'Finish recovery' };
  const left = resultView({ kind: 'finished', step: 'run', outcome: 'ok', message: '', result: { status: 'already', detail: 'epoch 2 already began', nextSteps: ['the previous slot md_cdc_logs_e1 still exists and retains WAL: run recover-slot again (it reports already and drops it once no reader is attached)'] } }, finish);
  assert.deepEqual([left.title, left.text], ['Old Slot Still In Use', 'The previous replication slot md_cdc_logs_e1 still has a reader and keeps retaining WAL. Run Finish recovery again once the reader stops.']);
  const kept = resultView({ kind: 'finished', step: 'run', outcome: 'ok', message: '', result: { status: 'already', nextSteps: ['slot md_cdc_logs_e2 was kept: run recover-slot again to finish'] } }, { action: 'recover-slot', verb: 'recover slot' });
  assert.deepEqual([kept.title, kept.text], ['Recovery Not Finished', 'Slot md_cdc_logs_e2 was kept. Run Recover Slot again to finish.']);
  const clean = resultView({ kind: 'finished', step: 'run', outcome: 'ok', message: '', result: { status: 'already', nextSteps: [] } }, finish);
  assert.equal(clean.title, 'Finish Recovery Completed');
});

test('waiting keeps its reason; the Flights’ real refusal texts read plainly', () => {
  const del = { action: 'delete-source' as const, verb: 'delete' };
  const w = resultView({ kind: 'finished', step: 'run', outcome: 'waiting', message: '', previewId: 'p', result: { status: 'waiting', detail: 'an ingest reader is still attached to md_cdc_logs_e1; it is refused from now on, so run delete-source again to finish' } }, del);
  assert.deepEqual([w.title, w.text], ['Blocked By Another Operation', 'Reason: A sync reader is still attached to slot md_cdc_logs_e1. Run it again once the reader stops.']);
  assert.equal(plainReason('replication slot is active; retry when its reader has stopped'), 'The replication slot still has an active reader. Run it again once the reader stops.');
  assert.equal(plainReason('state changed: expected epoch 1, found 2'), 'This source moved to epoch 2 since the preview.');
  assert.equal(plainReason('state changed: expected blocked unit 1A/7E..1A/7F, found 1A/80..1A/90'), 'The failed batch changed since the preview.');
  assert.equal(plainReason('state changed: expected blocked commit 1A/7E80, found none'), 'This table is no longer blocked.');
  assert.equal(plainReason('state changed: expected blocked commit 1A/7E80, found 1A/8000'), 'The failed batch changed since the preview.');
  assert.equal(plainReason('state changed: the blocked commit is 1A/8000 (KeyError)'), 'The failed batch changed since the preview.');
  assert.equal(plainReason('state changed: expected live, found paused'), 'This table’s state changed (it is now paused).');
  assert.equal(plainReason('a backfill is already running for this table'), 'A rebuild is already running for this table.');
  assert.equal(plainReason('public.t: already has an active backfill'), 'A rebuild is already running for this table.');
});
