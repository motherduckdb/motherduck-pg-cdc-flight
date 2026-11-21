// The copy gallery: every action panel stage and result, the drawer option cards and the
// Danger zone cards, rendered side by side from the REAL components (PanelView, OptionCard,
// DangerCard, ActionButton) with forced states, for reviewing copy. Dev only: served from
// dive/gallery.html; never imported from src/, so the Dive bundle (src/index.tsx) never
// carries it (src/gallery.test.ts checks).
//
// States come from the real reducer (actions.reduce) fed the events a run produces; specs
// come from the real builders (fixOptions, dangerCards, finishRecovery, resyncNextSpec,
// compactSpec). Labels under each snapshot are the string IDs the copy uses.
import { useEffect, useRef, type ReactNode } from 'react';
import { ActionButton, ActionContext, PanelView, type ActionSpec, type PanelHandlers, type PanelState, type Row, type Started } from '../src/action';
import { flightLiteral, reduce, typedConfirmation, type AuditResult, type Event, type Outcome, type Phase } from '../src/actions';
import { finishRecovery, fixOptions } from '../src/fixes';
import type { SourceRow, TableRow } from '../src/model';
import { OptionCard } from '../src/screens/drawer';
import { compactSpec, DangerCard, dangerCards, resyncNextSpec } from '../src/screens/source';
import { caps, CSS, INK, MONO, MUTED, PAGE } from '../src/theme';
import { doneText, NAME, runningTitle } from '../src/panel';

const NOW = Date.now();
const MIN = 60_000;
const FLIGHT = '00000000-0000-0000-0000-000000000001';
const OWNER = 'alice';

// ── Example data ───────────────────────────────────────────────────────────────────────

const table = (o: Partial<TableRow>): TableRow => ({
  source_id: 'logs', table_name: 'public.accounts', status: 'ok', kind: 'ok', state: 'live', detail: null, behind_s: 0,
  expires_in_s: null, expires_at_s: null, since_at_s: null, held_for_s: null, actor: null, actor_action: null,
  chunks: null, chunks_total: null, generation_id: null, gen_state: null, updated_at_s: null, durable_at_s: null,
  epoch: 1, source_epoch: 1, applied_lsn: null, checkpoint_lsn: null, blocked_commit_lsn: null, blocked_unit: null,
  pin_lsn: null, pin_moved_at_s: null, quarantines: 0, changes_7d: 0,
  reason: null, capture_mode: null, ignore_deletes: null, deletes_ignored: null, last_truncate_lsn: null,
  log_start_lsn: null, kept_aside: null, rebuild_error: null, refused_detail: null, waits_for_ingest: null, ...o,
});
const T = {
  live: table({}),
  paused: table({ kind: 'paused', status: 'paused', state: 'paused' }),
  blocked: table({ kind: 'blocked', status: 'needs_action', state: 'needs_action', detail: 'KeyError', blocked_commit_lsn: '1A/7E80', blocked_unit: '1A/7E..1A/7F' }),
  held: table({ kind: 'registry', status: 'needs_action', state: 'live', detail: 'dropped: the source table was dropped' }),
  notEnabled: table({ kind: 'not_enabled', status: 'not_enabled', state: null }),
  insertChanged: table({ kind: 'append_changed', status: 'needs_action', state: 'needs_action', capture_mode: 'append', reason: 'append_update', blocked_commit_lsn: '1A/7E80', blocked_unit: '1A/7E..1A/7F' }),
  insertLive: table({ capture_mode: 'append', ignore_deletes: true }),
  needsResync: table({ kind: 'needs_resync', status: 'needs_action', state: 'needs_resync' }),
  rebuilding: table({ kind: 'rebuilding', status: 'rebuilding', state: 'needs_resync', chunks: 14 }),
  rebuildFailed: table({ kind: 'rebuild_failed', status: 'needs_action', state: 'needs_resync' }),
  typeChanged: table({ kind: 'type_changed', status: 'needs_action', state: 'needs_action', reason: 'type_changed', blocked_commit_lsn: '1A/7E80' }),
  notStartedKeyed: table({ kind: 'not_started', status: 'needs_action', state: null, detail: 'public.accounts: no free replication slot or walsender; retry later' }),
  notStartedOver: table({ kind: 'not_started', status: 'needs_action', state: null, capture_mode: 'append', detail: 'public.accounts: is 5368709120 bytes, over the append snapshot limit 1073741824 (CDC_APPEND_SNAPSHOT_MAX_BYTES): use start-from-log, or raise the limit' }),
  insertInvalid: table({ kind: 'append_invalid', status: 'needs_action', state: null, detail: 'public.accounts: append mode needs REPLICA IDENTITY FULL' }),
  unknown: table({ kind: 'unknown', status: 'unknown', state: 'odd' }),
};
const SOURCE = { source_id: 'logs', epoch: 1 } as unknown as SourceRow;
const opt = (t: TableRow, name: string): ActionSpec => fixOptions(t).find((o) => o.name === name)!.spec!;
const danger = Object.fromEntries(dangerCards(SOURCE, true).map((d) => [d.name, d.spec!]));

// ── One gallery entry per action ────────────────────────────────────────────────────────

type Entry = {
  code: string; // the card-ID code, fixed (G-<code>-<nn>)
  id: string; // the inventory prefix
  name: string;
  opener: string; // the button's label and where it is
  spec: ActionSpec;
  would: string; // the Flight's dry-run detail (realistic)
  observed?: Record<string, unknown>;
  lists?: { drops: string[]; dba: string[] };
  done: string; // the Flight's done detail
  triggered?: string;
  remaining?: number;
  continueRow?: AuditResult; // the "needs a real run to finish" preview
  drain?: boolean;
  /** stage numbers this action can never reach (from the Flights' exit codes and statuses) */
  unreachable?: number[];
  skipTo?: string;
};

const ENTRIES: Entry[] = [
  { code: 'PAUSE', unreachable: [17], id: 'pause', name: 'Pause', opener: 'Drawer (live table) → Pause card → “Preview pause”', spec: opt(T.live, 'Pause'), would: 'pause: apply skips it until resumed', done: 'pause: done' },
  { code: 'RESUME', unreachable: [17], id: 'resume', name: 'Resume', opener: 'Drawer (paused) → Resume card → “Preview resume”; row button “Resume”', spec: opt(T.paused, 'Resume'), would: 'resume: apply continues from its checkpoint', done: 'resume: done' },
  { code: 'RETRY', unreachable: [17], id: 'retry', name: 'Retry', opener: 'Drawer (blocked) → Retry card → “Preview retry”; row button “Fix”', spec: opt(T.blocked, 'Retry'), would: 'retry: apply re-reads from its checkpoint, replaying the whole unit 1A/7E..1A/7F', done: 'retry: done' },
  { code: 'RESYNC', unreachable: [17], id: 'resync', name: 'Resync (keyed, blocked table)', opener: 'Drawer → Resync card → “Preview resync”', spec: opt(T.blocked, 'Resync'), would: 'resync public.accounts: replaces the held mirror table with a full reload from the source', done: 'resync public.accounts: started', triggered: 'backfill run 12' },
  { code: 'RESYNC-IO', unreachable: [17], id: 'resync', name: 'Resync (insert-only, DELETEs skipped)', opener: 'Drawer → Resync card → “Preview resync”', spec: opt(T.insertLive, 'Resync'), would: 'resync public.accounts: an exact snapshot of the source table replaces the mirror; the current mirror is kept aside as accounts__gen_3f9c2a7b. DELETEs are skipped for this table, so rows the source has purged remain only in accounts__gen_3f9c2a7b', done: 'resync public.accounts: started', triggered: 'backfill run 12' },
  { code: 'RESYNCNEXT', unreachable: [17], id: 'resyncnext', name: 'Resync next queued table', opener: 'Source page → Recovery → “Resync next queued table”', spec: resyncNextSpec('logs'), would: 'resync public.accounts (longest held): a full reload of the table from the source', done: 'resync public.accounts: started', triggered: 'backfill run 12', remaining: 2 },
  { code: 'REMOVE', unreachable: [17], id: 'remove', name: 'Remove (a table the source holds)', opener: 'Drawer (held) → Remove card → “Preview remove”', spec: opt(T.held, 'Remove'), would: 'remove public.accounts: stop capturing it and release its pins; the mirror table is kept', done: 'remove public.accounts: done' },
  { code: 'REMOVE-F', unreachable: [17], id: 'remove', name: 'Remove (forced)', opener: 'Drawer (live) → Remove card → “Preview remove”', spec: opt(T.live, 'Remove'), would: 'remove public.accounts: stop capturing it and release its pins; the mirror table is kept', done: 'remove public.accounts: done' },
  { code: 'ENABLE', unreachable: [17], id: 'add', name: 'Enable', opener: 'Source page → Not enabled row → “Enable”; drawer → Enable card', spec: opt(T.notEnabled, 'Enable'), would: 'register public.accounts, then rebuild it from a snapshot', done: 'add public.accounts: started', triggered: 'backfill run 12', drain: true },
  { code: 'QUARANTINE', unreachable: [17], id: 'quarantine', name: 'Quarantine', opener: 'Drawer (blocked) → Quarantine card → “Preview quarantine”', spec: opt(T.blocked, 'Quarantine'), would: "quarantine: the whole unit 1A/7E..1A/7F (3 commits), held on commit 1A/7E80 (KeyError: 'id'); rows touched by unit 1A/7E..1A/7F (3 commits) in public.accounts may diverge until a resync", done: 'quarantine: done', observed: { state: 'needs_action', unit_commit_count: 3 } },
  { code: 'SFL', unreachable: [17], id: 'sfl', name: 'Start from log', opener: 'Drawer (insert-only hold) → Start from log card → “Preview start from log”', spec: opt(T.insertChanged, 'Start from log'), would: 'start the mirror from the log at the current position: only rows inserted from then on are captured (rows already in the table are not copied); it replaces the current generation (held): its mirror table is renamed aside, rows kept', done: 'started from the log at epoch 1, lsn 1A/80000000: rows inserted before it are not in the mirror; the previous mirror is kept as public.accounts__gen_3f9c2a7b' },
  { code: 'COMPACT', unreachable: [14, 17], id: 'compact', name: 'Compact now', opener: 'Source page → Change log retention → “Compact now”', spec: compactSpec('logs'), would: "compact now with the owner's settings", done: 'compact: started', triggered: 'compact run 7' },
  { code: 'RECOVER', unreachable: [16, 23], id: 'recover', name: 'Recover slot', opener: 'Source page → Danger zone → Recover slot → “Preview recover slot” (Overview: “Recover slot”, banner: “Go to Recover slot”)', spec: danger['Recover slot'], would: 'create md_cdc_logs_e2 and begin epoch 2; WAL after 1A/7E000000 that the old slot lost is abandoned; every mirror then needs a resync', done: 'epoch 2 begun', observed: { new_slot: 'md_cdc_logs_e2', epoch: 1 }, continueRow: { status: 'already', detail: 'epoch 2 is begun; the old slot md_cdc_logs_e1 is still to drop', nextSteps: ['run it again to drop md_cdc_logs_e1'] } },
  { code: 'FINISH', unreachable: [16, 23], id: 'finish', name: 'Finish recovery', opener: 'Source page → Slots → leftover slot → “Finish recovery”', spec: finishRecovery('logs', 2, null)!, would: 'repeat the recover-slot that began epoch 2: drop the leftover slot md_cdc_logs_e1', done: 'dropped md_cdc_logs_e1' },
  { code: 'SKIP', unreachable: [16, 23], id: 'skip', name: 'Skip to LSN', opener: 'Source page → Danger zone → Skip to LSN → “Preview skip”', spec: danger['Skip to LSN'], would: 'advance md_cdc_logs_e1 to 1A/80000000 and begin epoch 2: everything from 1A/7E000000 up to 1A/80000000 is never ingested', done: 'epoch 2 begun', skipTo: '1A/80000000', observed: { slot: 'md_cdc_logs_e1', skipped_bytes: 33554432 }, continueRow: { status: 'already', detail: 'epoch 2 is begun; the old slot md_cdc_logs_e1 is still to drop', nextSteps: ['run it again to drop md_cdc_logs_e1'] } },
  { code: 'DELETE', unreachable: [16, 23], id: 'delete', name: 'Delete source', opener: 'Source page → Danger zone → Delete source → “Preview delete”', spec: danger['Delete source'], would: 'delete source logs: stop capturing it; drop 2 object(s) this code created; 1 left for the DBA; mirrors and the log are kept', done: 'source logs deleted', lists: { drops: ['publication md_cdc_logs_pub', 'role md_cdc_logs_ingest'], dba: ['DROP ROLE md_cdc_logs_owner;'] }, continueRow: { status: 'already', lifecycle: 'deleting', detail: 'the source is being deleted; its slot is still to drop', nextSteps: ['run delete-source again to finish'] } },
];

// ── States, through the real reducer ───────────────────────────────────────────────────

const through = (events: Event[]): Phase => events.reduce<Phase>(reduce, { kind: 'idle' });
const started = (step: 'preview' | 'run', agoMs: number): Started => ({ runNumber: step === 'preview' ? 12 : 13, runId: `run-${step}`, requestId: `req-${step}`, step, launchedAt: NOW - agoMs, config: {} });
const NONE: PanelHandlers = { close() {}, setTech() {}, setTyped() {}, setConfirmName() {}, setToLsn() {}, preview() {}, run() {}, finishRun() {}, runAgain() {}, stopWaiting() {}, setConfirming() {} };

/** `n` is the stage's fixed number in its card ID (G-<ACTION>-<n>): one number per stage
 * kind, never renumbered; a new stage gets the next unused number (01 is the button). */
type Snap = { n: number; label: string; ids: string[]; state: PanelState };

function snaps(e: Entry): Snap[] {
  const literal = flightLiteral(e.spec.action, { ...e.spec.inputs, toLsn: e.skipTo ?? e.spec.inputs.toLsn }, e.spec.source);
  const typed = e.spec.inputs.confirm ?? literal ?? '';
  const base: PanelState = { spec: e.spec, flightId: FLIGHT, phase: { kind: 'idle' }, confirming: false, row: null, current: null, terminal: null, waiting: false, now: NOW, readyAt: null, typed, confirmName: '', toLsn: e.skipTo ?? '', startError: null, tech: false, startPending: false };
  const would: Row = { status: 'would', detail: e.would, observed: e.observed ?? null, lists: e.lists ?? null };
  const previewed = (atMs: number): Event[] => [{ type: 'preview' }, { type: 'preview_done', runId: 'run-preview', outcome: 'ok', at: atMs }, { type: 'row', result: would }];
  const ready = through(previewed(NOW - 30_000));
  const confirmed = [...previewed(NOW - 30_000), { type: 'confirm', at: NOW - 20_000 } as Event];
  const ran = (outcome: Outcome, result: AuditResult | null, ambiguous = false): PanelState => ({
    ...base, row: result as Row | null, current: started('run', 40_000), terminal: { outcome, doneAt: NOW - 10_000 },
    phase: through([...confirmed, { type: 'run_done', outcome, result, exitZeroAmbiguous: ambiguous }]),
  });
  const p = e.id;
  const nameNeeded = typedConfirmation(e.spec.action, e.spec.inputs);
  const out: Snap[] = [];
  if (literal && e.spec.inputs.confirm === undefined)
    out.push({ n: 2, label: 'Before the preview (typed first, text typed in)', ids: ['typed.would', 'typed.cost', 'typed.prompt', 'typed.btn', `${p}.typed`, `${p}.would`], state: { ...base } });
  out.push(
    { n: 3, label: 'Preview running (Cancel waits for it)', ids: ['preview.running.title', 'preview.running.body', 'panel.btn.cancel'], state: { ...base, phase: through([{ type: 'preview' }]), current: started('preview', 12_000) } },
    { n: 4, label: 'Preview Actions', ids: ['preview.header', 'preview.would', `${p}.would`, `${p}.consequence`, `${p}.cost`, 'preview.observed.title', 'preview.btn.continue'], state: { ...base, phase: ready, row: would } },
    { n: 5, label: 'Preview expired', ids: ['preview.expired', 'preview.btn.rerun'], state: { ...base, phase: through(previewed(NOW - 15 * MIN)), row: would } },
    { n: 6, label: nameNeeded ? 'Confirm (name typed in)' : 'Confirm', ids: ['confirm.title', 'confirm.would', 'confirm.consequence', 'confirm.cost', ...(nameNeeded ? ['confirm.prompt', `${p}.typed`] : []), 'confirm.btn.back', 'confirm.btn.run'], state: { ...base, phase: ready, row: would, confirming: true, readyAt: NOW - 5_000, confirmName: nameNeeded ?? '' } },
    { n: 7, label: 'Confirm, first 3 s (Run not enabled yet)', ids: ['confirm.countdown'], state: { ...base, phase: ready, row: would, confirming: true, readyAt: NOW - 1_000, confirmName: nameNeeded ?? '' } },
    { n: 8, label: 'Confirm, preview expired', ids: ['confirm.expired', 'confirm.btn.rerun'], state: { ...base, phase: through(previewed(NOW - 15 * MIN)), row: would, confirming: true, readyAt: NOW - 5_000, confirmName: nameNeeded ?? '' } },
    { n: 9, label: 'Running', ids: ['run.title', 'run.body'], state: { ...base, phase: through(confirmed), current: started('run', 8_000) } },
    { n: 10, label: 'Waiting for the result', ids: ['wait.title', 'wait.body', 'wait.btn.stop'], state: { ...base, phase: through(confirmed), current: started('run', 30_000), terminal: { outcome: 'ok', doneAt: NOW - 20_000 }, waiting: true } },
  );
  if (e.continueRow)
    out.push({ n: 11, label: 'Needs a real run to finish', ids: ['continue.title', 'continue.body', 'continue.btn', 'result.nextsteps'], state: { ...base, phase: through([{ type: 'preview' }, { type: 'preview_done', runId: 'run-preview', outcome: 'ok', at: NOW - 30_000 }, { type: 'row', result: e.continueRow, continuable: true }]), row: e.continueRow as Row } });
  out.push(
    e.triggered
      ? { n: 13, label: 'Result: started', ids: ['result.started', `${p}.started`, ...(e.remaining ? ['result.remaining'] : [])], state: ran('ok', { status: 'done', detail: e.done, triggered: e.triggered, remaining: e.remaining ?? null }) }
      : { n: 12, label: 'Result: done', ids: ['result.done', `${p}.done`], state: ran('ok', { status: 'done', detail: e.done }) },
    {
      n: 14,
      label: e.id === 'finish' ? 'Result: already, the old slot still in use' : e.id === 'recover' ? 'Result: already, a slot kept' : 'Result: already',
      ids: ['result.already'],
      state: ran('ok', {
        status: 'already',
        detail: e.id === 'finish' || e.id === 'recover' ? 'epoch 2 already began' : undefined,
        nextSteps:
          e.id === 'finish' ? ['the previous slot md_cdc_logs_e1 still exists and retains WAL: run recover-slot again (it reports already and drops it once no reader is attached)']
          : e.id === 'recover' ? ['slot md_cdc_logs_e2 was kept: run recover-slot again to finish']
          : [],
      }),
    },
    { n: 15, label: 'Result: refused (changed since the preview)', ids: ['result.refused.stale'], state: ran('refused', { status: 'refused', detail: 'state changed: expected live, found paused' }) },
    { n: 16, label: 'Result: busy', ids: ['result.busy'], state: ran('busy', null) },
    { n: 17, label: 'Result: waiting (a reader on the slot)', ids: ['result.waiting', 'result.btn.again'], state: ran('ok', { status: 'waiting', detail: e.id === 'delete' ? 'an ingest reader is still attached to md_cdc_logs_e1; it is refused from now on, so run delete-source again to finish' : 'replication slot is active; retry when its reader has stopped' }) },
    ...(e.drain ? [{ n: 18, label: 'Result: an open batch first', ids: ['result.drain'], state: ran('refused', { status: 'drain_required', detail: 'an open unit must drain first' }) }] : []),
    { n: 19, label: 'Result: failed', ids: ['result.failed'], state: ran('failed', { status: 'failed', detail: 'OperationalError: connection reset' }) },
    { n: 20, label: 'Result: not recorded', ids: ['result.notrecorded'], state: ran('ok', null, true) },
    { n: 21, label: 'Result: rejected inputs', ids: ['result.rejected'], state: ran('refused', null) },
    { n: 22, label: 'Result: cancelled', ids: ['result.cancelled'], state: ran('cancelled', null) },
    { n: 23, label: 'Preview: nothing to do', ids: ['presult.nothing', 'presult.btn'], state: { ...base, phase: through([{ type: 'preview' }, { type: 'preview_done', runId: 'run-preview', outcome: 'ok', at: NOW }, { type: 'row', result: { status: 'already', detail: 'already in that state' } }]) } },
    { n: 24, label: 'Preview: refused', ids: ['presult.refused'], state: { ...base, phase: through([{ type: 'preview' }, { type: 'preview_done', runId: 'run-preview', outcome: 'refused', at: NOW, result: { status: 'refused', detail: 'the table is paused: resume it first' } }]) } },
    { n: 25, label: 'Preview: failed', ids: ['presult.failed'], state: { ...base, phase: through([{ type: 'preview' }, { type: 'preview_done', runId: 'run-preview', outcome: 'failed', at: NOW, result: { status: 'failed', detail: 'OperationalError: connection reset' } }]) } },
    { n: 26, label: 'Preview: none recorded', ids: ['presult.none'], state: { ...base, phase: through([{ type: 'preview' }, { type: 'preview_done', runId: 'run-preview', outcome: 'ok', at: NOW }, { type: 'row', result: null }]) } },
    { n: 27, label: 'Preview: no result after 3 min (Cancel waits for it)', ids: ['preview.timeout.title', 'preview.timeout.body'], state: { ...base, phase: through([{ type: 'preview' }]), current: started('preview', 4 * MIN) } },
    { n: 28, label: 'Run details open (on the done result)', ids: ['details.*'], state: { ...ran('ok', { status: 'done', detail: e.done }), tech: true } },
  );
  return out.filter((x) => !e.unreachable?.includes(x.n));
}

// ── Page ────────────────────────────────────────────────────────────────────────────────

const OPEN = { open() {}, blocked: () => null, readOnly: null };

// ── Changed copy: marked in yellow, so a reviewer reads only the changes ──────────────────
/** Whole texts that are new in this round (exact match). */
const CHANGED_EXACT = new Set([
  'Preview Actions', 'Impact:', 'Continue', 'Run', 'No preview available', 'Finish Required', 'Action Rejected',
  'Blocked By Another Operation', 'Result Unknown', 'Preview Failed', 'Not available', 'Old Slot Still In Use', 'Recovery Not Finished',
  ...Object.values(NAME).flatMap((n) => [`${n} Completed`, `${n} Started`, `Confirm ${n}`, `${n} completed, fetching result`]),
  'Finish Recovery Completed', 'Confirm Finish Recovery', 'Finish Recovery completed, fetching result', 'Finishing recovery',
]);
/** Sentences new in this round (the text contains one). */
const CHANGED_PART = [
  'Generating preview', 'Preview timed out after 3 minutes', 'to preview this action.', 'to confirm.', 'Run it to finish.',
  'Conflicting action running on this', 'state changed since preview', 'Flight inputs were rejected', 'Check status again in a few minutes',
  'An open batch of changelogs must finish first', 'The preview ran but its result didn’t reach this page', 'Reason: ',
  'is already paused', 'is already updating', 'no longer blocked', 'already being rebuilt', 'already reads from the changelogs',
  'No table needs a resync', 'already enabled', 'already removed', 'already set aside', 'already deleted',
  'is being rebuilt from a fresh snapshot', 'is being built from a fresh snapshot', 'is being replaced with an exact snapshot', 'change log is being compacted',
  'more table', 'Changelogs after', 'drop out of the replica table', 'may be wrong or missing in the replica table', 'appears once its first snapshot finishes',
  'Not available while the source', 'Not available: the source',
  // later changes
  'still has a reader and keeps retaining WAL', 'was kept. Run', 'A sync reader is still attached', 'still has an active reader',
  'moved to epoch', 'The failed batch changed since the preview', 'state changed (it is now', 'A rebuild is already running',
];
function changedTexts(): string[] {
  const out = new Set<string>();
  for (const e of ENTRIES) {
    out.add(e.spec.would);
    out.add(doneText(e.spec.action, e.spec.title));
    out.add(runningTitle(e.spec));
  }
  return [...out];
}
/** Wraps each changed text node inside the snapshots (never the card IDs or string-ID lines). */
function markChanges(root: HTMLElement | null) {
  if (!root) return;
  const whole = new Set([...CHANGED_EXACT, ...changedTexts()]);
  const parts = [...CHANGED_PART, ...changedTexts()];
  for (const panel of root.querySelectorAll('[data-snapshot]')) {
    const walker = document.createTreeWalker(panel, NodeFilter.SHOW_TEXT);
    const hits: Text[] = [];
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      const t = (n.textContent ?? '').trim();
      if (t && (whole.has(t) || parts.some((p) => t.includes(p)))) hits.push(n as Text);
    }
    for (const n of hits) {
      const m = document.createElement('mark');
      m.style.background = '#FFF176';
      m.style.color = 'inherit';
      n.replaceWith(m);
      m.appendChild(n);
    }
  }
}

function Ids({ ids }: { ids: string[] }) {
  return <p style={{ margin: '6px 2px 0', fontFamily: MONO, fontSize: 10.5, color: MUTED, lineHeight: 1.5 }}>{ids.join(' · ')}</p>;
}
function Shot({ cid, label, ids, children }: { cid: string; label: string; ids: string[]; children: ReactNode; width?: number }) {
  return (
    <figure id={cid} style={{ margin: 0, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 8, background: '#FFFFFF', border: `1.5px solid ${INK}`, borderRadius: 12, padding: 12 }}>
      <figcaption style={{ display: 'flex', gap: 10, alignItems: 'baseline', flexWrap: 'wrap' }}>
        <span style={{ fontFamily: MONO, fontSize: 15, fontWeight: 700, padding: '2px 8px', background: INK, color: '#FFFFFF', borderRadius: 6 }}>{cid}</span>
        <span style={{ ...caps(12, 0.12), fontWeight: 700 }}>{label}</span>
      </figcaption>
      <div data-snapshot="">{children}</div>
      <Ids ids={ids} />
    </figure>
  );
}
/** Stage cards wrap onto as many rows as needed: everything is visible without scrolling sideways. */
function Row_({ children, min = 520 }: { children: ReactNode; min?: number }) {
  return <div style={{ display: 'grid', gridTemplateColumns: `repeat(auto-fill, minmax(min(${min}px, 100%), 1fr))`, gap: 18, alignItems: 'start' }}>{children}</div>;
}

const CARD_ID: Record<string, string> = { Retry: 'card.retry', Quarantine: 'card.quarantine', Resync: 'card.resync', Resume: 'card.resume', Pause: 'card.pause', Remove: 'card.remove', 'Start from log': 'card.sfl', 'Accept types': 'card.accept', Enable: 'card.enable' };
// Card IDs: fixed codes and numbers, never renumbered (append new ones).
const CARD_STATES: [string, TableRow, string][] = [
  ['Live table (Options)', T.live, 'LIVE'], ['Paused', T.paused, 'PAUSED'], ['Blocked (a failed batch)', T.blocked, 'BLOCKED'], ['Held by the source', T.held, 'HELD'],
  ['Not enabled', T.notEnabled, 'NOTENABLED'], ['Needs resync', T.needsResync, 'NEEDSRESYNC'], ['Rebuilding', T.rebuilding, 'REBUILDING'], ['Rebuild failed', T.rebuildFailed, 'REBUILDFAILED'],
  ['Insert-only: a row changed', T.insertChanged, 'IOCHANGED'], ['Column type changed', T.typeChanged, 'TYPECHANGED'], ['Not started (keyed)', T.notStartedKeyed, 'NSKEYED'],
  ['Not started (insert-only, over the limit)', T.notStartedOver, 'NSOVER'], ['Can’t capture as insert-only', T.insertInvalid, 'IOINVALID'], ['Unknown state', T.unknown, 'UNKNOWN'],
];
const OPT_NO: Record<string, string> = { Retry: '01', Quarantine: '02', Resync: '03', Resume: '04', Pause: '05', Remove: '06', 'Start from log': '07', 'Accept types': '08', Enable: '09' };
const DANGER_NO: Record<string, string> = { 'Recover slot': '01', 'Skip to LSN': '02', 'Delete source': '03' };
const BLOCKED = [
  ['G-BTN-02', 'No Flight for this source', 'blocked.noflight', 'No table-action Flight is set up for logs.'],
  ['G-BTN-03', 'Source being deleted', 'blocked.lifecycle', 'source deleting'],
  ['G-BTN-04', 'Another action running', 'blocked.busy', 'Another action is running.'],
] as const;
export const cardId = (code: string, n: number) => `G-${code}-${String(n).padStart(2, '0')}`;

/** Every card: ID → section, title, string IDs (the index at the bottom). */
export function cardIndex(): [string, string, string, string][] {
  const rows: [string, string, string, string][] = [];
  for (const e of ENTRIES) {
    rows.push([cardId(e.code, 1), e.name, 'The button that opens it (owner view)', ['open.*', `${e.id}.verb / title`].join(', ')]);
    for (const s of snaps(e)) rows.push([cardId(e.code, s.n), e.name, s.label, s.ids.join(', ')]);
  }
  rows.push(['G-BTN-01', 'Buttons', 'Enabled (owner)', 'open.default']);
  for (const [cid, label, id] of BLOCKED) rows.push([cid, 'Buttons', `Disabled: ${label}`, id]);
  for (const [label, t, code] of CARD_STATES)
    for (const o of fixOptions(t)) rows.push([`G-CARD-${code}-${OPT_NO[o.name]}`, `Option cards: ${label}`, o.name, [CARD_ID[o.name] ?? 'card.*', ...(o.na ? ['na.*'] : [])].join(', ')]);
  for (const o of fixOptions(T.blocked)) rows.push([`G-CARD-RO-${OPT_NO[o.name]}`, 'Option cards: as a non-owner sees them', o.name, `${CARD_ID[o.name] ?? 'card.*'}, readonly.card`]);
  for (const [label, lost, code] of [['Slot lost', true, 'LOST'], ['Slot fine', false, 'OK']] as const)
    for (const d of dangerCards(SOURCE, lost)) rows.push([`G-DANGER-${code}-${DANGER_NO[d.name]}`, `Danger zone: ${label}`, d.name, d.name === 'Recover slot' ? 'danger.recover' : d.name === 'Skip to LSN' ? 'danger.skip' : 'danger.delete']);
  return rows;
}

export default function Gallery() {
  const main = useRef<HTMLElement>(null);
  useEffect(() => markChanges(main.current), []);
  return (
    <ActionContext.Provider value={OPEN}>
      <style>{CSS}</style>
      <main ref={main} style={{ padding: '28px 32px 80px', display: 'flex', flexDirection: 'column', gap: 36, background: PAGE, color: INK, fontFamily: 'system-ui, sans-serif' }}>
        <header style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <h1 style={{ margin: 0, ...caps(18, 0.12) }}>Action copy gallery</h1>
          <p style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Each action shows every stage below its button; nothing here is clickable.</p>
          <p style={{ margin: 0, fontSize: 14 }}><mark style={{ background: '#FFF176' }}>Yellow</mark> marks text changed in the latest copy round. Stages an action can never reach are left out.</p>
          <p style={{ margin: 0, fontSize: 14, maxWidth: 900 }}>
            Every action panel stage and result, the drawer option cards and the Danger zone cards, rendered by the Dive’s own components with forced states (dev only, never deployed).
            Example data: table public.accounts, source logs, owner alice, unit 1A/7E..1A/7F, 3 commits. The small grey IDs under each snapshot are the copy's string IDs.
          </p>
          <nav style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 14px', fontSize: 13 }}>
            {ENTRIES.map((e, i) => <a key={i} href={`#a${i}`} style={{ color: INK }}>{e.name}</a>)}
            <a href="#buttons" style={{ color: INK }}>Buttons</a>
            <a href="#cards" style={{ color: INK }}>Option cards</a>
            <a href="#danger" style={{ color: INK }}>Danger zone</a>
            <a href="#index" style={{ color: INK }}>Card ID index</a>
          </nav>
        </header>

        {ENTRIES.map((e, i) => (
          <section key={i} id={`a${i}`} style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <h2 style={{ margin: 0, ...caps(15, 0.12) }}>{e.name} <span style={{ fontFamily: MONO, fontSize: 12, color: MUTED, textTransform: 'none', letterSpacing: 0 }}>{e.id}.*</span></h2>
            <p style={{ margin: 0, fontSize: 13 }}>Opens from: {e.opener}</p>
            <Row_>
              <Shot cid={cardId(e.code, 1)} label="The button that opens it (owner view)" ids={['open.*', `${e.id}.verb / title`]}>
                <div style={{ padding: 16, background: PAGE, borderRadius: 10 }}><ActionButton spec={e.spec} /></div>
              </Shot>
              {snaps(e).map((s) => (
                <Shot key={s.n} cid={cardId(e.code, s.n)} label={s.label} ids={s.ids}>
                  <PanelView s={s.state} on={NONE} inline />
                </Shot>
              ))}
            </Row_>
          </section>
        ))}

        <section id="buttons" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <h2 style={{ margin: 0, ...caps(15, 0.12) }}>Buttons: enabled, and disabled with their reasons</h2>
          <p style={{ margin: 0, fontSize: 13 }}>Only here are buttons shown disabled: the owner sees them greyed out, with the reason as a tooltip.</p>
          <Row_ min={300}>
            <Shot cid="G-BTN-01" label="Enabled (owner)" ids={['open.default']} width={260}><ActionButton spec={opt(T.live, 'Resync')} /></Shot>
            {BLOCKED.map(([cid, label, id, why]) => (
              <Shot key={id} cid={cid} label={`Disabled: ${label}`} ids={[id, '(visible line under the button, and its tooltip)']}>
                <ActionContext.Provider value={{ ...OPEN, blocked: () => why }}>
                  <ActionButton spec={opt(T.live, 'Resync')} />
                </ActionContext.Provider>
              </Shot>
            ))}
          </Row_>
        </section>

        <section id="cards" style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
          <h2 style={{ margin: 0, ...caps(15, 0.12) }}>Drawer option cards, per table state</h2>
          {CARD_STATES.map(([label, t, code]) => (
            <div key={label} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <h3 style={{ margin: 0, ...caps(12, 0.12) }}>{label}</h3>
              <Row_ min={240}>
                {fixOptions(t).map((o) => (
                  <Shot key={o.name} cid={`G-CARD-${code}-${OPT_NO[o.name]}`} label={o.na ? `${o.name} (greyed: doesn’t apply here)` : o.name} ids={[CARD_ID[o.name] ?? 'card.*', ...(o.na ? ['na.*'] : []), ...(o.suggested ? ['card.suggested'] : [])]}>
                    <OptionCard o={o} reason={null} owner={OWNER} />
                  </Shot>
                ))}
                {!fixOptions(t).length ? <p style={{ margin: 0, color: MUTED }}>No options (a removed table).</p> : null}
              </Row_>
            </div>
          ))}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <h3 style={{ margin: 0, ...caps(12, 0.12) }}>As a non-owner sees them</h3>
            <Row_ min={240}>
              {fixOptions(T.blocked).map((o) => (
                <Shot key={o.name} cid={`G-CARD-RO-${OPT_NO[o.name]}`} label={o.name} ids={[CARD_ID[o.name] ?? 'card.*', 'readonly.card']} width={240}>
                  <OptionCard o={o} reason="Read-only: actions are run by alice." owner={OWNER} />
                </Shot>
              ))}
            </Row_>
          </div>
        </section>

        <section id="danger" style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
          <h2 style={{ margin: 0, ...caps(15, 0.12) }}>Danger zone cards</h2>
          {([['Slot lost', true, 'LOST'], ['Slot fine', false, 'OK']] as const).map(([label, lost, code]) => (
            <div key={label} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <h3 style={{ margin: 0, ...caps(12, 0.12) }}>{label}</h3>
              <Row_ min={300}>
                {dangerCards(SOURCE, lost).map((d) => (
                  <Shot key={d.name} cid={`G-DANGER-${code}-${DANGER_NO[d.name]}`} label={d.name} ids={[d.name === 'Recover slot' ? 'danger.recover' : d.name === 'Skip to LSN' ? 'danger.skip' : 'danger.delete']} width={300}>
                    <DangerCard d={d} readOnly={false} owner={OWNER} />
                  </Shot>
                ))}
              </Row_>
            </div>
          ))}
        </section>
        <section id="index" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <h2 style={{ margin: 0, ...caps(15, 0.12) }}>Card ID index</h2>
          <p style={{ margin: 0, fontSize: 13 }}>Write edits as “G-REMOVE-04: change … to …”. IDs are fixed: a new stage gets a new number, existing ones never change.</p>
          <table style={{ borderCollapse: 'collapse', fontSize: 12.5, background: '#FFFFFF' }}>
            <thead><tr>{['Card ID', 'Section', 'Stage / card', 'String IDs'].map((h) => <th key={h} style={{ textAlign: 'left', padding: '6px 10px', borderBottom: `2px solid ${INK}` }}>{h}</th>)}</tr></thead>
            <tbody>
              {cardIndex().map(([cid, sec, title, ids]) => (
                <tr key={cid}>
                  <td style={{ padding: '4px 10px', fontFamily: MONO, fontWeight: 700, whiteSpace: 'nowrap' }}><a href={`#${cid}`} style={{ color: INK }}>{cid}</a></td>
                  <td style={{ padding: '4px 10px' }}>{sec}</td>
                  <td style={{ padding: '4px 10px' }}>{title}</td>
                  <td style={{ padding: '4px 10px', fontFamily: MONO, color: MUTED }}>{ids}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </main>
    </ActionContext.Provider>
  );
}
