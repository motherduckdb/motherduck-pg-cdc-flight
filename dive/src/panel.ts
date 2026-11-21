// The action panel's pure parts: the result box's words and the preview's observed facts
// (kept out of action.tsx so they test without a browser runtime).
import { ACTIONS, flightLiteral, needsNewPreview, TYPED_FIRST, type ActionInputs, type ActionName, type Phase } from './actions';
import { PAGE, STATUS } from './theme';

/** `observed` as label/value pairs: keys in words, LSN numbers as X/Y, nested values compact. */
export function observedFacts(observed: Record<string, unknown> | null): [string, string][] {
  if (!observed) return [];
  return Object.entries(observed)
    .filter(([, v]) => v !== null && v !== undefined && v !== '')
    .map(([k, v]) => {
      const label = k.replace(/_/g, ' ').replace(/^./, (c) => c.toUpperCase());
      if (typeof v === 'number' && /lsn|checkpoint|anchor|_end$|_start$/.test(k) && Number.isInteger(v)) {
        const n = BigInt(v);
        return [label, `${(n >> 32n).toString(16).toUpperCase()}/${(n & 0xffffffffn).toString(16).toUpperCase()}`];
      }
      if (typeof v === 'boolean') return [label, v ? 'yes' : 'no'];
      if (typeof v === 'object') return [label, JSON.stringify(v)];
      return [label, String(v)];
    });
}

type ResultView = { title: string; text: string; next: string; bg: string; again: boolean };

/** delete-source's lists (`audit.result.drop` / `.dba`): what it drops itself, as
 * "kind name", and the statements it leaves for a DBA; null when the result has none. */
export function deleteLists(drops: unknown, dba: unknown): { drops: string[]; dba: string[] } | null {
  const parse = (v: unknown): unknown[] | null => {
    if (v == null || v === '') return null;
    try {
      const x = typeof v === 'string' ? JSON.parse(v) : v;
      return Array.isArray(x) ? x : null;
    } catch {
      return null;
    }
  };
  const d = parse(drops);
  const b = parse(dba);
  if (d === null && b === null) return null;
  return {
    drops: (d ?? []).map((x) => (x && typeof x === 'object' ? `${(x as { kind?: string }).kind ?? ''} ${(x as { name?: string }).name ?? ''}`.trim() : String(x))),
    dba: (b ?? []).map(String),
  };
}

/** The result box: the run's outcome in the design's words (Action.dc.html, "Result"). */
/**
 * The panel's words per action (the first copy round's themes, extended to every action). NAME is the action in title case ("Pause
 * Completed"); RUNNING the progressive title while it runs; DONE the past-tense restatement
 * of its Impact; STARTED what a run that started another Flight did; ALREADY a preview's
 * "nothing to do". The Flight's own text stays in Run details.
 */
export const NAME: Record<ActionName, string> = {
  pause: 'Pause', resume: 'Resume', retry: 'Retry', resync: 'Resync', 'start-from-log': 'Start From Log',
  'resync-all': 'Resync', 'compact-now': 'Compaction', add: 'Enable', remove: 'Remove', quarantine: 'Quarantine',
  'recover-slot': 'Recover Slot', 'skip-to-lsn': 'Skip To LSN', 'delete-source': 'Delete Source',
};
export const nameOf = (spec: { action: ActionName; title?: string }) => (spec.title === 'Finish recovery' ? 'Finish Recovery' : NAME[spec.action]);

export const RUNNING: Record<ActionName, string> = {
  pause: 'Pausing table sync', resume: 'Resuming table sync', retry: 'Retrying table sync', resync: 'Resyncing table',
  'start-from-log': 'Starting table from the changelogs', 'resync-all': 'Resyncing next queued table', 'compact-now': 'Compacting change log',
  add: 'Enabling table', remove: 'Removing table', quarantine: 'Quarantining batch', 'recover-slot': 'Recovering slot',
  'skip-to-lsn': 'Skipping to LSN', 'delete-source': 'Deleting source',
};
export const runningTitle = (spec: { action: ActionName; title?: string }) => (spec.title === 'Finish recovery' ? 'Finishing recovery' : RUNNING[spec.action]);

const DONE: Partial<Record<ActionName, string>> = {
  pause: 'Updates to this table have been paused. Changelogs are still being ingested into MotherDuck, and will be applied to the replica table upon Resume.',
  resume: 'Updates to this table have resumed. Changelogs ingested while it was paused are being applied to the replica table in order.',
  retry: 'The failed batch has been retried on the replica table.',
  remove: 'This table has been removed. Changelogs are no longer ingested for it; the replica table is kept as it is.',
  quarantine: 'The failed batch has been set aside, and newer changelogs are being applied to the replica table. The batch is a recorded gap until a resync.',
  'start-from-log': 'This table has restarted from the current position in the changelogs. Rows inserted before then aren’t in the replica table.',
  'recover-slot': 'A new replication slot and epoch have begun. Every replica table needs a rebuild (from the Recovery panel; insert-only tables from their drawers).',
  'skip-to-lsn': 'The slot has moved forward and a new epoch has begun. Every replica table needs a resync.',
  'delete-source': 'This source has been deleted. The replica databases and the change log are kept.',
};
export const doneText = (action: ActionName, title?: string): string =>
  title === 'Finish recovery' ? 'The leftover slot has been dropped.' : DONE[action] ?? `${NAME[action]} completed.`;

const STARTED: Partial<Record<ActionName, string>> = {
  resync: 'The replica table is being rebuilt from a fresh snapshot',
  'resync-all': 'The queued table that waited longest is being rebuilt from a fresh snapshot',
  add: 'The replica table is being built from a fresh snapshot',
  'compact-now': 'The change log is being compacted',
};
export const startedText = (action: ActionName, triggered: string, insertOnly = false) =>
  `${action === 'resync' && insertOnly ? 'The replica table is being replaced with an exact snapshot of the source table' : STARTED[action] ?? `${NAME[action]} started`} (${triggered}).`;

const ALREADY: Record<ActionName, string> = {
  pause: 'This table is already paused.', resume: 'This table is already updating.', retry: 'This table is no longer blocked.',
  resync: 'This table is already being rebuilt.', 'start-from-log': 'This table already reads from the changelogs.',
  'resync-all': 'No table needs a resync.', 'compact-now': 'The change log is already compacted.', add: 'This table is already enabled.',
  remove: 'This table is already removed.', quarantine: 'This batch is already set aside.',
  'recover-slot': 'This source already has a working slot.', 'skip-to-lsn': 'This source is already past that position.',
  'delete-source': 'This source is already deleted.',
};

/** A Flight refusal in plain words (its raw text stays in Run details). */
export function plainReason(detail: string | null | undefined): string {
  const d = detail ?? '';
  const known: [RegExp, string | ((m: RegExpExecArray) => string)][] = [
    [/is paused: resume it|it is paused: resume it/, 'This table is paused. Resume it first.'],
    // "state changed" has several forms: epoch, blocked unit, blocked
    // commit, and a table state; the most specific first
    [/state changed: expected epoch \d+, found (\d+)/, (m) => `This source moved to epoch ${m[1]} since the preview.`],
    [/state changed: expected blocked commit \S+, found none/, 'This table is no longer blocked.'],
    [/state changed: (expected blocked (unit|commit)|the blocked (unit|commit) is)/, 'The failed batch changed since the preview.'],
    [/state changed: expected [a-z_]+, found ([a-z_]+)$/, (m) => `This table’s state changed (it is now ${m[1].replace(/_/g, ' ')}).`],
    [/the table's state changed since it was read/, 'This table’s state changed since the preview.'],
    // waiting: a reader on the slot, which no Dive action ends
    [/an ingest reader is still attached to (\S+?);/, (m) => `A sync reader is still attached to slot ${m[1]}. Run it again once the reader stops.`],
    [/replication slot is active; retry when its reader has stopped/, 'The replication slot still has an active reader. Run it again once the reader stops.'],
    [/(\S+) is in use; run delete-source again/, (m) => `${m[1]} is still in use on Postgres. Run it again to finish.`],
    [/ingest holds the lease/, 'A sync run is in progress. Try again in a minute.'],
    [/a backfill is already running for this table/, 'A rebuild is already running for this table.'],
    [/the source table is held \(([^)]+)\)/, (m) => `The source table is held (${m[1].replace(/_/g, ' ')}).`],
    [/already has an active backfill/, 'A rebuild is already running for this table.'],
    [/still kept aside/, 'An older kept-aside table still exists. Drop it first.'],
    [/start-from-log is for insert-only/, 'Start from log is only for insert-only tables.'],
    [/over the append snapshot limit/, 'This table is too large for a snapshot. Start it from the log, or raise the limit.'],
    [/no free replication slot or walsender/, 'The source has no free replication slot. Try again later.'],
    [/ingest has not yet passed the source key change/, 'The next sync hasn’t run yet. Try again after it.'],
    [/no table needs a resync/, 'No table needs a resync.'],
  ];
  for (const [re, text] of known) {
    const m = re.exec(d);
    if (m) return typeof text === 'string' ? text : text(m);
  }
  const t = d.replace(/^[^\s:]+\.[^\s:]+: /, '').replace(/\bmirror table\b/g, 'replica table').replace(/\bmirror\b/g, 'replica table').replace(/\.$/, '');
  return t ? `${t.charAt(0).toUpperCase()}${t.slice(1)}.` : '';
}

export function resultView(
  phase: Extract<Phase, { kind: 'finished' }>,
  spec: { action: ActionName; verb: string; title?: string; inputs?: { table?: string } },
): ResultView {
  const r = phase.result;
  const name = nameOf(spec);
  const where = spec.inputs?.table ? 'table' : 'source';
  const reason = r?.detail ? `Reason: ${plainReason(r.detail)}` : '';
  const remaining = r?.remaining ? `${r.remaining} more table${r.remaining === 1 ? '' : 's'} wait for a rebuild.` : '';
  const blocked = { title: 'Blocked By Another Operation', text: `Conflicting action running on this ${where}. Please try again in a minute.`, next: '', bg: STATUS.stale.bg };
  if (phase.outcome === 'busy') return { ...blocked, again: true };
  if (phase.outcome === 'cancelled') return { title: 'Cancelled', text: 'The run was cancelled.', next: '', bg: PAGE, again: true };
  // waiting: the epoch and delete Flights, often a reader on the slot: keep the reason
  if (phase.outcome === 'waiting') return { ...blocked, text: reason || blocked.text, again: !phase.previewId };
  if (r?.status === 'drain_required')
    return { title: 'Blocked By Another Operation', text: 'An open batch of changelogs must finish first. Please try again after the next sync.', next: '', bg: STATUS.stale.bg, again: true };
  // A refusal with no audit row: the Flight rejected its inputs before acting; it writes a
  // row only once they parse, so which input and why is only in its run log (Run details).
  if (phase.outcome === 'refused' && !r)
    return { title: 'Action Rejected', text: 'Flight inputs were rejected. The run’s log says which one and why (the run is in Run details).', next: '', bg: STATUS.needs_action.bg, again: true };
  if (phase.step === 'preview') {
    if (phase.outcome === 'refused') return { title: 'Action Rejected', text: reason || 'The preview was refused.', next: '', bg: STATUS.needs_action.bg, again: true };
    if (phase.outcome === 'failed') return { title: 'Preview Failed', text: reason || phase.message, next: '', bg: STATUS.needs_action.bg, again: true };
    if (r?.status === 'already') return { title: 'Preview Actions', text: ALREADY[spec.action], next: '', bg: PAGE, again: false };
    return { title: 'No preview available', text: 'The preview ran but its result didn’t reach this page. Preview again; if this keeps happening, check Run details.', next: '', bg: STATUS.stale.bg, again: true };
  }
  if (phase.outcome === 'refused')
    return needsNewPreview(phase)
      ? { title: 'Action Rejected', text: `${where === 'table' ? 'Table' : 'Source'} state changed since preview. Preview the action again to continue.`, next: '', bg: STATUS.needs_action.bg, again: true }
      : { title: 'Action Rejected', text: reason, next: '', bg: STATUS.needs_action.bg, again: true };
  if (phase.outcome === 'failed') return { title: 'Failed', text: r?.detail ? reason : phase.message, next: `Check the ${where}’s status before trying again.`, bg: STATUS.needs_action.bg, again: true };
  if (!r) return { title: 'Result Unknown', text: 'Check status again in a few minutes.', next: '', bg: STATUS.stale.bg, again: true };
  if (r.status === 'already') {
    // Completed only when it is: Finish recovery always reports
    // `already`, and a leftover or kept slot means it is not done
    const steps = r.nextSteps ?? [];
    const leftover = steps.map((x) => /the previous slot (\S+) still exists and retains WAL/.exec(x)?.[1]).find(Boolean);
    const pending = steps.map((x) => /slot (\S+) was kept: run/.exec(x)?.[1]).find(Boolean);
    if (leftover) return { title: 'Old Slot Still In Use', text: `The previous replication slot ${leftover} still has a reader and keeps retaining WAL. Run Finish recovery again once the reader stops.`, next: '', bg: STATUS.stale.bg, again: false };
    if (pending) return { title: 'Recovery Not Finished', text: `Slot ${pending} was kept. Run ${name} again to finish.`, next: '', bg: STATUS.stale.bg, again: false };
    return { title: `${name} Completed`, text: '', next: '', bg: PAGE, again: false };
  }
  if (r.triggered) return { title: `${name} Started`, text: startedText(spec.action, r.triggered, /exact snapshot|kept aside/.test(r.detail ?? '')), next: remaining, bg: STATUS.rebuilding.bg, again: false };
  return { title: `${name} Completed`, text: doneText(spec.action, spec.title), next: '', bg: STATUS.ok.bg, again: false };
}

/**
 * The panel's step (0 Preview, 1 Confirm, 2 Run, 3 Result). A finished preview stays on
 * Preview, showing what it would do, until the viewer continues to Confirm; Back returns
 * to it (user report, 2026-11-11: it used to jump straight to Confirm).
 */
export function panelStep(kind: string, confirming: boolean, finishedStep?: 'preview' | 'run'): 0 | 1 | 2 | 3 {
  if (kind === 'ready') return confirming ? 1 : 0;
  if (kind === 'continue') return 1;
  if (kind === 'running') return 2;
  if (kind === 'finished' && finishedStep === 'run') return 3;
  return 0;
}

/**
 * After a run finishes: settle on its audit row, or read again while the share may still
 * be catching up (about a minute), until `waitMs` or the viewer stops waiting. Only then
 * does a missing row read as "result not recorded" (or, for a refusal, rejected inputs).
 */
export function auditNext(hasRow: boolean, elapsedMs: number, gaveUp: boolean, waitMs: number): 'settle' | 'read_again' {
  return hasRow || gaveUp || elapsedMs >= waitMs ? 'settle' : 'read_again';
}

/** A dry run counts as timed out only while it hasn't finished: once it has, the panel is
 * waiting for its audit row (auditNext), which has its own limit. */
export function previewTimedOut(kind: string, launchedAt: number | null, finished: boolean, now: number, limitMs: number): boolean {
  return (kind === 'previewing' || kind === 'awaiting_row') && launchedAt !== null && !finished && now - launchedAt > limitMs;
}

/**
 * The preview's second line: only a fact the plain "What it would do", its cost line and
 * the observed facts don't already give, in the Dive's words (replica, never mirror), read
 * from the Flight's detail where needed; null otherwise (user, 2026-11-12). The Flight's
 * raw text stays in Run details.
 * - quarantine: how many commits the batch holds (an insert-only table: its inserts are
 *   missing until a resync);
 * - a forced remove: what undoes it;
 * - recover slot: from which position changes are lost;
 * - an insert-only resync under ignore_deletes: purged rows survive only in the kept table.
 * Start from log, skip to LSN and delete source say all of theirs in the first line, the
 * cost, the observed facts or the drop lists.
 */
export function consequence(action: string, inputs: { force?: boolean }, detail: string | null | undefined): string | null {
  const d = detail ?? '';
  switch (action) {
    case 'quarantine': {
      const m = /\((\d+) commits?\) in \S+ (are not in the mirror|may diverge)/.exec(d);
      if (!m) return null;
      const n = Number(m[1]);
      const commits = `${n} commit${n === 1 ? '' : 's'}`;
      return m[2].startsWith('are not')
        ? `That batch holds ${commits}; its inserts won’t be in the replica until a resync.`
        : `That batch holds ${commits}.`;
    }
    case 'remove':
      return inputs.force ? 'Only re-adding the table and a full backfill undoes this. The replica table is kept as it is.' : null;
    case 'recover-slot': {
      const m = /WAL after (\S+) that the old slot lost/.exec(d);
      return m ? `Changelogs after ${m[1]} that the old slot lost are never ingested into MotherDuck.` : null;
    }
    case 'resync': {
      if (!/DELETEs are skipped/.test(d)) return null;
      const kept = /kept aside as ([^\s.;,]+)/.exec(d)?.[1];
      return `Rows the source deleted drop out of the replica table; they survive only in the kept-aside table${kept ? ` ${kept}` : ''}.`;
    }
    default:
      return null;
  }
}

/** The note an expired preview shows where its buttons are (user, 2026-11-12). */
export const EXPIRED_NOTE = 'Preview expired, re-run preview to continue.';

/**
 * The buttons of a finished preview (Cancel aside). No countdown is shown; once the preview
 * is older than PREVIEW_OFFER_MS, Continue to confirm and Run give way to Re-run Preview (a
 * fresh dry run with a new request id), so a stale preview can never be run.
 */
export function readyButtons(confirming: boolean, expired: boolean): ('back' | 'continue' | 'run' | 'rerun')[] {
  if (!confirming) return expired ? ['rerun'] : ['continue'];
  return expired ? ['back', 'rerun'] : ['back', 'run'];
}

/**
 * What a panel sends and whether it may preview yet, from its spec and the viewer's typing:
 * one place for both the panel's buttons (PanelView) and what preview() sends (ActionPanel).
 * Typed-first actions send their exact confirmation on every run.
 */
export function panelInputs(spec: { action: ActionName; inputs: ActionInputs; source: string }, typed: string, toLsn: string) {
  const typedFirst = TYPED_FIRST.includes(spec.action);
  const inputs: ActionInputs = typedFirst
    ? { ...spec.inputs, toLsn: spec.action === 'skip-to-lsn' ? toLsn : spec.inputs.toLsn, confirm: spec.inputs.confirm ?? typed }
    : spec.inputs;
  const literal = typedFirst ? spec.inputs.confirm ?? flightLiteral(spec.action, { ...inputs, confirm: undefined }, spec.source) : null;
  const literalOk = !typedFirst || (literal !== null && (spec.inputs.confirm !== undefined || typed === literal));
  return { typedFirst, inputs, literal, literalOk };
}

/** A run is in flight (the panel can't close, other actions wait). */
export const panelRunning = (kind: Phase['kind'], startPending: boolean) =>
  kind === 'previewing' || kind === 'awaiting_row' || kind === 'running' || startPending;

const LIFECYCLE_TEXT: Record<string, string> = {
  'source deleting': 'Not available while the source is being deleted.',
  'source deleted': 'Not available: the source is deleted.',
  'source deleting or deleted': 'Not available: the source is being deleted or is deleted.',
};
/** The visible line under a disabled action button (user, 2026-11-12): blocked()'s reason,
 * a source lifecycle fragment written as a sentence. */
export const disabledReason = (why: string): string => LIFECYCLE_TEXT[why] ?? why;
