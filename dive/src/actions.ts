// The action buttons' pure core (docs/dive.md, docs/operations.md §Action Flights):
// which Flight and inputs each action takes, the config a run sends, the typed
// confirmation it needs, and the state machine from preview to result.
//
// The Flights are the authority: each run re-reads the stores and checks its preview, so
// nothing here is a guard. It only avoids sending a run that would surely be refused.
// scripts/dump-queries.ts renders every action's config into queries.snapshot.json,
// and tests/test_dive_queries.py checks them against chain.FLIGHT_CONFIG and the
// Flights' own parser.

export type FlightKind = 'table_action' | 'table_membership' | 'quarantine' | 'epoch' | 'delete_source';

/** The keys each action Flight declares (chain.FLIGHT_CONFIG): a run sends all of them,
 * blank meaning unset. */
export const FLIGHT_KEYS: Record<FlightKind, string[]> = {
  table_action: ['CDC_ACTION', 'CDC_TABLE', 'CDC_EXPECTED_STATE', 'CDC_EXPECTED_BLOCKED_LSN', 'CDC_CONFIRM', 'CDC_DRY_RUN', 'CDC_PREVIEW_ID', 'CDC_REQUEST_ID'],
  table_membership: ['CDC_ACTION', 'CDC_TABLE', 'CDC_FORCE', 'CDC_CONFIRM', 'CDC_DRY_RUN', 'CDC_PREVIEW_ID', 'CDC_REQUEST_ID'],
  quarantine: ['CDC_ACTION', 'CDC_TABLE', 'CDC_EXPECTED_EPOCH', 'CDC_CONFIRM', 'CDC_DRY_RUN', 'CDC_PREVIEW_ID', 'CDC_REQUEST_ID'],
  epoch: ['CDC_ACTION', 'CDC_EXPECTED_EPOCH', 'CDC_TO_LSN', 'CDC_CONFIRM', 'CDC_DRY_RUN', 'CDC_PREVIEW_ID', 'CDC_REQUEST_ID'],
  delete_source: ['CDC_ACTION', 'CDC_EXPECTED_EPOCH', 'CDC_CONFIRM', 'CDC_DRY_RUN', 'CDC_PREVIEW_ID', 'CDC_REQUEST_ID'],
};

export type ActionName =
  | 'pause'
  | 'resume'
  | 'retry'
  | 'resync'
  | 'start-from-log'
  | 'resync-all'
  | 'compact-now'
  | 'add'
  | 'remove'
  | 'quarantine'
  | 'recover-slot'
  | 'skip-to-lsn'
  | 'delete-source';

export const ACTIONS: Record<ActionName, { flight: FlightKind; label: string; triggers: boolean }> = {
  pause: { flight: 'table_action', label: 'Pause', triggers: false },
  resume: { flight: 'table_action', label: 'Resume', triggers: false },
  retry: { flight: 'table_action', label: 'Retry', triggers: false },
  resync: { flight: 'table_action', label: 'Resync', triggers: true },
  'start-from-log': { flight: 'table_action', label: 'Start from log', triggers: false },
  'resync-all': { flight: 'table_action', label: 'Resync next queued table', triggers: true },
  'compact-now': { flight: 'table_action', label: 'Compact now', triggers: true },
  add: { flight: 'table_membership', label: 'Enable', triggers: true },
  remove: { flight: 'table_membership', label: 'Remove', triggers: false },
  quarantine: { flight: 'quarantine', label: 'Quarantine', triggers: false },
  'recover-slot': { flight: 'epoch', label: 'Recover slot', triggers: false },
  'skip-to-lsn': { flight: 'epoch', label: 'Skip to LSN', triggers: false },
  'delete-source': { flight: 'delete_source', label: 'Delete source', triggers: false },
};

/** The typed-first actions take their exact confirmation on every run, dry runs included: the
 * viewer types it before the preview. */
export const TYPED_FIRST: ActionName[] = ['quarantine', 'recover-slot', 'skip-to-lsn', 'delete-source'];

/** action_flight.RESYNC_COST, shown in a resync's preview. */
export const RESYNC_COST: Record<string, string> = {
  needs_resync: 'a full reload of the table from the source',
  needs_action: 'replaces the held mirror table with a full reload from the source',
  paused: 'ends the pause; a full reload of the table from the source',
  live: 'a full reload of a current table from the source',
};

/** action_flight.PREVIEW_MINUTES: a preview is spendable this long after its dry run. */
export const PREVIEW_MINUTES = 15;
/** The Dive stops offering Confirm a minute early, rather than send a sure refusal. */
export const PREVIEW_OFFER_MS = (PREVIEW_MINUTES - 1) * 60_000;

/** Until V6 is verified (a run a non-owner starts may start another Flight). */
export const TRIGGER_NOTE =
  'May fail for non-owners until verified: the preview is spent and the run reports a trigger failure. Ask the Dive\'s owner to run it, or run the backfill Flight.';

export type ActionInputs = {
  table?: string;
  expectedState?: string;
  /** "X/Y", or "none" for a hold with no blocked commit */
  expectedBlockedLsn?: string;
  force?: boolean;
  confirm?: string;
  /** quarantine: the observed blocked unit's start and end, "X/Y". Never sent: they
   * build the exact confirmation literal, which the Flight compares with its own. */
  unitStart?: string;
  unitEnd?: string;
  expectedEpoch?: number;
  /** "X/Y" */
  toLsn?: string;
};

/** The inputs a run sends; the rest only build a confirmation literal. */
type SentInput = Exclude<keyof ActionInputs, 'unitStart' | 'unitEnd'>;

/** Which inputs each action uses; every other key is sent blank. */
const USES: Record<ActionName, SentInput[]> = {
  pause: ['table', 'expectedState'],
  resume: ['table', 'expectedState'],
  retry: ['table', 'expectedBlockedLsn'],
  resync: ['table', 'expectedState'],
  'start-from-log': ['table'],
  'resync-all': [],
  'compact-now': [],
  add: ['table'],
  remove: ['table', 'force', 'confirm'],
  quarantine: ['table', 'expectedEpoch', 'confirm'],
  'recover-slot': ['expectedEpoch', 'confirm'],
  'skip-to-lsn': ['expectedEpoch', 'toLsn', 'confirm'],
  'delete-source': ['expectedEpoch', 'confirm'],
};

const KEY_OF: Record<SentInput, string> = {
  table: 'CDC_TABLE',
  expectedState: 'CDC_EXPECTED_STATE',
  expectedBlockedLsn: 'CDC_EXPECTED_BLOCKED_LSN',
  force: 'CDC_FORCE',
  confirm: 'CDC_CONFIRM',
  expectedEpoch: 'CDC_EXPECTED_EPOCH',
  toLsn: 'CDC_TO_LSN',
};

/** An LSN as the Flights write it (lsn_text): "X/Y" in upper-case hex, no leading
 * zeros; null when the text isn't one. */
export function normalizeLsn(text: string): string | null {
  const m = /^\s*([0-9a-fA-F]{1,8})\/([0-9a-fA-F]{1,8})\s*$/.exec(text);
  if (!m) return null;
  return `${parseInt(m[1], 16).toString(16).toUpperCase()}/${parseInt(m[2], 16).toString(16).toUpperCase()}`;
}

/** The exact confirmation each typed-first Flight requires (its own `confirmation()`), for
 * `source`; null when an input it needs is missing. */
export function flightLiteral(action: ActionName, inputs: ActionInputs, source: string): string | null {
  switch (action) {
    case 'quarantine': {
      // action_flight.unit_literal: the whole blocked unit (format 2).
      const start = inputs.unitStart ? normalizeLsn(inputs.unitStart) : null;
      const end = inputs.unitEnd ? normalizeLsn(inputs.unitEnd) : null;
      return inputs.table && inputs.expectedEpoch != null && start && end
        ? `QUARANTINE ${inputs.table} UNIT ${inputs.expectedEpoch} ${start}..${end}`
        : null;
    }
    case 'recover-slot':
      return inputs.expectedEpoch != null ? `RECOVER ${source} EPOCH ${inputs.expectedEpoch + 1}` : null;
    case 'skip-to-lsn': {
      const to = inputs.toLsn ? normalizeLsn(inputs.toLsn) : null;
      return to ? `SKIP ${source} TO ${to}` : null;
    }
    case 'delete-source':
      return `DELETE ${source}`;
    default:
      return null;
  }
}

/** The config a run sends: every declared key, as strings. A dry run has
 * CDC_DRY_RUN=1; a real run carries its dry run's id as CDC_PREVIEW_ID. Each launch
 * sends its own CDC_REQUEST_ID, which the Flight copies to its audit row: the panel
 * finds the run's row by it, not by assuming its run id. */
export function buildConfig(
  action: ActionName,
  inputs: ActionInputs,
  run: ({ dryRun: true } | { previewId: string }) & { requestId?: string },
): Record<string, string> {
  const config: Record<string, string> = Object.fromEntries(FLIGHT_KEYS[ACTIONS[action].flight].map((k) => [k, '']));
  config.CDC_ACTION = action;
  for (const name of USES[action]) {
    const value = inputs[name];
    if (value === undefined || value === false) continue;
    config[KEY_OF[name]] = value === true ? '1' : name === 'toLsn' ? normalizeLsn(String(value)) ?? String(value) : String(value);
  }
  if ('dryRun' in run) config.CDC_DRY_RUN = '1';
  else config.CDC_PREVIEW_ID = run.previewId;
  if (run.requestId !== undefined) {
    if (!REQUEST_ID.test(run.requestId)) throw new Error(`not a request id: ${run.requestId}`);
    config.CDC_REQUEST_ID = run.requestId;
  }
  return config;
}

/** The Flights' format for CDC_REQUEST_ID (action_base.REQUEST_ID). */
export const REQUEST_ID = /^[A-Za-z0-9_-]{1,64}$/;

/** A fresh request id per launch: a UUID where the runtime has one, else time and
 * randomness (it only has to be unique among this owner's runs, not secret). */
export function newRequestId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `r${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
}

/** The text a viewer must type before the real run, or null for a plain confirm:
 * a forced remove needs the Flight's exact `REMOVE <table>`; a resync of a table that
 * serves data (live, paused, needs_action) needs its name; resync from needs_resync is
 * the expected recovery. */
export function typedConfirmation(action: ActionName, inputs: ActionInputs): string | null {
  if (action === 'remove' && inputs.force) return `REMOVE ${inputs.table}`;
  if (action === 'resync' && inputs.expectedState !== 'needs_resync') return inputs.table ?? null;
  return null;
}

// ── The state machine ────────────────────────────────────────────────────────────

/** A run's terminal outcome from useFlightRun: the exit code is authoritative
 * (0 done/already/would, 2 refused, 3 busy, 1 failed); the audit row only adds detail. */
export type Outcome = 'ok' | 'refused' | 'busy' | 'failed' | 'cancelled' | 'waiting';

export function outcomeOf(status: string | undefined, exitCode: number | null | undefined): Outcome | null {
  if (status === 'CANCELLED') return 'cancelled';
  if (status !== 'SUCCEEDED' && status !== 'FAILED') return null;
  if (exitCode === 0) return 'ok';
  if (exitCode === 2) return 'refused';
  if (exitCode === 3) return 'busy';
  return 'failed';
}

export type AuditResult = {
  status: string;
  detail?: string;
  nextSteps?: string[];
  triggered?: string | null;
  remaining?: number | null;
  lifecycle?: string | null;
  startedBy?: string | null;
};

/** The two `already` dry runs that still need a real run: delete on a
 * `deleting` source, and an epoch transition's repeat path (a leftover slot to drop).
 * They record no preview; the real run ignores CDC_PREVIEW_ID on these paths. */
export function continuable(action: ActionName, result: AuditResult | null): boolean {
  if (!result || result.status !== 'already') return false;
  if (action === 'delete-source') return result.lifecycle === 'deleting';
  return action === 'recover-slot' || action === 'skip-to-lsn';
}

export type Phase =
  | { kind: 'idle' }
  | { kind: 'previewing' }
  | { kind: 'awaiting_row'; previewId: string; finishedAt: number }
  | { kind: 'ready'; previewId: string; finishedAt: number; result: AuditResult }
  | { kind: 'continue'; previewId: string; result: AuditResult }
  | { kind: 'running'; previewId: string }
  | { kind: 'finished'; step: 'preview' | 'run'; outcome: Outcome; result: AuditResult | null; message: string; previewId?: string };

export type Event =
  | { type: 'preview' }
  | { type: 'preview_done'; runId: string; outcome: Outcome; at: number; result?: AuditResult | null }
  | { type: 'row'; result: AuditResult | null; continuable?: boolean }
  | { type: 'confirm'; at: number }
  | { type: 'finish' }
  | { type: 'again' }
  | { type: 'run_done'; outcome: Outcome; result: AuditResult | null; exitZeroAmbiguous?: boolean }
  | { type: 'reset' };

export const MESSAGES = {
  nothing: 'Nothing to do: no preview was recorded.',
  notRecorded: 'The preview was not recorded: preview again.',
  expired: 'The preview expired: preview again.',
  noDetails: 'Details weren\'t recorded (invalid input, or the audit copy failed); the Flight\'s run log has them.',
  busy: 'Busy: another run holds the table or source. Try again shortly.',
  cancelled: 'The run was cancelled.',
  waitingPreview: 'preview again later',
  waitingRun: 'click Run again later (if the preview was spent, preview again)',
  unrecorded: 'finished (exit 0), but the result wasn\'t recorded: check Audit, or preview again',
};

/** Whether a preview can still be confirmed at `now` (a minute's margin). */
export const offerable = (finishedAt: number, now: number) => now - finishedAt < PREVIEW_OFFER_MS;

export function reduce(phase: Phase, event: Event): Phase {
  switch (event.type) {
    case 'reset':
      return { kind: 'idle' };
    case 'preview':
      return phase.kind === 'idle' || phase.kind === 'finished' ? { kind: 'previewing' } : phase;
    case 'preview_done':
      if (phase.kind !== 'previewing') return phase;
      if (event.outcome === 'ok') return { kind: 'awaiting_row', previewId: event.runId, finishedAt: event.at };
      // A refused or busy dry run shows its audit row's detail when there is one.
      return finish('preview', event.outcome, event.result ?? null);
    case 'row':
      if (phase.kind !== 'awaiting_row') return phase;
      // Only a `would` recorded a preview: a dry run exits 0 for `already` too.
      if (event.result?.status === 'would') return { kind: 'ready', previewId: phase.previewId, finishedAt: phase.finishedAt, result: event.result };
      if (event.result?.status === 'waiting')
        return { kind: 'finished', step: 'preview', outcome: 'waiting', result: event.result, message: `waiting: ${event.result.detail ?? ''}; ${MESSAGES.waitingPreview}` };
      if (event.continuable && event.result) return { kind: 'continue', previewId: phase.previewId, result: event.result };
      if (event.result?.status === 'already') return { kind: 'finished', step: 'preview', outcome: 'ok', result: event.result, message: MESSAGES.nothing };
      return { kind: 'finished', step: 'preview', outcome: 'ok', result: event.result, message: MESSAGES.notRecorded };
    case 'confirm':
      if (phase.kind !== 'ready') return phase;
      if (!offerable(phase.finishedAt, event.at)) return { kind: 'finished', step: 'preview', outcome: 'ok', result: null, message: MESSAGES.expired };
      return { kind: 'running', previewId: phase.previewId };
    case 'finish':
      // C2: finish a deleting source or an epoch transition; no preview to check.
      return phase.kind === 'continue' ? { kind: 'running', previewId: phase.previewId } : phase;
    case 'again':
      // After a `waiting` real run: the same inputs again (the Flight decides whether
      // the preview still counts; a refusal leads to a new preview).
      return phase.kind === 'finished' && phase.outcome === 'waiting' && phase.previewId ? { kind: 'running', previewId: phase.previewId } : phase;
    case 'run_done':
      if (phase.kind !== 'running') return phase;
      if (event.result?.status === 'waiting')
        return {
          kind: 'finished',
          step: 'run',
          outcome: 'waiting',
          result: event.result,
          message: `waiting: ${event.result.detail ?? ''}; ${MESSAGES.waitingRun}`,
          previewId: phase.previewId,
        };
      // For the typed-first actions exit 0 also means `waiting`, so without the row it's
      // unknown whether the run finished its work.
      if (event.outcome === 'ok' && !event.result && event.exitZeroAmbiguous)
        return { kind: 'finished', step: 'run', outcome: 'ok', result: null, message: MESSAGES.unrecorded, previewId: phase.previewId };
      return finish('run', event.outcome, event.result);
  }
}

function finish(step: 'preview' | 'run', outcome: Outcome, result: AuditResult | null): Phase {
  const message =
    outcome === 'busy'
      ? MESSAGES.busy
      : outcome === 'cancelled'
        ? MESSAGES.cancelled
        : outcome === 'ok'
          ? result?.detail ?? 'Done.'
          : result?.detail ?? MESSAGES.noDetails;
  return { kind: 'finished', step, outcome, result, message };
}

/** A refused real run whose preview can't be spent (expired, used, or the table moved)
 * leads back to a new preview, never a retry with the same id. */
export const needsNewPreview = (phase: Phase) =>
  phase.kind === 'finished' && phase.step === 'run' && phase.outcome === 'refused';

/** Every form of each action the screens send: the snapshot renders them all for the
 * Python check against the Flights' own parser. */
export const EXAMPLES: Record<ActionName, ActionInputs[]> = {
  pause: [{ table: 'public.items', expectedState: 'live' }],
  resume: [{ table: 'public.items', expectedState: 'paused' }],
  retry: [
    { table: 'public.items', expectedBlockedLsn: '0/5A' },
    { table: 'public.items', expectedBlockedLsn: 'none' },
  ],
  resync: ['needs_resync', 'needs_action', 'paused', 'live'].map((state) => ({ table: 'public.items', expectedState: state })),
  'start-from-log': [{ table: 'public.items' }],
  'resync-all': [{}],
  'compact-now': [{}],
  add: [{ table: 'public.items' }],
  remove: [
    { table: 'public.items' },
    { table: 'public.items', force: true, confirm: 'REMOVE public.items' },
  ],
  // The typed-first forms carry their literal for the snapshot source (phase2); a finish of an
  // earlier transition sends that transition's epoch and literal.
  quarantine: [
    { table: 'public.items', unitStart: '0/5A', unitEnd: '0/6B', expectedEpoch: 1, confirm: 'QUARANTINE public.items UNIT 1 0/5A..0/6B' },
  ],
  'recover-slot': [
    { expectedEpoch: 1, confirm: 'RECOVER phase2 EPOCH 2' },
    { expectedEpoch: 2, confirm: 'RECOVER phase2 EPOCH 3' },
  ],
  'skip-to-lsn': [{ expectedEpoch: 1, toLsn: '0/5a', confirm: 'SKIP phase2 TO 0/5A' }],
  'delete-source': [{ expectedEpoch: 1, confirm: 'DELETE phase2' }],
};

/** Why a source's lifecycle disables an action, or null. A deleting source can still be
 * finished by delete-source; nothing else acts on a deleting or deleted source (the
 * Flights refuse it anyway). */
export function lifecycleBlocks(action: ActionName, lifecycle: string | undefined): string | null {
  if (!lifecycle) return null;
  if (action === 'delete-source' && lifecycle !== 'deleted') return null;
  return `source ${lifecycle.replace(/_/g, ' ')}`;
}

