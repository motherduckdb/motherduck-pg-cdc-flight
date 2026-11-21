// The action panel (the 2026-11-10 design, Action.dc.html): one action at a time,
// Preview → Confirm → Run → Result, on the source's action Flights. Only the Flights'
// owner can open it (src/owner.ts); MotherDuck's RUN permission is the enforcement.
// The state machine is src/actions.ts; this file wires it to the Flight-run hooks, the
// share refresh and the run's audit row, found by the CDC_REQUEST_ID each launch sends.
import { createContext, useCallback, useContext, useEffect, useReducer, useRef, useState, type ReactNode } from 'react';
import { useFlightRun, useRunFlight, useSQLQuery } from '@motherduck/react-sql-query';
import {
  ACTIONS,
  PREVIEW_OFFER_MS,
  TYPED_FIRST,
  buildConfig,
  continuable,
  flightLiteral,
  lifecycleBlocks,
  newRequestId,
  normalizeLsn,
  outcomeOf,
  reduce,
  typedConfirmation,
  type ActionInputs,
  type ActionName,
  type AuditResult,
  type Outcome,
  type Phase,
} from './actions';
import { N, S, formatTs, listOf, rowsOf } from './format';
import { auditNext, consequence, deleteLists, disabledReason, EXPIRED_NOTE, nameOf, plainReason, runningTitle, panelInputs, panelRunning, previewTimedOut, readyButtons, observedFacts, panelStep, resultView } from './panel';
import { flightFor } from './flights';
import { RefreshTick, useReadOnly, useRows } from './kit';
import { sourceRow, who } from './model';
import * as Q from './queries';
import { caps, CARD, DISABLED, INK, MONO, MUTED, PAGE, STATUS } from './theme';

/** What a button opens: the action, its inputs (from the row on screen) and the panel's
 * words for it. */
export type ActionSpec = {
  source: string;
  action: ActionName;
  inputs: ActionInputs;
  /** what it acts on: a table name, "change log", the source */
  target: string;
  /** the panel's verb ("retry", "resume", "enable") */
  verb: string;
  /** the panel's title, when it isn't the action's label ("Finish recovery") */
  title?: string;
  /** what it will do, in the design's words */
  would: string;
  /** what it costs, if anything */
  cost?: string;
};

type Ctx = {
  open: (spec: ActionSpec) => void;
  /** why `spec` can't be opened now (not the owner, no Flight, source deleted, another run), or null */
  blocked: (spec: Pick<ActionSpec, 'source' | 'action'>) => string | null;
  readOnly: string | null;
};
/** Exported for the dev copy gallery, which renders buttons without a runtime. */
export const ActionContext = createContext<Ctx>({ open: () => {}, blocked: () => 'loading', readOnly: 'loading' });
export const useActions = () => useContext(ActionContext);

export function ActionProvider({ children }: { children: ReactNode }) {
  const [tick, setTick] = useState(0);
  return (
    <RefreshTick.Provider value={tick}>
      <ActionRoot bump={() => setTick((t) => t + 1)}>{children}</ActionRoot>
    </RefreshTick.Provider>
  );
}

/** Under the refresh tick, so its source lifecycles re-read after a run too. */
function ActionRoot({ children, bump }: { children: ReactNode; bump: () => void }) {
  const [spec, setSpec] = useState<ActionSpec | null>(null);
  const [busy, setBusy] = useState(false);
  const readOnly = useReadOnly();
  const lifecycles: Record<string, string> = {};
  for (const r of useRows(Q.sourceStatus(Q.ALL)).rows.map(sourceRow)) if (r.lifecycle) lifecycles[r.source_id] = r.lifecycle;
  const blocked = useCallback(
    (s: Pick<ActionSpec, 'source' | 'action'>) =>
      readOnly ??
      (!flightFor(s.source, ACTIONS[s.action].flight)
        ? `No ${ACTIONS[s.action].flight.replace('_', '-')} Flight is set up for ${s.source}.`
        : lifecycleBlocks(s.action, lifecycles[s.source]) ?? (busy ? 'Another action is running.' : null)),
    [readOnly, busy, JSON.stringify(lifecycles)], // eslint-disable-line react-hooks/exhaustive-deps
  );
  const value: Ctx = { open: (s) => (blocked(s) ? undefined : setSpec(s)), blocked, readOnly };
  return (
    <ActionContext.Provider value={value}>
      {children}
      {spec && !readOnly ? (
        <ActionPanel key={`${spec.source}:${spec.action}:${JSON.stringify(spec.inputs)}`} spec={spec} close={() => setSpec(null)} onBusy={setBusy} onDone={bump} />
      ) : null}
    </ActionContext.Provider>
  );
}

const BUTTON = (bg: string, disabled = false) => ({
  ...caps(12, 0.1),
  display: 'inline-flex',
  alignItems: 'center',
  boxSizing: 'border-box' as const,
  padding: '9px 14px',
  minHeight: 40,
  border: `2px solid ${disabled ? DISABLED : INK}`,
  borderRadius: 999,
  background: disabled ? PAGE : bg,
  color: disabled ? '#6B645C' : INK,
});

/** A button that opens an action: shown only to the owner; disabled, with the reason, where
 * the source has no such Flight, is being deleted, or another run is going. */
export function ActionButton({ spec, label, bg = CARD, align = 'start', compact = false }: {
  spec: ActionSpec;
  label?: string;
  bg?: string;
  /** where a disabled button and its reason line up (end: a right-aligned table cell) */
  align?: 'start' | 'end';
  /** a narrow place: the line reads "Not available", the full reason stays in the tooltip */
  compact?: boolean;
}) {
  const { open, blocked, readOnly } = useActions();
  if (readOnly) return null;
  const why = blocked(spec);
  const button = (
    <button type="button" disabled={why !== null} title={why ?? undefined} onClick={() => open(spec)} style={BUTTON(bg, why !== null)}>
      {label ?? `Preview ${spec.verb}`}
    </button>
  );
  // A disabled button says why, visibly (user, 2026-11-12), not only as a tooltip.
  return why === null ? button : (
    <span style={{ display: 'inline-flex', flexDirection: 'column', alignItems: align === 'end' ? 'flex-end' : 'flex-start', gap: 4 }}>
      {button}
      <span role="note" title={compact ? disabledReason(why) : undefined} style={{ fontSize: 12, color: MUTED, textAlign: align === 'end' ? 'right' : 'left' }}>{compact ? 'Not available' : disabledReason(why)}</span>
    </span>
  );
}

// ── The panel ───────────────────────────────────────────────────────────────────

/** A dry run with no result after this long: the Flight probably refused its inputs. */
export const PREVIEW_TIMEOUT_MS = 3 * 60_000;
/** Run unlocks this long after the preview arrives (the design's pause to read it). */
export const CONFIRM_DELAY_MS = 3_000;
/** After a run finishes, its audit row can lag on the share (UPDATE AUTOMATIC: about a
 * minute): the panel re-reads every AUDIT_POLL_MS for up to AUDIT_WAIT_MS before it says
 * the result wasn't recorded. */
export const AUDIT_POLL_MS = 5_000;
export const AUDIT_WAIT_MS = 3 * 60_000;

export type Started = { runNumber: number; runId: string; requestId: string; step: 'preview' | 'run'; launchedAt: number; config: Record<string, string> };
export type Row = AuditResult & { runId?: string; actor?: string | null; atS?: number | null; observed?: Record<string, unknown> | null; lists?: { drops: string[]; dba: string[] } | null };

function ActionPanel({ spec, close, onBusy, onDone }: { spec: ActionSpec; close: () => void; onBusy: (b: boolean) => void; onDone: () => void }) {
  const flightId = flightFor(spec.source, ACTIONS[spec.action].flight)!;
  const [phase, dispatch] = useReducer(reduce, { kind: 'idle' } as Phase);
  const start = useRunFlight(flightId);
  const [current, setCurrent] = useState<Started | null>(null);
  const currentRef = useRef<Started | null>(null);
  const handled = useRef<string | null>(null);
  const [terminal, setTerminal] = useState<(Started & { outcome: Outcome; doneAt: number }) | null>(null);
  // Re-reads of the audit row while the share catches up (AUDIT_POLL_MS), and the viewer's
  // "Stop waiting".
  const [attempt, setAttempt] = useState(0);
  const [gaveUp, setGaveUp] = useState(false);
  const polling = useRef<number | null>(null);
  const pollTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (pollTimer.current) clearTimeout(pollTimer.current); }, []);
  const [row, setRow] = useState<Row | null>(null);
  const [readyAt, setReadyAt] = useState<number | null>(null);
  const [now, setNow] = useState(Date.now());
  const [typed, setTyped] = useState('');
  const [confirmName, setConfirmName] = useState('');
  // A finished preview stays on screen until the viewer continues to Confirm (panelStep).
  const [confirming, setConfirming] = useState(false);
  const [toLsn, setToLsn] = useState(spec.inputs.toLsn ?? '');
  const [startError, setStartError] = useState<string | null>(null);
  const [tech, setTech] = useState(false);

  // A terminal state counts only for the run this panel started.
  useFlightRun(flightId, current?.runNumber, {
    onFinished: (run: { status: string; exitCode: number | null; runNumber?: number }) => {
      const c = currentRef.current;
      if (!c || handled.current === c.runId) return;
      if (run.runNumber !== undefined && run.runNumber !== c.runNumber) return;
      handled.current = c.runId;
      setTerminal({ ...c, outcome: outcomeOf(run.status, run.exitCode) ?? 'failed', doneAt: Date.now() });
    },
  });

  // Refresh the share once per run, then read the run's audit row by its request id.
  const pass = `/* run ${terminal?.runId ?? 'none'} read ${attempt} */`;
  const refresh = useSQLQuery(`REFRESH DATABASE "ops" ${pass}`, { enabled: terminal !== null });
  const audit = useSQLQuery(`${Q.auditRun(terminal?.requestId ?? Q.EXAMPLE_REQUEST)}\n${pass}`, { enabled: terminal !== null && (refresh.isSuccess || refresh.isError) });
  const settled = useRef<string | null>(null);
  useEffect(() => {
    if (!terminal || settled.current === terminal.runId) return;
    if (!(audit.isSuccess || audit.isError)) return;
    const r = rowsOf(audit.data)[0];
    // No row yet: the share may not have caught up. Read again, until the row shows,
    // AUDIT_WAIT_MS pass, or the viewer stops waiting.
    if (auditNext(!!r, Date.now() - terminal.doneAt, gaveUp, AUDIT_WAIT_MS) === 'read_again') {
      if (polling.current !== attempt) {
        polling.current = attempt;
        pollTimer.current = setTimeout(() => setAttempt((a) => a + 1), AUDIT_POLL_MS);
      }
      return;
    }
    settled.current = terminal.runId;
    if (pollTimer.current) clearTimeout(pollTimer.current);
    let observed: Record<string, unknown> | null = null;
    try {
      observed = r?.observed ? (JSON.parse(S(r.observed)) as Record<string, unknown>) : null;
    } catch {
      observed = null;
    }
    const result: Row | null = r
      ? {
          status: S(r.status),
          detail: S(r.detail),
          nextSteps: listOf(r.next_steps).map(S),
          triggered: (r.triggered as string) ?? null,
          remaining: r.remaining == null ? null : N(r.remaining),
          lifecycle: (r.lifecycle as string) ?? null,
          startedBy: (r.actor as string) ?? null,
          runId: r.run_id ? S(r.run_id) : undefined,
          actor: (r.actor as string) ?? null,
          atS: r.at_s == null ? null : N(r.at_s),
          observed,
          lists: deleteLists(r.drops, r.dba),
        }
      : null;
    setRow(result);
    if (terminal.step === 'preview') {
      // The preview id is the dry run's own run id, from its audit row when there is one;
      // its clock starts no later than its launch, so the time left is never overstated.
      dispatch({ type: 'preview_done', runId: result?.runId ?? terminal.runId, outcome: terminal.outcome, at: terminal.launchedAt, result });
      if (terminal.outcome === 'ok') dispatch({ type: 'row', result, continuable: continuable(spec.action, result) });
    } else {
      dispatch({ type: 'run_done', outcome: terminal.outcome, result, exitZeroAmbiguous: TYPED_FIRST.includes(spec.action) });
      onDone(); // every screen re-reads the refreshed share
    }
  }, [terminal, audit.isSuccess, audit.isError, audit.data, spec.action, onDone, attempt, gaveUp]);

  // The confirm delay counts from reaching Confirm, not from the preview's result.
  useEffect(() => {
    if (phase.kind === 'ready' && confirming) setReadyAt((t) => t ?? Date.now());
    else setReadyAt(null);
  }, [phase.kind, confirming]);
  // A clock for the countdown, the timeout and the preview's expiry.
  const ticking = phase.kind === 'previewing' || phase.kind === 'awaiting_row' || phase.kind === 'ready' || phase.kind === 'running';
  useEffect(() => {
    if (!ticking) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [ticking]);

  // The typed-first actions send their exact confirmation on every run, the dry run too.
  const { typedFirst, inputs, literal, literalOk } = panelInputs(spec, typed, toLsn);
  // The inputs a preview covered, frozen when it starts: the real run sends exactly these.
  const frozen = useRef<ActionInputs>(inputs);

  const launch = async (step: 'preview' | 'run', build: (requestId: string) => Record<string, string>) => {
    setStartError(null);
    const launchedAt = Date.now();
    const requestId = newRequestId();
    const config = build(requestId);
    try {
      const data = await start.runAsync(config);
      const started = { runNumber: data.runNumber, runId: data.runId, requestId, step, launchedAt, config };
      currentRef.current = started;
      setTerminal(null);
      setAttempt(0);
      setGaveUp(false);
      polling.current = null;
      if (pollTimer.current) clearTimeout(pollTimer.current);
      setCurrent(started);
      setNow(Date.now());
    } catch (err) {
      setStartError(`Couldn't start the run: ${(err as Error)?.message ?? err}. Only the Flights' owner can run them.`);
      dispatch({ type: 'reset' });
    }
  };
  const preview = () => {
    setConfirmName('');
    setConfirming(false);
    setRow(null);
    dispatch({ type: 'reset' });
    dispatch({ type: 'preview' });
    frozen.current = inputs;
    void launch('preview', (requestId) => buildConfig(spec.action, inputs, { dryRun: true, requestId }));
  };
  const run = () => {
    if (phase.kind !== 'ready' || !confirming) return;
    dispatch({ type: 'confirm', at: Date.now() });
    if (Date.now() - phase.finishedAt < PREVIEW_OFFER_MS) void launch('run', (requestId) => buildConfig(spec.action, frozen.current, { previewId: phase.previewId, requestId }));
  };
  const finishRun = () => {
    if (phase.kind !== 'continue') return;
    dispatch({ type: 'finish' });
    void launch('run', (requestId) => buildConfig(spec.action, frozen.current, { previewId: phase.previewId, requestId }));
  };
  const runAgain = () => {
    if (phase.kind !== 'finished' || phase.outcome !== 'waiting' || !phase.previewId) return;
    const previewId = phase.previewId;
    dispatch({ type: 'again' });
    void launch('run', (requestId) => buildConfig(spec.action, frozen.current, { previewId, requestId }));
  };

  // The design opens straight into the preview; a typed-first action waits for its literal,
  // unless the spec carries it (Finish recovery repeats a recorded transition).
  const autoStarted = useRef(false);
  useEffect(() => {
    if ((!typedFirst || spec.inputs.confirm !== undefined) && !autoStarted.current) {
      autoStarted.current = true;
      preview();
    }
  }); // eslint-disable-line react-hooks/exhaustive-deps

  const running = panelRunning(phase.kind, start.isPending);
  useEffect(() => onBusy(running), [running, onBusy]);
  useEffect(() => () => onBusy(false), [onBusy]);

  return (
    <PanelView
      s={{ spec, flightId, phase, confirming, row, current, terminal, waiting: terminal !== null && settled.current !== terminal.runId, now, readyAt, typed, confirmName, toLsn, startError, tech, startPending: start.isPending }}
      on={{ close, setTech, setTyped, setConfirmName, setToLsn, preview, run, finishRun, runAgain, stopWaiting: () => setGaveUp(true), setConfirming }}
    />
  );
}

/** Everything the panel shows, from its state: what ActionPanel renders, and what the dev
 * copy gallery (dive/gallery, never bundled) renders with forced states. */
export type PanelState = {
  spec: ActionSpec;
  flightId: string;
  phase: Phase;
  confirming: boolean;
  row: Row | null;
  current: Started | null;
  terminal: { outcome: Outcome; doneAt: number } | null;
  /** the run finished; its audit row hasn't shown yet */
  waiting: boolean;
  now: number;
  readyAt: number | null;
  typed: string;
  confirmName: string;
  toLsn: string;
  startError: string | null;
  tech: boolean;
  startPending: boolean;
};
export type PanelHandlers = {
  close: () => void;
  setTech: (b: boolean) => void;
  setTyped: (v: string) => void;
  setConfirmName: (v: string) => void;
  setToLsn: (v: string) => void;
  preview: () => void;
  run: () => void;
  finishRun: () => void;
  runAgain: () => void;
  stopWaiting: () => void;
  setConfirming: (b: boolean) => void;
};

export function PanelView({ s, on, inline = false }: { s: PanelState; on: PanelHandlers; inline?: boolean }) {
  const { spec, flightId, phase, confirming, row, current, terminal, waiting, now, readyAt, typed, confirmName, toLsn, startError, tech, startPending } = s;
  const { typedFirst, inputs, literal, literalOk } = panelInputs(spec, typed, toLsn);
  const running = panelRunning(phase.kind, startPending);
  const step = panelStep(phase.kind, confirming, phase.kind === 'finished' ? phase.step : undefined);
  const timedOut = previewTimedOut(phase.kind, current?.launchedAt ?? null, terminal !== null, now, PREVIEW_TIMEOUT_MS);
  const countdown = readyAt ? Math.max(0, Math.ceil((CONFIRM_DELAY_MS - (now - readyAt)) / 1000)) : 0;
  const nameNeeded = phase.kind === 'ready' ? typedConfirmation(spec.action, spec.inputs) : null;
  const expired = phase.kind === 'ready' && now - phase.finishedAt >= PREVIEW_OFFER_MS;
  const runDisabled = phase.kind !== 'ready' || !confirming || countdown > 0 || expired || (nameNeeded !== null && confirmName !== nameNeeded) || startPending;
  const title = spec.title ?? ACTIONS[spec.action].label;
  // The run finished; its audit row hasn't shown on the share yet.
  // Plain words while waiting (user, 2026-11-11); the exit code and audit detail are in Run details.
  const secs = (from: number | null | undefined) => (from ? Math.max(0, Math.round((now - from) / 1000)) : 0);
  const previewText = `Generating preview (${secs(current?.launchedAt)} s)`;
  const waitDetail = terminal
    ? `finished: ${terminal.outcome === 'ok' ? 'exit 0' : terminal.outcome}; ${waiting ? `waiting for its audit row on the share (read every ${AUDIT_POLL_MS / 1000} s, up to ${AUDIT_WAIT_MS / 60_000} min${terminal.outcome === 'refused' ? '; a Flight that rejects its inputs writes none' : ''})` : 'audit row read'}`
    : 'running';
  const view = phase.kind === 'finished' ? resultView(phase, spec) : null;

  return (
    <div style={inline ? { display: 'flex' } : { position: 'fixed', inset: 0, zIndex: 20, display: 'flex', alignItems: 'flex-start', justifyContent: 'center', overflowY: 'auto', padding: '48px 16px' }}>
      {inline ? null : <div aria-hidden="true" style={{ position: 'fixed', inset: 0, background: 'rgba(56, 56, 56, 0.35)' }} />}
      <section role={inline ? 'region' : 'dialog'} aria-modal={inline ? undefined : true} aria-labelledby={inline ? undefined : 'ap-title'} style={{ position: 'relative', width: '100%', maxWidth: 760, boxSizing: 'border-box', background: CARD, border: `2px solid ${INK}`, borderRadius: 14, boxShadow: `-8px 8px 0 ${INK}`, display: 'flex', flexDirection: 'column' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'flex-start', padding: '20px 24px 0' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 }}>
            <h2 id="ap-title" style={{ margin: 0, ...caps(18, 0.1), fontWeight: 700 }}>{title}</h2>
            <p style={{ margin: 0, fontSize: 13, color: MUTED }}><span style={{ fontFamily: MONO, color: INK }}>{spec.target}</span> · source {spec.source}</p>
          </div>
          <button type="button" aria-label="Close" onClick={on.close} disabled={running} title={running ? 'A run is in progress: its result also appears in the table’s history.' : undefined} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 40, height: 40, border: `2px solid ${INK}`, borderRadius: 999, background: CARD, flex: 'none' }}>
            <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><path d="M2 2l10 10M12 2L2 12" stroke={INK} strokeWidth="2" strokeLinecap="round" /></svg>
          </button>
        </div>

        <ol aria-label="Steps" style={{ listStyle: 'none', margin: 0, padding: '16px 24px 0', display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 8 }}>
          {['Preview', 'Confirm', 'Run', 'Result'].map((label, i) => (
            <li key={label} aria-current={i === step ? 'step' : undefined} style={{ ...caps(11, 0.12), padding: '8px 10px', borderRadius: 999, textAlign: 'center', whiteSpace: 'nowrap', border: `2px solid ${i <= step ? INK : DISABLED}`, background: i === step ? INK : i < step ? CARD : PAGE, color: i === step ? CARD : i < step ? INK : '#6B645C' }}>
              {i + 1} {label}
            </li>
          ))}
        </ol>

        <div style={{ padding: '18px 24px', display: 'flex', flexDirection: 'column', gap: 14 }}>
          {typedFirst && (phase.kind === 'idle' || (phase.kind === 'finished' && !(phase.outcome === 'waiting' && phase.previewId))) && spec.inputs.confirm === undefined ? (
            <Box bg={PAGE}>
              <p style={{ margin: 0, fontWeight: 600 }}>{spec.would}</p>
              {spec.cost ? <p style={{ margin: 0, fontSize: 13 }}>{spec.cost}</p> : null}
              {spec.action === 'skip-to-lsn' && spec.inputs.toLsn === undefined ? (
                <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                  <span style={{ ...caps(11, 0.12), color: MUTED }}>Target LSN</span>
                  <input value={toLsn} onChange={(e) => on.setToLsn(e.target.value.trim())} placeholder="e.g. 1A/80000000" style={INPUT} />
                  {toLsn && !normalizeLsn(toLsn) ? <span style={{ fontSize: 12, color: '#B3261E' }}>Not an LSN (X/Y in hex).</span> : null}
                </label>
              ) : null}
              {literal ? (
                <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                  <span style={{ fontSize: 13 }}>Type <code style={{ fontFamily: MONO }}>{literal}</code> to preview this action.</span>
                  <input value={typed} onChange={(e) => on.setTyped(e.target.value)} style={INPUT} aria-label="Confirmation" />
                </label>
              ) : null}
            </Box>
          ) : null}

          {(phase.kind === 'previewing' || phase.kind === 'awaiting_row') && !timedOut ? (
            <Box bg={PAGE} role="status">
              <p style={{ margin: 0, fontWeight: 600 }}>Preview Actions</p>
              <p style={{ margin: 0, fontSize: 13 }}>{previewText}</p>
            </Box>
          ) : null}
          {timedOut ? (
            <Box bg={STATUS.needs_action.bg} role="alert">
              <p style={{ margin: 0, fontWeight: 600 }}>No preview available</p>
              <p style={{ margin: 0, fontSize: 13 }}>Preview timed out after 3 minutes. Please try again.</p>
            </Box>
          ) : null}

          {phase.kind === 'ready' && !confirming ? (
            <>
              <Box bg={CARD} role="status">
                <p style={{ margin: 0, fontWeight: 600, fontSize: 16 }}>Preview Actions</p>
                <p style={{ margin: 0 }}><strong style={{ fontWeight: 600 }}>Impact:</strong> {spec.would}</p>
                {(() => {
                  const c = consequence(spec.action, spec.inputs, phase.result.detail);
                  return c ? <p style={{ margin: 0, fontSize: 13 }}>{c}</p> : null;
                })()}
              </Box>
              <Observed row={row} />
              <Lists row={row} />
              {spec.cost ? <p style={{ margin: 0, fontSize: 13 }}>{spec.cost}</p> : null}
              {expired ? <p role="status" style={{ margin: 0, fontSize: 13, fontWeight: 600 }}>{EXPIRED_NOTE}</p> : null}
            </>
          ) : null}
          {phase.kind === 'ready' && confirming ? (
            <>
              <Box bg={CARD}>
                <p style={{ margin: 0, fontWeight: 600, fontSize: 16 }}>Confirm {nameOf(spec)}</p>
                <p style={{ margin: 0 }}>{spec.would}</p>
                {(() => {
                  // the same consequence line, next to the typed confirmation
                  const c = consequence(spec.action, spec.inputs, phase.result.detail);
                  return c ? <p style={{ margin: 0, fontSize: 13, fontWeight: 600 }}>{c}</p> : null;
                })()}
                {spec.cost ? <p style={{ margin: 0, fontSize: 13 }}>{spec.cost}</p> : null}
              </Box>
              {nameNeeded ? (
                <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                  <span style={{ fontSize: 13 }}>Type <code style={{ fontFamily: MONO }}>{nameNeeded}</code> to confirm.</span>
                  <input value={confirmName} onChange={(e) => on.setConfirmName(e.target.value)} style={INPUT} aria-label="Confirmation" />
                </label>
              ) : null}
              {expired ? <p role="status" style={{ margin: 0, fontSize: 13, fontWeight: 600 }}>{EXPIRED_NOTE}</p> : null}
            </>
          ) : null}

          {phase.kind === 'continue' ? (
            <>
            <Box bg={STATUS.stale.bg}>
              <p style={{ margin: 0, fontWeight: 600 }}>Finish Required</p>
              <p style={{ margin: 0 }}>{plainReason(phase.result.detail)} Run it to finish.</p>
              <Steps steps={withoutListed(phase.result.nextSteps, row)} />
            </Box>
            <Lists row={row} />
            </>
          ) : null}

          {phase.kind === 'running' ? (
            <Box bg={PAGE} role="status">
              <p style={{ margin: 0, fontWeight: 600 }}>{waiting ? `${nameOf(spec)} completed, fetching result` : runningTitle(spec)}</p>
            </Box>
          ) : null}

          {view ? (
            <>
              <Box bg={view.bg} role="status">
                <p style={{ margin: 0, fontWeight: 600, fontSize: 16 }}>{view.title}</p>
                {view.text ? <p style={{ margin: 0 }}>{view.text}</p> : null}
                {view.next ? <p style={{ margin: 0, fontSize: 13 }}>{view.next}</p> : null}
                {phase.kind === 'finished' ? <Steps steps={withoutListed(phase.result?.nextSteps, row)} /> : null}
              </Box>
              {phase.kind === 'finished' && phase.step === 'run' ? <Lists row={row} real /> : null}
              {phase.kind === 'finished' && phase.step === 'preview' ? <Observed row={row} /> : null}
            </>
          ) : null}

          {startError ? <p role="alert" style={{ margin: 0, color: '#B3261E' }}>{startError}</p> : null}

          <div>
            <button type="button" aria-expanded={tech} onClick={() => on.setTech(!tech)} style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: 'none', border: 'none', padding: 0, ...caps(12, 0.12), color: INK }}>
              <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true" style={{ transform: `rotate(${tech ? 0 : -90}deg)` }}><path d="M4 6l4 4 4-4" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
              Run details
            </button>
            {tech ? (
              <dl style={{ margin: '10px 0 0', display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '10px 18px' }}>
                <Kv k="Flight" v={`${ACTIONS[spec.action].flight.replace('_', '-')} · ${flightId}`} />
                {Object.entries(current?.config ?? buildConfig(spec.action, inputs, { dryRun: true })).filter(([, v]) => v !== '').map(([k, v]) => <Kv key={k} k={k} v={v} />)}
                <Kv k="Run" v={current ? `${current.step === 'preview' ? 'dry run' : 'real run'} #${current.runNumber}` : '—'} />
                <Kv k="Audit row" v={current ? `${row ? 'matched' : 'waiting'} on request_id ${current.requestId}` : '—'} />
                <Kv k="Status" v={current ? waitDetail : '—'} />
                {row?.detail ? <Kv k="Flight’s detail" v={row.detail} /> : null}
              </dl>
            ) : null}
          </div>
        </div>

        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'center', justifyContent: 'flex-end', padding: '14px 24px 20px', borderTop: `2px solid ${INK}` }}>
          {phase.kind === 'ready' && confirming && countdown > 0 ? <span style={{ fontSize: 13, color: MUTED }}>Ready in {countdown} s</span> : null}
          {waiting ? <button type="button" onClick={() => on.stopWaiting()} style={BUTTON(CARD)}>Stop waiting</button> : null}
          {phase.kind !== 'running' ? (
            <button type="button" onClick={on.close} disabled={running} style={BUTTON(CARD, running)}>{phase.kind === 'ready' || phase.kind === 'previewing' || phase.kind === 'awaiting_row' || phase.kind === 'idle' ? 'Cancel' : 'Close'}</button>
          ) : null}
          {typedFirst && phase.kind === 'idle' ? (
            <button type="button" onClick={on.preview} disabled={!literalOk || startPending} style={BUTTON(INK, !literalOk)}><span style={{ color: literalOk ? CARD : undefined }}>Preview {spec.verb}</span></button>
          ) : null}
          {phase.kind === 'ready'
            ? readyButtons(confirming, expired).map((b) =>
                b === 'back' ? <button key={b} type="button" onClick={() => on.setConfirming(false)} style={BUTTON(CARD)}>Back</button>
                : b === 'continue' ? <button key={b} type="button" onClick={() => on.setConfirming(true)} style={{ ...BUTTON(INK), color: CARD }}>Continue</button>
                : b === 'run' ? <button key={b} type="button" onClick={on.run} disabled={runDisabled} style={{ ...BUTTON(INK, runDisabled), color: runDisabled ? '#6B645C' : CARD }}>Run</button>
                : <button key={b} type="button" onClick={on.preview} disabled={startPending || !literalOk} style={{ ...BUTTON(INK, !literalOk), color: CARD }}>Re-run Preview</button>)
            : null}
          {phase.kind === 'continue' ? (
            <button type="button" onClick={on.finishRun} disabled={startPending} style={{ ...BUTTON(INK), color: CARD }}>Finish</button>
          ) : null}
          {phase.kind === 'finished' && phase.outcome === 'waiting' && phase.previewId ? (
            <button type="button" onClick={on.runAgain} disabled={startPending} style={BUTTON(CARD)}>Run again</button>
          ) : null}
          {timedOut || (view && view.again) ? (
            <button type="button" onClick={on.preview} disabled={startPending || !literalOk} style={BUTTON(CARD, !literalOk)}>Preview again</button>
          ) : null}
        </div>
      </section>
    </div>
  );
}

const INPUT = { fontFamily: MONO, fontSize: 13, padding: '9px 12px', border: `2px solid ${INK}`, borderRadius: 10, background: CARD, color: INK, minHeight: 40, boxSizing: 'border-box' as const };

function Box({ bg, children, role }: { bg: string; children: ReactNode; role?: string }) {
  return <div role={role} style={{ display: 'flex', flexDirection: 'column', gap: 8, background: bg, border: `2px solid ${INK}`, borderRadius: 12, padding: '16px 18px' }}>{children}</div>;
}

function Kv({ k, v }: { k: string; v: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
      <dt style={{ ...caps(11, 0.12), color: MUTED }}>{k}</dt>
      <dd style={{ margin: 0, fontFamily: MONO, fontSize: 12, overflowWrap: 'anywhere' }}>{v}</dd>
    </div>
  );
}

/** A delete's next steps repeat its DBA statements ("the DBA runs: …"); the lists show them. */
const withoutListed = (steps: string[] | undefined, row: Row | null) =>
  row?.lists?.dba.length ? steps?.filter((x) => !x.startsWith('the DBA runs: ')) : steps;

/** The Flight's next steps, one per line. */
function Steps({ steps }: { steps?: string[] }) {
  if (!steps?.length) return null;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <span style={{ fontSize: 13 }}>Next:</span>
      <ul style={{ margin: 0, paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 4 }}>
        {steps.map((x, i) => <li key={i} style={{ fontFamily: MONO, fontSize: 12, overflowWrap: 'anywhere' }}>{x}</li>)}
      </ul>
    </div>
  );
}

/** delete-source's lists: what it drops itself, and the statements left for a DBA. A source
 * set up before the pipeline tracked what it created drops nothing itself. */
function Lists({ row, real = false }: { row: Row | null; real?: boolean }) {
  const l = row?.lists;
  if (!l) return null;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        <h3 style={{ margin: 0, ...caps(12, 0.12), fontWeight: 400, color: MUTED }}>{real ? 'Dropped' : 'It drops'}</h3>
        {l.drops.length ? (
          <ul style={{ margin: 0, paddingLeft: 18 }}>{l.drops.map((d) => <li key={d} style={{ fontFamily: MONO, fontSize: 12 }}>{d}</li>)}</ul>
        ) : (
          <p style={{ margin: 0, fontSize: 13 }}>Nothing itself: none of these objects is recorded as created by this pipeline.</p>
        )}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        <h3 style={{ margin: 0, ...caps(12, 0.12), fontWeight: 400, color: MUTED }}>Left for a DBA to run on Postgres</h3>
        {l.dba.length ? (
          l.dba.map((d, i) => <pre key={i} style={{ margin: 0, padding: '8px 10px', background: PAGE, border: `1.5px solid ${INK}`, borderRadius: 8, fontFamily: MONO, fontSize: 12, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{d}</pre>)
        ) : (
          <p style={{ margin: 0, fontSize: 13 }}>Nothing.</p>
        )}
      </div>
    </div>
  );
}

/** What the preview saw (`audit.result.observed`), as the Flight wrote it. */
function Observed({ row }: { row: Row | null }) {
  const facts = observedFacts(row?.observed ?? null);
  if (!facts.length) return null;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <h3 style={{ margin: 0, ...caps(12, 0.12), fontWeight: 400, color: MUTED }}>What the preview saw</h3>
      <dl style={{ margin: 0, display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '10px 18px' }}>
        {facts.map(([k, v]) => <Kv key={k} k={k} v={v} />)}
      </dl>
      {row?.atS ? <p style={{ margin: 0, fontSize: 12, color: MUTED }}>Previewed by {who(row.actor)} at {formatTs(row.atS)}.</p> : null}
    </div>
  );
}
