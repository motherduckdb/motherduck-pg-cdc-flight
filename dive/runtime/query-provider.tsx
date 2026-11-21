// Adapted from motherduckdb/md-migration-assessment (dive/runtime/query-provider.tsx, e4ae932), Copyright 2026 MotherDuck, Apache-2.0. Modified by MotherDuck, 2026; see dive/THIRD_PARTY.md.
/**
 * Local-mode implementation of the `@motherduck/react-sql-query` surface.
 *
 * The Dive source imports `useSQLQuery`, `useDiveState` and `useExport` from
 * that package. In MotherDuck the platform provides them; locally Vite aliases
 * the package to this file, which answers the same API against the loopback
 * query endpoint (`api/query` relative to the page, served by the Vite dev
 * proxy over the preview `cdc_ops`).
 *
 * Kept deliberately close to the production semantics the Dive relies on:
 * `data` is the row array or undefined; results of a previous query stay
 * visible while a new one loads; `useDiveState` persists to the URL hash so a
 * local link reproduces a view.
 */
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { decodeState, encodeState, type StateBag } from './state-codec.ts';

type Row = Record<string, unknown>;
type QueryStatus = 'idle' | 'loading' | 'success' | 'error';

export type ExportQueryOptions = {
  format: 'csv' | 'json' | 'parquet' | 'xlsx';
  title?: string;
  filename?: string;
  [key: string]: unknown;
};

export type UseSQLQueryResult<TData> = {
  data: TData | undefined;
  isLoading: boolean;
  isSuccess: boolean;
  isError: boolean;
  isPlaceholderData: boolean;
  error: Error | null;
  refetch: () => void;
  exportAs: (options: ExportQueryOptions) => Promise<void>;
  status: QueryStatus;
};

type UseSQLQueryOptions<TData> = {
  enabled?: boolean;
  select?: (rows: Row[]) => TData;
  initialData?: TData;
  placeholderData?: TData | ((previous: TData | undefined) => TData | undefined);
};

// ── query endpoint ────────────────────────────────────────────────────────────

function endpoint(name: string): string {
  return new URL(name, document.baseURI).toString();
}

export async function runQuery(sql: string, signal?: AbortSignal): Promise<Row[]> {
  // MotherDuck-only: the local preview reads a file, which needs no refresh.
  if (/^\s*REFRESH\s+DATABASE/i.test(sql)) return [];
  const res = await fetch(endpoint('api/query'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
    signal,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error ?? `query failed (${res.status})`);
  return body.data as Row[];
}

// ── useSQLQuery ───────────────────────────────────────────────────────────────

export function useSQLQuery<TData = Row[]>(
  sql: string,
  options?: UseSQLQueryOptions<TData>,
): UseSQLQueryResult<TData> {
  const enabled = options?.enabled ?? true;
  const select = options?.select;
  const [rows, setRows] = useState<Row[] | undefined>(undefined);
  const [status, setStatus] = useState<QueryStatus>('idle');
  const [error, setError] = useState<Error | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const seq = useRef(0);

  const execute = useCallback(async () => {
    if (!enabled || !sql.trim()) {
      setStatus('idle');
      return;
    }
    const id = ++seq.current;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setStatus('loading');
    setError(null);
    try {
      const result = await runQuery(simulatedAudit(sql), controller.signal);
      if (id !== seq.current) return;
      setRows(result);
      setStatus('success');
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || id !== seq.current) return;
      setError(err instanceof Error ? err : new Error(String(err)));
      setStatus('error');
    }
  }, [sql, enabled]);

  useEffect(() => {
    execute();
    return () => abortRef.current?.abort();
  }, [execute]);

  const data = useMemo<TData | undefined>(() => {
    if (rows === undefined) return options?.initialData;
    return select ? select(rows) : (rows as unknown as TData);
  }, [rows, select, options?.initialData]);

  const exportAs = useCallback(async (opts: ExportQueryOptions) => {
    console.info('export requested in local mode', { sql, opts });
    window.alert(
      `Exports (${opts.format}) are available when the dashboard runs as a MotherDuck Dive.`,
    );
  }, [sql]);

  return {
    data,
    isLoading: status === 'loading',
    isSuccess: status === 'success',
    isError: status === 'error',
    isPlaceholderData: false,
    error,
    refetch: execute,
    exportAs,
    status,
  };
}

// ── useDiveState: URL-hash persisted state shared across call sites ───────────

const listeners = new Set<() => void>();
let bag: StateBag = decodeState(window.location.hash);

function writeHash(next: StateBag) {
  bag = next;
  const encoded = encodeState(next);
  const hash = encoded ? `#${encoded}` : '';
  try {
    window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}${hash}`);
  } catch {
    // history API unavailable: state still lives in `bag` for this page
  }
  listeners.forEach((l) => l());
}

window.addEventListener('hashchange', () => {
  bag = decodeState(window.location.hash);
  listeners.forEach((l) => l());
});

function subscribe(l: () => void) {
  listeners.add(l);
  return () => {
    listeners.delete(l);
  };
}

export function useDiveState<T>(key: string, initial: T): [T, (value: T | undefined) => void] {
  const snapshot = useSyncExternalStore(subscribe, () => bag, () => bag);
  const value = (key in snapshot ? snapshot[key] : initial) as T;
  const set = useCallback(
    (v: T | undefined) => {
      const next = { ...bag };
      if (v === undefined) delete next[key];
      else next[key] = v;
      writeHash(next);
    },
    [key],
  );
  return [value, set];
}

// ── useExport ─────────────────────────────────────────────────────────────────

export function useExport() {
  const exportQuery = useCallback(async ({ sql, ...opts }: { sql: string } & ExportQueryOptions) => {
    console.info('export requested in local mode', { sql, opts });
    window.alert(
      `Exports (${opts.format}) are available when the dashboard runs as a MotherDuck Dive.`,
    );
  }, []);
  return { exportQuery };
}

// ── useRunFlight / useFlightRun: local simulation of the Flight-run hooks ─────────
// The preview can't run Flights. A simulated run finishes after a moment with an exit
// code chosen from the config, so every branch of the action panel can be seen:
// a table name containing "refuse" exits 2, "busy" exits 3, "fail" exits 1, else 0.
// No audit row is written (the preview database is read-only): a simulated run's request
// id is read as one of the preview database's example rows (scripts/dive_preview.py
// EXAMPLES): a dry run as its action's `would` row, a real run as the
// row for its outcome (done, refused, busy; a failed run has none). Each row shows only
// SIMULATED_LAG_MS after the run finishes, as the share's lag would, so the panel's
// "waiting for the result" state can be seen too.

const EXAMPLE_REQUEST = 'dive-example-request';
const SIMULATED_LAG_MS = 6_000;
const REAL_ROW: Record<number, string> = { 0: 'dive-example-done', 2: 'dive-example-refused', 3: 'dive-example-busy' };
const simulatedRows = new Map<string, { as: string; visibleAt: number }>();
const simulatedAudit = (sql: string): string =>
  sql.replace(/request_id = '([A-Za-z0-9_-]{1,64})'/, (match, id: string) => {
    const s = simulatedRows.get(id);
    return s && Date.now() >= s.visibleAt ? `request_id = '${s.as}'` : match;
  });

type SimulatedRun = { runId: string; runNumber: number; exitCode: number; finishAt: number };
const simulated = new Map<string, SimulatedRun>();
let nextRunNumber = 1;

export function useRunFlight(flightId: string) {
  const [data, setData] = useState<{ runId: string; runNumber: number } | undefined>(undefined);
  const [isPending, setPending] = useState(false);
  const runAsync = useCallback(
    async (config: Record<string, string> = {}) => {
      setPending(true);
      const table = config.CDC_TABLE ?? '';
      const exitCode = /refuse/.test(table) ? 2 : /busy/.test(table) ? 3 : /fail/.test(table) ? 1 : 0;
      // A dry run gets the preview database's example dry-run id (queries.EXAMPLE_RUN),
      // whose audit row is a `would`, so the confirm step can be seen too.
      const runId = config.CDC_DRY_RUN === '1' ? '00000000-0000-4000-8000-000000000000' : crypto.randomUUID();
      const run = { runId, runNumber: nextRunNumber++, exitCode, finishAt: Date.now() + 800 };
      // a dry run reads its own action's example `would` row (scripts/dive_preview.py WOULD)
      const would = config.CDC_ACTION ? `dive-example-would-${config.CDC_ACTION}` : EXAMPLE_REQUEST;
      const as = config.CDC_DRY_RUN === '1' ? (exitCode === 0 ? would : REAL_ROW[exitCode]) : REAL_ROW[exitCode];
      if (config.CDC_REQUEST_ID && as) simulatedRows.set(config.CDC_REQUEST_ID, { as, visibleAt: run.finishAt + SIMULATED_LAG_MS });
      simulated.set(`${flightId}:${run.runNumber}`, run);
      setData({ runId: run.runId, runNumber: run.runNumber });
      setPending(false);
      return { runId: run.runId, runNumber: run.runNumber };
    },
    [flightId],
  );
  const run = useCallback((config?: Record<string, string>) => void runAsync(config), [runAsync]);
  return { run, runAsync, mutate: run, mutateAsync: runAsync, data, isPending, error: null as Error | null };
}

export function useFlightRun(
  flightId: string,
  runNumber: number | undefined,
  options?: { onFinished?: (run: { status: string; exitCode: number; runNumber: number }) => void },
) {
  const [now, setNow] = useState(Date.now());
  const run = runNumber === undefined ? undefined : simulated.get(`${flightId}:${runNumber}`);
  const finished = run !== undefined && now >= run.finishAt;
  useEffect(() => {
    if (!run || finished) return;
    const timer = setTimeout(() => setNow(Date.now()), Math.max(0, run.finishAt - Date.now()) + 20);
    return () => clearTimeout(timer);
  }, [run, finished]);
  const status = run === undefined ? undefined : finished ? (run.exitCode === 0 ? 'SUCCEEDED' : 'FAILED') : 'RUNNING';
  const reported = useRef<number | undefined>(undefined);
  useEffect(() => {
    if (finished && run && reported.current !== run.runNumber) {
      reported.current = run.runNumber;
      options?.onFinished?.({ status: status!, exitCode: run.exitCode, runNumber: run.runNumber });
    }
  }, [finished, run, status, options]);
  return {
    status,
    exitCode: finished ? run!.exitCode : null,
    isFinished: finished,
    isChecking: run !== undefined && !finished,
    error: null as Error | null,
    check: () => setNow(Date.now()),
  };
}
