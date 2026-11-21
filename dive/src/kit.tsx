// Building blocks of the redesign's screens: pills, cards, the read-only notice, links that
// change the view, and the loading / error / empty states every query shares.
import { createContext, useContext, type CSSProperties, type ReactNode } from 'react';
import { useDiveState, useSQLQuery } from '@motherduck/react-sql-query';
import { rowsOf } from './format';
import { OWNER, readOnlyReason, VIEWER_SQL } from './owner';
import * as Q from './queries';
import { standbyRow, type StandbyRow } from './model';
import { card, caps, CARD, INK, MONO, MUTED, pill, RULE, SOURCE_STATUS, STATUS, TICK, type SourceStatus, type Status, type Tick } from './theme';

/** Where the viewer is: kept in the Dive's state, so a copied link reopens the same view. */
export type View = 'overview' | 'sources' | 'source' | 'tables' | 'activity' | 'help';
export function useView() {
  const [view, setView] = useDiveState<View>('view', 'overview');
  const [source, setSource] = useDiveState<string | null>('source', null);
  const [table, setTable] = useDiveState<string | null>('table', null);
  return {
    view, source, table,
    go: (v: View, s: string | null = null) => { setView(v); setSource(s); setTable(null); },
    openTable: (s: string, t: string) => { setSource(s); setTable(t); },
    closeTable: () => setTable(null),
  };
}

/** Bumped after every action run: each query's text changes, so every screen re-reads the
 * refreshed share (src/action.tsx provides it). */
export const RefreshTick = createContext(0);

/** Rows of a query, with what to render instead while it loads or fails. */
export function useRows(sql: string, opts: { enabled?: boolean } = {}) {
  const tick = useContext(RefreshTick);
  const r = useSQLQuery(tick ? `${sql}\n/* refresh ${tick} */` : sql, { enabled: opts.enabled ?? true });
  const rows = rowsOf(r.data);
  const fallback: ReactNode = r.isError
    ? <p role="alert" style={{ margin: 0, color: '#B3261E', fontSize: 13 }}>Couldn’t read the status: {String(r.error?.message ?? r.error)}</p>
    : r.isLoading && !r.data ? <div className="skeleton" /> : null;
  return { rows, fallback, loading: r.isLoading && !r.data };
}

/**
 * Every table's status, after probing which optional columns the ops db has (an older
 * writer's lacks some: queries.ts OPTIONAL_COLUMNS). A failed probe reads as none present,
 * which is always safe: the status then decides from the stored texts.
 */
export function useTableStatus() {
  // once per page, without the refresh tick: a schema doesn't change between refreshes
  const probe = useSQLQuery(Q.opsColumns());
  const ready = probe.isSuccess || probe.isError;
  const has = new Set(rowsOf<{ col: unknown }>(probe.data).map((r) => String(r.col)));
  const t = useRows(Q.tableStatus(Q.ALL, has), { enabled: ready });
  return ready ? t : { rows: [] as Record<string, unknown>[], fallback: <div className="skeleton" />, loading: true };
}

/** Read replica sources' diagnostics, by source (queries.ts sourceStandby), after the same
 * column probe: an ops db without `standby` has no read replica source. */
export function useSourceStandby(): Map<string, StandbyRow> {
  const probe = useSQLQuery(Q.opsColumns());
  const ready = probe.isSuccess || probe.isError;
  const has = new Set(rowsOf<{ col: unknown }>(probe.data).map((r) => String(r.col)));
  const t = useRows(Q.sourceStandby(Q.ALL, has), { enabled: ready });
  return new Map((ready ? t.rows : []).map((r) => { const x = standbyRow(r); return [x.source_id, x]; }));
}

/** Who may act: the owner injected at deploy, compared with the viewer (owner.ts). */
export function useReadOnly(): string | null {
  const r = useSQLQuery(VIEWER_SQL, { enabled: OWNER !== null });
  const name = rowsOf<{ viewer: string }>(r.data)[0]?.viewer ?? null;
  return readOnlyReason(OWNER, { loading: r.isLoading, error: r.isError, name });
}

export const StatusPill = ({ status }: { status: Status }) => <span style={pill(STATUS[status].bg)}>{STATUS[status].label}</span>;
export const SourcePill = ({ status, label }: { status: SourceStatus; label?: string }) => (
  <span style={pill(SOURCE_STATUS[status].bg)}>{label ?? SOURCE_STATUS[status].label}</span>
);

export function Card({ children, style, label }: { children: ReactNode; style?: CSSProperties; label?: string }) {
  return <section aria-label={label} style={{ ...card, ...style }}>{children}</section>;
}

export function SectionTitle({ children, id }: { children: ReactNode; id?: string }) {
  return <h3 id={id} style={{ margin: 0, fontWeight: 400, ...caps(13, 0.14) }}>{children}</h3>;
}

/** The read-only notice (every viewer but the owner). `what` is "row" or "section". */
export function ReadOnlyNotice({ reason, what }: { reason: string | null; what: string }) {
  if (!reason) return null;
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 16px', alignItems: 'center', justifyContent: 'space-between', border: `2px solid ${INK}`, borderRadius: 14, background: CARD, padding: '12px 18px' }}>
      <p style={{ margin: 0, fontSize: 14 }}>
        <strong style={{ fontWeight: 600 }}>{reason.split(':')[0]}:</strong>{reason.slice(reason.indexOf(':') + 1)} To hand off a problem, copy the link to its {what}.
      </p>
    </div>
  );
}

/** Copies the Dive's address: the view state travels with it. `select` first moves the
 * view to what the link is about (a row's drawer, a section), so the link reopens it. */
export function CopyLink({ label = 'Copy link', button = false, select }: { label?: string; button?: boolean; select?: () => void }) {
  const copy = () => {
    select?.();
    setTimeout(() => { void navigator.clipboard?.writeText(window.location.href); }, 0);
  };
  return button
    ? <button type="button" onClick={copy} style={{ ...caps(12, 0.1), padding: '8px 14px', border: `2px solid ${INK}`, borderRadius: 999, background: CARD, color: INK }}>{label}</button>
    : <button type="button" className="linkish" onClick={copy}>{label}</button>;
}

/** A source or table name that opens its page or drawer. */
export function NameLink({ children, onClick, size = 13 }: { children: ReactNode; onClick: () => void; size?: number }) {
  return (
    <button type="button" onClick={onClick} style={{ background: 'none', border: 'none', padding: 0, color: INK, fontFamily: MONO, fontSize: size, borderBottom: `1px solid ${RULE}`, textAlign: 'left', maxWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
      {children}
    </button>
  );
}

/** 28 marks, 6 h each, oldest first. */
export function Ticks({ ticks, label, tall = false }: { ticks: Tick[]; label: string; tall?: boolean }) {
  return (
    <span role="img" aria-label={label} style={{ display: 'flex', gap: tall ? 3 : 2, alignItems: 'center' }}>
      {ticks.map((t, i) => {
        const c = TICK[t];
        return (
          <span key={i} style={{
            display: 'inline-block', width: tall ? undefined : 5, flex: tall ? '1 1 0' : undefined, maxWidth: tall ? 16 : undefined,
            height: tall ? 28 : 16, borderRadius: tall ? 2 : 1, background: c.bg, boxSizing: 'border-box',
            border: 'border' in c ? `1px solid ${c.border}` : undefined,
          }} />
        );
      })}
    </span>
  );
}

export function TickLegend() {
  return (
    <span style={{ display: 'inline-flex', flexWrap: 'wrap', gap: '10px 22px', alignItems: 'center' }}>
      <span style={caps(12, 0.13)}>Sync history (6 h each)</span>
      {(Object.keys(TICK) as Tick[]).map((t) => (
        <span key={t} style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
          <Ticks ticks={[t]} label="" />
          {TICK[t].label}
        </span>
      ))}
    </span>
  );
}

/** Label over value, as the design's fact grids. */
export function Fact({ k, v, mono = true }: { k: ReactNode; v: ReactNode; mono?: boolean }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, minWidth: 0 }}>
      <dt style={{ ...caps(11, 0.12), color: MUTED }}>{k}</dt>
      <dd style={{ margin: 0, fontFamily: mono ? MONO : undefined, fontSize: mono ? 13 : 14, overflowWrap: 'anywhere' }}>{v}</dd>
    </div>
  );
}

export const LAG_NOTE = 'Status is read from operational logs that can lag or miss runs; actions always re-check before changing anything.';
export const SOURCE_CLOCK = 'Commit time on the source database.';
