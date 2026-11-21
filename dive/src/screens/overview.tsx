// Overview (the landing screen) and the Sources tab: one card per source, problems first,
// tables within each source problems first. The Sources tab shows the same cards collapsed.
import { useState } from 'react';
import { useDiveState } from '@motherduck/react-sql-query';
import { formatInt, formatTs } from '../format';
import { CopyLink, LAG_NOTE, NameLink, ReadOnlyNotice, SOURCE_CLOCK, SourcePill, StatusPill, TickLegend, Ticks, useReadOnly, useRows, useSourceStandby, useTableStatus, useView } from '../kit';
import { byProblem, details, isIssue, lastUpdated, shownStatus, sourceHeadline, sourceName, sourceRow, tableRow, ticksOf, usually, visible, wal, type SourceRow, type TableRow } from '../model';
import * as Q from '../queries';
import { caps, card, CARD, INK, LABEL, MONO, MUTED, PAGE, RULE, STATUS, toggle, walColor, type Tick } from '../theme';

const GRID = '150px 230px minmax(0, 1fr) 200px 190px 100px 120px';

/** Every source with its tables and marks, read once, sorted problems first. */
export function useSources() {
  const src = useRows(Q.sourceStatus(Q.ALL));
  const tbl = useTableStatus();
  const mk = useRows(Q.syncMarks(Q.ALL));
  const standby = useSourceStandby();
  const tables = tbl.rows.map(tableRow);
  const marks = new Map(mk.rows.map((r) => [`${r.source_id}\u0000${r.table_name}`, ticksOf(r.marks)]));
  const list = src.rows.map(sourceRow).map((s) => {
    const own = tables.filter((t) => t.source_id === s.source_id);
    return { s, tables: own.filter(visible).sort(byProblem), head: sourceHeadline(s, own), replica: standby.has(s.source_id) };
  });
  list.sort((a, b) => a.head.rank - b.head.rank || sourceName(a.s).localeCompare(sourceName(b.s)));
  return { list, marks, fallback: src.fallback ?? tbl.fallback, loading: src.loading || tbl.loading };
}

export function StatusAsOf() {
  const { rows } = useRows(Q.statusAsOf(Q.ALL));
  return (
    <p style={{ margin: 0, fontSize: 14, color: MUTED }}>
      Status as of <strong style={{ color: INK, fontWeight: 600 }}>{formatTs(rows[0]?.as_of_at_s)}</strong>
    </p>
  );
}

export function Overview() {
  const reason = useReadOnly();
  const { list, marks, fallback } = useSources();
  const [pick, setPick] = useDiveState<string>('pick', 'all');
  const [only, setOnly] = useDiveState<boolean>('only', false);
  const shown = list.filter((x) => pick === 'all' || x.s.source_id === pick);
  return (
    <>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '14px 24px', justifyContent: 'space-between', alignItems: 'center' }}>
        <StatusAsOf />
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 18, alignItems: 'center' }}>
          <div role="group" aria-label="Source" style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
            <span style={{ ...caps(11, 0.14), color: MUTED, marginRight: 4 }}>Source</span>
            {[{ id: 'all', label: 'All' }, ...list.map((x) => ({ id: x.s.source_id, label: sourceName(x.s) }))].map((p) => (
              <button key={p.id} type="button" aria-pressed={pick === p.id} onClick={() => setPick(p.id)} style={toggle(pick === p.id)}>{p.label}</button>
            ))}
          </div>
          <div role="group" aria-label="Show" style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <span style={{ ...caps(11, 0.14), color: MUTED, marginRight: 4 }}>Show</span>
            <button type="button" aria-pressed={!only} onClick={() => setOnly(false)} style={toggle(!only)}>All tables</button>
            <button type="button" aria-pressed={only} onClick={() => setOnly(true)} style={toggle(only)}>Only issues</button>
          </div>
        </div>
      </div>
      <ReadOnlyNotice reason={reason} what="row" />
      {fallback}
      {!fallback && !list.length ? <p style={{ margin: 0, color: MUTED }}>No source has reported status yet.</p> : null}
      {shown.map((x) => <SourceCard key={x.s.source_id} {...x} marks={marks} only={only} readOnly={reason !== null} />)}
      <footer style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 22px', alignItems: 'center', fontSize: 12, color: MUTED, paddingTop: 4 }}>
        <TickLegend />
        <span>{LAG_NOTE}</span>
      </footer>
    </>
  );
}

/** The Sources tab: the Overview's cards, collapsed; one source goes straight to its page. */
export function Sources() {
  const { list, marks, fallback, loading } = useSources();
  const v = useView();
  if (!loading && list.length === 1) {
    queueMicrotask(() => v.go('source', list[0].s.source_id));
    return null;
  }
  return (
    <>
      <StatusAsOf />
      {fallback}
      {list.map((x) => <SourceCard key={x.s.source_id} {...x} marks={marks} only={false} readOnly collapsedOnly />)}
      <footer style={{ fontSize: 12, color: MUTED }}>{LAG_NOTE}</footer>
    </>
  );
}

export function WalMeter({ s }: { s: SourceRow }) {
  const w = wal(s);
  return (
    <span title="How much of Postgres’s WAL limit this pipeline is using. When it reaches 100%, Postgres invalidates the replication slot and all tables must be rebuilt." style={{ display: 'inline-flex', alignItems: 'center', gap: 8, fontSize: 13, cursor: 'help' }}>
      <span style={{ color: MUTED, borderBottom: `1px dotted ${MUTED}` }}>WAL limit</span>
      <span aria-hidden="true" style={{ display: 'inline-block', width: 72, height: 10, border: `1.5px solid ${INK}`, borderRadius: 2, background: PAGE, overflow: 'hidden' }}>
        <span style={{ display: 'block', height: '100%', width: `${Math.min(100, w.pct ?? 0)}%`, background: walColor(w.pct ?? 0) }} />
      </span>
      <span style={{ fontFamily: MONO, fontSize: 12 }}>{w.label}</span>
    </span>
  );
}

export function LastSync({ s, failed = false }: { s: SourceRow; failed?: boolean }) {
  const every = usually(s);
  return (
    <span style={{ fontSize: 13, color: MUTED }}>
      Last sync <span style={{ fontFamily: MONO, fontSize: 12, color: INK }}>{formatTs(s.last_sync_at_s)}{failed && s.last_sync_status === 'failed' ? ' (failed)' : ''}</span>
      {every ? ` · ${every}` : ''}
    </span>
  );
}

function SourceCard({ s, tables, head, marks, only, readOnly, collapsedOnly = false, replica = false }: {
  replica?: boolean;
  s: SourceRow; tables: TableRow[]; head: ReturnType<typeof sourceHeadline>; marks: Map<string, Tick[]>;
  only: boolean; readOnly: boolean; collapsedOnly?: boolean;
}) {
  const v = useView();
  const [open, setOpen] = useState(true);
  const expanded = open && !collapsedOnly;
  const rows = only ? tables.filter((t) => isIssue(shownStatus(t))) : tables;
  const name = sourceName(s);
  return (
    <section aria-label={`Source ${name}`} style={{ ...card, overflow: 'hidden' }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '14px 24px', alignItems: 'center', justifyContent: 'space-between', padding: '16px 20px', background: CARD }}>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 16px', alignItems: 'center', minWidth: 0, flex: '1 1 520px' }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            {collapsedOnly ? null : (
              <button type="button" aria-expanded={open} aria-label={`Show or hide tables for ${name}`} onClick={() => setOpen(!open)} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 32, height: 32, background: 'transparent', border: 'none', padding: 0 }}>
                <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true" style={{ transition: 'transform 120ms', transform: `rotate(${open ? 0 : -90}deg)` }}>
                  <path d="M4 6l4 4 4-4" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button>
            )}
            <button type="button" onClick={() => v.go('source', s.source_id)} style={{ background: 'none', border: 'none', padding: 0, color: INK, fontFamily: LABEL, fontSize: 22, letterSpacing: '0.02em', borderBottom: `2px solid ${RULE}` }}>{name}</button>
            {replica ? <span title="Captured from a read replica (standby)" style={{ ...caps(10, 0.1), padding: '1px 6px', borderRadius: 999, border: `1.5px solid ${INK}`, whiteSpace: 'nowrap', marginLeft: 6 }}>Read replica</span> : null}
          </span>
          {s.label && (s.description || s.owner_user) ? (
            <span style={{ fontSize: 13, color: MUTED }}>{[s.description, s.owner_user ? `contact ${s.owner_user.split('@')[0]}` : null].filter(Boolean).join(' · ')}</span>
          ) : null}
          <SourcePill status={head.status} label={head.label} />
          <span style={{ fontSize: 14, minWidth: 0 }}>{head.headline}</span>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 18px', alignItems: 'center' }}>
          <LastSync s={s} />
          <WalMeter s={s} />
          {head.rank === 0 && readOnly ? <CopyLink select={() => v.go('source', s.source_id)} /> : null}
          {wal(s).lost && !readOnly ? (
            <button type="button" onClick={() => v.go('source', s.source_id)} style={{ ...caps(12, 0.1), padding: '8px 14px', border: `2px solid ${INK}`, borderRadius: 999, background: STATUS.needs_action.bg, color: INK }}>Recover slot</button>
          ) : null}
        </div>
      </div>
      {head.detail ? <p style={{ margin: 0, padding: '0 20px 14px 50px', fontSize: 14 }}>{head.detail}</p> : null}
      {expanded ? (
        <div style={{ overflowX: 'auto', borderTop: `2px solid ${INK}` }}>
          <div style={{ minWidth: 1120 }}>
            <div style={{ display: 'grid', gridTemplateColumns: GRID, gap: 16, alignItems: 'center', padding: '8px 20px', background: PAGE, ...caps(11, 0.13) }}>
              <span>Status</span><span>Table</span><span>Details</span>
              <span title={SOURCE_CLOCK} style={{ cursor: 'help', borderBottom: `1px dotted ${INK}`, justifySelf: 'start' }}>Last updated</span>
              <span>Syncs, last 7 days</span><span style={{ textAlign: 'right' }}>Changes, 7 d</span><span />
            </div>
            {rows.map((t) => {
              const st = shownStatus(t);
              const d = details(t);
              const quiet = st === 'ok' || st === 'waiting';
              return (
                <div key={t.table_name} className="rowhover" style={{ display: 'grid', gridTemplateColumns: GRID, gap: 16, alignItems: 'center', padding: `${quiet ? 7 : 12}px 20px`, borderTop: `1px solid ${RULE}`, color: st === 'waiting' ? MUTED : INK }}>
                  <span>{st === 'unknown' ? <span style={{ ...pillUnknown }}>Unknown</span> : <StatusPill status={st} />}</span>
                  <span style={{ minWidth: 0 }}><NameLink onClick={() => v.openTable(s.source_id, t.table_name)}>{t.table_name}</NameLink></span>
                  <span style={{ minWidth: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <span>{d.why}</span>
                    {d.since ? <span style={{ fontSize: 12, color: MUTED }}>{d.since}</span> : null}
                  </span>
                  <span style={{ fontFamily: MONO, fontSize: 12 }}>{lastUpdated(t)}</span>
                  <Ticks ticks={marks.get(`${s.source_id}\u0000${t.table_name}`) ?? []} label={`Sync results over the last 7 days for ${t.table_name}`} />
                  <span style={{ fontFamily: MONO, fontSize: 13, textAlign: 'right' }}>{t.kind === 'not_enabled' ? '—' : formatInt(t.changes_7d)}</span>
                  <span style={{ textAlign: 'right' }}>
                    {isIssue(st) && readOnly ? <CopyLink select={() => v.openTable(s.source_id, t.table_name)} /> : null}
                    {isIssue(st) && !readOnly ? (
                      <button type="button" onClick={() => v.openTable(s.source_id, t.table_name)} style={rowAction(st)}>{rowActionLabel(t)}</button>
                    ) : null}
                  </span>
                </div>
              );
            })}
            {!rows.length ? <p style={{ margin: 0, padding: '14px 20px', borderTop: `1px solid ${RULE}`, color: MUTED }}>No issues on {name} · {tables.length} tables replicating.</p> : null}
          </div>
        </div>
      ) : null}
    </section>
  );
}

/** The owner's row button (the design's Fix / Resume / Enable): it opens the drawer, whose
 * options preview the action. */
export const rowActionLabel = (t: TableRow): string =>
  t.kind === 'paused' ? 'Resume' : t.kind === 'not_enabled' ? 'Enable' : t.kind === 'rebuilding' ? 'Progress' : t.kind === 'stale' ? 'Details' : 'Fix';
const rowAction = (st: string) => ({ ...caps(12, 0.1), padding: '7px 14px', border: `2px solid ${INK}`, borderRadius: 999, color: INK, background: st === 'needs_action' ? STATUS.needs_action.bg : st === 'stale' || st === 'paused' ? STATUS.stale.bg : CARD });

/** Unknown states have no designed pill yet (open item): plain grey, labelled. */
const pillUnknown = { ...caps(11, 0.1), padding: '3px 9px', borderRadius: 999, border: `1.5px dashed ${INK}`, background: STATUS.waiting.bg, color: INK } as const;
