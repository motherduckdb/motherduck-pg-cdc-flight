// The Source page: one source's pipeline, WAL limit and slot, recovery, tables, change log
// retention, setup check and danger zone (read-only until phase 2 brings the actions).
import { useState, type ReactNode } from 'react';
import { formatAge, formatBytes, formatInt, formatTs, listOf, N, S } from '../format';
import { Card, CopyLink, Fact, LAG_NOTE, NameLink, ReadOnlyNotice, SectionTitle, SOURCE_CLOCK, SourcePill, StatusPill, useReadOnly, useRows, useSourceStandby, useTableStatus, useView } from '../kit';
import { byProblem, conflictReason, lastSuccessLine, noSuccess, details, READ_REPLICA_FIRST, REPLICA_WAITS, STANDBY_FOOTER, standbyView, type StandbyRow, extraSlots, lastUpdated, slotRole, tlsText, type SlotRow, recovery, shownStatus, sourceHeadline, sourceName, sourceRow, tableRow, usually, visible, wal, who, type Recovery, type SourceRow, type TableRow } from '../model';
import { OWNER } from '../owner';
import * as Q from '../queries';
import { caps, card, CARD, INK, MONO, MUTED, PAGE, pill, RULE, SOURCE_STATUS, STATUS, walColor, type SourceStatus } from '../theme';
import { LastSync, rowActionLabel, StatusAsOf } from './overview';
import { ActionButton, type ActionSpec } from '../action';
import { finishRecovery, fixOptions } from '../fixes';

const day = (d: unknown) => { const s = S(d); return /^\d{8}$/.test(s) ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6)}` : s; };
type Stage = { name: string; flight: boolean; status: SourceStatus; label: string; sub: string; line1: string; line2: string };

export function SourcePage() {
  const v = useView();
  const reason = useReadOnly();
  const src = useRows(Q.sourceStatus(Q.ALL));
  const tbl = useTableStatus();
  const pipe = useRows(Q.sourcePipeline(Q.ALL));
  const standby = useSourceStandby();
  const [dangerOpen, setDangerOpen] = useState<boolean | null>(null);
  const s = src.rows.map(sourceRow).find((x) => x.source_id === v.source);
  if (src.fallback) return <>{src.fallback}</>;
  if (!s) return <p style={{ margin: 0, color: MUTED }}>No status for source {v.source ?? '—'}.</p>;
  const tables = tbl.rows.map(tableRow).filter((t) => t.source_id === s.source_id);
  const shown = tables.filter(visible).sort(byProblem);
  const enabled = shown.filter((t) => t.kind !== 'not_enabled');
  const notEnabled = shown.filter((t) => t.kind === 'not_enabled');
  const head = sourceHeadline(s, tables);
  const rec = recovery(s, shown);
  const p = pipe.rows.find((r) => r.source_id === s.source_id) ?? {};
  const w = wal(s);
  const replica = standby.get(s.source_id) ?? null;
  const conflict = conflictReason(s);
  const danger = dangerOpen ?? w.lost;
  const summary = [...new Set(enabled.map((t) => shownStatus(t)))]
    .map((k) => `${enabled.filter((t) => shownStatus(t) === k).length} ${k === 'ok' ? 'ok' : k.replace('_', ' ')}`)
    .join(' · ');
  return (
    <>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <p style={{ margin: 0, fontSize: 13, color: MUTED }}><button type="button" className="linkish" onClick={() => v.go('sources')}>Sources</button> / {s.source_id}</p>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px 18px', alignItems: 'center' }}>
          <h2 style={{ margin: 0, fontFamily: 'Cousine, ui-monospace, Menlo, monospace', fontWeight: 400, fontSize: 34, letterSpacing: '0.02em' }}>{sourceName(s)}</h2>
          {s.label && (s.description || s.owner_user) ? <span style={{ fontSize: 14, color: MUTED }}>{[s.description, s.owner_user ? `contact ${who(s.owner_user)}` : null].filter(Boolean).join(' · ')}</span> : null}
          <SourcePill status={head.status} label={head.label} />
          <span style={{ fontSize: 15 }}>{head.headline}</span>
        </div>
        {replica ? <p style={{ margin: 0, fontSize: 14 }}>Captured from a {READ_REPLICA_FIRST}</p> : null}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 24px', fontSize: 13, color: MUTED }}>
          <StatusAsOf />
          <LastSync s={s} failed />
          {OWNER ? <span>Owner <strong style={{ color: INK, fontWeight: 600 }}>{who(OWNER)}</strong></span> : null}
        </div>
      </div>

      <ReadOnlyNotice reason={reason} what="section" />

      {w.lost ? (
        <section aria-label={conflict != null ? 'Replication slot invalidated by a recovery conflict' : 'Replication slot invalidated'} style={{ ...card, display: 'flex', flexWrap: 'wrap', gap: '14px 28px', alignItems: 'center', justifyContent: 'space-between', background: STATUS.needs_action.bg, padding: '20px 24px' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6, flex: '1 1 560px', minWidth: 0 }}>
            <h3 style={{ margin: 0, ...caps(15, 0.12), fontWeight: 700 }}>{conflict != null ? 'Replication slot invalidated by a recovery conflict' : 'Replication slot invalidated'}</h3>
            <p style={{ margin: 0 }}>{s.ingest_error ? `Sync failing: ${s.ingest_error}` : 'Sync failing'}{s.failing_since_at_s ? ` · last failure ${formatTs(s.failing_since_at_s)}` : ''}</p>
            {conflict != null ? (
              <p style={{ margin: 0 }}>The read replica dropped the slot to resolve a conflict with activity on the primary{conflict ? ` (reason: ${conflict})` : ''}. Turn hot_standby_feedback back on if it was off, then run Recover slot: it recreates the slot on the read replica and starts a new epoch. All {enabled.length} tables are then rebuilt; they keep their data as of {formatTs(s.durable_at_s)} until each one finishes.</p>
            ) : (
              <p style={{ margin: 0 }}>Postgres removed WAL this source had not synced yet. All {enabled.length} tables must be rebuilt. Replica tables keep their data as of {formatTs(s.durable_at_s)} until each one is rebuilt.</p>
            )}
          </div>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
            <button type="button" onClick={() => { setDangerOpen(true); document.getElementById('danger')?.scrollIntoView(); }} style={{ ...caps(12, 0.1), padding: '10px 16px', border: `2px solid ${INK}`, borderRadius: 999, background: CARD, color: INK }}>Go to Recover slot</button>
            <span style={{ fontSize: 13 }}>Runbook R1</span>
          </div>
        </section>
      ) : null}

      <Pipeline s={s} p={p} enabled={enabled} rec={rec} replica={replica != null} />

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 24, alignItems: 'stretch' }}>
        <WalCard s={s} />
        {replica ? <ReplicaCard r={replica} lastRun={s.last_sync_status} /> : null}
        {rec ? <RecoveryCard s={s} rec={rec} total={enabled.length} /> : null}
      </div>

      <section aria-labelledby="tbl-h" style={{ ...card, overflow: 'hidden' }}>
        <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, alignItems: 'baseline', padding: '18px 22px' }}>
          <SectionTitle id="tbl-h">Tables · {enabled.length} enabled</SectionTitle>
          <span style={{ fontSize: 13, color: MUTED }}>{summary}</span>
        </div>
        <div style={{ overflowX: 'auto', borderTop: `2px solid ${INK}` }}>
          <div style={{ minWidth: 980 }}>
            <TableHeader />
            {enabled.map((t) => <TableLine key={t.table_name} t={t} readOnly={reason !== null} />)}
            {notEnabled.length ? (
              <>
                <div style={{ padding: '8px 22px', background: PAGE, borderTop: `2px solid ${INK}`, ...caps(11, 0.13) }}>Not enabled · {notEnabled.length}</div>
                {notEnabled.map((t) => <TableLine key={t.table_name} t={t} readOnly={reason !== null} />)}
              </>
            ) : null}
          </div>
        </div>
      </section>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 24, alignItems: 'stretch' }}>
        <RetentionCard s={s} p={p} />
        <SetupCard s={s} />
      </div>

      <DangerZone s={s} open={danger} toggle={() => setDangerOpen(!danger)} tables={enabled} readOnly={reason !== null} />

      <footer style={{ fontSize: 12, color: MUTED }}>{LAG_NOTE} Times in Last updated are commit times on the source database.</footer>
    </>
  );
}

const GRID = '150px 220px minmax(0, 1fr) 210px 120px';
function TableHeader() {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: GRID, gap: 16, alignItems: 'center', padding: '8px 22px', background: PAGE, ...caps(11, 0.13) }}>
      <span>Status</span><span>Table</span><span>Details</span>
      <span title={SOURCE_CLOCK} style={{ cursor: 'help', borderBottom: `1px dotted ${INK}`, justifySelf: 'start' }}>Last updated</span><span />
    </div>
  );
}
function TableLine({ t, readOnly }: { t: TableRow; readOnly: boolean }) {
  const v = useView();
  const st = shownStatus(t);
  const d = details(t);
  return (
    <div className="rowhover" style={{ display: 'grid', gridTemplateColumns: GRID, gap: 16, alignItems: 'center', padding: '9px 22px', borderTop: `1px solid ${RULE}`, color: st === 'waiting' ? MUTED : INK }}>
      <span>{st === 'unknown' ? 'Unknown' : <StatusPill status={st} />}</span>
      <span style={{ minWidth: 0, display: 'flex', flexWrap: 'wrap', gap: '4px 8px', alignItems: 'center' }}>
        <NameLink onClick={() => v.openTable(t.source_id, t.table_name)}>{t.table_name}</NameLink>
        {t.capture_mode === 'append' ? <span title="Insert-only (append mode)" style={{ ...caps(10, 0.1), padding: '1px 6px', borderRadius: 999, border: `1.5px solid ${INK}`, whiteSpace: 'nowrap' }}>Insert-only</span> : null}
      </span>
      <span style={{ minWidth: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
        <span>{d.why}</span>
        {d.since ? <span style={{ fontSize: 12, color: MUTED }}>{d.since}</span> : null}
      </span>
      <span style={{ fontFamily: MONO, fontSize: 12 }}>{lastUpdated(t)}</span>
      <span style={{ textAlign: 'right' }}>
        {readOnly && st !== 'ok' && st !== 'waiting' && st !== 'queued' ? <CopyLink select={() => v.openTable(t.source_id, t.table_name)} /> : null}
        {!readOnly && t.kind === 'not_enabled' && fixOptions(t)[0].spec ? <ActionButton spec={fixOptions(t)[0].spec!} label="Enable" align="end" compact /> : null}
        {!readOnly && t.kind !== 'not_enabled' && st !== 'ok' && st !== 'waiting' && st !== 'queued' ? (
          <button type="button" onClick={() => v.openTable(t.source_id, t.table_name)} style={{ ...caps(12, 0.1), padding: '7px 14px', border: `2px solid ${INK}`, borderRadius: 999, color: INK, background: st === 'needs_action' ? STATUS.needs_action.bg : CARD }}>{rowActionLabel(t)}</button>
        ) : null}
      </span>
    </div>
  );
}

function Pipeline({ s, p, enabled, rec, replica }: { s: SourceRow; p: Record<string, unknown>; enabled: TableRow[]; rec: Recovery | null; replica: boolean }) {
  const w = wal(s);
  const conflict = conflictReason(s);
  const slot = s.slot_name ? `slot ${s.slot_name}` : 'no slot reported';
  const stopped = w.lost || s.ingest_health === 'failed';
  const caught = enabled.filter((t) => t.kind === 'ok').length;
  const newest = Math.max(0, ...enabled.map((t) => N(t.updated_at_s)));
  const applyEvery = N(p.apply_gaps) >= 3 && p.apply_every_s != null ? `usually every ~${formatAge(p.apply_every_s).replace(/ \d+ min$/, '')} · ` : '';
  const quiet = noSuccess(s);
  const stages: Stage[] = [
    {
      name: 'Postgres', flight: false, sub: replica ? `read replica · ${slot}` : slot,
      ...(w.lost ? { status: 'needs_action', label: 'Needs action', line1: conflict != null ? `Slot invalidated by a recovery conflict${conflict ? ` (${conflict})` : ''}` : `Slot invalidated · wal_status ${S(s.wal_status)}`, line2: s.failing_since_at_s ? `Since ${formatTs(s.failing_since_at_s)}` : '' }
        : { status: w.alarm ? 'needs_action' : (w.pct ?? 0) >= 50 ? 'warning' : 'healthy', label: w.alarm ? 'Needs action' : (w.pct ?? 0) >= 50 ? 'Warning' : 'OK', line1: `WAL limit ${w.label} · wal_status ${S(s.wal_status) || '—'}`, line2: s.oldest_xact_age_s != null ? `Oldest open transaction ${formatAge(s.oldest_xact_age_s)}` : `Slot ${s.slot_active ? 'active' : 'idle between runs'}` }),
    },
    {
      name: 'Ingest', flight: true, sub: 'Flight',
      status: stopped || s.ingest_urgent ? 'needs_action' : s.ingest_health === 'stale' ? 'warning' : 'healthy',
      label: stopped ? 'Failing' : s.ingest_urgent ? 'Needs action' : s.ingest_health === 'stale' ? 'Stale' : 'OK',
      line1: stopped && s.ingest_error ? `Sync failing: ${s.ingest_error}`
        : s.ingest_health === 'stale' ? lastSuccessLine(s.last_ok_at_s, usually(s)) : `Last run ${formatTs(s.last_sync_at_s)}`,
      line2: `${usually(s) ? `${usually(s)} · ` : ''}${formatInt(p.ingest_failed_24h ?? 0)} failed in 24 h`,
    },
    {
      name: 'Change log', flight: false, sub: 'asset', status: stopped ? 'in_progress' : 'healthy', label: stopped ? 'Waiting' : 'OK',
      line1: `Has changes up to ${formatTs(s.durable_at_s)}`,
      line2: p.log_days != null ? `${formatInt(p.log_days)} d kept · ${formatBytes(p.log_bytes)}` : stopped ? 'No new changes arriving' : 'Size not reported yet',
    },
    {
      name: 'Apply', flight: true, sub: 'Flight',
      status: s.apply_health === 'failed' ? 'needs_action' : rec ? 'recovering' : stopped ? 'in_progress' : s.apply_urgent ? 'needs_action' : s.apply_health === 'stale' ? 'warning' : 'healthy',
      label: s.apply_health === 'failed' ? 'Failing' : rec ? 'Recovering' : stopped ? 'Waiting' : s.apply_urgent ? 'Needs action' : s.apply_health === 'stale' ? 'Stale' : 'OK',
      line1: s.apply_health === 'stale' && !stopped && !rec ? lastSuccessLine(s.apply_ok_at_s, applyEvery.replace(/ · $/, '')) : `Last run ${formatTs(p.apply_last_at_s)}`,
      line2: N(p.active_backfills) ? `${formatInt(p.active_backfills)} backfill${N(p.active_backfills) === 1 ? '' : 's'} running` : `${applyEvery}${formatInt(p.apply_failed_24h ?? 0)} failed in 24 h`,
    },
    {
      name: 'MotherDuck replica', flight: false, sub: 'asset',
      status: rec ? 'recovering' : caught === enabled.length ? 'healthy' : stopped ? 'in_progress' : 'warning',
      label: rec ? 'Recovering' : caught === enabled.length ? 'OK' : stopped ? 'Waiting' : 'Behind',
      line1: rec ? `${rec.rebuilt.length} of ${enabled.length} tables rebuilt` : `${caught} of ${enabled.length} tables caught up`,
      line2: rec && rec.failed.length ? `${rec.failed.length} rebuild failed` : newest ? `Last updated ${formatTs(newest)}` : '',
    },
  ];
  return (
    <Card label="Pipeline" style={{ padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, alignItems: 'baseline' }}>
        <SectionTitle>Pipeline</SectionTitle>
        <span style={{ display: 'inline-flex', gap: 16, fontSize: 12, color: MUTED }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}><span aria-hidden="true" style={{ width: 14, height: 12, border: `2px solid ${INK}`, borderRadius: 3, background: CARD, boxShadow: `-3px 3px 0 ${INK}` }} />Flight</span>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}><span aria-hidden="true" style={{ width: 14, height: 12, border: `2px solid ${INK}`, borderRadius: 3, background: PAGE }} />Asset</span>
        </span>
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'stretch' }}>
        {stages.map((g, i) => (
          <div key={g.name} style={{ display: 'flex', alignItems: 'center', gap: 10, flex: '1 1 220px', minWidth: 0 }}>
            <div style={{ flex: '1 1 auto', minWidth: 0, border: `2px solid ${INK}`, borderRadius: 12, padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 6, ...(g.flight ? { background: CARD, boxShadow: `-5px 5px 0 ${INK}` } : { background: PAGE }) }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
                <span style={caps(12, 0.12)}>{g.name}</span>
                <span style={pill(SOURCE_STATUS[g.status].bg)}>{g.label}</span>
              </div>
              <span style={{ fontFamily: MONO, fontSize: 11, color: MUTED, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{g.sub}</span>
              <span style={{ fontSize: 13 }}>{g.line1}</span>
              <span style={{ fontSize: 12, color: MUTED }}>{g.line2}</span>
            </div>
            {i < stages.length - 1 ? <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true" style={{ flex: 'none' }}><path d="M3 9h11M10 5l4 4-4 4" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg> : null}
          </div>
        ))}
      </div>
      {quiet && !stopped ? <p style={{ margin: 0, fontSize: 13 }}>{quiet.hint}</p> : null}
    </Card>
  );
}

/** A read replica source's diagnostics (advisory; the wording is in model.ts). A stale
 * heartbeat warns here only, never on the source's own status. */
function ReplicaCard({ r, lastRun }: { r: StandbyRow; lastRun: string | null }) {
  const x = standbyView(r, lastRun);
  const Row = ({ k, children }: { k: string; children: ReactNode }) => (
    <div style={{ display: 'grid', gridTemplateColumns: '150px minmax(0, 1fr)', gap: 12, padding: '8px 0', borderTop: `1px solid ${RULE}`, fontSize: 14 }}>
      <span style={{ color: MUTED }}>{k}</span><span style={{ minWidth: 0 }}>{children}</span>
    </div>
  );
  return (
    <Card label="Read replica" style={{ flex: '1 1 460px', minWidth: 0, padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, alignItems: 'baseline' }}>
        <SectionTitle>Read replica</SectionTitle>
        <span style={pill(SOURCE_STATUS[x.status].bg)}>{x.status === 'warning' ? 'Warning' : 'OK'}</span>
      </div>
      <div>
        <Row k="Replay lag">{x.lag}</Row>
        <Row k="Last replayed commit">{x.replayed}</Row>
        <Row k="Upstream">{x.upstream}</Row>
        <Row k="Heartbeat">
          <span style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <span>{x.heartbeat}</span>
            {x.staleHint ? <span style={{ fontSize: 13, color: MUTED }}>{x.staleHint}</span> : null}
          </span>
        </Row>
        {x.lastRun ? <Row k="Last run">{x.lastRun}</Row> : null}
      </div>
      <p style={{ margin: 0, fontSize: 12, color: MUTED }}>{STANDBY_FOOTER}{r.observed_at_s != null ? ` Measured ${formatAge(Math.max(0, Date.now() / 1000 - r.observed_at_s))} ago.` : ''}</p>
    </Card>
  );
}

function WalCard({ s }: { s: SourceRow }) {
  const w = wal(s);
  const hist = useRows(Q.walHistory(Q.ALL)).rows.filter((r) => r.source_id === s.source_id);
  const limit = s.retained_bytes != null && s.safe_wal_size != null ? N(s.retained_bytes) + N(s.safe_wal_size) : null;
  const now = Date.now() / 1000;
  const x0 = now - 7 * 86400;
  const peak = Math.max(limit ?? 0, ...hist.map((r) => N(r.retained_bytes)), 1);
  const pts = hist.filter((r) => r.retained_bytes != null).map((r) => `${(((N(r.at_s) - x0) / (7 * 86400)) * 560).toFixed(1)},${(122 - (N(r.retained_bytes) / peak) * 110).toFixed(1)}`).join(' ');
  const slots = useRows(Q.sourceSlots(Q.ALL)).rows.filter((r) => r.source_id === s.source_id).map((r) => ({ ...r, retained_bytes: r.retained_bytes == null ? null : N(r.retained_bytes), epoch: r.epoch == null ? null : N(r.epoch) }) as unknown as SlotRow);
  const others = extraSlots(slots);
  // Finish recovery: the newest completed transition again (fixes.ts finishRecovery).
  const last = useRows(Q.lastTransition(Q.ALL)).rows.find((r) => r.source_id === s.source_id);
  const finish: ActionSpec | null = finishRecovery(
    s.source_id,
    s.epoch == null ? null : N(s.epoch),
    last ? { action: S(last.action), expected_epoch: N(last.expected_epoch), to_lsn: last.to_lsn ? S(last.to_lsn) : null, confirm: S(last.confirm) } : null,
  );
  const status: SourceStatus = w.alarm ? 'needs_action' : (w.pct ?? 0) >= 50 || others.length ? 'warning' : 'healthy';
  return (
    <Card label="WAL limit and slot" style={{ flex: '1 1 560px', minWidth: 0, padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, alignItems: 'baseline' }}>
        <SectionTitle>WAL limit and slot</SectionTitle>
        <span style={pill(SOURCE_STATUS[status].bg)}>{w.lost ? 'Invalidated' : status === 'healthy' ? 'OK' : others.length && !w.alarm && (w.pct ?? 0) < 50 ? 'Extra slot' : SOURCE_STATUS[status].label}</span>
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '16px 28px', alignItems: 'center' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span title="How much of Postgres’s WAL limit this pipeline is using. When it reaches 100%, Postgres invalidates the replication slot and all tables must be rebuilt." style={{ fontSize: 12, color: MUTED, borderBottom: `1px dotted ${MUTED}`, alignSelf: 'flex-start', cursor: 'help' }}>WAL limit used</span>
          <span style={{ fontFamily: 'Cousine, ui-monospace, Menlo, monospace', fontSize: 40, lineHeight: 1 }}>{w.label}</span>
        </div>
        <div style={{ flex: '1 1 260px', minWidth: 0, display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span aria-hidden="true" style={{ display: 'block', height: 16, border: `2px solid ${INK}`, borderRadius: 4, background: PAGE, overflow: 'hidden' }}>
            <span style={{ display: 'block', height: '100%', width: `${Math.min(100, w.pct ?? 0)}%`, background: walColor(w.pct ?? 0) }} />
          </span>
          <span style={{ fontSize: 12, color: MUTED }}>{w.text}{s.slot_at_s ? ` · read ${formatAge(now - N(s.slot_at_s))} ago` : ''}</span>
        </div>
      </div>
      <dl style={{ margin: 0, display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '14px 20px' }}>
        <Fact k="Slot" v={s.slot_name ?? '—'} />
        <Fact k="wal_status" v={s.wal_status ?? '—'} />
        <Fact k="Retained WAL" v={formatBytes(s.retained_bytes)} />
        <Fact k="Sync lag" v={formatBytes(s.lag_bytes)} />
        <Fact k="Oldest open transaction" v={s.oldest_xact_age_s != null ? formatAge(s.oldest_xact_age_s) : 'not reported (under 1 GiB behind)'} />
        <Fact k="Epoch" v={S(s.epoch ?? '—')} />
      </dl>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div style={{ ...caps(11, 0.12), color: MUTED, paddingBottom: 6 }}>Slots</div>
        {slots.map((x) => (
          <div key={x.slot_name} style={{ display: 'grid', gridTemplateColumns: '200px minmax(0, 1fr) 120px', gap: 12, alignItems: 'baseline', padding: '8px 0', borderTop: `1px solid ${RULE}` }}>
            <span style={{ fontFamily: MONO, fontSize: 13, overflowWrap: 'anywhere' }}>{x.slot_name}</span>
            <span style={{ fontSize: 13 }}>{slotRole(x, slots)}{x.wal_status ? ` · wal_status ${x.wal_status}` : ''}{x.epoch != null ? ` · epoch ${x.epoch}` : ''}</span>
            <span style={{ fontFamily: MONO, fontSize: 12, textAlign: 'right' }}>{formatBytes(x.retained_bytes)} retained</span>
            {!x.is_current && slotRole(x, slots).startsWith('leftover') && finish ? <span style={{ gridColumn: '1 / -1' }}><ActionButton spec={finish} label="Finish recovery" /></span> : null}
          </div>
        ))}
        {others.length ? <p style={{ margin: '6px 0 0', fontSize: 12, color: MUTED }}>A slot other than the current one keeps retaining WAL on the source until it is dropped (runbook R1).</p> : null}
      </div>
      <figure style={{ margin: 0, display: 'flex', flexDirection: 'column', gap: 6 }}>
        <figcaption style={{ ...caps(11, 0.12), color: MUTED }}>Retained WAL, last 7 days{limit ? <span style={{ fontFamily: 'Inter, sans-serif', letterSpacing: 0, textTransform: 'none' }}> · dashed: today’s limit</span> : null}</figcaption>
        <svg viewBox="0 0 560 130" preserveAspectRatio="none" role="img" aria-label={`Retained WAL over the last 7 days for ${s.source_id}. Now ${formatBytes(s.retained_bytes)}.`} style={{ width: '100%', height: 130, display: 'block', background: PAGE, border: `1.5px solid ${INK}`, borderRadius: 8 }}>
          {limit ? <line x1="0" y1={122 - (limit / peak) * 110} x2="560" y2={122 - (limit / peak) * 110} stroke="#E23F35" strokeWidth="1.5" strokeDasharray="4 4" /> : null}
          {pts ? <polyline points={pts} fill="none" stroke={INK} strokeWidth="2.5" strokeLinejoin="round" /> : null}
        </svg>
        {!pts ? <span style={{ fontSize: 12, color: MUTED }}>No ingest runs in the last 7 days.</span> : null}
      </figure>
    </Card>
  );
}

function RecoveryCard({ s, rec, total }: { s: SourceRow; rec: Recovery; total: number }) {
  const buckets: { name: string; color: string; list: TableRow[]; text: (t: TableRow) => string }[] = [
    { name: 'Rebuilt', color: STATUS.ok.bg, list: rec.rebuilt, text: (t) => t.table_name },
    { name: 'Rebuilding', color: STATUS.rebuilding.bg, list: rec.rebuilding, text: (t) => `${t.table_name} (${formatInt(t.chunks ?? 0)}${t.chunks_total ? ` of ~${formatInt(t.chunks_total)}` : ''} chunks)` },
    { name: 'Failed', color: STATUS.needs_action.bg, list: rec.failed, text: (t) => `${t.table_name} · ${t.detail ?? 'unknown error'}` },
    { name: 'Queued', color: STATUS.queued.bg, list: rec.queued, text: (t) => t.table_name },
    { name: 'Insert-only', color: STATUS.needs_action.bg, list: rec.insertOnly, text: (t) => t.table_name },
  ];
  return (
    <Card label="Recovery" style={{ flex: '1 1 560px', minWidth: 0, padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, alignItems: 'baseline' }}>
        <SectionTitle>Recovery</SectionTitle>
        <span style={pill(STATUS.rebuilding.bg)}>Recovering · {rec.rebuilt.length} of {total} rebuilt</span>
      </div>
      <dl style={{ margin: 0, display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '14px 20px' }}>
        <Fact k="Started" v="not recorded yet" mono={false} />
        <Fact k="By" v="—" mono={false} />
        <Fact k="Epoch" v={`now ${S(s.epoch)}`} />
      </dl>
      <div role="img" aria-label={`Recovery progress: ${buckets.map((b) => `${b.list.length} ${b.name.toLowerCase()}`).join(', ')}, out of ${total} tables`} style={{ display: 'flex', height: 22, border: `2px solid ${INK}`, borderRadius: 6, overflow: 'hidden' }}>
        {buckets.filter((b) => b.list.length).map((b, i, arr) => <span key={b.name} style={{ display: 'block', height: '100%', flex: `${b.list.length} 1 0`, background: b.color, borderRight: i < arr.length - 1 ? `2px solid ${INK}` : undefined }} />)}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {buckets.map((b) => (
          <div key={b.name} style={{ display: 'grid', gridTemplateColumns: '150px 40px minmax(0, 1fr)', gap: 12, alignItems: 'baseline', padding: '9px 0', borderTop: `1px solid ${RULE}` }}>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}><span aria-hidden="true" style={{ display: 'inline-block', width: 14, height: 14, border: `1.5px solid ${INK}`, borderRadius: 3, background: b.color }} /><span style={{ fontWeight: 600 }}>{b.name}</span></span>
            <span style={{ fontFamily: MONO, fontSize: 13, textAlign: 'right' }}>{b.list.length}</span>
            <span style={{ fontFamily: MONO, fontSize: 12, color: MUTED, overflowWrap: 'anywhere' }}>{b.list.map(b.text).join(', ')}</span>
          </div>
        ))}
      </div>
      {rec.queued.length + rec.failed.length ? (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'center' }}>
          <ActionButton
            label="Resync next queued table"
            spec={resyncNextSpec(s.source_id)}
          />
          <span style={{ fontSize: 12, color: MUTED }}>Starts one rebuild per run; {rec.queued.length + rec.failed.length} left.</span>
        </div>
      ) : null}
      {rec.insertOnly.length ? <p style={{ margin: 0, fontSize: 13 }}>Insert-only tables aren’t in the queue: resync each one, or start it from the log, from its drawer.</p> : null}
      <p style={{ margin: 0, fontSize: 13, color: MUTED }}>Recovery is complete when no table is behind the current epoch. A table can look one sync behind for a cycle after its rebuild finishes. Removed and unmanaged tables are not counted.</p>
    </Card>
  );
}

function RetentionCard({ s, p }: { s: SourceRow; p: Record<string, unknown> }) {
  const pins = useRows(Q.sourcePins(Q.ALL)).rows.filter((r) => r.source_id === s.source_id);
  const v = useView();
  const keep = p.keep_hours != null ? formatAge(N(p.keep_hours) * 3600) : '—';
  const reasonOf = (r: Record<string, unknown>) =>
    r.kind === 'generation' ? (r.gen_state === 'failed' ? 'Rebuild failed' : 'Rebuild in progress')
      : r.state === 'paused' ? 'Paused' : r.state === 'needs_action' ? 'Blocked' : r.state === 'needs_resync' ? 'Needs resync' : 'Applied position';
  return (
    <Card label="Change log retention" style={{ flex: '1 1 560px', minWidth: 0, padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
        <SectionTitle>Change log retention</SectionTitle>
        <ActionButton label="Compact now" spec={compactSpec(s.source_id)} />
      </div>
      <dl style={{ margin: 0, display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '14px 20px' }}>
        <Fact k="Keeps" v={keep} mono={false} />
        <Fact k="Retained now" v={p.log_days != null ? `${formatInt(p.log_days)} days · ${formatBytes(p.log_bytes)}` : '—'} mono={false} />
        <Fact k="Oldest change" v={p.oldest_day ? `on ${day(p.oldest_day)}` : '—'} />
      </dl>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div style={{ ...caps(11, 0.12), color: MUTED, paddingBottom: 6 }}>Pins · oldest first</div>
        {pins.map((r, i) => (
          <div key={i} style={{ display: 'grid', gridTemplateColumns: '170px minmax(0, 1fr) 220px', gap: 12, alignItems: 'baseline', padding: '9px 0', borderTop: `1px solid ${RULE}` }}>
            <span style={{ minWidth: 0 }}><NameLink onClick={() => v.openTable(s.source_id, S(r.table_name))}>{S(r.table_name)}</NameLink></span>
            <span style={{ display: 'flex', flexDirection: 'column', gap: 2 }}><span>{reasonOf(r)}</span><span style={{ fontSize: 12, color: MUTED }}>Updated {formatTs(r.updated_at_s)}</span></span>
            <span style={{ fontSize: 12, color: MUTED, textAlign: 'right' }}>est. expiry <span style={{ fontFamily: MONO, color: INK }}>{r.expires_at_s != null ? formatTs(r.expires_at_s) : 'none'}</span></span>
          </div>
        ))}
        {!pins.length ? <p style={{ margin: 0, padding: '10px 0', borderTop: `1px solid ${RULE}`, color: MUTED }}>No pins reported.</p> : null}
      </div>
      <p style={{ margin: 0, fontSize: 13, color: MUTED }}>A pin keeps changes a table still needs. When a pinned change reaches the retention limit, that table needs a resync.</p>
    </Card>
  );
}

function SetupCard({ s }: { s: SourceRow }) {
  const r = useRows(Q.sourceSetup(Q.ALL)).rows.find((x) => x.source_id === s.source_id);
  const tls = useRows(Q.tls(Q.ALL)).rows.find((x) => x.source_id === s.source_id);
  const warnings = listOf(r?.warnings).map(S);
  const Check = ({ ok, children, detail }: { ok: boolean; children: ReactNode; detail?: string }) => (
    <li style={{ display: 'grid', gridTemplateColumns: '22px minmax(0, 1fr)', gap: 10, alignItems: 'start', padding: '9px 0', borderTop: `1px solid ${RULE}` }}>
      {ok ? (
        <svg width="20" height="20" viewBox="0 0 20 20" role="img" aria-label="Passed"><circle cx="10" cy="10" r="8.5" fill={STATUS.ok.bg} stroke={INK} strokeWidth="1.5" /><path d="M6 10.5l2.6 2.5L14 7.5" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
      ) : (
        <svg width="20" height="20" viewBox="0 0 20 20" role="img" aria-label="Warning"><path d="M10 2.5l8.5 15h-17z" fill={STATUS.stale.bg} stroke={INK} strokeWidth="1.5" strokeLinejoin="round" /><path d="M10 8v4.5M10 14.8v.2" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" /></svg>
      )}
      <span style={{ display: 'flex', flexDirection: 'column', gap: 2 }}><span>{children}</span>{detail ? <span style={{ fontFamily: MONO, fontSize: 12, color: MUTED }}>{detail}</span> : null}</span>
    </li>
  );
  return (
    <Card label="Setup check" style={{ flex: '1 1 460px', minWidth: 0, padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 14 }}>
      <SectionTitle>Setup check</SectionTitle>
      <p style={{ margin: 0, fontSize: 13, color: MUTED }}>
        {r ? <>Last checked <span style={{ fontFamily: MONO, color: INK }}>{formatTs(r.at_s)}</span> · {S(r.status)} · {warnings.length} warning{warnings.length === 1 ? '' : 's'}</> : 'No setup check reported yet.'}
      </p>
      <ul style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column' }}>
        {r && r.status === 'waiting_for_primary' ? (
          <Check ok={false} detail={REPLICA_WAITS.waiting_for_primary.fix}>{REPLICA_WAITS.waiting_for_primary.why}</Check>
        ) : r ? <Check ok={r.status === 'ready'} detail={r.error ? S(r.error) : undefined}>{r.status === 'ready' ? 'Preflight passed' : 'Preflight failed'}</Check> : null}
        {warnings.map((w) => <Check key={w} ok={false}>{w}</Check>)}
        {tls ? <Check ok={!tls.warn} detail={tlsText(tls.breakdown)}>TLS: {S(tls.tls)}</Check> : null}
      </ul>
    </Card>
  );
}

function DangerZone({ s, open, toggle, tables, readOnly }: { s: SourceRow; open: boolean; toggle: () => void; tables: TableRow[]; readOnly: boolean }) {
  const w = wal(s);
  const owner = OWNER ? who(OWNER) : 'the Flights’ owner';
  const cards = dangerCards(s, w.lost);
  return (
    <section id="danger" aria-labelledby="danger-h" style={{ background: CARD, border: `2px solid ${INK}`, borderRadius: 14, overflow: 'hidden' }}>
      <h3 id="danger-h" style={{ margin: 0 }}>
        <button type="button" aria-expanded={open} aria-controls="danger-body" onClick={toggle} style={{ width: '100%', display: 'flex', alignItems: 'center', gap: 12, justifyContent: 'space-between', padding: '16px 22px', background: CARD, border: 'none', color: INK, textAlign: 'left' }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 12 }}>
            <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true" style={{ transform: `rotate(${open ? 0 : -90}deg)` }}><path d="M4 6l4 4 4-4" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
            <span style={{ ...caps(13, 0.14), fontWeight: 400 }}>Danger zone</span>
            <span style={{ fontSize: 13, color: MUTED }}>Recover slot · Skip to LSN · Delete source</span>
          </span>
          {w.lost ? <span style={badge}>Needed for this incident</span> : null}
        </button>
      </h3>
      {open ? (
        <div id="danger-body" style={{ borderTop: `2px solid ${INK}`, padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 20, background: '#FBF8F5' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div style={{ ...caps(11, 0.12), color: MUTED }}>Epoch and checkpoint</div>
            <dl style={{ margin: 0, display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: '14px 20px' }}>
              <Fact k="Current epoch" v={S(s.epoch ?? '—')} />
              <Fact k="Replica tables at current" v={`${tables.filter((t) => t.epoch === s.epoch).length} of ${tables.length}`} />
              <Fact k="Checkpoint LSN" v={s.checkpoint_lsn ?? '—'} />
              <Fact k="Checkpoint commit time" v={formatTs(s.durable_at_s)} />
            </dl>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 18 }}>
            {cards.map((d) => <DangerCard key={d.name} d={d} readOnly={readOnly} owner={owner} />)}
          </div>
          <p style={{ margin: 0, fontSize: 13, color: MUTED }}>Every action starts with a preview. Nothing changes until you confirm, and the run re-checks Postgres and the change log before it acts.</p>
        </div>
      ) : null}
    </section>
  );
}

const badge = { ...caps(11, 0.1), padding: '3px 9px', borderRadius: 999, background: STATUS.needs_action.bg, border: `1.5px solid ${INK}`, color: INK, whiteSpace: 'nowrap' } as const;

export type DangerCardSpec = { name: string; spec: ActionSpec | null; recommended: boolean; does: string; cost: string; note: string };

/** The Danger zone's cards for a source (also rendered by the dev copy gallery). */
export function dangerCards(s: SourceRow, lost: boolean): DangerCardSpec[] {
  const w = { lost };
  const danger = (action: 'recover-slot' | 'skip-to-lsn' | 'delete-source', verb: string, would: string): ActionSpec | null =>
    s.epoch == null ? null : { source: s.source_id, action, verb, target: s.source_id, would, inputs: { expectedEpoch: N(s.epoch) } };
  return [
    { name: 'Recover slot', spec: w.lost ? danger('recover-slot', 'recover slot', 'Create a new replication slot and begin a new epoch. Every replica table then needs a rebuild.') : null, recommended: w.lost, does: 'Creates a new replication slot and starts a new epoch; every table then needs a rebuild.', cost: 'Every table is rebuilt from a fresh snapshot. Replica tables keep their old data until each one finishes.', note: w.lost ? 'Progress shows in the Recovery panel on this page.' : 'Available when the slot is invalidated.' },
    { name: 'Skip to LSN', spec: danger('skip-to-lsn', 'skip', 'Move the slot forward to the LSN you enter and begin a new epoch. Changelogs in between are never ingested into MotherDuck.'), recommended: false, does: 'Moves the slot forward to the LSN you enter, skipping the changes in between.', cost: 'Skipped changes never reach the replica. Every table needs a resync to match Postgres again.', note: 'Uses the same Recovery panel to track tables that need a resync.' },
    { name: 'Delete source', spec: danger('delete-source', 'delete', `Stop ingesting changelogs from ${s.source_id} for good, and drop what this pipeline created on Postgres (slot, publication). The replica databases and the change log are kept.`), recommended: false, does: `Stops syncing ${s.source_id} and removes its slot and publication.`, cost: 'The replica databases and the change log are kept; you drop them by hand later (runbooks).', note: 'The preview lists everything that would be removed.' },
  ];
}

/** One Danger zone card. */
export function DangerCard({ d, readOnly, owner }: { d: DangerCardSpec; readOnly: boolean; owner: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0, background: CARD, border: `2px solid ${INK}`, borderRadius: 12, padding: 16, boxShadow: d.recommended ? '-6px 6px 0 #E23F35' : undefined }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <h4 style={{ margin: 0, ...caps(14, 0.1), fontWeight: 700 }}>{d.name}</h4>
        {d.recommended ? <span style={badge}>Recommended</span> : null}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}><span style={{ ...caps(11, 0.12), color: MUTED }}>What it does</span><span>{d.does}</span></div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}><span style={{ ...caps(11, 0.12), color: MUTED }}>What you give up</span><span>{d.cost}</span></div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 'auto' }}>
        {readOnly ? <CopyLink /> : d.spec ? <ActionButton spec={d.spec} bg={d.recommended ? STATUS.needs_action.bg : CARD} /> : null}
        <span style={{ fontSize: 12, color: MUTED }}>{readOnly ? `Only ${owner} can run this. ` : ''}{d.note}</span>
      </div>
    </div>
  );
}

/** The Recovery panel's "Resync next queued table" and the retention card's "Compact now". */
export const resyncNextSpec = (source: string): ActionSpec => ({ source, action: 'resync-all', verb: 'resync the next table', target: source, would: 'Rebuild the queued table that has waited longest from a fresh snapshot. Run it again for the next one.', inputs: {} });
export const compactSpec = (source: string): ActionSpec => ({ source, action: 'compact-now', verb: 'compaction', target: 'change log', would: 'Compact the change log in MotherDuck now, with the owner’s settings.', inputs: {} });
