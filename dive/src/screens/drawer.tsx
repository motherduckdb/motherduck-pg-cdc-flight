// The table drawer: one table's status, fix options (read-only until phase 2 brings the
// action panel), backfill, sync history, gaps, retention pin, history and technical details.
import { useState, type ReactNode } from 'react';
import { formatAge, formatInt, formatTs, N, S } from '../format';
import { CopyLink, Fact, LAG_NOTE, NameLink, SOURCE_CLOCK, StatusPill, Ticks, useReadOnly, useRows, useTableStatus, useView } from '../kit';
import { ActionButton } from '../action';
import { PREVIEW_MINUTES } from '../actions';
import { fixOptions, type FixOption } from '../fixes';
import { afterTable, appendFacts, details, help, lastUpdated, shownStatus, sourceName, sourceRow, tableRow, ticksOf, who, type TableRow } from '../model';
import { OWNER } from '../owner';
import * as Q from '../queries';
import { caps, CARD, DISABLED, INK, MONO, MUTED, PAGE, RULE, STATUS } from '../theme';

const FIXES = ['blocked', 'paused', 'rebuild_failed', 'needs_resync', 'registry', 'append_changed', 'catalog_changed', 'type_changed', 'append_invalid', 'not_started'];
const fixTitle = (t: TableRow) => (FIXES.includes(t.kind) ? 'Fix options' : 'Options');

/** The design's preview notice: the newest dry run that would act on this table, still
 * spendable (under ~14 of its 15 min) and not followed by a real run of that action. */
export function openPreview(history: Record<string, unknown>[], nowS: number): { action: string; actor: string; ageS: number; leftS: number } | null {
  const newest = history[0];
  if (!newest || !newest.dry_run || newest.status !== 'would') return null;
  const ageS = nowS - N(newest.at_s);
  const leftS = (PREVIEW_MINUTES - 1) * 60 - ageS;
  return leftS > 0 ? { action: S(newest.action), actor: who(S(newest.actor)), ageS, leftS } : null;
}

function Section({ title, children, aside }: { title: string; children: ReactNode; aside?: ReactNode }) {
  return (
    <section style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'baseline', flexWrap: 'wrap' }}>
        <h3 style={{ margin: 0, fontWeight: 400, ...caps(13, 0.14) }}>{title}</h3>
        {aside}
      </div>
      {children}
    </section>
  );
}

/** The kept-aside table's DROP (append_apply.kept_refusal), quoted per part. */
export const dropKept = (kept: string): string => {
  const [schema, ...rest] = kept.split('.');
  // cdc_ops doesn't name the replica database: the owner fills it in
  return `DROP TABLE "<replica database>"."${schema.replace(/"/g, '""')}"."${rest.join('.').replace(/"/g, '""')}";`;
};

const AUDIT_VERB: Record<string, string> = { pause: 'paused the table', resume: 'resumed the table', retry: 'ran Retry', resync: 'ran Resync', 'start-from-log': 'started the table from the log', quarantine: 'ran Quarantine', add: 'enabled the table', remove: 'removed the table', 'compact-now': 'ran Compact now', 'resync-all': 'ran Resync all' };
const RESULT_LABEL: Record<string, string> = { done: 'Done', already: 'Nothing to do', would: 'Preview ok', refused: 'Refused', busy: 'Busy', waiting: 'Waiting', failed: 'Failed' };

/** One option card in the drawer (also rendered by the dev copy gallery, dive/gallery). */
export function OptionCard({ o, reason, owner }: { o: FixOption; reason: string | null; owner: string }) {
  const na = !!o.na;
  const note = na ? o.na : reason ? `Only ${owner} can run this.` : '';
  const suggested = !!o.suggested && !reason;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, minWidth: 0, border: `2px solid ${na ? DISABLED : INK}`, borderRadius: 12, padding: 14, background: na ? PAGE : CARD, color: na ? MUTED : INK, boxShadow: suggested ? `-5px 5px 0 ${INK}` : undefined }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
        <h4 style={{ margin: 0, ...caps(13, 0.1), fontWeight: 700 }}>{o.name}</h4>
        {suggested ? <span style={{ ...caps(10, 0.1), padding: '2px 7px', borderRadius: 999, background: STATUS.stale.bg, border: `1.5px solid ${INK}` }}>Suggested</span> : null}
      </div>
      <span style={{ fontSize: 13 }}>{o.does}</span>
      {!na && o.cost ? <span style={{ fontSize: 12, color: MUTED }}>{o.cost}</span> : null}
      {!na && o.cli ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span style={{ fontSize: 12, color: MUTED }}>{owner} runs these from the command line:</span>
          {o.cli.map((c) => c.startsWith('cdc-flight-')
            ? <code key={c} style={{ fontFamily: MONO, fontSize: 11, userSelect: 'all', overflowWrap: 'anywhere' }}>{c}</code>
            : <span key={c} style={{ fontSize: 12 }}>{c}</span>)}
        </div>
      ) : null}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 'auto' }}>
        {!na && o.spec ? <ActionButton spec={o.spec} /> : null}
        {note ? <span style={{ fontSize: 12, color: MUTED }}>{note}</span> : null}
      </div>
    </div>
  );
}

export function Drawer() {
  const v = useView();
  const reason = useReadOnly();
  const src = useRows(Q.sourceStatus(Q.ALL));
  const tbl = useTableStatus();
  const mk = useRows(Q.syncMarks(Q.ALL));
  const hist = useRows(Q.tableHistory(Q.ALL));
  const gaps = useRows(Q.tableGaps(Q.ALL));
  const [tech, setTech] = useState(false);
  if (!v.table || !v.source) return null;
  const match = (r: Record<string, unknown>) => r.source_id === v.source && r.table_name === v.table;
  const raw = tbl.rows.find(match);
  const s = src.rows.map(sourceRow).find((x) => x.source_id === v.source);
  const t = raw ? tableRow(raw) : null;
  const st = t ? shownStatus(t) : null;
  const d = t ? details(t) : null;
  const ticks = ticksOf(mk.rows.find(match)?.marks);
  const audit = hist.rows.filter(match);
  const gapRows = gaps.rows.filter(match);
  const owner = OWNER ? who(OWNER) : 'the Flights’ owner';
  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 10, display: 'flex', justifyContent: 'flex-end' }}>
      <div aria-hidden="true" onClick={v.closeTable} style={{ position: 'absolute', inset: 0, background: 'rgba(56, 56, 56, 0.35)' }} />
      <aside aria-labelledby="drawer-title" style={{ position: 'relative', width: '100%', maxWidth: 720, height: '100%', overflowY: 'auto', boxSizing: 'border-box', background: CARD, borderLeft: `2px solid ${INK}`, padding: '24px 28px 56px', display: 'flex', flexDirection: 'column', gap: 22 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12 }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 }}>
            <p style={{ margin: 0, fontSize: 13, color: MUTED }}>Source <NameLink onClick={() => v.go('source', v.source)}>{s ? sourceName(s) : v.source}</NameLink></p>
            <h2 id="drawer-title" style={{ margin: 0, fontFamily: MONO, fontWeight: 500, fontSize: 24, overflowWrap: 'anywhere' }}>{v.table}</h2>
            {t && st ? (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 12px', alignItems: 'center' }}>
                {st === 'unknown' ? <span>Unknown</span> : <StatusPill status={st} />}
                <span style={{ fontSize: 13, color: MUTED }}>Last updated <span title={SOURCE_CLOCK} style={{ fontFamily: MONO, fontSize: 12, color: INK, borderBottom: `1px dotted ${INK}`, cursor: 'help' }}>{lastUpdated(t)}</span></span>
              </div>
            ) : null}
          </div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center', flex: 'none' }}>
            <CopyLink button />
            <button type="button" aria-label="Close" onClick={v.closeTable} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 44, height: 44, boxSizing: 'border-box', border: `2px solid ${INK}`, borderRadius: 999, background: CARD }}>
              <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><path d="M2 2l10 10M12 2L2 12" stroke={INK} strokeWidth="2" strokeLinecap="round" /></svg>
            </button>
          </div>
        </div>

        {tbl.fallback}
        {!tbl.loading && !t ? <p style={{ margin: 0, color: MUTED }}>This table has no status in cdc_ops (removed, or not reported yet).</p> : null}

        {t && d ? (
          <>
            {reason ? <p style={{ margin: 0, border: `2px solid ${INK}`, borderRadius: 12, padding: '10px 14px', fontSize: 13 }}><strong style={{ fontWeight: 600 }}>Read-only:</strong> actions are run by {owner}. Copy the link to hand this off.</p> : null}

            <section aria-label="Status" style={{ display: 'flex', flexDirection: 'column', gap: 8, background: PAGE, border: `2px solid ${INK}`, borderRadius: 14, padding: '16px 18px' }}>
              <h3 style={{ margin: 0, fontWeight: 400, ...caps(12, 0.14), color: MUTED }}>Status</h3>
              <p style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>{d.why === '—' ? 'Replicating · caught up' : d.why}</p>
              {d.since ? <p style={{ margin: 0, fontSize: 14 }}>{d.since}</p> : null}
              <p style={{ margin: 0, fontSize: 14 }}>{help(t)}</p>
              {t.rebuild_error ? <p style={{ margin: 0, fontSize: 14 }}><strong style={{ fontWeight: 600 }}>Rebuild failed:</strong> {t.rebuild_error} The replica table keeps its current data.</p> : null}
              {t.refused_detail && !t.waits_for_ingest ? <p style={{ margin: 0, fontSize: 14 }}><strong style={{ fontWeight: 600 }}>Last resync refused:</strong> {afterTable(t.refused_detail)}</p> : null}
              {t.waits_for_ingest && t.kind !== 'catalog_changed' ? <p style={{ margin: 0, fontSize: 14 }}>Resync waits for the next ingest run.</p> : null}
            </section>

            {appendFacts(t).length || t.kept_aside ? (
              <Section title="Insert-only (append mode)">
                <ul style={{ margin: 0, paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 4, fontSize: 14 }}>
                  {appendFacts(t).slice(1).map((f) => <li key={f}>{f}</li>)}
                </ul>
                {t.kept_aside ? (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 13 }}>
                    <span>The previous replica table is kept aside as <code style={{ fontFamily: MONO }}>{t.kept_aside}</code>, unless it has already been dropped. A later resync or start-from-log replacement is refused while it exists. Keep a copy if you need its rows, then drop it (name the source’s replica database):</span>
                    <code style={{ fontFamily: MONO, fontSize: 12, userSelect: 'all', background: PAGE, border: `1.5px solid ${INK}`, borderRadius: 8, padding: '8px 10px', overflowWrap: 'anywhere' }}>{dropKept(t.kept_aside)}</code>
                  </div>
                ) : null}
              </Section>
            ) : null}

            {(() => {
              const p = openPreview(audit, Date.now() / 1000);
              return p ? (
                <div role="status" style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 16px', alignItems: 'center', justifyContent: 'space-between', background: STATUS.rebuilding.bg, border: `2px solid ${INK}`, borderRadius: 12, padding: '12px 16px' }}>
                  <span><strong style={{ fontWeight: 600 }}>{p.action.replace(/^./, (c) => c.toUpperCase())} previewed by {p.actor} {formatAge(p.ageS)} ago</strong> · can still be confirmed for about {formatAge(p.leftS)}</span>
                </div>
              ) : null;
            })()}

            <Section title={fixTitle(t)}>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12 }}>
                {fixOptions(t).map((o) => <OptionCard key={o.name} o={o} reason={reason} owner={owner} />)}
              </div>
            </Section>

            {t.generation_id && (t.kind === 'rebuilding' || t.kind === 'rebuild_failed') ? (
              <Section title="Backfill" aside={<span style={{ fontSize: 12, color: MUTED }}>Generation {S(t.generation_id)} · {d.since}</span>}>
                <div role="img" aria-label={`Backfill ${formatInt(t.chunks ?? 0)} of about ${formatInt(t.chunks_total)} chunks${t.kind === 'rebuild_failed' ? ', failed' : ''}`} style={{ height: 18, border: `2px solid ${INK}`, borderRadius: 6, background: PAGE, overflow: 'hidden' }}>
                  <span style={{ display: 'block', height: '100%', width: `${t.chunks_total ? Math.min(100, Math.round((N(t.chunks) / N(t.chunks_total)) * 100)) : 0}%`, background: t.kind === 'rebuild_failed' ? STATUS.needs_action.bg : STATUS.rebuilding.bg, borderRight: `2px solid ${INK}` }} />
                </div>
                <p style={{ margin: 0, fontSize: 13 }}>{t.kind === 'rebuild_failed' ? `Stopped at chunk ${formatInt(t.chunks ?? 0)} of ~${formatInt(t.chunks_total)}: ${t.detail ?? 'unknown error'}` : d.why.replace('Backfill · ', '')}</p>
              </Section>
            ) : null}

            <Section title="Syncs, last 7 days">
              <Ticks tall ticks={ticks} label={`Sync results over the last 7 days for ${t.table_name}`} />
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: MUTED, fontFamily: MONO }}><span>7 d ago</span><span>6 h per mark</span><span>now</span></div>
            </Section>

            <Section title="Gaps and quarantines">
              {gapRows.length ? gapRows.map((g, i) => (
                <div key={i} style={{ display: 'flex', flexDirection: 'column', gap: 2, padding: '8px 0', borderTop: `1px solid ${RULE}` }}>
                  <span>{S(g.what)}{g.open == null ? '' : g.open ? ' · open' : ' · closed'}</span>
                  <span style={{ fontSize: 12, color: MUTED }}>{formatTs(g.at_s)}</span>
                </div>
              )) : <p style={{ margin: 0, color: MUTED }}>None in the last 30 days.</p>}
            </Section>

            <Section title="Change log retention">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <span>{t.pin_lsn ? `Holding change history from ${t.pin_lsn} · last moved ${formatTs(t.pin_moved_at_s)}${t.expires_at_s ? ` · est. expiry ${formatTs(t.expires_at_s)}` : ''}` : 'Not holding any change history.'}</span>
                {t.pin_lsn && t.kind === 'paused' ? <span style={{ fontSize: 12, color: MUTED }}>Resume before the estimated expiry to avoid a rebuild.</span> : null}
                {t.pin_lsn && t.kind === 'blocked' ? <span style={{ fontSize: 12, color: MUTED }}>If the table is still blocked when the oldest held change reaches the retention limit, it needs a resync.</span> : null}
              </div>
            </Section>

            <Section title="History">
              {audit.length ? audit.slice(0, 20).map((a, i) => (
                <div key={i} style={{ display: 'grid', gridTemplateColumns: '190px minmax(0, 1fr) 110px', gap: 12, padding: '8px 0', borderTop: `1px solid ${RULE}`, alignItems: 'baseline' }}>
                  <span style={{ fontFamily: MONO, fontSize: 12 }}>{formatTs(a.at_s)}</span>
                  <span>{who(S(a.actor))} {a.dry_run ? `previewed ${S(a.action)}` : AUDIT_VERB[S(a.action)] ?? `ran ${S(a.action)}`}</span>
                  <span style={{ fontSize: 12, color: MUTED, textAlign: 'right' }}>{RESULT_LABEL[S(a.status)] ?? S(a.status)}</span>
                </div>
              )) : <p style={{ margin: 0, color: MUTED }}>No actions on this table in the last 30 days.</p>}
            </Section>

            <section>
              <button type="button" aria-expanded={tech} onClick={() => setTech(!tech)} style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: 'none', border: 'none', padding: 0, ...caps(12, 0.12), color: INK }}>
                <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true" style={{ transform: `rotate(${tech ? 0 : -90}deg)` }}><path d="M4 6l4 4 4-4" fill="none" stroke={INK} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
                Technical details
              </button>
              {tech ? (
                <dl style={{ margin: '12px 0 0', display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '12px 20px' }}>
                  <Fact k="Epoch (table / source)" v={`${t.epoch ?? '—'} / ${t.source_epoch ?? '—'}`} />
                  <Fact k="Generation" v={t.generation_id ?? '—'} />
                  <Fact k="Capture mode" v={t.capture_mode === 'append' ? `append${t.ignore_deletes ? ', DELETEs skipped' : ''}` : 'keyed'} />
                  {t.reason ? <Fact k="Hold reason" v={t.reason} /> : null}
                  {t.kind === 'not_started' && t.detail ? <Fact k="Refusal" v={t.detail} /> : null}
                  <Fact k="Applied through LSN" v={t.applied_lsn ?? '—'} />
                  <Fact k="Source checkpoint LSN" v={t.checkpoint_lsn ?? '—'} />
                  <Fact k="Source durable through" v={formatTs(t.durable_at_s)} />
                  {t.blocked_unit ? <Fact k="Blocked batch" v={t.blocked_unit} /> : null}
                  {t.pin_lsn ? <Fact k="Pin LSN" v={t.pin_lsn} /> : null}
                  <Fact k="Quarantined batches" v={formatInt(t.quarantines)} />
                </dl>
              ) : null}
            </section>

            <p style={{ margin: 0, fontSize: 12, color: MUTED }}>{LAG_NOTE}</p>
          </>
        ) : null}
      </aside>
    </div>
  );
}
