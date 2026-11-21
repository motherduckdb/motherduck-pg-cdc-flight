/**
 * The CDC operations Dive (the 2026-11-10 design): replication status organized around
 * sources and tables, read from the share of `cdc_ops` only (docs/dive.md). Overview lists
 * every source with its tables, problems first; a source opens its Source page; a table
 * opens the drawer. The view lives in useDiveState, so a copied link reopens it.
 * Only the Flights' owner can act, through the action panel (src/action.tsx).
 */
import { ActionProvider } from './action';
import { ErrorBoundary, ErrorPanel } from './errors';
import { Drawer } from './screens/drawer';
import { Overview, Sources } from './screens/overview';
import { SourcePage } from './screens/source';
import { useView, type View } from './kit';
import { caps, CSS, MUTED, navTab, RULE } from './theme';

// The cdc_ops share, declared as a share, as a literal (src/share.test.ts checks it against
// share.ts); scripts/deploy_dive.py replaces the placeholder with the share's URL.
export const REQUIRED_DATABASES = [{ type: 'share', path: 'md:_share/cdc_ops/REPLACE_WITH_SHARE_ID', alias: 'ops' }];

const TABS: { key: View; label: string }[] = [
  { key: 'overview', label: 'Overview' },
  { key: 'sources', label: 'Sources' },
  { key: 'tables', label: 'Tables' },
  { key: 'activity', label: 'Activity' },
  { key: 'help', label: 'Help' },
];

/** Tabs not designed yet (open items): say so rather than invent them. */
function NotDesigned({ name }: { name: string }) {
  return (
    <div style={{ border: `2px dashed ${RULE}`, borderRadius: 14, padding: '28px 24px', color: MUTED }}>
      <p style={{ margin: 0, ...caps(12, 0.12) }}>{name}</p>
      <p style={{ margin: '8px 0 0' }}>Not designed yet. Overview and the Source pages show every source and table.</p>
    </div>
  );
}

export default function CdcOperations() {
  const v = useView();
  const tab = v.view === 'source' ? 'sources' : v.view;
  return (
    <div className="ops">
      <style>{CSS}</style>
      <ActionProvider>
      <div style={{ maxWidth: 1360, margin: '0 auto', padding: '28px 40px 72px', display: 'flex', flexDirection: 'column', gap: 22 }}>
        <header style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', gap: 16 }}>
          <h1 style={{ margin: 0, fontWeight: 400, ...caps(15, 0.16) }}>Postgres → MotherDuck replication</h1>
          <nav aria-label="Sections" style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {TABS.map((t) => (
              <button key={t.key} type="button" aria-current={tab === t.key ? 'page' : undefined} onClick={() => v.go(t.key)} style={navTab(tab === t.key)}>{t.label}</button>
            ))}
          </nav>
        </header>
        <ErrorBoundary key={`${v.view}:${v.source ?? ''}`}>
          {v.view === 'overview' ? <Overview /> : null}
          {v.view === 'sources' ? <Sources /> : null}
          {v.view === 'source' ? <SourcePage /> : null}
          {v.view === 'tables' ? <NotDesigned name="Tables" /> : null}
          {v.view === 'activity' ? <NotDesigned name="Activity" /> : null}
          {v.view === 'help' ? <NotDesigned name="Help" /> : null}
        </ErrorBoundary>
      </div>
      <ErrorBoundary key={v.table ?? ''}>
        <Drawer />
      </ErrorBoundary>
      </ActionProvider>
      <ErrorPanel />
    </div>
  );
}
