# Developing the operations Dive

The customer guide is [docs/dive.md](../docs/dive.md).

## Develop

```sh
uv run python scripts/dive_preview.py          # .dive-preview/cdc_ops.duckdb: the test scenario plus one source per state
cd dive && npm ci
CDC_OPS_DB=../.dive-preview/cdc_ops.duckdb npm run dev   # http://127.0.0.1:5173
CDC_DIVE_OWNER=duckdb CDC_OPS_DB=… npm run dev           # as the owner (the preview's current_user is duckdb)
# with action Flights, against simulated runs (a table name containing refuse/busy/fail exits 2/3/1):
CDC_DIVE_OWNER=duckdb CDC_DIVE_FLIGHTS='{"tpcc":{"table_action":"<uuid>","table_membership":"<uuid>","quarantine":"<uuid>","epoch":"<uuid>","delete_source":"<uuid>"}}' \
  CDC_OPS_DB=../.dive-preview/cdc_ops.duckdb npm run dev
npm run typecheck && npm test && npm run bundle
npm run dump-queries                           # after changing src/queries.ts
```

- **The preview** reads a local `cdc_ops` read-only through Node DuckDB. It needs no
  MotherDuck and no token.
- **The queries are data:**
  - `npm run dump-queries` renders every one into `queries.snapshot.json`.
  - `npm test` fails if that file is stale.
  - `tests/test_dive_queries.py` runs exactly those strings, against a `cdc_ops` filled by
    the Flights' own writers. Python CI needs no Node.
- **Where the code comes from:** the layout and runtime follow
  `motherduckdb/md-migration-assessment` (see `dive/THIRD_PARTY.md`).
- **What the code may use:**
  - only the Dive runtime's libraries: React, Recharts, d3, lucide-react and
    `@motherduck/react-sql-query`;
  - inline styles only;
  - times as epoch seconds from SQL, formatted in the browser in the viewer's zone.

## Design decisions

The reasons behind choices the code alone doesn't explain. What each screen shows is in
[docs/dive.md](../docs/dive.md).

- **Status is decided in SQL.** `tableStatus` in `src/queries.ts` decides each table's
  status, so `tests/test_dive_queries.py` can check every rule against a real `cdc_ops`.
  `model.ts` only fills in fixed text. Insert-only and catalog holds match on
  `apply_status.hold_code`, and fall back to the writers' stored texts for older writers.
- **Show only what `cdc_ops` records.** Where the screen would need data `cdc_ops` doesn't
  keep, the Dive says so instead of guessing:
  - an apply state it doesn't know reads "Unknown state: …";
  - "Behind by" reads unknown, never stale, when a commit time isn't at its row's own
    position;
  - the Recovery panel's start reads "not recorded yet", because no audit row records it.

  Removed tables are left off every screen.
- **Some fields read narrower than they look.**
  - A `key_change_pending` hold is left out of a table's holds, because apply clears it
    when its unit closes.
  - A pin shows when it "last moved", not when it was set ("pinned since"), because
    `pin_updated_at` changes on every apply run.
  - Quarantined batches have no open or closed state. The writer lists only the current
    generation's batches, so a resync removes them.
- **Times are viewer-local.** Queries return epoch seconds, and the browser formats them as
  `YYYY-MM-DD HH:MM:SS TZ` in the viewer's zone.
- **Actions are owner-only, and they fail closed.**
  - The Dive compares `SELECT current_user` (its queries run as the viewer) with the owner
    `deploy_dive.py --expect-user` writes into the bundle (`src/owner.ts`). If the viewer
    is unknown, the query fails or no owner was injected, the Dive is read-only.
  - Other viewers see the buttons disabled, with the reason, rather than hidden, so they can
    tell what the owner can do.
  - The real enforcement is the Flights' RUN grant. The deploy refuses an action Flight that
    grants RUN to anyone but its owner.
- **A read-only build carries no Flights placeholder.** `deploy_dive.py` then refuses
  `--flights`. The deploy also refuses a share of any database other than the one the Dive
  reads, because the share's DDL has to reach that database.
- **Every action previews first.**
  - The panel steps through Preview, Confirm, Run and Result. Each step's text is in
    `src/panel.ts`, with tests.
  - Run unlocks 3 s after Confirm, and only within 14 of the preview's 15 minutes
    (`PREVIEW_MINUTES`). After that, Re-run Preview starts a fresh dry run, so a stale
    preview can never run. No countdown is shown.
  - Opening an option always previews again, rather than reusing an older dry run. A
    preview is cheap and never out of date.
  - Actions that need typed input (quarantine, recover slot, skip to LSN, delete source)
    show the literal to type before the preview, because the Flights require it on the dry
    run.
  - Insert-only holds offer Resync first and never Retry.
- **A finished run waits for its audit row.** The share lags by about a minute. The panel
  re-reads the row every 5 s for up to 3 minutes (`AUDIT_POLL_MS`, `AUDIT_WAIT_MS`) before
  it reports a missing row. The preview simulates that lag (`runtime/query-provider.tsx`).
- **Runtime errors stay on screen.** Error boundaries and window listeners feed a fixed
  panel with Copy and Dismiss. It's kept in sessionStorage where the browser allows it, so
  a reload doesn't lose a stack trace.
- **Copy link moves the view first.** The view lives in the Dive's state, so Copy link opens
  the page or drawer it names and then copies the address.
