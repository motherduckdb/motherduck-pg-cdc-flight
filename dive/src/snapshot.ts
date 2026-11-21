// The rendered queries (see scripts/dump-queries.ts). The filtered rendering uses the
// Phase 2 test harness's source id (tests/test_dive_queries.py).
import { ACTIONS, EXAMPLES, FLIGHT_KEYS, PREVIEW_MINUTES, RESYNC_COST, buildConfig, type ActionName } from './actions.ts';
import * as Q from './queries.ts';
import { ALL, QUERIES } from './queries.ts';

export const SNAPSHOT_SOURCE = 'phase2';
export const SNAPSHOT_PREVIEW = '00000000-0000-4000-8000-000000000001';
export const SNAPSHOT_REQUEST = 'dive-snapshot-request';

export function render(): string {
  const entries = Object.entries(QUERIES).map(([name, query]) => [
    name,
    { columns: query.columns, all: query.sql(ALL), source: query.sql({ source: SNAPSHOT_SOURCE }) },
  ]);
  // Every action's dry-run and real-run config, for the check against the Flights.
  const actions = Object.fromEntries(
    (Object.keys(ACTIONS) as ActionName[]).map((name) => [
      name,
      {
        flight: ACTIONS[name].flight,
        forms: EXAMPLES[name].map((inputs) => ({
          dry: buildConfig(name, inputs, { dryRun: true, requestId: SNAPSHOT_REQUEST }),
          real: buildConfig(name, inputs, { previewId: SNAPSHOT_PREVIEW, requestId: SNAPSHOT_REQUEST }),
        })),
      },
    ]),
  );
  return (
    JSON.stringify(
      {
        source: SNAPSHOT_SOURCE,
        queries: Object.fromEntries(entries),
        actions,
        flight_keys: FLIGHT_KEYS,
        resync_cost: RESYNC_COST,
        hold_code_kind: Q.HOLD_CODE_KIND,
        preview_minutes: PREVIEW_MINUTES,
      },
      null,
      2,
    ) + '\n'
  );
}
