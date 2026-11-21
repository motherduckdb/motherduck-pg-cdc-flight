// The drawer's options per table state (the design's drawer, plus Pause and Remove): what
// each does and costs, and the action it previews, built
// from the row on screen (so a stale screen is refused by the Flight, never guessed past).
// An option that doesn't apply says why instead.
import type { ActionSpec } from './action';
import { RESYNC_COST } from './actions';
import { formatInt } from './format';
import type { TableRow } from './model';

export type FixOption = {
  name: string;
  does: string;
  cost: string;
  /** why it doesn't apply here; the card is greyed */
  na?: string;
  suggested?: boolean;
  /** the action it previews (absent when it doesn't apply) */
  spec?: ActionSpec;
  /** the panel doesn't send it yet (accept-types: the Flight takes it; wiring it
   * is the follow-up): the owner runs these commands instead (in order) */
  cli?: string[];
};

const NOTHING_FAILED = 'Nothing has failed.';

/** The blocked unit's bounds, from the drawer's `A..B`. */
const unitOf = (unit: string | null): [string, string] | null => {
  const m = /^([0-9A-F]+\/[0-9A-F]+)\.\.([0-9A-F]+\/[0-9A-F]+)$/.exec(unit ?? '');
  return m ? [m[1], m[2]] : null;
};

export function fixOptions(t: TableRow): FixOption[] {
  const base = { source: t.source_id, target: t.table_name };
  const retry = (): FixOption => ({
    name: 'Retry',
    does: 'Retry the failed batch.',
    cost: 'Nothing. If the same error comes back, the table stays blocked.',
    spec: { ...base, action: 'retry', verb: 'retry', would: 'Retry the failed batch of changelogs on the replica table. If it fails the same way, the table stays blocked.', inputs: { table: t.table_name, expectedBlockedLsn: t.blocked_commit_lsn ?? 'none' } },
  });
  const unit = unitOf(t.blocked_unit);
  const quarantine = (): FixOption => ({
    name: 'Quarantine',
    does: 'Set aside the failed batch and continue with newer changes.',
    cost: 'Rows in that batch may be wrong or missing in the replica until a resync. The table is marked with a gap.',
    ...(unit && t.epoch != null
      ? {
          spec: {
            ...base,
            action: 'quarantine' as const,
            verb: 'quarantine',
            would: `Set aside the failed batch ${unit[0]}..${unit[1]} and apply newer changelogs to the replica table.`,
            cost: 'Rows in that batch may be wrong or missing in the replica table until a resync.',
            inputs: { table: t.table_name, expectedEpoch: t.epoch, unitStart: unit[0], unitEnd: unit[1] },
          },
        }
      : { na: 'The blocked batch isn’t reported yet.' }),
  });
  const append = t.capture_mode === 'append';
  const resync = (over: Partial<FixOption> = {}): FixOption => ({
    name: 'Resync',
    ...(append
      ? {
          does: 'Take an exact re-snapshot of the source table; the current replica table is kept aside.',
          cost:
            'Holds again if rows keep changing. Refused while the table is paused, over the snapshot size limit, or while an older kept-aside table exists.' +
            (t.ignore_deletes ? ' DELETEs are skipped for this table, so rows the source has purged leave the replica; they stay only in the kept-aside table.' : ''),
        }
      : {
          does: 'Rebuild the table from a fresh snapshot.',
          cost: 'May take a long time, and the replica will be stale until the rebuild completes.',
        }),
    spec: t.state
      ? {
          ...base,
          action: 'resync',
          verb: 'resync',
          would: append
            ? 'Replace the replica table with an exact snapshot of the source table. The current replica table is kept aside.'
            : 'Rebuild the replica table from a fresh snapshot of the source table. Changelogs ingested meanwhile are applied once the rebuild finishes.',
          cost: RESYNC_COST[t.state] ? `Cost: ${RESYNC_COST[t.state]}.` : undefined,
          inputs: { table: t.table_name, expectedState: t.state },
        }
      : undefined,
    ...over,
  });
  const resume = (): FixOption => ({
    name: 'Resume',
    does: 'Apply the waiting changes in order, then keep the table updating.',
    cost: '',
    suggested: true,
    spec: { ...base, action: 'resume', verb: 'resume', would: 'Resume updates to this table. Changelogs ingested while it was paused are applied to the replica table in order.', inputs: { table: t.table_name, expectedState: 'paused' } },
  });
  const pause = (): FixOption => ({
    name: 'Pause',
    does: 'Stop applying changes to this table; they wait in the change log.',
    cost: 'The replica table stops updating. If the oldest kept change reaches the retention limit before you resume, the table needs a resync.',
    spec: { ...base, action: 'pause', verb: 'pause', would: 'Pause updates to this table. Changelogs are still being ingested into MotherDuck, and will be applied to the replica table upon Resume.', inputs: { table: t.table_name, expectedState: 'live' } },
  });
  const remove = (): FixOption => {
    const held = t.kind === 'registry';
    return {
      name: 'Remove',
      does: 'Stop replicating this table.',
      cost: 'The replica table is kept as it is. Undoing it needs Enable and a full rebuild.',
      spec: {
        ...base,
        action: 'remove',
        verb: 'remove',
        would: held
          ? `Stop ingesting changelogs for this table, which the source holds (${t.detail ?? 'a registry hold'}), and stop holding change history for it.`
          : 'Stop ingesting changelogs for this table and stop holding change history for it. The source table still exists.',
        // a forced remove's consequence line says it (panel.consequence)
        cost: held ? 'The replica table is kept as it is.' : undefined,
        // A held table needs a plain confirm (the Flight checks the source); any other is a
        // forced remove, which the Flight takes only with `REMOVE <table>` typed.
        inputs: held ? { table: t.table_name } : { table: t.table_name, force: true, confirm: `REMOVE ${t.table_name}` },
      },
    };
  };
  const startFromLog = (over: Partial<FixOption> = {}): FixOption => ({
    name: 'Start from log',
    does: 'Start the table again from the change log’s current position, with no snapshot.',
    cost: 'Rows inserted before the new start aren’t in the replica. The current replica table is kept aside; refused while an older kept-aside table exists.',
    spec: {
      ...base,
      action: 'start-from-log',
      verb: 'start from log',
      title: 'Start from log',
      would: 'Restart this table from the current position in the changelogs, with no snapshot. Rows inserted before then won’t be in the replica table.',
      cost: 'The current replica table is kept aside.',
      inputs: { table: t.table_name },
    },
    ...over,
  });
  const acceptTypes = (): FixOption => ({
    name: 'Accept types',
    does: 'Keep the replica’s column types and accept the source’s new ones, then retry.',
    cost: 'Only when the retype changed no value: otherwise the replica keeps values that differ from the source.',
    cli: [
      'cdc-flight-tables types',
      `cdc-flight-tables accept-types --table ${t.table_name} --dry-run`,
      `cdc-flight-tables accept-types --table ${t.table_name} --confirm "<the text the dry run printed>"`,
      'Then Retry from this drawer.',
    ],
  });
  const na = (o: FixOption, why: string): FixOption => ({ ...o, na: why, spec: undefined, suggested: false });

  switch (t.kind) {
    // Insert-only and catalog holds: Resync first.
    case 'append_changed':
      return [
        { ...resync(), suggested: true },
        { ...quarantine(), cost: 'The inserts in that batch become a recorded gap in the replica until a resync.' },
        startFromLog(),
        remove(),
      ];
    case 'catalog_changed':
      return append ? [{ ...resync(), suggested: true }, startFromLog(), remove()] : [{ ...resync(), suggested: true }, remove()];
    case 'type_changed':
      return [
        { ...resync(), suggested: true },
        { ...acceptTypes(), spec: undefined },
        { ...retry(), does: 'Retry after accepting the new types.', cost: 'Refused until the types are accepted.' },
        ...(append ? [startFromLog()] : []),
        remove(),
      ];
    case 'append_invalid':
      return [remove()];
    case 'not_started':
      // over the snapshot limit, start-from-log is the way in; otherwise resync retries it
      // (the panel's resync names the table's state, and a table with no row has none yet)
      {
        const again = t.state ? resync() : na(resync(), append || /over the append snapshot limit/.test(t.detail ?? '')
          ? 'A table with no replica yet can’t be resynced: start it from the log, or let the backfill Flight start its first copy.'
          : 'A table with no replica yet can’t be resynced: the backfill Flight starts its first copy.');
        const first = startFromLog({
          does: 'Start the table from the change log’s current position, with no snapshot.',
          cost: 'Rows already in the source table aren’t copied to the replica.',
        });
        if (first.spec) first.spec = { ...first.spec, would: 'Start this table from the current position in the changelogs, with no snapshot. Rows already in the source table won’t be in the replica table.', cost: undefined };
        return /over the append snapshot limit/.test(t.detail ?? '')
          ? [{ ...first, suggested: true }, { ...again, cost: 'Refused until the snapshot size limit is raised.' }, remove()]
          : [{ ...again, suggested: !!again.spec }, ...(append ? [first] : []), remove()];
      }
    case 'blocked':
      return [{ ...retry(), suggested: true }, quarantine(), resync(), remove()];
    case 'paused':
      return [resume(), na(quarantine(), NOTHING_FAILED), resync(), remove()];
    case 'rebuild_failed':
      return [
        na(retry(), 'Doesn’t apply to a failed rebuild.'),
        na(quarantine(), 'Doesn’t apply to a failed rebuild.'),
        resync({ does: 'Start the rebuild again from a fresh snapshot.', cost: 'Chunks done so far are discarded. The replica table keeps its old data until the rebuild finishes.', suggested: true }),
        ...(append ? [startFromLog()] : []),
        remove(),
      ];
    case 'rebuilding':
      return [
        na(retry(), NOTHING_FAILED),
        na(quarantine(), NOTHING_FAILED),
        na(resync({ does: 'Restart the rebuild from a new snapshot.', cost: `The ${formatInt(t.chunks ?? 0)} chunks done so far are discarded.` }), 'Refused while a rebuild runs: wait for it to finish or fail.'),
        remove(),
      ];
    case 'needs_resync':
    case 'queued':
      return [na(retry(), 'The change log can’t bring this table up to date.'), na(quarantine(), NOTHING_FAILED), resync({ suggested: true }), ...(append ? [startFromLog()] : []), remove()];
    case 'registry':
      return [na(retry(), 'The source table changed: nothing to retry.'), na(quarantine(), NOTHING_FAILED), na(resync(), 'Refused while the source table is held.'), { ...remove(), suggested: true }];
    case 'not_enabled':
      return [
        {
          name: 'Enable',
          does: 'Start capturing this table and rebuild it in the replica from a fresh snapshot.',
          cost: 'The table appears in the replica once its first rebuild finishes.',
          suggested: true,
          spec: { ...base, action: 'add', verb: 'enable', would: 'Start ingesting changelogs for this table into MotherDuck, and build the replica table from a fresh snapshot.', cost: 'The replica table appears once its first snapshot finishes.', inputs: { table: t.table_name } },
        },
      ];
    case 'removed': // the registry removed it: nothing to do
      return [];
    case 'unknown':
      return [na(retry(), NOTHING_FAILED), na(quarantine(), NOTHING_FAILED), na(resync(), 'The Dive doesn’t know this state.'), remove()];
    default: // ok, stale, waiting: a live table
      return [na(retry(), NOTHING_FAILED), na(quarantine(), NOTHING_FAILED), resync(), pause(), remove()];
  }
}

/**
 * Finish recovery for a leftover slot: the source's newest completed transition again,
 * exactly (its action, epoch, LSN and confirmation), so it drops the old slot and never
 * begins another epoch. The audit copy is best effort, so with no recorded transition it is
 * keyed on the source's current epoch E (E >= 2): the recover-slot that began E, expected
 * epoch E - 1 and `RECOVER <source> EPOCH E`, which the epoch Flight treats as its repeat.
 */
export function finishRecovery(
  source: string,
  epoch: number | null,
  last: { action: string; expected_epoch: number; to_lsn: string | null; confirm: string } | null,
): ActionSpec | null {
  const base = { source, target: source, verb: 'finish recovery', title: 'Finish recovery', would: 'Repeat the last recovery exactly to drop the leftover slot. It never begins another epoch.' };
  if (last && (epoch == null || last.expected_epoch + 1 === epoch))
    return {
      ...base,
      action: last.action === 'skip-to-lsn' ? 'skip-to-lsn' : 'recover-slot',
      inputs: { expectedEpoch: last.expected_epoch, toLsn: last.to_lsn ?? undefined, confirm: last.confirm },
    };
  if (epoch == null || epoch < 2) return null;
  return { ...base, action: 'recover-slot', inputs: { expectedEpoch: epoch - 1, confirm: `RECOVER ${source} EPOCH ${epoch}` } };
}
