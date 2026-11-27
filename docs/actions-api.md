# Action Flights API

This page is the single reference for operating a source through its action Flights: pausing,
resyncing, adding or removing tables, repairing a held table, recovering a lost slot, and
deleting a source. It's written for anything that runs the Flights directly, whether a script,
an AI agent, or another MCP client. The Dive's buttons use the same API
([dive.md](dive.md#actions)).

- **Machine-readable spec:** [`flights/actions.json`](../flights/actions.json) has the inputs,
  formats, actions, confirmations, decision fields, statuses and result fields.
  `tests/test_actions_spec.py` checks it against the code.
- **Background:** [operations.md](operations.md#action-flights) covers each action's effect
  and its guards in depth.

## Contents

- [Operating a source as an agent](#operating-a-source-as-an-agent)
- [Finding a source's action Flights](#finding-a-sources-action-flights)
- [Running a Flight and reading its result](#running-a-flight-and-reading-its-result)
- [Inputs every action Flight takes](#inputs-every-action-flight-takes)
- [The five Flights and their actions](#the-five-flights-and-their-actions)
- [Confirmations](#confirmations)
- [The preview: dry run, then real run](#the-preview-dry-run-then-real-run)
- [Results](#results)
- [Where the expected inputs come from](#where-the-expected-inputs-come-from)

## Operating a source as an agent

Action Flights change production data paths. Some of them, such as quarantine, recover-slot,
skip-to-lsn, delete-source and a forced remove, lose data or stop capture for good. An agent
follows these steps every time:

1. **Read the current state** from `cdc_ops` ([below](#where-the-expected-inputs-come-from)).
   The expected values you send come from it.
2. **Dry run first**, with `CDC_DRY_RUN=1` and a fresh `CDC_REQUEST_ID`. A dry run changes
   nothing but its own preview row.
3. **Show the user the preview** before anything else: `detail`, `next_steps`, the relevant
   `observed` values, and for delete-source the `drop` and `dba` lists. For an action with a
   confirmation, show the exact literal: the result's `confirm`.
4. **Get the user's explicit confirmation.** For an action with a typed confirmation, the user
   approves that literal. Never confirm on the user's behalf, and never run a real run the user
   didn't approve.
5. **Run the real run** within 15 minutes, with `CDC_PREVIEW_ID` set to the dry run's run id,
   the **same inputs** (`CDC_CONFIRM` included, byte for byte, where the dry run had it), and
   a new `CDC_REQUEST_ID`.
6. **Read the result** by its request id ([Results](#results)). Read `status`, never the exit
   code alone.
7. **React to the status:**
   - `refused`: tell the user why (`detail`). If the state moved or the preview expired, start
     again at step 1. Never retry the same preview.
   - `busy` or `waiting`: nothing changed. Wait, then run again. If the real run is refused,
     dry-run again.
   - `failed`: the preview is spent. Follow `next_steps`. Never rerun the same preview.
   - `done` or `already`: follow `next_steps` (for example, resync a table after a quarantine).
8. **A `done` that starts another Flight** means it **started**: follow `triggered.run_id` to
   that run, or read `cdc_ops.backfill_status`.

## Finding a source's action Flights

- **Only the Flights' owner can run them.** They have no RUN grants, so an agent runs SQL
  with the owner's token.
- **Names:** `cdc-<source>-<command>` by default, where the command is `table-action`,
  `quarantine`, `table-membership`, `epoch` or `delete-source`. The deploy file can override a
  name (`[flights.<command>] name`), so check the deploy file if a lookup finds nothing.
- **Ids:**

  ```sql
  SELECT flight_id, flight_name FROM md_list_flights() WHERE flight_name = 'cdc-shop-table-action';
  ```

## Running a Flight and reading its result

Start a run with every value as a string. Keys you don't set may be left out or sent as `""`:

```sql
SELECT run_id, run_number
FROM MD_RUN_FLIGHT(flight_id := ?::UUID,
                   config := map(['CDC_ACTION', 'CDC_TABLE', 'CDC_EXPECTED_STATE', 'CDC_DRY_RUN', 'CDC_REQUEST_ID'],
                                 ['pause', 'public.orders', 'live', '1', 'agent-7f3a']));
```

Wait until the run is terminal (`SUCCEEDED`, `FAILED` or `CANCELLED`):

```sql
SELECT status, exit_code FROM MD_GET_FLIGHT_RUN(flight_id := ?::UUID, run_number := ?);
```

Then read the result. There are two places:

1. **`cdc_ops.audit`**, by the request id you sent: `SELECT * FROM cdc_ops.main.audit WHERE request_id = ?`.
   - `result` is the JSON result ([Results](#results)).
   - `run_id` is the run's id. For a dry run, it's the preview id.
   - The row also has `action`, `inputs` (JSON, LSNs as text), `dry_run`, `started_by` and
     `written_at`.
   - The Flight runs `CREATE SNAPSHOT OF cdc_ops` after writing it. A reader on a share runs
     `REFRESH DATABASE` to see it. The snapshot is best effort: if the row doesn't show,
     use the log.
2. **The run's log:** the last line of `MD_GET_FLIGHT_LOGS(flight_id := ?::UUID, run_number := ?)`
   is the same JSON result. Lines starting `@flight-run` are progress, not results.

**No audit row is written** in three cases, so the log is the only record:
- an input was refused before the Flight connected (exit 2; `action` is null when it wasn't one
  of the Flight's actions, and `table` and `observed` are empty);
- the run crashed (exit 1, a traceback, no result line);
- `cdc_ops` doesn't exist yet.

## Inputs every action Flight takes

| Key | Format | Rule |
| --- | --- | --- |
| `CDC_ACTION` | one of the Flight's actions | Required. |
| `CDC_DRY_RUN` | `0` or `1` (blank: `0`) | `1` for the dry run. |
| `CDC_PREVIEW_ID` | a UUID | Required on every real run: the dry run's run id. Must be blank on a dry run. |
| `CDC_REQUEST_ID` | 1–64 of `A-Z a-z 0-9 _ -` | Optional. Copied to `cdc_ops.audit.request_id` on every run that writes an audit row. Not part of the preview. Use a fresh one per run. |

- **A key the action doesn't use must be blank.**
- **Every input is checked before the Flight connects.** A refusal names the key, never the
  value.
- **Config is visible to the whole org.** Table names and LSNs are fine; nothing secret goes in
  config.

## The five Flights and their actions

Formats: a table is `schema.table`. A state is `live`, `paused`, `needs_action` or
`needs_resync`. An LSN is `X/Y` in hex, in any case. An epoch is an integer from 1.

### `table-action`

Keys: `CDC_ACTION`, `CDC_TABLE`, `CDC_EXPECTED_STATE`, `CDC_EXPECTED_BLOCKED_LSN`,
`CDC_CONFIRM`, `CDC_DRY_RUN`, `CDC_PREVIEW_ID`, `CDC_REQUEST_ID`. No PostgreSQL connection.

| Action | Inputs | Confirmation | Starts | Effect |
| --- | --- | --- | --- | --- |
| `pause` | `CDC_TABLE`, `CDC_EXPECTED_STATE` | – | – | `live` → `paused` |
| `resume` | `CDC_TABLE`, `CDC_EXPECTED_STATE` | – | – | `paused` → `live` |
| `retry` | `CDC_TABLE`, `CDC_EXPECTED_BLOCKED_LSN` (`X/Y`, or `none`) | – | – | `needs_action` → `live` after a repair; replays the whole blocked unit |
| `resync` | `CDC_TABLE`, `CDC_EXPECTED_STATE` | – | backfill | a full reload of the table |
| `start-from-log` | `CDC_TABLE` | – | – | an append table's first mirror generation, from the log |
| `start-snapshot` | `CDC_TABLE` | – | backfill | a registered table's first snapshot |
| `accept-types` | `CDC_TABLE`; `CDC_CONFIRM` on the real run | from the dry run | – | accepts a held source type change under the same mirror type; `retry` next |
| `resync-all` | none | – | backfill | resyncs one `needs_resync` table, the one held longest, and reports `remaining` |
| `compact-now` | none | – | compact | compaction with the owner's settings |

### `quarantine`

Keys: `CDC_ACTION`, `CDC_TABLE`, `CDC_EXPECTED_EPOCH`, `CDC_CONFIRM`, plus the framework keys.
No PostgreSQL connection.

| Action | Inputs | Confirmation | Effect |
| --- | --- | --- | --- |
| `quarantine` | `CDC_TABLE`, `CDC_EXPECTED_EPOCH`, `CDC_CONFIRM` | `QUARANTINE <schema.table> UNIT <epoch> <start>..<end>`, on both runs | Skips the whole unit a table is held before (a poison unit only). The rows it touched may diverge until a `resync`. |

### `table-membership`

Keys: `CDC_ACTION`, `CDC_TABLE`, `CDC_FORCE`, `CDC_CONFIRM`, `CDC_MODE`,
`CDC_IGNORE_DELETES`, plus the framework keys. `add` reads PostgreSQL.

| Action | Inputs | Confirmation | Starts | Effect |
| --- | --- | --- | --- | --- |
| `add` | `CDC_TABLE`; optional `CDC_MODE` (`keyed` or `append`) and `CDC_IGNORE_DELETES` (`0` or `1`, append only) | – | backfill | Registers a published table, then starts its backfill. An append add starts the snapshot only when the backfill Flight can take one; otherwise the next step is `start-from-log`. |
| `remove` | `CDC_TABLE`; for a table that still exists, `CDC_FORCE=1` and `CDC_CONFIRM` | `REMOVE <schema.table>`, on both runs | – | Stops capturing the table. The mirror table is kept. |

### `epoch`

Keys: `CDC_ACTION`, `CDC_EXPECTED_EPOCH`, `CDC_TO_LSN`, `CDC_CONFIRM`, plus the framework keys.
Changes PostgreSQL replication slots.

| Action | Inputs | Confirmation (both runs) | Effect |
| --- | --- | --- | --- |
| `recover-slot` | `CDC_EXPECTED_EPOCH`, `CDC_CONFIRM` | `RECOVER <source> EPOCH <epoch+1>` | Creates a new slot and begins the next epoch. WAL the old slot lost is gone. |
| `skip-to-lsn` | `CDC_EXPECTED_EPOCH`, `CDC_TO_LSN`, `CDC_CONFIRM` | `SKIP <source> TO <X/Y>` | Advances the slot and begins the next epoch. WAL before the target is never ingested. |

Afterwards every mirror needs a resync: run `resync-all` on `table-action` until `remaining`
is 0.

### `delete-source`

Keys: `CDC_ACTION`, `CDC_EXPECTED_EPOCH`, `CDC_CONFIRM`, plus the framework keys. Changes
PostgreSQL.

| Action | Inputs | Confirmation (both runs) | Effect |
| --- | --- | --- | --- |
| `delete-source` | `CDC_EXPECTED_EPOCH`, `CDC_CONFIRM` | `DELETE <source>` | Stops capturing the source for good and drops the PostgreSQL objects this code created. The mirrors and the change log are kept. The result lists what was dropped and the DDL left for the DBA. |

## Confirmations

Each confirmation is compared exactly. There are three kinds:

- **Typed first** (`quarantine`, `recover-slot`, `skip-to-lsn`, `delete-source`, and a
  `remove` with `CDC_FORCE=1`): the literal goes on the **dry run and the real run**, identical.
  It's part of the preview, so a real run with different text is refused.
  - Build it from the current state ([below](#where-the-expected-inputs-come-from)). LSNs are
    written in canonical form: uppercase hex, no leading zeros (`0/5a` is written `0/5A`).
  - A wrong literal is refused with `type the confirmation exactly: "…"`, which shows the
    right one. That happens only after the epoch (and, for quarantine, the blocked unit) are
    checked: if those have moved, the refusal says "state changed" instead.
  - A blank `CDC_CONFIRM` is refused before the Flight connects, with no literal.
- **From the dry run** (`accept-types`): the dry run takes no `CDC_CONFIRM` and returns the
  literal `ACCEPT TYPES <schema.table> <digest>` in `confirm` (also in `observed.confirm` and
  `next_steps`).
  Only the real run sends it. Its digest names the exact change, so a moved state needs a new
  dry run.
- **None:** every other action. `CDC_CONFIRM` stays blank.

Every `would` result has `confirm`: the exact literal its real run sends, or null for an action
without one. For a typed-first action it's the literal the dry run checked.

## The preview: dry run, then real run

1. **The dry run** reads and decides. On `would`, it records a preview, its only write, and
   returns `preview_id`, which is its own run id.
   - It takes no lease and changes no data.
   - It can also answer `already`, `refused`, `busy` (membership, while an open unit drains) or
     `waiting` (epoch, while ingest holds the lease). Those record no preview.
2. **The real run** names the preview in `CDC_PREVIEW_ID`, with the same inputs. It spends the
   preview once, only if all of these hold:
   - the preview is unused and under **15 minutes** old;
   - the action, the table, the inputs and the action's **decision fields** hash the same as
     what the real run reads now. Decision fields are the state the decision rests on, such as
     the table's state, generation, epoch and blocked unit (per action in the spec).

   Otherwise it's `refused`: "no matching unused preview from the last 15 minutes; run a dry
   run again".
3. **When a preview is spent:**
   - Actions that change one database (`pause`, `resume`, `retry`, `quarantine`, `accept-types`,
     `start-from-log`, `add`, `remove`) spend it in the change's own transaction. A `busy` or a
     failure leaves it unspent.
   - Actions that start another Flight (`resync`, `start-snapshot`, `resync-all`,
     `compact-now`) spend it, then start the Flight. If the start fails, it's spent.
   - `recover-slot`, `skip-to-lsn` and `delete-source` spend it before changing PostgreSQL. If
     the run then stops, it's spent.
4. **Two paths finish earlier work without a new preview.**
   - A `delete-source` of a source already `deleting` continues the delete.
   - A repeat of a committed epoch transition sends the previous epoch with that transition's
     literal; it reports `already` and drops a leftover slot.

   Both still need `CDC_PREVIEW_ID`: send the dry run's run id. It isn't checked on these
   paths.

## Results

Each run prints one JSON line, which is also `cdc_ops.audit.result`. Every result has these
fields, whichever Flight and however the run ended; an empty one is `null`, `[]` or `{}`:

| Field | Meaning |
| --- | --- |
| `status` | `would`, `done`, `already`, `waiting`, `refused`, `busy` or `failed` |
| `action` | the action |
| `table` | the table; `null` for `epoch` and `delete-source`, and for actions without one |
| `detail` | what happened or would happen, in words |
| `next_steps` | what to do next, in words |
| `observed` | what the run read: the preview's facts. LSNs here are raw integers ([LSNs](#lsns)). |
| `run_id` | this run's id (`null` for an input refusal outside a Flight run) |

A `would` also has:

| Field | Meaning |
| --- | --- |
| `preview_id` | the id the real run sends as `CDC_PREVIEW_ID` |
| `confirm` | the exact `CDC_CONFIRM` the real run sends, or `null` ([Confirmations](#confirmations)) |

Per Flight:
- **`table-action`:** `triggered` (`flight`, `run_id`, `run_number` of the started run), and
  `remaining` (`resync-all`).
- **`table-membership`:** `result`, the core's own word (`added`, `adopted`, `reactivated`,
  `relabelled`, `removed`, `drain_required`, …), and `triggered`.
- **`epoch`:** `epoch`, `slot`, `start_lsn`, `abandoned`, `clamped`, `requested_to_lsn`,
  `leftover_slot`, `pending_slot`, and the abandoned unit (`abandoned_unit_start_lsn`, …).
  These LSNs are `X/Y` text.
- **`delete-source`:** `lifecycle`, and `drop`, `dba`, `retained_bytes` on a dry run, or
  `dropped`, `dba`, `retained_bytes` on a real run. Its `observed` is empty: the lists are
  top-level.

### Statuses and exit codes

| `status` | Exit code | Meaning |
| --- | --- | --- |
| `would` | 0 | The dry run decided. A preview was recorded. |
| `done` | 0 | The change was made. For an action that starts a Flight, it was started. |
| `already` | 0 | Nothing to do. |
| `waiting` | 0 | `epoch`, `delete-source`: nothing (more) changed yet. Run again. |
| `refused` | 2 | Invalid, stale or not allowed. `detail` says why. |
| `busy` | 3 | Another run holds the table or source. Run again later. |
| `failed` | 1 | The change was made, but starting the next Flight failed. The preview is spent; follow `next_steps`. |
| (none) | 1 | The run crashed: there's no result line and no audit row. See the log. A membership Flight without its PostgreSQL secret fails this way. |

**Read `status`, not the exit code.** Exit 0 covers four statuses.

## Where the expected inputs come from

| Input | Read |
| --- | --- |
| `CDC_EXPECTED_STATE` | `cdc_ops.apply_status.state` for the table |
| `CDC_EXPECTED_BLOCKED_LSN` | `apply_status.blocked_commit_lsn` as `X/Y`; `none` when it's NULL |
| `CDC_EXPECTED_EPOCH` (quarantine) | `apply_status.epoch` |
| quarantine's unit | `apply_status.blocked_unit_start_lsn` and `blocked_unit_end_lsn`, as `X/Y` |
| `CDC_EXPECTED_EPOCH` (epoch, delete-source) | `cdc_ops.ingest_status.epoch` |

### LSNs

`cdc_ops` stores LSNs as `UBIGINT`, and so does a result's `observed`. Inputs and literals
take `X/Y` text. To convert:

```sql
SELECT printf('%X/%X', blocked_commit_lsn >> 32, blocked_commit_lsn & 4294967295) AS lsn
FROM cdc_ops.main.apply_status WHERE source_id = ? AND schema_name = ? AND table_name = ?;
```

For example, a quarantine literal for one held table:

```sql
SELECT 'QUARANTINE ' || schema_name || '.' || table_name || ' UNIT ' || epoch || ' '
       || printf('%X/%X', blocked_unit_start_lsn >> 32, blocked_unit_start_lsn & 4294967295) || '..'
       || printf('%X/%X', blocked_unit_end_lsn >> 32, blocked_unit_end_lsn & 4294967295) AS confirm
FROM cdc_ops.main.apply_status WHERE source_id = ? AND schema_name = ? AND table_name = ?;
```
