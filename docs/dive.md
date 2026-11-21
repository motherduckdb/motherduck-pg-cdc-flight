# The operations Dive

`dive/` is a MotherDuck Dive that shows Postgres → MotherDuck replication from the status the
Flights write to `cdc_ops` ([operations.md](operations.md)). It is organized around sources and
tables, not Flights: the Overview lists every source with its tables, problems first; a source
opens its Source page; a table opens its drawer. The runbooks in [runbooks.md](runbooks.md) start
from its screens.

Only the Flights' owner can act, through the action panel (see [Actions](#actions)); every
other viewer sees it read-only. The Dive never writes: the action Flights do.

## Data access

- **The Dive reads one thing: a share of `cdc_ops`.** Its `REQUIRED_DATABASES` names that share
  under the alias `ops`, and every query reads `"ops"."main".<table>`. It never reads
  `cdc_internal`, the stores or the mirrors. `tests/test_dive_queries.py` runs every query
  with only `ops` attached and checks every table reference.
- **The share must be `UPDATE AUTOMATIC`.**
  - Otherwise viewers see the snapshot taken when it was created, and every Flight turns
    stale.
  - Even with automatic updates, viewers lag the Flights by about a minute. Every staleness
    threshold is well above that.
- **Who sees what:**
  - The Dive and the share are viewable by the whole org.
  - Viewers see source ids, table, slot and generation names, LSNs, exception types and
    action inputs, but no row data.
  - `audit.token_user` is the Flight owner's MotherDuck user name, an email address, and is
    visible to the whole org.

A fresh `cdc_ops` gets all ten tables on the first status report (including
preflight), so no compaction run is needed before its queries can bind. Reporting
is best effort; deployment also creates missing tables and columns before its
precheck. See [operations setup](operations.md#deploying).

## Screens

The view (screen, source, open table, filters) is kept in the Dive's state, so a copied link
reopens it. Tables, Activity and Help aren't designed yet and say so.

| Screen | Shows | Reads |
| --- | --- | --- |
| **Overview** | "Status as of" (the newest status any Flight wrote); one card per source, problems first: its status and headline, last sync and its usual interval, WAL limit used; its tables, problems first, with status, details, Last updated, 28 sync marks of 6 h and changes over 7 days. Filters: one source, only issues. | `tableStatus`, `sourceStatus`, `syncMarks`, `statusAsOf` |
| **Sources** | The same cards, collapsed. With one source, its Source page. | the same |
| **Source page** | The pipeline (Postgres, Ingest, Change log, Apply, MotherDuck replica); WAL limit and slot, with every slot (current, pending, leftover: the last two keep retaining WAL) and 7 days of retained WAL; recovery progress while a recovery runs; the tables, then those not enabled; change log retention and its pins; the setup check (preflight, TLS); the danger zone's context. A read replica source (see [Read replica sources](#read-replica-sources)) also shows the Read replica card. | the above, `sourcePipeline`, `sourceSlots`, `walHistory`, `sourcePins`, `sourceSetup`, `tls`, `sourceStandby` |
| **Table drawer** | The table's status, details and fixed help text; its fix options (read-only); backfill progress; sync marks; gaps and quarantines over 30 days; its retention pin; its action history; technical details (epoch, generation, LSNs). | `tableStatus`, `syncMarks`, `tableGaps`, `tableHistory` |

**Wording on screen:** "replica", never "mirror"; "Blocked", not "held"; "WAL limit", not
"slot", on the Overview, which also keeps LSNs, `wal_status` and epochs off. Sources show
"Last sync" (the newest ingest run); tables show "Last updated".

**Times** read `YYYY-MM-DD HH:MM:SS TZ` in the viewer's time zone. Queries return epoch
seconds and the browser formats them. Last updated is a commit time on the source database's
clock (its tooltip says so).

### Table status

Each table's status is decided in SQL (`tableStatus` in `dive/src/queries.ts`), first match
wins; `dive/src/model.ts` fills the fixed details text. The states behind these statuses, and
what moves a table between them, are in [tables.md](tables.md#table-states).

| Status | When | Details |
| --- | --- | --- |
| (left out) | the registry removed it | |
| Not enabled | in the publication, never added (registry `unmanaged`) | In publication, not enabled · First seen {ts} |
| Needs action | registry hold `append_identity` or `append_partitioned` | Can’t capture as insert-only: its replica identity isn’t FULL / the table is partitioned |
| Needs action | any other registry hold (dropped, renamed, no key, …) | Blocked by the source: {reason}: {detail} |
| Rebuilding | `needs_action` with a resync running (a keyed table's backfill, or an insert-only table's re-snapshot), beside its held replica; if it fails, the table reads Needs action again | as Rebuilding below |
| Needs action | `needs_action`, detail `append_update: …` / `append_delete: …` | A row was changed / deleted in an insert-only table · Blocked since {ts} ({age}) |
| Needs action | `needs_action`, detail `column '<c>' changed type …` | A column changed type: {c} · Blocked since {ts} ({age}) |
| Needs action | `needs_action`, an insert-only table's catalog-check detail | Source table changed: {reason} |
| Needs action | apply stopped on a failed batch (`needs_action`) | Sync failed: {apply_status.detail} · Blocked since {ts} ({age}) |
| Rebuilding | `needs_resync`, or an add or resync, with a generation snapshotting or catching up | Backfill · {done} of ~{total} chunks · Started {ts} by {who} ({action}) |
| Needs action | `needs_resync` and its newest generation failed | Rebuild failed: {error} · Newest backfill run {ts} |
| Queued | `needs_resync` during a recovery (its epoch is behind ingest's); never an insert-only table | Queued for rebuild |
| Needs action | `needs_resync`, a keyed table's catalog-check detail | Source table changed: {reason} · or "Resync waits for the next ingest run" |
| Needs action | `needs_resync` otherwise | Needs resync: {detail} |
| Paused | paused (Needs action within 24 h of its pin's estimated expiry) | Paused · retention expires in {left} · Paused by {who} {ts} · est. expiry {ts} |
| Needs action | no replica yet: its first copy failed | Rebuild failed: {detail} |
| Needs action | no replica yet: registered, no generation (`apply_runs` `awaiting_generation`, a refused first snapshot, or a current `not_started` row) | Not started: {refusal} |
| Unknown | an apply state the Dive doesn't know | Unknown state: {state} |
| Waiting | its source's sync stopped: ingest or apply failed or stale ([Health](#health)), slot lost or missing, or source deleted | Source sync stopped |
| Stale | not caught up for longer than apply's own staleness limit (Needs action past 24 h) | Stale over {age} |
| OK | otherwise | — |

- **Caught up:** the table's `(epoch, applied_lsn)` reached ingest's `(epoch, checkpoint_lsn)`.
  It means every logged change is applied, not that the replica table equals the source table:
  Most DDL that writes no logical changes (ADD COLUMN with a DEFAULT, ATTACH or DETACH
  PARTITION, a replaced primary key, a source type change) holds the table. A same-type
  `USING` rewrite still leaves a table that reads OK and differs until a resync. Each table is caught up on its own, with no atomicity across tables.
  See [operations.md](operations.md#current-as-of).
- **Why a table is held** is `apply_status.hold_code` ([operations.md](operations.md#hold-codes)), each
  code mapped to a state by `HOLD_CODE_KIND` in queries.ts (`test_every_hold_code_has_a_dive_state`).
  A registry code there counts as a hold even before ingest's holds list carries it. A NULL code
  on a held table (an older writer, or a code its writer found stale) falls back to the stored
  text (`apply_status.detail` and the registry hold's reason), the writers' contract with the Dive: `append_update: ` and `append_delete: `,
  `catalog_checks.REASONS` and `APPEND_REASONS` (the codes stay in `cdc_internal`), and
  `column '<c>' changed type`. `test_the_dives_hold_texts_are_the_writers` checks each against
  the source.
- **Insert-only (append mode) tables** carry a tag on the Source page, and the drawer lists:
  DELETEs skipped (`deletes_ignored`, under `ignore_deletes`); emptied by a source TRUNCATE
  (`last_truncate_lsn`); started from the log at S (the table's own `log_incomplete`
  generation's `pin_lsn`): rows inserted before it aren't in the replica; and the previous
  replica kept aside as `<table>__gen_<first 8 hex of the replaced generation>`, with its DROP
  (fully qualified; cdc_ops doesn't name the replica database, so the owner fills it in). cdc_ops can't tell whether it was dropped since.
- **A failed rebuild beside a live replica** (an insert-only re-snapshot) shows in the drawer
  as "Rebuild failed: {backfill_runs.detail}"; a resync refused since the newest generation
  shows as "Last resync refused", or "Resync waits for the next ingest run" for the refusal
  that clears by itself.
- **Behind by** is ingest's `durable_through_commit_ts` minus the table's
  `applied_through_commit_ts`. Each counts only when its `(epoch, lsn)` pair is the row's own
  position; otherwise it is unknown, which never reads stale, and Last updated reads "unknown".
- **Who** paused a table or started a rebuild comes from the newest real-run `audit` row of
  that action (a recovery's rebuilds: `recover-slot` or `resync-all`), shown as the user name
  before the @.
- **A source's pill** is its own problem first (slot lost, sync failing), then a recovery,
  then the worst of its tables and its WAL limit (Warning from 50 %).
- **A recovery** runs while a counted table's epoch is behind ingest's. Its buckets: rebuilt
  (live at the current epoch), rebuilding, failed and queued. Removed and unmanaged tables
  don't count.
- **WAL limit used** is `retained_bytes / (retained_bytes + safe_wal_size)` of the current
  slot: `safe_wal_size` is what Postgres can still retain before it invalidates the slot. NULL
  `safe_wal_size` is no cap ("disk is the limit"); `wal_status` `lost` or `missing` reads Lost,
  `unreserved` an alarm. It is read once per ingest run; the card shows its age.
- **Sync marks:** per table, 28 marks of 6 h: ok (only ok apply results), some failed, all
  failed, or nothing synced. A failed apply run reached no table, so it counts for every table
  of its source.

### Health

The Flights' health feeds the Source page's pipeline strip and decides when a source's
tables read Waiting.
Each Flight's health comes from its own run rows, never from a snapshot's `written_at`: apply
refreshes `apply_status` even on a run that failed.

| Flight | Read from |
| --- | --- |
| ingest | `ingest_status.status_as_of` (the last leased run) and `last_failure_*`; the cadence comes from `ingest_runs` |
| apply | the newest `apply_runs` row with `table_name` NULL (the run's own row) |
| backfill | the newest `backfill_runs` row (it has no cadence: it runs only while a backfill does) |
| compaction | `compaction_status.run_as_of` and `status` |

Each Flight's run statuses map to health through an explicit list (`STATUS_HEALTH` in
`dive/src/queries.ts`). A status not on the list reads **unknown**, never ok. A change that adds a run status to a Flight adds it to that list. The health
values:
- **failed:** the newest run failed (for compaction, also `invariant`).
- **attention:** apply stopped on held tables (`needs_operator`; the tables show why), or
  compaction is `held` (a published table lost its pin, so nothing more drops).
- **stale:** the newest run is older than `max(floor, 2 × the upper-quartile interval of the
  last 20 runs)`. With fewer than 5 runs, the floor alone applies. The floors are 10 min for
  ingest, 30 min for apply and 26 h for compaction.
  - **Why the upper quartile:** manual runs only ever add short intervals. A burst of them
    (a test, an incident) would pull a median below the schedule, and every scheduled gap
    would then read stale. The upper quartile holds while more than a quarter of the last
    20 intervals are the schedule's.
  - **Apply is never stale sooner than its source's ingest**, because ingest starts apply
    (its runs aren't scheduled on their own). So a broken chain, where ingest keeps running
    but no longer starts apply, shows as stale apply only after ingest's limit (2 h with
    hourly ingest), not apply's 30 min floor.
  - **Needs action when no run succeeds for long:** a stale ingest or apply whose last
    successful run is older than `max(1 h, 6 × the same interval)` (1 h with fewer than 5
    runs) turns the source to Needs action: "No successful ingest for {age}" (or apply). Apply
    is never urgent sooner than its ingest. The Ingest or Apply stage reads "Last successful
    run {age} ago (usually every …)" ("No successful run recorded" when no ingest has ever
    succeeded; the headline then reads "No successful ingest recorded"), and a hint under the pipeline says that runs failing
    before the pipeline starts (installing the package, reading a secret) leave no record
    here, so the Flight's run log has the cause. Compaction stays stale.
- **ok:** none of the above.
- **Busy runs** never set the status or reset staleness: the newest non-busy run does, and
  `busy since` notes a newer busy run. So a crash followed by busy runs, while its leases
  expire, still reads failed.

### Read replica sources

A source captured from a read replica (standby) is one whose `ingest_status.standby` is set
(`sourceStandby`; [read-replica.md](read-replica.md) has the fields and their clocks). The sources
list tags it `Read replica`, and its Source page adds a Read replica card: replay lag, the last
replayed commit (the primary's clock), the upstream connection, the heartbeat and the last run's
end (`drained`: reached the replay position; `capped`: stopped at its runtime cap; other ends show on the pipeline). Every age is
taken at the run's observation, on the read replica's clock.

All of it is advisory: no status but the card's own reads it. The heartbeat is stale when it is
older than 3 × its declared interval, or missing, and reads "stale or upstream unreachable"
(the card warns; the source's status doesn't change). When no WAL receiver is visible (it isn't
running, or the role lacks `pg_read_all_stats`) the upstream reads "unknown", never "unreachable". A slot invalidated by a recovery conflict
(`invalidated_by_conflict`) names its reason in the banner and leads to Recover slot. The read
replica's backfill waits are `waiting_for_replay` and `waiting_past_replay` (ok) and
`waiting_for_primary` (attention: the primary's DBA may need to act); a table shows each wait
with its fix, and the setup check shows preflight's `waiting_for_primary`.

The card also reads the heartbeat job from the source's last 3 ingest runs (`ingest_runs.standby`).
When all 3 ended `capped` with the same replicated marker, it reads "heartbeat job not running" if
the WAL receiver streamed in each run and its last contact moved forward from the oldest run to
the newest. If any run's upstream contact was unknown, it says it can't tell a stopped job from a
read replica that isn't receiving. Either warns on the card only and replaces the stale heartbeat
line. The marker is compared for equality only, contact against its own clock, and the span uses
the runs' own write times, so no clock is compared with another. A drain, a new marker, fewer
runs, or replay standing still with WAL waiting (paused, or held by a recovery conflict: the
marker can't move then) reads nothing. A heartbeat table shared by several sources can let another source's heartbeat
drain this one's runs, so the signal may not fire there.

### PostgreSQL TLS

The Source page's setup check shows TLS, with a warning when a source is short of
`verify-full` (the target: [flights.md](flights.md), PostgreSQL TLS). It reads the run-row
columns `tls_mode`, `tls_ca` and `tls_in_use` ([operations.md](operations.md#tables)).

- **Every PostgreSQL-connecting writer counts.** TLS is baked per Flight
  (`--pg-ca`), so a backfill or a connecting action built without it can be weaker than
  ingest. The writers are:
  - ingest (`ingest_runs`);
  - backfill (`backfill_runs`);
  - the connecting actions (`audit` rows of `add`, `remove`, `recover-slot`, `skip-to-lsn`
    and `delete-source` that recorded a value).
  - Other actions never connect: their NULLs are left out, not read as unknown.
- **A writer's TLS state is that of its last observed run in its window:** ingest 7 days (its
  retention); backfill and actions 30 days (`backfill_runs` and `audit` are never pruned,
  so an old one-off run can't mark a source forever).
  - The requested mode is the writer's newest row's.
  - The negotiated value is its newest row's that has one, so a run that opened no
    connection doesn't blank it.
- **The states,** weakest first; a source shows its weakest known writer, and the notice
  lists every writer with its time:

  | State | Meaning | Notice |
  | --- | --- | --- |
  | `PLAINTEXT` | a connection opened without TLS (`disable`, or `prefer` falling back) | yes |
  | `encrypted, not verified` | TLS, but requested `require`, `prefer` or `verify-ca` | yes |
  | `requested only` | a mode short of `verify-full`, no connection observed yet | yes |
  | `not yet connected` | `verify-full` requested, no verified connection observed yet (it fails closed) | no |
  | `verify-full (<ca>)` | requested `verify-full` and that run negotiated TLS | no |
  | `unknown` | no TLS-aware run in the window (a pre-TLS build) | no |

- **What a build records** (`preflight.connection_options`): built with `--pg-ca`,
  `verify-full`; without it, the DSN's `sslmode`, else `require` (`disable` for a local test
  DSN). A TLS-aware build never records NULL, so an unverified Flight can't read as
  `unknown`.
- **Audit** shows each action's `tls_mode` and `tls_in_use`, NULL for actions that never
  connect.

### Gaps

The drawer's **Gaps and quarantines** lists the table's quarantined batches and its
expirations over 30 days.
- **A quarantined batch** is listed while it belongs to the table's current generation (the
  writer reports only those): a resync, which starts a new generation, removes it.
- **An expiration** is open while the table still serves the expired generation or isn't
  `live`; a resync closes it.

## Actions

The owner acts from the screens, through the **action panel** (`dive/src/action.tsx`, the
design's Action panel): Preview → Confirm → Run → Result, on the source's action Flights
([operations.md](operations.md#action-flights)).

**Where the actions are:**

| Where | Actions |
| --- | --- |
| Table drawer → options | Retry, Quarantine, Resync, Resume, Pause, Remove; for a table not enabled, Enable. An option that doesn't apply says why. |
| Table drawer → an insert-only or catalog hold | Resync first (for an insert-only table an exact re-snapshot; the current replica is kept aside), then Quarantine (a changed row's batch), Start from log (`start-from-log`: no snapshot, rows before it are missing), Remove. Never Retry on an insert-only hold. A changed column type also offers Accept types: the table-action Flight takes it, but the panel doesn't send it yet, so the card shows the owner's `cdc-flight-tables` commands, then Retry. A not-started table's first copy (`start-snapshot`) isn't sent from the panel either; meanwhile an insert-only table's card offers Start from log, and a keyed one waits for the backfill Flight. |
| Overview and Source page rows | Fix / Resume / Enable / Progress open the table's drawer; Source page → Not enabled → Enable opens the panel. |
| Source page → Change log retention | Compact now |
| Source page → Recovery | Resync next queued table (one rebuild per run; it says how many remain) |
| Source page → WAL limit and slot → a leftover slot | Finish recovery: the newest completed transition again, exactly (`lastTransition`), which drops the old slot and never begins another epoch. With no recorded transition (the audit copy is best effort), the recover-slot that began the current epoch E (expected E − 1, `RECOVER <source> EPOCH E`); if E began with a skip instead, the run is refused (nothing changes), and the skip is repeated with the epoch Flight and its original LSN |
| Source page → Danger zone | Recover slot (while the slot is lost or missing), Skip to LSN, Delete source |

**What the panel does:**
1. **Preview:** a dry run (`CDC_DRY_RUN=1`). It starts when the panel opens, except for the
   actions whose exact confirmation the Flight checks on every run: quarantine
   (`QUARANTINE <schema.table> UNIT <epoch> <X/Y>..<X/Y>`, from the drawer's blocked batch),
   recover slot (`RECOVER <source> EPOCH <n+1>`), skip to LSN (`SKIP <source> TO <X/Y>`, after
   the LSN is typed) and delete source (`DELETE <source>`). For those it is typed first.
   Finish recovery carries the earlier transition's literal, so nothing is typed.
2. **Preview result, then Confirm:** the finished preview stays on the Preview step, showing what it would do, the Flight's own detail and what the
   preview saw (`audit.result.observed`). Delete source also lists what it drops itself
   (`result.drop`, and after a real run what it dropped, `result.dropped`; nothing, for a
   source set up before the pipeline recorded what it created) and the statements it leaves
   for a DBA (`result.dba`), shown once. **Continue to confirm** opens the Confirm step (Back
   returns to the preview). Run unlocks 3 s after reaching Confirm. No countdown is shown: after
   14 of the preview's 15 minutes, Continue to confirm and Run give way to **Re-run Preview** (a
   fresh dry run), with "Preview expired, re-run preview to continue.", so a stale preview is
   never run. A forced remove needs `REMOVE <schema.table>` typed, and a resync of a
   table that serves data (live, paused, blocked) its name.
3. **Run:** the real run, with `CDC_PREVIEW_ID` the dry run's run id and exactly the inputs the
   preview covered. The Flight spends the preview only if nothing it decided on changed.
4. **Result:** done, started (a backfill or compaction Flight), busy, refused (the table
   changed: preview again), waiting (run again later), an open batch to drain first
   (membership), or failed. Then `REFRESH DATABASE` once, and every screen re-reads the share.

**How the panel reads a run:**
- **The exit code** (from `useFlightRun`) is authoritative: 0 done, already or would; 2 refused;
  3 busy; 1 failed.
- **The run's `audit` row** adds the detail. Each launch sends its own `CDC_REQUEST_ID`, and the
  panel reads the row with that `request_id`. The share lags about a minute, so after the run
  finishes the panel refreshes and re-reads every 5 s for up to 3 min ("Waiting for the result",
  with Stop waiting) before it reads a missing row as not recorded. A run whose inputs the Flight rejects exits 2
  within seconds and writes none: the panel says the Flight rejected its inputs and points at
  the run's log (the run is in Run details). A dry run still unfinished after 3 minutes is
  reported too.
- **Run details** list the Flight, every input sent, the run number and the request id.
- **The preview notice** in the drawer shows the newest dry run that would act on the table,
  while it can still be confirmed.

**Only the owner can act.** The deploy injects `--expect-user` as the owner
(`dive/src/owner.ts`), and the Dive compares it with `SELECT current_user`: its queries run as
the viewer.
- **Everyone else** sees "Read-only: actions are run by <owner>", no action buttons, and Copy
  link instead. So does a viewer the Dive can't identify (the query failed or is still
  loading), and everyone when the Dive was deployed without an owner.
- **A source without an action Flight** (`--flights` without that kind) shows the button
  disabled, saying so. So does a deleting or deleted source, except Delete source to finish.
- **The enforcement is MotherDuck's RUN permission,** not the Dive: the action Flights have no
  RUN grants, so only their owner can start them. The deploy refuses a wired Flight with a RUN
  grant to anyone else.

## Deploy

Run this as the owner of `cdc_ops` and its Flights, only with approval:

1. Create the share of `cdc_ops`, named after it, with `UPDATE AUTOMATIC`
   (`CREATE SHARE cdc_ops FROM cdc_ops … UPDATE AUTOMATIC`).
2. Bundle the Dive: `cd dive && npm ci && npm run bundle`.
3. Dry run: `uv run python scripts/deploy_dive.py --share md:_share/cdc_ops/<id>`. It prints the
   plan and connects to nothing.
4. Deploy: `uv run python scripts/deploy_dive.py --share … --flights flights.json --expect-user <owner> --apply`.
   - `flights.json` is `{"<source>": {"table_action": "<uuid>", "table_membership": "<uuid>",
     "quarantine": "<uuid>", "epoch": "<uuid>", "delete_source": "<uuid>"}}`; a kind left out
     shows its buttons disabled.
   - Each wired Flight must be the owner's, declare exactly its inputs and have no RUN grant
     to anyone else, or the deploy refuses. So **redeploy the action Flights before the Dive**
     when their inputs change: the deploy compares them with this checkout's.
   - Without `--flights`, nothing is wired: every action button is disabled, and the RUN grant
     check is skipped ("no action Flights wired").
   - `--expect-user` is injected as the owner, the only viewer the Dive lets act.
   - It checks that the token is set and that the connection is `--expect-user`.
   - It refuses a share whose name isn't `--database` (default `cdc_ops`): its DDL must reach
     the database the Dive reads, and the share URL names only the share. So name the share
     after its database (step 1). For a status database named `sales_ops`, share it as `sales_ops` and pass
     `--database sales_ops`.
   - It creates every status table and column the Dive reads (add-only, the writers' own
     DDL), so the Dive never names a missing table, even before the first Flight run.
   - It then creates the Dive titled "CDC operations", or updates the one with that title.
5. Make the Dive and the share viewable by the whole org.

**Updating:** re-run steps 2–4. The same title updates the Dive in place.
