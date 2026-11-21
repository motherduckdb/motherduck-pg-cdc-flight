# Runbooks

Each runbook starts from the operations Dive ([dive.md](dive.md)). It lists:
- the **trigger:** the screen, panel and field that shows the problem;
- the **diagnosis:** what to read next, and where;
- the **action:** the owner's buttons, each opening the action panel with its preview.

`tests/test_dive_queries.py` asserts every trigger and diagnosis field below against a real
`cdc_ops`. A runbook step without an asserted field is a gap.

**Every action goes through the action panel** ([dive.md](dive.md#actions)):
1. **Preview (dry run).** It changes nothing, and shows what the action would do now and what
   it saw. It starts when the panel opens.
2. **Confirm.** The preview stays on screen until you click **Continue to confirm** (Back returns
   to it). Run unlocks 3 s later. Where marked, type the table name, or
   `REMOVE <schema.table>`, first. A preview must be confirmed within about 14 minutes; after
   that the panel offers only **Re-run Preview**.
3. **The run.** Then every screen re-reads the share.

The CLI and the action Flights work too, for the owner without the Dive: `cdc-flight-tables`
(pause, resume, retry, quarantine, add, remove; `--dry-run` first) and the action Flights run
with `CDC_DRY_RUN=1`, then `CDC_PREVIEW_ID` = the dry run's run id
([operations.md](operations.md#dry-run-first)).

**Results:**
- **busy:** try again shortly.
- **refused:** the table changed since the preview, or the preview was already used.
  Preview again.
- **already:** there's nothing to do. The exceptions are finishing a deletion and finishing
  a recovery: the panel then offers **Finish**.
- **waiting:** the source or its slot is busy (ingest's lease, a reader on the slot). Click
  **Run again** later; if the preview was already spent, preview again.

**Typed first:** recover slot, skip to LSN, quarantine and delete source take their exact
confirmation **before** the preview (`RECOVER <source> EPOCH <n+1>`, `SKIP <source> TO
<X/Y>`, `QUARANTINE <schema.table> UNIT <epoch> <X/Y>..<X/Y>`, `DELETE <source>`): the panel
shows the literal to type. The Flights check it on every run.

**Who can act:** only the Flights' owner. Everyone else sees the Dive read-only, with
"Read-only: actions are run by <owner>"; the Flights have no RUN grants.

**Everything runs from the Dive:** an operator who owns the Flights carries out R1–R3 from the
Dive's screens and their action panel.

**General rules:**
- The Dive is about a minute behind, and best effort. Never act on it alone: every action
  re-reads the stores, and its dry run shows what it would do now.
- An `error` is an exception **type**. `SystemExit` means a run refused or stopped on purpose;
  its log line says which.
- **No row data** goes into any action input, such as a quarantine's reason. Action inputs are
  visible to the whole org, in the audit.

## R1: Slot invalidated (lost) or missing

**Trigger:**
- **Overview:** the source reads Needs action, "Replication slot invalidated", WAL limit Lost
  (the current slot's `wal_status` is `lost` or `missing`).
- **Source page → the red banner:** "Sync failing: <error>" (ingest's error, for example
  `SlotLost`).

**Diagnosis:**
1. **Source page → Pipeline → Ingest:** the last run, and when the last failure was.
2. **Source page → WAL limit and slot → Slots:** every slot with its role. A **pending**
   `<slot>_e<n>` means an earlier recovery was interrupted; Recover slot resumes it.
3. **Source page → Retained WAL, last 7 days:** whether retained WAL grew toward the limit
   before the loss. That's a sign of the source disk filling (see below).

**Action (the owner):**
1. **Source page → Danger zone → Recover slot** (recommended while the slot is lost or
   missing; the Overview's Recover slot button leads there). Type `RECOVER <source> EPOCH
   <n+1>`, preview, then run. The preview shows the new slot name and that every table becomes
   `needs_resync`.
   - **If Slots then lists a leftover old slot,** click **Finish recovery** on its row. It
     repeats the same transition and drops the old slot; it never starts another recovery.
     (Without a recorded transition it repeats the recover-slot that began the current epoch.
     If that epoch began with Skip to LSN instead, the run is refused and nothing changes:
     run the epoch Flight with `CDC_ACTION=skip-to-lsn` and the skip's original LSN.)
2. **Source page → Recovery → Resync next queued table.** Each run starts one rebuild and says
   how many remain; run it again until none remain.
   - **Insert-only tables** (the Recovery panel's Insert-only bucket) aren't in that queue:
     open each one's drawer and choose Resync (an exact re-snapshot) or Start from log.
3. **Source page → Recovery:** watch rebuilt, rebuilding, failed, queued and insert-only.

**Done when:**
- no Recovery panel shows, and every table reads OK (Source page → Tables);
- the Pipeline's Apply reads OK;
- the epoch moved up by one (Source page → Danger zone → Current epoch);
- Slots lists only the current slot (Source page → WAL limit and slot).

## R2: Poison transaction (a table in `needs_action`)

**Trigger:**
- **Overview:** a table reads Needs action, "Sync failed: <detail>" (the exception type),
  "Blocked since <ts>".
- **Its drawer → Technical details:** the blocked batch (its unit's start and end).

**Diagnosis:**
1. **Read the detail.** A conversion or constraint failure is a poison value. Anything that
   names a schema change is the schema case (below).
2. **The drawer → Gaps and quarantines** lists earlier gaps in the same table.

**Action, one of (the owner), in the table's drawer:**
- **(a) Quarantine.** Type `QUARANTINE <schema.table> UNIT <epoch> <X/Y>..<X/Y>` (the panel
  shows it, built from the blocked batch), then preview: it states the gap (the rows that
  unit's commits touched may diverge). Then run.
  - Apply goes on at its next run. **Resync** the table when convenient to close the gap.
- **(b) Resync.** Type the table name to confirm. It rebuilds from the source, so nothing is
  lost.
- **Expert path:** repair the mirror by SQL, then **Retry**. The blocked commit is taken from
  the row, and it must still be the one blocking.

**Done when:**
- the table reads OK;
- with (a), its drawer lists the quarantined batch under Gaps and quarantines, and its
  History shows the quarantine.

## R3: Paused (or stalled) past retention

**Trigger:**
- **Overview:** a paused table reads Needs action, "Paused · retention expires in <time>",
  with its estimated expiry (Paused, yellow, while more than a day remains).

**Diagnosis:**
1. **Source page → Change log retention → Pins:** the table's pin and its estimated expiry.
2. **The drawer → Change log retention:** the pin and when it last moved. In compaction's `report`
   mode nothing is released yet; `on` releases it ([operations.md](operations.md)).

**Action (the owner), in the table's drawer:**
- **Before it expires:** **Resume**; or fix why apply isn't running (Source page → Pipeline
  → Apply).
- **After it expires:** the table is `needs_resync` (Needs action, "Needs resync: …"), and
  its drawer's Gaps and quarantines lists the expiration as open. Resume is refused, because
  the state isn't `paused`. **Resync** (a plain confirm from `needs_resync`).

**Done when:** the table reads OK on a new generation, and its expiration shows closed.

## Other runbooks

These are shorter, and each points to the same screens:

- **Source disk filling:**
  - Source page → WAL limit and slot: WAL limit used climbing (Warning from 50 %), and the
    oldest open transaction (reported past 1 GiB behind).
  - Check Source page → Pipeline → Ingest (is it running?).
  - End the long transaction on the source.
  - If the slot is lost anyway, follow R1.
- **Schema change (Needs action with a schema detail):** resync the table (R2 (b)).
- **Quarantine a transaction:** R2 (a).
- **Resync one table:** its drawer → **Resync**. Watch it in its drawer (Backfill). Done when
  it reads OK on the new generation.
- **Pause a table:** its drawer → **Pause** (a live table). Resume it before its pin's
  estimated expiry (R3).
- **Add a table:** a Not enabled table (Source page → Not enabled) is in the publication:
  **Enable**. Then watch it in its drawer.
- **Remove a table:** its drawer → **Remove**.
  - A table whose source table is gone needs a plain confirm.
  - A table that still exists needs `REMOVE <schema.table>` typed. Only a re-add plus a full
    backfill undoes it. The replica table is kept.
- **Skip to LSN (destructive):** Source page → Danger zone → **Skip to LSN**. Type the LSN,
  then `SKIP <source> TO <X/Y>`, then preview: it shows the WAL it abandons. Then run. It
  begins a new epoch: every table needs a resync, and the skipped changes are lost.
- **Compact now:** Source page → Change log retention → **Compact now**. A compaction already
  running makes it exit `busy`.
- **Delete a source:**
  - Source page → Danger zone → **Delete source**. Type `DELETE <source>`, then preview: it
    lists what it drops (what this pipeline created) and the DDL a DBA runs for the rest. Then
    run.
  - **waiting** (a reader on the slot, or ingest's lease): Run again later. A source left
    `deleting` is finished with **Finish**.
  - Done when the source's syncs fail with `SourceDeleted` (Overview: Needs action, "Source
    deleted"). The replicas and the log are kept.
  - **Then unschedule the source's Flights (owner).** From the first step, each of its runs
    fails with `SourceDeleted`.
  - **Compaction never runs again on a deleted source,** so its log stays as it is. Drop the
    source's schema in `cdc_internal` and its mirror by hand when nobody needs them (owner).

## Incomplete format-2 bootstrap

A failed first setup can leave initialized protocol metadata without a registered
source. A retry refuses before contacting PostgreSQL, naming the configured slot
and publication. Inspect `pg_replication_slots` and the publication before
recreating the change-log store. A slot created by the failed attempt retains WAL
until it is dropped; remove it only after confirming it is unused and owned by
that setup. Do not drop a retained slot or publication shared with a live source.
The retry never adopts or drops these objects automatically. Once inspection and
cleanup are complete, recreate the setup explicitly and re-backfill.
