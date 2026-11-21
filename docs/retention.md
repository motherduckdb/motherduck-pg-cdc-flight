# Retention: compaction and consumer expiry

How the change log is compacted, and when a paused table's history expires.

## Compaction

`cdc-flight compact` drops sealed change-log days that no consumer needs any more, and
prunes the ledger rows nothing can read again. It runs per source, on its own cron
(`0 3 * * *` UTC, see [flights.md](flights.md)), and writes only `cdc_internal`. It
needs no PostgreSQL connection and no secret.

### Results

The run prints one JSON summary. It exits 2 when a published table has lost its pin
(`held`) and 3 when a drop would raise the floor past the ingest checkpoint
(`invariant`); `busy` and `capped` runs exit 0, since the next run continues.

| Status | Meaning |
| --- | --- |
| `done` | Every droppable day was dropped; `kept_reason` says why the next day stays (`pinned`, `not_sealed`, `no_pins`) |
| `busy` | Another run holds the lease, or the lease was lost or a concurrent catalog change stopped a day (`kept_reason` `lease` or `conflict`) |
| `capped` | The runtime cap; the next run continues from `kept_day` |
| `held` | A table with `pin_state='pinned'` has no pin (`held_relation`); nothing drops until it is fixed |
| `invariant` | An invariant broke: investigate before the next run. Either a drop would have raised the floor to or past the ingest checkpoint (`FloorAboveCheckpoint`; nothing was dropped past it), or expiry could not fail a generation (`InvariantViolation`: rolled back, its pin kept, outcome `invariant`; the rest of the run completed first). Both exit 3. A generation that expiry failed although its claim was already missing (outcome `invariant_repaired`) also sets `invariant` (and `error` `invariant_repaired`, kept when the run ends `capped` or `held`), but the run completes and exits 0 |
| `failed` | An exception (its type in `error`); the day in progress rolled back |

The run's JSON summary also carries the expiry counts (`expired`, `would_expire`,
`skipped`) and the budget's state (`off`, `ok`, `over`, `unknown`); each holder's outcome is
in `expirations`.

### Cost

On MotherDuck, with a 10M-row ledger (1M rows pruned per day), a day's guarded drop
took about 1 s and its prune about 60 s. With a 1M-row ledger the whole day took about
7 s. Most of the prune is deleting the day's `batches` rows (about 28 s per 1M) and its
commit; the manifest anti-join is about 2 s. A source with millions of commits a day
spends minutes per day pruning, renewing the 900 s lease per day, and pin writes don't
wait on it.

## Consumer expiry

Bounded storage, indefinite pause and lossless resume cannot all hold at once. Each compaction run first judges every retention-pin holder, then drops.

**What expires** (sealed days only: the active day never counts, so an ingest outage
expires nothing while it lasts):

| Reason | Rule |
| --- | --- |
| `lag` | The holder **hasn't advanced within the limit** (`CDC_MAX_RETAINED_LAG_HOURS`, default 168: its pin's last advance is older) **and has unread data on a sealed day that newer data has followed for longer than the limit** (the next ingestion day began more than the limit ago). The limit is how long a consumer may stall behind newer data, not the age of the data: right after an outage, the day that seals the last old day is new, so nothing expires, and a table catching up keeps advancing. Generation pins never advance, so a backfill running longer than the limit expires |
| `budget` | `CDC_LOG_STORAGE_BUDGET_GB` is set (default off) and the retained days exceed it: the holders of the oldest sealed days the budget can't keep. Day sizes are ingest's charged bytes (`frame_partitions_v2.bytes`); if no sealed day has them, the budget reports `unknown` and expires nothing |
| `needs_resync` | The mirror is already `needs_resync`, whoever set it (TRUNCATE, a key change, an epoch transition, an earlier expiry): it is rebuilt from the source, so its pins protect nothing. Not `needs_action` or `paused`, which resume from the log |
| `stale` | A generation pin that protects nothing (the same predicate backfill's sweep uses) |

The reported `held_day` is the first sealed day with data **above** the pin's
complete-unit END, not a day kept only to preserve that END as a valid cut. A pin
equal to a day's high can therefore physically retain that day under the strict
DROP floor while `held_day` names a later day. The lag decision and its ETA both
use the first unread-data day, including when a unit spans days or a new epoch
starts at a lower LSN. The end of an outage does not by itself expire consumers.

Budget expiry still counts physical retention, including equality at a day's
high. A known limit is that budget pressure can expire an otherwise caught-up
consumer to free an endpoint-held day; it then requires a resync. Budget expiry
is separate from the lag rule and remains disabled by default.

**Report mode first.** `CDC_COMPACT_EXPIRE=report` (the default) only records, in
`expirations`, which holders would expire. The owner sets `on` to expire them. All of
these are owner settings: the compact Flight's `RUN` stays with its owner.

### Held tables and the mirror

`apply_state.held_since` records when a table first left `live`; it is kept across held
states and cleared by `live`. Each run's outcomes go to `expirations` (owner, reason,
mode, outcome, and the floor after the run); records older than 90 days are deleted.

**The mirror database** is attached only when needed, and only from MotherDuck when the
change-log store is itself a MotherDuck database and a token is set; otherwise the run
fails rather than reach for MotherDuck (which, without a token, opens a browser login).
