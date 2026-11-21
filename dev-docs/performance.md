# Performance work

How to measure, and the cost model behind the design. The customer envelope is
[docs/performance.md](../docs/performance.md).

## Where the ingest ceiling is

Measured on F16 with the earlier log format (2026-11-05); the shape still holds.
- Spool processing alone (decode, canonical encoding, chunking) tops out at about 6,700 events/s for
  1 KB inserts. With spool and append perfectly overlapped across the Flight's two cores, the
  per-stage costs give about 6,400 events/s, so no single ingest process reaches the 20,000 events/s
  burst target. That needs both stages about 3× cheaper (decode **and** canonical encoding, since
  the format is fixed, plus append), overlapped, or several ingest processes or sources.
- In a laptop profile, `pgoutput` decoding was about 15% of spool CPU and the canonical encoding
  about 35%, so faster decoding alone gains at most about 15%.
- On the current format, a TPC-C log analysis found ingest bound by MotherDuck appends (about 1.13 s
  per ~5.9k-frame batch, about 230 transactions/s). STATUS.md has the open work.

## Historical format-1 performance: destination round trips

At small batch sizes, MotherDuck latency comes from the number of remote
statements, not data volume:
- each remote statement costs about 85 ms, and so does a commit with writes;
- the changes insert, which ships the batch, costs about three times that;
- sending several statements in one string, or using `executemany`, saves
  nothing.

The ingest log therefore keeps statements few, without weakening any check:
- `append` runs one guard UPDATE, then one combined read: the checkpoint state,
  partition status, replayed chunk identities, per-commit counts and chain
  digests, and complete manifests. It then writes each metadata table once,
  from bound JSON strings decoded into explicitly typed lists. Exact Python
  type and NULL checks run before encoding, because SQL's strict JSON conversion
  checks ranges but still coerces types. The combined replay lookup uses the
  same transport and keeps its epoch/ID/commit filters inside SQL. Fixed aliases
  on the metadata and lookup projections keep folded JSON values out of plan
  labels. They preserve the multi-UNNEST zip and explicit INSERT target order;
  persisted bytes, statement counts and transaction boundaries are unchanged.
  - The chain digest is computed rather than re-read: contiguity places every
    fresh chunk after every stored one.
  - The day's `CREATE TABLE` runs only when the client catalog lacks the table,
    and the seal UPDATE only when an older active day exists.
- `initialize` reads the client catalog (no round trip) and creates only what
  is missing, in one transaction. On an existing store it issues no statement.
- Guarded writes stay ordinary `UPDATE`s with an affected-row check. DuckDB
  1.5.6 skips write-write conflict detection for `UPDATE … RETURNING`, so it
  is never used on a fenced row.

`tests/test_roundtrips.py` pins each call's statement sequence. Measured on
MotherDuck, the `test_phase1` means went from 2.15 s to 1.00 s for `append`,
4.86 s to 2.90 s for `run_once`, and 2.35 s to 1.27 s for a new store's
`initialize`.

## Run-scoped page cache

On the Python fold (the SQL fold reads no frames to the Flight), ApplyRun reads the
log once per page position rather than once per table:

- Every table still runs its own `units()` read, with its own floor check in the
  same statement.
- Tables whose pages are identical share one decoded frames read and one
  descriptors read. A page is identified by each unit's epoch, start, end and
  digest, plus the high, the binding digest and the config revision.
- The cache holds unfiltered events; each table applies its own quarantine skip and
  guarded write.
- It holds one page. It is cleared when the run ends and on any error, and a
  failed read is never cached.

A page is admitted only if its charged ledger bytes are at most `CACHE_CAP_BYTES`
(128 MiB). A larger page falls back to the per-table read, with the same result;
page planning never shrinks a page to fit. Ledger bytes are not a memory bound:
at a 127 MiB page the measured peak RSS was 737 MiB above the uncached path, about
2.5 GiB in total.

## Ingest timing lines

The ingest also prints `@flight-log timing <kind> name=value ... ts=...` lines, to read its
throughput limits off the Flight logs. They are plain log lines, not chart nodes, and
carry only durations (seconds), counts, bytes and two client settings. Nothing reads them
for a decision, and emitting them never fails a run.

- **`connect`**, once: `wall` (the MotherDuck connect and attach), `age` (the process's
  age then, so startup and imports are `age − wall`), and DuckDB's `threads` and
  `checkpoint_threshold`.
- **`append`**, once per durable batch, in order (`n`):
  - the batch: `items`, `frames`, `bytes` (frame payload), `cum_bytes` this run, and
    `why` it was submitted (`full`, `drained`, `time` or `end`);
  - the main thread: `fill` and `fill_cpu`, the wall and CPU time spent building the batch
    up to when it became full or reached the drain marker (otherwise up to its submit); `blocked`, how long a
    full batch then waited for the one in flight; `wait`, how long it blocked collecting
    this one, and `lag`, how long a finished append waited to be collected;
  - the upload thread: `queue` (submit to start), `wall` and `cpu` (thread CPU) of the
    append (`cpu` excludes DuckDB's own thread pool), split into `guard` (transaction, guard, snapshot and checks), `payload` (the
    frames INSERT and the chunk/descriptor ledgers), `ledger` (units, partitions,
    source) and `commit`.
- **`run`**, once at the end: `wall`, `appends`, `frames`, `bytes`, the cgroup CPU quota
  (`quota`, in CPUs) and `cpus` (what the process sees), and this run's deltas of process
  CPU (`cpu_s`), garbage collections (`gc`, `gc2`) and the cgroup's `nr_periods`,
  `nr_throttled`, `throttled_usec` and `usage_usec` where readable.

When an append is in flight almost all the time, `fill` on the `why=full` lines is the main
thread's work per batch, the bound on what a faster upload alone could gain.

## Format A/B harness

`scripts/perf/ab.py` runs the same end-to-end reps against two archived product trees
and records every metric with one definition. Each rep is a fresh child process that
imports exactly one tree: `ab.py prepare <arm> <sha> <dir>` writes `git archive <sha> src`
plus a digest manifest, and the child refuses a product imported from anywhere else.
The harness itself never imports `cdc_flight`; a thin adapter per arm does
(`arm_f2.py` here, which uses the public format-2 factory). The format-1 adapter is kept
off `main`, on an unmerged measurement branch.

A rep creates a source table on PostgreSQL (`PERF_DSN_ENV`, default `CDC_TEST_PG_DSN`;
local or remote), bootstraps a fresh log and an apply generation, generates the
workload, then times one ingest run and the apply that follows. Update workloads first
preload and apply the rows, unmeasured, after the slot exists (format 2's log-only start
needs the table empty when capture begins). The earlier step-3 harness preloaded before
the slot instead, so its update figures aren't directly comparable. Appends run on a
worker thread, overlapping the main loop, so ingest wall time and end-to-end events/s are
the throughput figures; per-method times are attribution, not additive. Per rep it records:

- per-kind statement counts and wall time on the log connection, COMMIT included;
- logical Arrow bytes handed to DuckDB (registered tables, `from_arrow`, Arrow
  parameters, replacement scans);
- wire bytes on the default-route interface only, or null when the source shares that
  route;
- exclusive wall and thread CPU of the spool, append, finish and MERGE methods; process
  CPU of ingest and apply; peak RSS;
- log storage as a bounded local copy of the log database, and its metadata rows;
- the CPU cap (`PERF_CPU_CAP=quota|affinity|none`), cgroup throttling, nproc and load.

The oracle gates every rep. The mirror's content must equal the source table's (the same
md5-quarter sums in both engines), and the log must hold exactly the generated events by
op and source transaction. Comparable units are per event and per source transaction:
chunks, units and frames differ between formats, so they're diagnostics only.

`PERF_STORE=md` uses owned `cdc_p0_perf_*` databases, journalled before creation and
released only after a verified drop; `PERF_STORE=local` uses DuckDB files and no
token. `ab.py summarize` prints the realised schedule first, then medians and
min–max per arm with ratios, each line labelled with the host CPU and cap. It refuses
results from different harness versions.

`PERF_BUDGET_S` only sizes the schedule against the estimates; `PERF_DEADLINE_S`
(default: the budget) enforces it. At the deadline, or on SIGTERM or SIGINT to
`ab.py run`, the running rep is stopped and unwinds through its normal cleanup,
dropping its PostgreSQL objects and store databases, and no later rep starts. The
skipped reps are listed in the `done` row, and `summarize` prints a `STOPPED` line.
The exit code is 143 for a signal and 124 for the deadline; the first stop wins. A
rep that hasn't exited `PERF_STOP_GRACE_S` (default 60) after the stop is killed;
what it held stays pending in the journal and is reported. A rep whose `ab.py run`
was killed outright stops itself shortly after the deadline. Worst-case wall time is
therefore the deadline plus the grace: to keep a run inside 1,800 s, set
`PERF_DEADLINE_S` to 1,800 minus the grace. Reps never install PostgreSQL
extensions: the payload generator uses the built-in `sha512`.
