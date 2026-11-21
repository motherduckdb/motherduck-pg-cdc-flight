# Performance envelope

Measurements on real MotherDuck Flights. This page covers:
- end-to-end throughput under an OLTP load (TPC-C) on the scheduled Flights;
- throughput per row size and operation;
- destination storage and compute per million events;
- backlog drains, backfill and apply MERGE time against mirror size;
- whether a 1-minute schedule is realistic.

Every figure is from an F16 Flight in us-east-1, against an RDS for PostgreSQL 16 source in the same
region. Figures marked "(laptop)" are the exception.

**Two kinds of measurement:**
- **End to end, TPC-C (2026-11-09):** the current change-log format, on the deployed ingest and
  apply Flights. These are the current throughput figures.
- **Per component (2026-11-05 and 2026-11-06):** ingest per row size, apply, backfill and storage.
  These used the earlier change-log format; the current format drains TPC-C about 1.5× faster than
  the earlier one did. Use them for relative costs (row size, mirror size, single-row against
  batched), not as current ceilings.

**Flight size: use F16 for ingest and apply.** Every number on this page is from an F16 (2 vCPU
by quota), and ingest is CPU-bound there. Smaller sizes weren't measured. At F4 (0.5 vCPU), ingest
is expected to fall well below these ceilings, so this is a recommendation, not a measured
figure.

## End to end: TPC-C on the scheduled Flights

Measured on 2026-11-09 on the scheduled F16 ingest and apply Flights. The source is an RDS for
PostgreSQL 16 database in the same region, driven by a TPC-C load generator (32 workers, 8 tables).
tpmC is the load generator's delivered rate.

**Drain: about 221 transactions/s.** A 5-minute backlog with no ingest, then one timed run (10,747
transactions in 48.6 s of streaming). Only one
drain was timed, so run-to-run variance isn't measured.

**Sustained load on a 2-minute ingest schedule (`*/2`).** A load holds if each judged ingest run
finishes within its gate (110 s, or 120 s for the burst test), the slot lag doesn't keep growing, and
the applies succeed.

| Delivered load | Ingest run wall | Apply run wall | Result |
| --- | --- | --- | --- |
| 3,050–3,962 tpmC | 69–95 s | 36–45 s | sustained |
| 4,554 tpmC | 82–87 s | 44–52 s | **sustained** |
| 4,953 tpmC, over a 26-minute window | 84–107 s; 114 s for the run that drained a WAL burst | 48–56 s | **sustained, with little margin** |

- **4,500 tpmC is sustained** on a 2-minute schedule.
- **5,000 tpmC is near the limit.** It held over one 26-minute window across two source WAL bursts,
  with the slot lag back near its starting level after each. The run that drained the first burst
  took 114 of its 120 s; a step judged on a shorter window failed on slot lag. The estimated
  ceiling on `*/2` is about 5,300–5,500 tpmC.
- **The default ingest schedule is `*/3`,** for headroom: each run's fixed setup (package install,
  connect, attach) is spread over 180 s instead of 120 s. The ceiling and the apply lag at `*/3`
  aren't measured. The ceiling should be above `*/2`'s, and the apply lag about 3–5.5 minutes (about
  2–4 on `*/2`). Data is up to a minute staler, and each slot retains about 1.5× the WAL between
  runs, far under `max_slot_wal_keep_size`.
- **The package install adds about 1 s per ingest run at the median.** These runs predate it, and
  5,000 tpmC hasn't been re-tested with it. See [flights.md](flights.md#package-install).

## Environment and method

**The Flight:** an F16 in us-east-1.
- 2 vCPU by cgroup quota, AMD EPYC 7571. The process sees 8 CPUs, so DuckDB's default
  thread count and any pool sized from the CPU count exceed the quota; that tuning isn't
  measured;
- Python 3.12 and duckdb 1.5.6.

**The source:** for the per-component numbers, a dedicated RDS `db.m8g.large`, PostgreSQL 16.15, in
the same region, created for the session and deleted afterwards.

**Round trips:**

| Path | p50 |
| --- | --- |
| Flight → MotherDuck, a one-database query | 8.6–10.2 ms |
| Flight → MotherDuck, a two-database query | 11–13 ms |
| Flight → PostgreSQL | 0.9 ms |
| A laptop on home broadband → MotherDuck, for comparison | 87 ms |

**Method (per component):**
- Every timed path is product code: preflight, ingest, apply and backfill.
- Backlogs are committed before the timed run starts.
- Payloads are random base64, so they are incompressible. Real data with repeated values
  compresses better, so the storage figures are at the upper end.
- Each workload ran 5 times, interleaved in a rotated order; backlog and backfill ran 3 times.
  Tables show the median with the min–max range.
- **Comparisons are within one session.** A CPU calibration at the start and end of each
  session agreed within 3%, both within one Flight and across the two Flights used.

**The Flight is CPU-limited compared with a laptop.** The same spool workloads run 4.0–6.1×
slower in the Flight than on an Apple-silicon laptop with Python 3.14. Round trips to
MotherDuck are about 10× faster. So laptop throughput numbers don't transfer to a Flight.

## Ingest throughput (per component)

The drain rate is measured end to end: through a real replication slot, from the first message
to the drained marker, after the backlog is committed. Earlier change-log format; see the top of the
page.

| Workload | Events/s | Limited by |
| --- | --- | --- |
| 1 KB insert, 1000 rows/txn, 50k events | **3,200** (3,190–3,290) | CPU: spool about 50%, append about 40% |
| 1 KB insert, 1000 rows/txn, 250k backlog | **3,250** (3,210–3,290) | CPU |
| 1 KB insert, 1000 rows/txn, 1M backlog (about 1 GB) | **2,950** (2,870–3,000) | CPU; append is about 25% slower per event at this size, and about 9.5 s (3%) of the window isn't attributed to a stage |
| 1 KB update, REPLICA IDENTITY FULL, 1000 rows/txn | **2,680** (2,380–2,720) | CPU |
| 1 KB insert, **one row per transaction** | **about 770 commits/s** (743–776) | CPU in append: about 0.93 ms per commit |
| 10 KB insert, 1000 rows/txn | **320** (315–324) | the destination: the upload runs at about 4 MB/s, CPU/wall 0.17 |
| 10 KB update, FULL, 1000 rows/txn | **710** (660–740) | the source: the walsender decoding old and new values |

- **A 1 GB backlog of 1 KB rows drains in 339 s,** inside one 540 s run. The 250k and 1M
  backlogs differ by about 9% per event, so the drain rate is close to linear in backlog
  size.
- **Single-row transactions cost per transaction, not per byte,** in the append path. The
  single-row figure is from a second session (2026-11-06, 20k commits), after append bookkeeping
  became bulk; most of the remaining 0.93 ms per commit is per-commit bookkeeping.
- **No single ingest process reaches 20,000 events/s.** Spool processing alone (decode, encoding,
  chunking) tops out at about 6,700 events/s for 1 KB inserts on this host, 2,900 at 10 KB, and
  2,800 for single-row transactions. Even with spool and append perfectly overlapped across the
  Flight's two cores, about 6,400 events/s. More needs several sources.

**Cold start of an ingest Flight run: about 7–8 s before the first change is read,** plus
scheduling:

| Step | Time |
| --- | --- |
| Platform start to the Flight's first line, including the environment build and package install | 2.2–3.8 s measured before the package install; the install adds about 1.0–1.3 s (median) |
| Imports | 1.2 s |
| First MotherDuck connect | 1.5 s |
| Initialize and register on an existing store | under 0.1 s |
| Preflight check | 0.1 s |
| Slot start to the first message | 0.2 s |

An apply run pays most of the same. The ingest operations-status write takes 0.08–0.16 s per run.

## Apply MERGE against mirror size

1 KB rows, with batches of updates to existing keys:

| Mirror rows | Batch | Apply run | of which MERGE |
| --- | --- | --- | --- |
| 1M | 10k, recent keys | 1.4 s | 0.17 s |
| 1M | 10k, uniform keys | 1.7 s | 0.43 s |
| 1M | 100k, uniform keys | 13.3 s | 0.97 s |
| 10M | 10k, recent keys | 1.4 s | 0.17 s |
| 10M | 10k, uniform keys | 2.0 s | 0.58 s |
| 10M | 100k, uniform keys | 13.2 s | 1.02 s |

- **Mirror size barely matters between 1M and 10M rows.**
- **The apply run costs about 0.4 s + 0.13 ms per event.** Most of that is outside the MERGE:
  reading the change events and folding them, which these runs don't split.
- **10 KB inserts are much slower to apply:** 20k events into a new mirror took **about 75 s**
  (about 3.7 ms per event), of which the MERGE was about 1.3 s.
- A 100M-row mirror was not measured.

## Backfill

| Table (1 KB rows) | Time | Rows/hour | Fixed cost per table |
| --- | --- | --- | --- |
| 1M rows | 55–56 s | **about 65M** | about 2 s |
| 100k rows | 7.5–8 s | about 47M | about 2 s |

The fixed cost covers start, catch-up and publication.

## Destination storage and compute per million events

Storage is the compressed DuckDB size of the change log and the mirror, from local copies
(full, or a sample of up to 200 MB scaled by rows). Repeated full copies of the same data
varied by ±7%; treat these figures as about ±15%. MotherDuck's own reported size lags writes
and is rounded above 1 GiB, so it wasn't used as a check.

| Workload | Change log per 1M events | Mirror per 1M rows |
| --- | --- | --- |
| 1 KB insert | 1.2–1.3 GB | 1.1 GB |
| 1 KB update, FULL | 2.2–2.4 GB | — |
| 10 KB insert | 11.6 GB | 11.2 GB |
| 10 KB update, FULL | 23 GB | — |

- A 10M-row mirror of 1 KB rows is about 10 GB.
- The figures use incompressible payloads; typical data is smaller.
- **Elapsed MotherDuck-side work per million 1 KB events:**
  - append, about 115–150 s;
  - MERGE in 100k batches, about 10 s;
  - the whole apply, about 130 s.

## What this means for a deployment

These models use the per-component numbers above and assume 1 KB rows, so they are conservative
for the current format. For an OLTP mix, the TPC-C results at the top are the better guide.

**A 1-hour backlog in one run.**
- One 540 s ingest run drains about **1.6M batched events.** So a 1-hour backlog fits in one run
  only up to about **435 events/s** of source traffic.
- At about 770 single-row commits/s, a run drains about 409k single-row commits on the same basis,
  so a 1-hour single-row backlog fits only up to about **114 commits/s**.
- A 1-hour burst at 20,000 events/s (72M events) takes about 46 runs to drain.

**Backfill.**
- **A 10M-row source over 20 tables loads in about 10 minutes of Flight time.**
- **A 100M-row table needs about 11 runs of 540 s.**

**A 1-minute schedule.**
- In each run, ingest takes about 6 s + events/2,950, and apply about 6 s + 0.13 ms per event.
  Each run's own fixed statements (lease, finish, status) add about 0.2 s.
- Single-row ingest takes about 6 s + commits/770.
- **For 1 KB rows, ingest is the first stage to fall behind.** For 10 KB inserts, apply
  (about 270 events/s) falls behind before ingest (about 320).
- The single-row rows use the 1 KB update apply cost. The measured single-row apply was
  about 0.09 ms per event, so those apply times are conservative.

| Source traffic | Ingest run | Apply run | Holds a 1-minute schedule? | Worst-case lag before chaining delay |
| --- | --- | --- | --- | --- |
| 1,000 events/s, batched | 26 s | 14 s | yes | about 100 s |
| 2,000 events/s, batched | 47 s | 22 s | yes | about 130 s |
| 2,500 events/s, batched | 57 s | 25 s | barely | about 140 s |
| 3,000 events/s, batched | 67 s | 29 s | **no** | grows |
| 300 commits/s, single-row | 29 s | 8 s | yes | about 100 s |
| 600 commits/s, single-row | 53 s | 11 s | barely | about 125 s |
| 800 commits/s, single-row | 68 s | 12 s | **no** | grows |

- **A 1-minute schedule holds a 3-minute lag up to about 2,000–2,500 events/s of batched 1 KB
  writes,** provided the delay between ingest finishing and apply starting stays well under a
  minute.
- **For single-row OLTP, the limit is about 600 commits/s per source.**
- **Above those rates, lag grows, and the source retains WAL for it.** Sustained traffic above
  the envelope eventually invalidates the slot (if `max_slot_wal_keep_size` is set; recovery
  means a new backfill) or fills the source's disk.
  - For example, WAL is about 1.24 KB per 1 KB event. At 4,000 events/s sustained, ingest
    falls about 1,050 events/s behind, about 1.3 MB/s of retained WAL, so a 20 GiB limit is
    reached in about 4.4 hours.
  - Watch retained bytes in slot health; see [runbooks.md](runbooks.md) R1.

## Not covered

- A 100M-row mirror.
- Scheduled-run start latency and the chaining delay between ingest and apply. Manual runs went
  from requested to running in 0.3–3.9 s.
- Sources other than RDS for PostgreSQL 16.
- Flight sizes other than F16.
- On the current format: 10 KB rows, and sources with more than 50 tables changing at once (the
  apply cost per page grows with the number of tables).
- The `*/3` schedule's ceiling, and 5,000 tpmC with the package install.
