# Testing policy and ten-minute budget

The full routine suite is all local/unit/PostgreSQL tests plus exactly **29 mandatory
MotherDuck nodes**, tracked with one-line reasons in
[`tests/md_allowlist.json`](../tests/md_allowlist.json). New `stores` tests default
to local only. Adding a remote case requires replacing an entry in that fixed budget.
No `--full` tier, `CDC_TEST_MD_FULL`, gate flag or marker expression restores the old
remote matrix. `md_gate` marks are historical evidence labels, not another tier.

```sh
uv sync --dev                    # install before the timed command
uv run pytest -q                 # local; missing PG/MD explicitly skip
# Set CDC_TEST_PG_DSN for a disposable PostgreSQL server, then:
python3 scripts/md_suite.py artifacts/md-suite.log
```

The tracked runner uses `.venv/bin/python`, four xdist workers, one-case scheduler
chunks and `--md-suite` to
require that all 29 remote cases are collected and pass. It enables MotherDuck,
excludes opt-in benchmarks, loads token authentication from environment/ignored
`.env`, and refuses browser authentication. A missing token or PG DSN fails before
tests run. A missing/renamed allowlisted node or skipped mandatory case cannot be green.
Normal collection generates only selected remote variants; the two mixed secondary
parameter tables preserve just the selected remote fault/catalog variant. A future
explicit non-budget remote marker/parameter is deselected, not silently skipped.
The old gate-selection meta-tests are replaced with budget-selection tests; every
local functional/PG case is retained.

## Deadline and admission

The ten-minute timer starts when the run is admitted, after any external queue:

| Elapsed | Action |
| --- | --- |
| 0–9 min | All selected tests, fixture setup and ordinary teardown run |
| 9 min | Soft-stop the pytest process group; report budget exceeded |
| 9m50s | Force-stop remaining workers, including stuck teardown |
| 10 min | Outer OS watchdog exits with failure; it also bounds supervisor accounting |

A remote case has a 150-second setup/call/teardown watchdog. Workers never restart
or retry a crashed case; the suite stops on its first failure. Target normal runs
below seven minutes; hitting the cutoff is failure, not evidence the suite meets
its performance target. No remote service can guarantee cleanup after a forced kill.

A machine that runs several suites at once can put a queue in front of the runner. Queue
wait is reported separately and is outside the cap. A queue execs the same tracked
supervisor; it cannot extend the deadline. A direct invocation of the tracked runner is
already admitted and has no queue. Each run is four workers, so concurrent runs share
MotherDuck and local infrastructure. Timing under contention must be measured, not
inferred by multiplying quiet-run timings.

## Coverage that remains, and coverage deliberately reduced

The remote budget covers MVCC and independent-writer conflicts, database clocks,
unsigned LSNs, atomic MERGE and publication, index/view catalog behavior, durable
capture and feedback, replay, native type fidelity, backfill/source equality,
resync/publication faults, stale-catalog publish refusal, pins/drop rollback,
retention serialization, slot recovery, one held-table recovery, compaction and
fresh-vs-cloned fixture equivalence.

The broad seeded live/backfill/slot-loss gates and full DDL matrix now run locally
with PG. Most remote key-version, membership, batching, retention and compaction
permutations are no longer routine checks. In particular, exact adjacent-commit
bounds and key-change/revert regressions retain local/PG coverage but lose their
dedicated remote executions. This accepts reduced remote combination coverage.
Historical Phase 0/Phase 2 sign-off results remain historical, not claims about the
current suite. The evidence maps identify this change.

## Targeted diagnostics

One exact removed remote node can be generated explicitly for investigation:

```sh
CDC_TEST_MD=1 uv run pytest -q 'tests/test_phase2.py::test_adjacent_commit_lsn_is_applied[motherduck]'
```

Set the PG DSN too for PG-backed diagnostics. A file, directory, `-k`, gate marker
or environment flag cannot widen remote collection. A diagnostic is not an archive
or mandatory long sign-off tier. Run it for a specific suspected regression; it does
not satisfy the complete bounded suite's `--md-suite` contract.

## Results, resources and failures

The runner writes progress/summary to the requested log and failed node ids to
`<log>.results` as they happen. A directory named `<log>.resources-<run-id>` holds
one JSONL journal per worker. It records MD database names before CREATE (an ambiguous
response may have created them), successful drops, and PG source namespaces before
allocation. Configured PG slots/publications carry those unique namespaces, including
epoch suffixes. Credentials and SQL payloads are not journaled. A green run requires
no unreleased journal entries; a forced stop preserves ownership evidence for
reconciliation. The supervisor never drops external resources. Inspect owner identity
before cleanup; do not delete other runs' or ambiguous objects.

A FAILED test or non-transient ERROR may be a correctness defect. Investigate it;
a later passing rerun does not clear it. Only setup/teardown infrastructure errors
matching the fixture's transient list are candidates for a targeted rerun.
Fixture-only idempotent admin DDL retries `DEADLINE_EXCEEDED`, `UNAVAILABLE`, connection
failure and server-timeout messages. Authentication/permission failures and other
errors are not retried; test bodies see MD errors unchanged. Soft-stop interrupts
retry an interrupted DROP once and continue teardown. Per-case/global watchdogs
bound even these paths.

PostgreSQL's walsender can hold a slot briefly after a client exits. Fixtures use
`wait_slot_released` before dropping/advancing slots. Keep isolated per-test resources
and keep WAL below the shared server budget; don't share mutable stores across tests.

## CI (GitHub Actions)

The `tests` job runs the local and PostgreSQL tier (no MotherDuck, no secrets) on
PostgreSQL 13, 16 and 18. GitHub's standard runner for this repository has 2 vCPUs, and
the suite is CPU-bound (in-process DuckDB plus the PostgreSQL server on the same cores).
Serially one major took 12-16 minutes, and `-n 4`/`-n 8` on one runner gained little and
exposed timing assumptions. So each major runs as **four shards**, 12 jobs in all, each
`pytest -n 2` on its own runner:

```sh
CDC_TEST_SHARD=2/4 uv run pytest -q -k 'not benchmark' -n 2   # one shard, locally
```

- `tests/ci_shard.py` assigns whole test files to shards, heaviest first onto the
  lightest shard, by the per-file weights in `tests/ci_weights.json`. Every shard
  computes the same assignment, so each test runs in exactly one shard per major
  (`tests/test_ci_shard.py` checks this against the real collection). A file missing from
  the table gets the median weight. Without `CDC_TEST_SHARD` nothing changes, and nested
  pytest runs never inherit it.
- **Regenerating the weights:** when a shard's job nears five minutes, or after adding a
  heavy file, concatenate each major's four shard job logs (each prints `--durations=0`)
  into one file per major, then run
  `python tests/ci_shard.py weights pg13.log pg16.log pg18.log > tests/ci_weights.json`.
  A stale table only unbalances shards; it never drops a test.
- **A new test file:** add it to `tests/ci_weights.json` in the same PR, with its
  worker-seconds from a local `--durations=0` run (or the median if it is light). A missing
  entry gets the median, which reshuffles the greedy assignment: one new file moved about
  ten files in and out of a shard. That is a balance issue only. A test that
  fails in some compositions has a hidden precondition to fix in the test, not in the
  weights.
- The weights are pooled across majors, so shard i holds the same files on every major,
  and per-shard pass/skip counts are comparable across majors.
- Each job prints its CPU model (`lscpu`). At least five models have been seen (AMD
  EPYC 9V74, 9V45 and 7763; Intel Xeon 8370C and 6973P-C). Serial runs of the same suite
  took 700-949 s across runners (about 1.4x), which explains most job-to-job timing
  differences, including the old "PG18 is slower" observation.
- Measured on 2026-11-08: 1.5-4 min per shard job, against 12-16 min per serial job,
  at fewer total runner minutes.

Under parallel tests on PostgreSQL 13/14, pgoutput sends other sessions' transactions in
the same database to every slot as empty members, at any point in the stream. Two kinds of
assumption break on that, so a real-PostgreSQL test that makes one says so with a fixture:

- **Where units or appends fall** (unit counts, "one unit per run", "the first append holds
  the seeded rows"): a slow read can close a unit early on `UnitConfig.max_commits` or
  `max_delay_ms`, holding only empty members. Use `drain_only_units`, so only a run's
  drain ends a unit. For append counts, also set `flush_seconds=60` and a large
  `batch_bytes`. A harness that routes "the first batch" somewhere should key on the first
  batch that carries an event.
- **What is in flight when a run stops on its time cap** (`partial_transaction`, a closed
  tail unit after `capped`): a foreign transaction can be mid-stream at the cap, and the run
  rightly keeps it as partial. Use `private_pg_database` (listed before `pg_phase1`). It
  gives the test a database of its own, which no other test's WAL reaches.

The original audit checked unit counts only, and missed the second kind until CI hit it.

On every major, a slow walsender (a loaded runner) breaks one more assumption: **that the
first collected append moves the checkpoint.** Once an open unit outgrows `chunk_bytes`,
the time flush (`flush_seconds`, 0.1 s in `pg_phase1`) can append one of its chunks before
any unit closes. A test that acts "after progress" on `after_append` must check that the
checkpoint moved (`World.progressed` in `test_ingest_compaction_conflict.py`). That file
runs those tests again with an eager flush, which makes the slow case deterministic.

Opt-in wider sweeps, not run in CI: `CDC_TEST_SQL_FOLD_SEEDS` (default 400),
`CDC_TEST_SQL_FOLD_E2E_SEEDS` (default 20), and `CDC_TEST_SQL_FOLD_E2E_FULL=1`, which runs
every fixed SQL fold scenario at all four (batch, unit) pairs (by default only the
boundary-sensitive scenarios do).

## Timing evidence

Before this policy, a 128-MD-case full run took 28m28s and reported 6,433.58 remote
worker-seconds; other runs took 15m15s and the user observed roughly 35 minutes.
The original 20-case proposal accounted for 853.53 worker-seconds in that slow log;
eight cheap engine/fixture proofs were added to make the approved budget 28. Phase 3
added a 29th case (concurrent `cdc_ops` status writers; about 11 s).
Those sums are not a measured runtime prediction. Record the bounded run's wall
time and slot/concurrency context when validating the change. The old multi-minute
`md_gate` tier timings no longer describe routine commands.
