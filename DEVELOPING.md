# Developing

High-level guidance for changing or extending this repository. The customer documentation is in
[docs/](docs/); the contracts the code relies on are in [dev-docs/](dev-docs/); the open work is in
[STATUS.md](STATUS.md). Coding agents also follow [AGENTS.md](AGENTS.md).

## What this is

PostgreSQL logical replication into MotherDuck, run as MotherDuck Flights. Ingest streams `pgoutput` from a
replication slot into a durable change log (`cdc_internal`), in whole units of complete transactions. Apply
folds the log into typed mirror tables (`<source>_mirror`), and backfill builds each table's mirror as a
generation from a snapshot, caught up from the log. Compaction drops sealed log days nothing needs. Every
Flight writes its status to `cdc_ops`, and the operations Dive reads it. The architecture and the four
correctness protocols are in [dev-docs/design.md](dev-docs/design.md).

## Repository layout

- `src/cdc_flight/`: the package every Flight installs (module map below).
- `scripts/`: building and deploying Flights (`build_flight.py`, `deploy_flights.py`), the Dive
  (`deploy_dive.py`, `dive_preview.py`), package checks and performance tools (`scripts/perf/`).
- `flights/`: the Flights' authority and budget registries, chart, and on-demand benchmarks.
- `dive/`: the operations Dive (React), its tests and third-party notices.
- `tests/`: the suite; `tests/reference/` holds frozen reference copies used as oracles.
- `docs/`: customer documentation. `dev-docs/`: developer contracts and guidance.

## Frozen protocols

The format-2 encoding, received-wire digests, chunk boundaries and unit rules
([dev-docs/log-format.md](dev-docs/log-format.md)) and the fold rules
([dev-docs/apply.md](dev-docs/apply.md)) are versioned persisted protocols. Stored frames are read back by
both folds, and the differential tests hold the two folds equal; the format-2 golden freezes the encoding.

- Never edit a binding or a checkpoint to make a change fit.
- A protocol change needs an approved version and cutover plan, plus golden and recovery evidence.
- `mirror.py` and `sql_fold.py` change together.
- The frozen text itself is pinned: `tests/test_docs.py` holds the sha256 of each
  `<!-- frozen:... -->` region, so a change to it is a reviewed change to the pin.

## Invariants that bite

- A fence must change the value it guards; DuckDB can report an affected row for an unchanged-value
  UPDATE without a conflicting write ([dev-docs/invariants.md](dev-docs/invariants.md)).
- Each `cdc_ops` table has one writer, and a status write is best effort: it never fails a run
  ([docs/operations.md](docs/operations.md)).
- Every INSERT names its columns (`tests/test_insert_columns.py`).
- Ingest can't be rolled back to a version from before the compaction tables once newer code has run on a store
  ([docs/upgrading.md](docs/upgrading.md)).

## Tests and CI

```sh
uv sync --dev
uv run pytest -q                       # local checks; the 29 remote cases skip

docker compose up -d --wait           # isolated Postgres 16, localhost:55432
export CDC_TEST_PG_DSN='host=127.0.0.1 port=55432 dbname=cdc_phase0 user=postgres password=phase0-local-only'
python3 scripts/md_suite.py artifacts/md-suite.log
```

The full routine suite runs every local/PostgreSQL test and exactly 29 MotherDuck
cases on four workers. It has a hard ten-minute run budget: soft stop at nine
minutes, forced stop at 9m50s, failure by ten minutes. Setup and teardown count;
any external admission queue is outside the run. Preinstall dependencies with
`uv sync --dev`. A timeout, missing mandatory case or skipped mandatory remote
case fails the run. There is no `--full` tier or environment switch that restores
the old remote matrix. The testing policy, the fixed allowlist, diagnostic selection, coverage
tradeoffs and the cleanup journal are in [dev-docs/testing.md](dev-docs/testing.md); which tests prove
which claims is in [dev-docs/test-map.md](dev-docs/test-map.md).

MotherDuck tests load `MOTHERDUCK_TOKEN` or `motherduck_token` from the environment
or the repo's ignored `.env`. They fail without a token instead of invoking browser
authentication. Never commit `.env`. Credentials are never printed by the harness.

The tests create uniquely named `cdc_p0_*` databases in the token's MotherDuck
account, and uniquely named schemas, publications and slots in the supplied
Postgres database. They remove their own resources in fixture teardown. Use a
**disposable Postgres database** with a role that can create publications and slots.
An abruptly killed process may leave resources requiring cleanup; inspect ownership
before deleting them. The local Compose password is only a test fixture credential.

The same suite runs on PostgreSQL 13 and 18. Those servers are behind the `matrix` compose
profile on ports 55413 and 55418; see [the version matrix](dev-docs/test-map.md#postgresql-version-matrix).
CI runs lint and this local suite against PostgreSQL 13, 16 and 18 on every pull request,
markdown-only ones included, without MotherDuck.

```sh
docker compose --profile matrix up -d --wait postgres13 postgres18
export CDC_TEST_PG_DSN='host=127.0.0.1 port=55413 dbname=cdc_phase0 user=postgres password=phase0-local-only'
CDC_TEST_PG_MAJOR=13 uv run pytest -q -rs -k 'not benchmark'   # 55418 for 18
```

Lint, and the opt-in client-side benchmark (a smoke measurement, **not a Flight performance
result**; `CDC_BENCHMARK_ROWS=100000` for the longer run):

```sh
uv run ruff check src tests
uv run ruff format --check src tests
CDC_BENCHMARK=1 uv run pytest tests/test_benchmark.py -q -s
docker compose --profile matrix down  # only this project's test containers
```

## Safety

`cdc_ops` is attached from MotherDuck only when the Flight's own store is a MotherDuck
database and a token is set: without a token, DuckDB would open a browser login. A
local store without `cdc_ops` attached skips the write.

## Authority and budgets

`flights/authority.json` and `flights/budgets.json` record what each Flight may do and every bound it runs
under; `tests/test_authority.py` checks both against the code. Update them in the same PR as the code
([dev-docs/authority.md](dev-docs/authority.md)).

## Building and deploying Flights

Every Flight is a package stub pinned to a merged commit: `scripts/build_flight.py <command> <output>
--package <SHA>`, then `scripts/deploy_flights.py` (plan, then apply). The customer steps are in
[docs/flights.md](docs/flights.md).

Chaining calls `SELECT run_id, run_number FROM MD_RUN_FLIGHT(flight_id := ?::UUID, config := map(?, ?))`
on the run's `attach_mode=single` connection, with every value bound as a parameter. That form was
verified end to end on MotherDuck (an ingest run triggering an apply run whose effective config equalled
the stored config).

## Release and rollback

Bump `pyproject.toml`'s version on each release merge; the stub's `EXPECTED_VERSION` is read from the
pinned commit. Rollback is deploying the previous merged commit. It works only while that commit still
installs: the commit is reachable (and a private copy's token is valid), so `main` stays protected against
force-pushes and history rewrites. Until the package is on PyPI, an install failure has no fallback.

**Rotating the RDS CA bundle.** AWS adds roots to the global bundle when it opens a region or a CA
generation. To pick them up:
- fetch `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` again;
- check every block is a self-signed CA certificate (`openssl x509 -noout -text`);
- replace `rds_global_ca.PEM` and `pg_ca.RDS_GLOBAL_SHA256` in one commit;
- release the package; every PostgreSQL Flight of each source is then rebuilt and redeployed from it
  ([docs/flights.md](docs/flights.md#postgresql-tls-verify-full)).

## The Dive

Developing, previewing and testing the Dive: [dev-docs/dive.md](dev-docs/dive.md). Third-party code in
`dive/` is listed in `dive/THIRD_PARTY.md`.

## Performance work

The ingest ceiling, the A/B harness, the ingest timing lines and the round-trip cost model are in
[dev-docs/performance.md](dev-docs/performance.md). Don't claim the 20,000 events/s design target; the
measured envelope is in [docs/performance.md](docs/performance.md).

## Module map

`src/cdc_flight/`, by area (each module's docstring says more):
- **Entry points:** `cli.py` (CLI and Flight entrypoint), `config.py`, `tables_cli.py`, `label_cli.py`.
- **Source side:** `preflight.py`, `pg_connect.py`, `pg_ca.py` and `rds_global_ca.py` (TLS roots),
  `replication.py` (the stream, sending every status update itself), `pgoutput.py` (strict decoder),
  `text_settings.py`, `standby.py`, `positions.py` (read replica capture).
- **Ingest and the change log:** `ingest.py`, `ingest_backend.py`, `ingest_common.py`, `ingest_log.py`,
  `ingest_log2.py`, `spool2.py`, `format2.py`, `unit_store.py`, `unit_progress.py`, `bootstrap.py`,
  `runtime.py` ([dev-docs/log-format.md](dev-docs/log-format.md)).
- **Apply:** `apply.py`, `apply_run.py`, `mirror.py`, `mirror2.py`, `sql_fold.py`, `sql_path.py`,
  `table_set.py`, `catalog_checks.py`, `type_identity.py`, `append_apply.py`
  ([dev-docs/apply.md](dev-docs/apply.md)).
- **Backfill:** `backfill.py`, `generations.py`, `append_backfill.py`, `standby_backfill.py`, and the
  typed generation specs `generation_spec.py`, `generation_commands.py`, `generation_spec_text.py`,
  `publication_commands.py` ([dev-docs/backfill.md](dev-docs/backfill.md)).
- **Retention and lifecycle:** `retention.py`, `retention2.py`, `retention_common.py`, `compact.py`,
  `expiry.py`, `epoch.py`, `delete_source.py`, `releases.py`, `lifecycle_spec.py`,
  `lifecycle_commands.py`, `lifecycle_spec_text.py` ([dev-docs/retention.md](dev-docs/retention.md)).
- **Operator actions:** `action_base.py`, `action_types.py`, `action_flight.py`, `append_action.py`,
  `table_actions.py`, `membership.py`, `membership_flight.py`, `quarantine_flight.py`,
  `epoch_flight.py`, `delete_source_flight.py`.
- **Operations status:** `ops.py`, `ops_schema.py`, `ops_init.py`, `ops_ingest.py`, `ops_apply.py`,
  `ops_compact.py`, `hold_codes.py`, `source_label.py`, `attach.py`, `audits.py`.
- **Flights:** `chain.py`, `explain.py`, `flightlog.py`.
- **Aliases and test models:** `apply2.py`, `backfill2.py` (format-2 aliases), `store.py` (the protocol
  model), `source.py` (a disposable PostgreSQL fixture).

## Docs rules

- `docs/` is customer-facing: no PR or commit numbers, no internal names, and "read replica (standby)" on
  first mention, then "read replica".
- Contracts and developer guidance go in `dev-docs/`; the open work goes in STATUS.md.
- History stays in git and in PR descriptions, not in tracked docs, and tracked docs never link to
  private notes.
- `tests/test_docs.py` checks every link and anchor, every doc path cited from code, and the frozen pins.
