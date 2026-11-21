# Running the commands by hand

The Flights run these commands for you (see [flights.md](flights.md)). This page is for running
them locally or by hand: the ingest side, then apply and backfill.

## Run locally or build a Flight

```sh
uv sync --dev
# Put connection secrets in the ignored .env, using the names in .env.example.
uv run --env-file .env cdc-flight preflight --create-objects
uv run --env-file .env cdc-flight ingest
uv run python scripts/build_flight.py preflight artifacts/preflight_flight.py --package <SHA>
uv run python scripts/build_flight.py ingest artifacts/ingest_flight.py --package <SHA>
```

Configuration is environment based: `CDC_SOURCE_ID`, `CDC_TABLES` (JSON array of
explicit `schema.table` names), `CDC_SLOT`, `CDC_PUBLICATION`, `CDC_HEARTBEAT`,
`CDC_DATABASE`, `CDC_EPOCH`, and `CDC_RUNTIME_SECONDS`. Names currently accept
lowercase letters, digits and underscores; globs and quoted mixed-case names are
not supported. Source identity and text-output binding are immutable. Membership, epoch and key
changes use their guarded lifecycle paths; unit configuration is revisioned and
may change only at a complete boundary. Do not edit bindings or checkpoints.

`CDC_PG_DSN` is a secret, not Flight config. `CDC_DSN_ENV` can name a different
secret environment variable. `MOTHERDUCK_TOKEN` (or `motherduck_token` locally)
is mandatory; browser authentication is never invoked. Flights supply their
MotherDuck token. Source credentials must permit direct logical replication,
SELECT on captured tables, and SELECT/INSERT/UPDATE on the heartbeat. Preflight
with creation additionally needs DDL and replication-slot creation privileges.
Without `--create-objects`, missing objects produce a DBA provisioning plan.
Existing objects are checked rather than silently modified.

Create two Flights from the generated files (`<SHA>` is a commit merged into `main`; see
[flights.md](flights.md#build)), each with its generated `requirements_txt`
(`artifacts/<command>_flight.requirements.txt`),
F16, and a 600-second hard runtime. Run preflight on demand with
`CDC_CREATE_OBJECTS=1` only when provisioning is intended. For ingest, use a
540-second soft runtime and, after validating connectivity and capacity, a
schedule appropriate to the source (the default is every three minutes, `*/3 * * * *`). Overlapping
runs exit as busy. No source-specific Flight or schedule is deployed by this
repository: it requires a reachable PostgreSQL endpoint and secret. The default
remote connection requires TLS and IPv4; local-only tests explicitly opt out.

## Run

```sh
# Same CDC_SOURCE_ID, CDC_TABLES and CDC_DATABASE as ingest. CDC_EPOCH is optional:
# when set, it must equal the registered source epoch (a new source starts at 1).
# CDC_MIRROR_DATABASE defaults to <source>_mirror. No Postgres secret is needed.
uv run --env-file .env cdc-flight start-from-log --table public.orders
uv run --env-file .env cdc-flight apply
uv run python scripts/build_flight.py apply artifacts/apply_flight.py --package <SHA>
```

`start-from-log` publishes a **log-only generation**: an empty mirror whose
checkpoint is the start of the log. Nothing reads existing rows, so it requires
evidence. At first registration, preflight records in `tables.empty_after_slot`
whether each table was empty after its slot existed. If it was, every earlier
row's delete is in the log and replay is exact. Otherwise `start-from-log`
refuses. `--accept-incomplete` overrides that refusal, records the generation as
mode `log_incomplete`, and mirrors only later changes: rows that existed before
capture and were never touched again stay missing. It never replaces an existing
generation. Apply skips tables without a generation and reports
`awaiting_generation`.

Apply exits with status 2 when any table is held. Use that as the alerting hook
when it runs as a Flight.

```sh
# Backfill needs the Postgres secret (CDC_PG_DSN) to read the table.
uv run --env-file .env cdc-flight backfill --table public.orders    # start or resume
uv run --env-file .env cdc-flight backfill --table public.orders --abandon
uv run python scripts/build_flight.py backfill artifacts/backfill_flight.py --package <SHA>
```

As a Flight, the table comes from `CDC_BACKFILL_TABLE` and abandonment from
`CDC_BACKFILL_ABANDON=1`. `CDC_BACKFILL_CHUNK_ROWS` defaults to 50,000. Each run
advances the generation until its runtime cap, or until it must wait for ingest
to commit the end marker. Repeat it, with ingest running, until it reports
`published`. Exit status 2 means the generation failed.
