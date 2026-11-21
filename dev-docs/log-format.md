# The durable change log (format 2)

The frozen format-2 encoding, chunk and unit rules, and how ingest writes them. The frozen regions are pinned
by `tests/test_docs.py`; changing one needs the versioned cutover described below and in
[DEVELOPING.md](../DEVELOPING.md).

<!-- frozen:log-format begin -->
## Durable contract: format 2

Only format 2 is supported by production entrypoints. Each source owns a schema in
`CDC_DATABASE` (default `cdc_internal`). `bootstrap.bootstrap_log` is the explicit
setup path; `runtime.open_log` opens an existing store read-only using its recorded
binding and immutable unit configuration, without a PostgreSQL connection. A
legacy, unknown or malformed store refuses before source mutation or streaming.
An incomplete bootstrap refuses with slot/publication inspection instructions;
see [runbooks](../docs/runbooks.md#incomplete-format-2-bootstrap).

### Structured cells and received-wire digests

The pgoutput v1 decoder preserves text bytes losslessly as UTF8 strings. Frame
images are nullable structs of aligned `values: LIST<VARCHAR>` and
`kinds: LIST<UTINYINT>`; the golden fixes TEXT, NULL, UNCHANGED and ABSENT tags.
An outer NULL image differs from a present zero-column image with empty lists.
Old tuple kind records none, K or O explicitly. Full descriptors are immutable and
identified by relation OID, schema version and key layout; TRUNCATE retains the
cached descriptor of every listed OID and refuses an unknown relation.

EVENT and inline COMMIT frames carry epoch/unit identity, frame ordinal, commit
LSN/time/transaction metadata, binding/config/mapping revisions and raw-wire
length. The hot path hashes the RECEIVED wire tuple or COMMIT bytes with the
versioned header, not a JSON serialization or a reconstructed row. Golden/fuzz
oracles reconstruct and compare frame projections, tables and digests; production
does not re-encode every row, download content or add a rendering fingerprint.
Relation/Type/Origin messages are session context only: they never consume frame
ordinals or change chunk boundaries. SQL tables remain structured text, not typed
per-relation change logs; type casts belong to apply.

The closed replication binding pins TimeZone, DateStyle, IntervalStyle,
extra_float_digits, bytea_output, lc_monetary, xmlbinary, xmloption and UTF8 client
and server encodings, plus installed extension versions and cell-mapping revision.
Connection options enforce the canonical defaults before bootstrap; SHOW values
are recorded from the actual replication session. Existing sources use recorded
values, and every stream verifies its own session before START_REPLICATION. A
binding/mapping/config mismatch refuses with a guarded needs_action reason.

### Chunks and complete units

Charged bytes are the 8-byte ordinal, frame header and received wire bytes.
Chunk boundaries are deterministic from frame count and charged bytes
(default 5,000 frames or 10 MiB), not flush timing or Arrow allocation size. A
COMMIT is a physical frame even for an empty transaction. The immutable unit
configuration closes a group at a transaction boundary: default max_commits=100,
4 MiB target and 250 ms max delay. Delay cannot close an active source transaction;
poll/request_close and the content-driven drain marker use the existing hooks.
A whole-unit hold/quarantine can therefore cover up to 100 commits by default;
previews expose that repair radius. A large transaction can span many chunks.

Fresh frames go to UTC ingestion-day `frames_v2_YYYYMMDD`; `chunks_v2` records
identity, ordinal range, counts, charged bytes, digest/chain, physical day and the
ORDERED descriptor keys. `units_v2` records contiguous chunk ranges and the final
unit digest/member count/end. `descriptors_v2`, `unit_configs_v2` and `protocol_v2`
retain the decoding contract. A chunk retry compares identity/digests/descriptors
and server-side physical counts in the append snapshot. Ledger-missing physical
rows refuse instead of duplicating data. The day cache is invalidated on every
exception after BEGIN, including a lost commit response.

All fresh frames, ledgers, descriptors and the checkpoint commit in ONE fenced
transaction in the log database. Only a COMPLETE unit advances confirmed_end to
its last member's transaction END. A durable open prefix leaves confirmed_end at
its anchor; replay must be contiguous and byte-identical. Abandoned prefixes are
explicit terminal epoch-transition metadata, never invented complete commits.
Readers accept only epoch starts/complete unit ends, refuse straddles/gaps and
history at or below the retention floor, and stop at the captured head.high.
<!-- frozen:log-format end -->

## Pipeline, feedback and resources

One destination worker uploads while the main thread reads/spools the next batch
and sends keepalives. The order is collect → fence → acknowledge → submit; no
main-thread destination SQL may run while the append worker owns that connection.
Restart begins explicitly at the durable complete END, even if the slot's feedback
lags after a crash. Every status update repeats only the fenced durable END. The
run drains only when the unit containing its own heartbeat marker is complete.
Slot loss, a slot ahead of the checkpoint or source identity drift fail closed.

Upload admission charges max(chunk charged bytes, Arrow table.nbytes) plus descriptor
bytes, with bounded terminal overhead; it never changes protocol chunk/unit
identity. A loop iteration can emit both feed and poll items before its full check.
Pending plus in-flight payload is bounded by twice (batch budget plus the greatest
per-iteration Arrow payload), in addition to the chunk builder/conversion state.
This bounds buffered payload, not DuckDB working memory or process RSS. Wide
nested chunks can make smaller, more numerous appends; that effect is unmeasured.
Wire-message and transaction limits remain resource limits, not throughput promises.

The source lease defaults to 900 seconds. Appends extend it; idle renewals use the
same changing guard column. Shutdown settles the worker before status/lease work.
Only rolled-back tagged guard conflicts become retryable: busy without own durable
progress, conflict with progress. Reports retain this run's OWN collected boundary,
not a later writer's checkpoint. Four total conflict-only release attempts (three
retries) precede the lease-expiry fallback. Repeated conflicts are visible in ops.

## Upgrade and Phase 2 entry requirements

Encoding, digests, schema, cell mapping and chunk boundaries are versioned
persisted protocols. Format 2 is frozen by its golden; format 1 survives only in
independent test references. Changing CELL_MAPPING_REVISION requires an approved
version/cutover plan and golden/recovery evidence; a binary cannot extend an open
unit with another mapping. The runtime reason is
`needs_action:mapping_revision_changed`.

There is no non-resync migration or dual reader. Existing format-1 test setups
must be explicitly recreated and every mirror re-backfilled. Old bundles must be
fenced/replaced before a new format-2 store exists. Database/share/slot drops and
Flight updates are operational actions requiring a separately approved, exact
cutover plan; changing code does not authorize them. Rollback after store deletion
requires another approved recreation and backfill, not an in-place downgrade.
