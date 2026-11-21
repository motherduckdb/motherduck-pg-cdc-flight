"""S5 invariant audits at known stable points of selected tests.

`audited(h)` audits a Phase 2 Harness store and asserts every audit passes (an
inconclusive audit fails the test: a placement expects its preconditions to hold).
One exception, by name: a mirror no backfill ever touched has no generation lease or
progress columns, so C4 and B6 have nothing to judge and say so ("no backfill yet");
that, and only that, is accepted.
Pass the returned Snapshot as `previous` at a later stable point for the pair audits.
MotherDuck stores are audited only where `remote=True`: one budgeted placement
(test_retention's allowlisted case). Never in teardown or in compaction.
"""

from cdc_flight import audits

NO_BACKFILL = ("C4", "B6")


def audited(h, previous=None, *, shape=False, remote=False):
    if h.store.remote and not remote:
        return None
    report, snapshot = audits.audit(
        h.store.con,
        h.store.database.strip('"'),
        h.ingest_config.source_id,
        h.store.mirror_database.strip('"'),
        lease_seconds=h.config.lease_seconds,
        previous=previous,
        shape=shape,
    )
    unexpected = [
        f
        for f in report.failures()
        if not (
            f.verdict == "inconclusive" and f.audit in NO_BACKFILL and "no backfill yet" in f.detail
        )
    ]
    assert report.findings and not unexpected, report.render()
    return snapshot
