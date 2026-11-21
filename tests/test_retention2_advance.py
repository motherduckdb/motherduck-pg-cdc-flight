"""Pin advances validate whole-unit endpoints in their single write snapshot."""

import pytest
from test_ingest_log2 import build, log2  # noqa: F401

from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.retention import HistoryDropped, generation_owner
from cdc_flight.retention2 import Retention2
from cdc_flight.retention_common import TABLES


@pytest.fixture
def advancing(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    retention.initialize()
    retention.register_generation(101, "g", 1, 90)
    return log2, retention, generation_owner("g")


def record(retention, monkeypatch):
    execute, statements = retention.execute, []

    def observed(sql, parameters=None):
        statements.append(sql.split()[0])
        return execute(sql, parameters)

    monkeypatch.setattr(retention, "execute", observed)
    return statements


def test_a_valid_advance_is_one_update_and_stale_ownership_gets_a_read_only_diagnostic(
    advancing, monkeypatch
):
    _, retention, owner = advancing
    statements = record(retention, monkeypatch)
    assert retention.advance(owner, "g", 1, 102)
    assert statements == ["UPDATE"]
    statements.clear()
    assert not retention.advance(owner, "replaced", 1, 102)
    assert statements == ["UPDATE", "SELECT"]
    assert retention.pin(owner)["lsn"] == 102


@pytest.mark.parametrize(
    "damage,exception,reason",
    [
        ("incomplete", SourceMismatch, "complete unit end"),
        ("abandoned", SourceMismatch, "complete unit end"),
        ("at_floor", HistoryDropped, "retention floor"),
        ("below_floor", HistoryDropped, "retention floor"),
        ("half_epoch", SourceMismatch, "inconsistent"),
        ("half_lsn", SourceMismatch, "inconsistent"),
        ("missing_epoch", SourceMismatch, "missing immutable epoch start"),
        ("null_epoch", SourceMismatch, "missing immutable epoch start"),
        ("missing_retention", SourceMismatch, "unit retention metadata missing"),
    ],
)
def test_an_invalid_advance_never_changes_the_pin(
    advancing, monkeypatch, damage, exception, reason
):
    log, retention, owner = advancing
    if damage in ("incomplete", "abandoned"):
        # Even a retained/corrupt END value is not authority without complete=true.
        log.execute(
            "UPDATE {s}units_v2 SET complete=false,abandoned_into_epoch=?",
            [2 if damage == "abandoned" else None],
        )
    elif damage in ("at_floor", "below_floor", "half_epoch", "half_lsn"):
        epoch, lsn = {
            "at_floor": (1, 102),
            "below_floor": (1, 103),
            "half_epoch": (1, None),
            "half_lsn": (None, 102),
        }[damage]
        log.execute("UPDATE {s}retention SET floor_epoch=?,floor_lsn=?", [epoch, lsn])
    elif damage == "missing_epoch":
        log.execute("DELETE FROM {s}source_epochs")
    elif damage == "null_epoch":
        log.execute("UPDATE {s}source_epochs SET start_lsn=NULL")
    else:
        log.execute("DELETE FROM {s}retention")
    before = retention.pin(owner)
    statements = record(retention, monkeypatch)
    with pytest.raises(exception, match=reason):
        retention.advance(owner, "g", 1, 102)
    assert statements == ["UPDATE", "SELECT"]
    assert retention.pin(owner) == before


def test_a_complete_end_from_another_epoch_is_not_an_advance_cut(advancing, monkeypatch):
    log, retention, _ = advancing
    log.begin_epoch(2, 50, "slot_lost", "slot_e2")
    retention.register_generation(101, "new", 2, 50)
    owner = generation_owner("new")
    before = retention.pin(owner)
    statements = record(retention, monkeypatch)
    with pytest.raises(SourceMismatch, match="complete unit end"):
        retention.advance(owner, "new", 2, 102)  # only epoch 1 has this END
    assert statements == ["UPDATE", "SELECT"]
    assert retention.pin(owner) == before


def test_a_valid_cut_with_a_stale_epoch_does_not_raise_a_successors_pin(advancing, monkeypatch):
    log, retention, owner = advancing
    log.begin_epoch(2, 50, "slot_lost", "slot_e2")
    before = retention.pin(owner)
    statements = record(retention, monkeypatch)
    assert not retention.advance(owner, "g", 2, 50)
    assert statements == ["UPDATE", "SELECT"]
    assert retention.pin(owner) == before


def test_fresh_compaction_schema_matches_the_missing_column_upgrade(log2):  # noqa: F811
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    fresh = log2.execute("DESCRIBE {s}compactions").fetchall()
    assert [(r[0], r[1]) for r in fresh[-3:]] == [
        ("format_version", "INTEGER"),
        ("chunks_pruned", "BIGINT"),
        ("units_pruned", "BIGINT"),
    ]
    assert retention.statements(retention.catalog()) == []
    log2.execute("DROP TABLE {s}compactions")
    old = TABLES["compactions"].replace(
        ", format_version INTEGER, chunks_pruned BIGINT, units_pruned BIGINT", ""
    )
    log2.execute(old)
    assert len(retention.statements(retention.catalog())) == 3
    retention.initialize()
    assert log2.execute("DESCRIBE {s}compactions").fetchall() == fresh


def test_another_epochs_initial_start_is_not_an_advance_cut(advancing):
    log, retention, _ = advancing
    log.begin_epoch(2, 50, "slot_lost", "slot_e2")
    retention.register_generation(101, "new", 2, 50)
    owner = generation_owner("new")
    before = retention.pin(owner)
    with pytest.raises(SourceMismatch, match="complete unit end"):
        retention.advance(owner, "new", 2, 90)  # epoch 1's initial start
    assert retention.pin(owner) == before
