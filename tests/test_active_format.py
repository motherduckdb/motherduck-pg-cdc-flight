"""Ordinary control paths refuse legacy inputs before mutation or PostgreSQL I/O."""

from types import SimpleNamespace

import duckdb
import pytest
from reference.legacy_store import create

from cdc_flight import compact, epoch, membership
from cdc_flight.action_types import Refused
from cdc_flight.config import IngestConfig
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.retention2 import Retention2


def test_compaction_refuses_a_legacy_backend_before_initializing():
    legacy = SimpleNamespace(format_version=1, initialize=lambda: pytest.fail("write attempted"))
    with pytest.raises(ValueError, match="compaction requires a format-2 store"):
        compact.run_compaction(None, compact.CompactConfig("s"), retention=legacy)


def test_membership_refuses_a_legacy_backend_before_observing_or_mutating():
    legacy = SimpleNamespace(format_version=1, unit_position=lambda: pytest.fail("read attempted"))
    with pytest.raises(Refused, match="membership requires a format-2 store"):
        membership._unit_observation(legacy, "public.items")


def test_retention_refuses_a_legacy_catalog_without_creating_metadata(tmp_path):
    config = IngestConfig("legacy", "slot", "pub", ("public.items",), database="legacy")
    with duckdb.connect(str(tmp_path / "legacy.duckdb")) as con:
        create(con, config)

        def columns():
            return con.execute(
                "SELECT table_name,column_name,data_type FROM duckdb_columns() ORDER BY ALL"
            ).fetchall()

        before = columns()
        with pytest.raises(SourceMismatch, match="retention requires a format-2 store"):
            Retention2(con, "legacy", "legacy").initialize()
        assert columns() == before


@pytest.mark.parametrize("flag", [False, 1, "yes", None])
def test_slot_advance_requires_literal_actual_start_before_postgresql(monkeypatch, flag):
    monkeypatch.setattr(epoch, "slot_info", lambda *args: pytest.fail("PostgreSQL read attempted"))
    with pytest.raises(epoch.EpochError, match="actual slot landing is required"):
        epoch._advance(None, "slot", 100, 200, "db", None, actual_start=flag)
